"""Terraform (AWS provider) and CloudFormation -> Terraform (google provider).

Approach: parse source into resources, apply per-type deterministic rules, emit a separate versioned
output. Unknown types are emitted as commented TODO blocks and listed as unsupported – never dropped silently.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from ..parsers import _unq, load_cfn
from .common import (DB_TIER_MAP, MACHINE_MAP, REGION_MAP, Block, ConversionResult, Raw, check_hcl, check_policy,
                     nested)

REF_RE = re.compile(r"\$\{(aws_[a-z0-9_]+)\.([A-Za-z0-9_-]+)\.([a-z_]+)\}")
TYPE_RENAME = {"aws_vpc": "google_compute_network", "aws_subnet": "google_compute_subnetwork",
               "aws_s3_bucket": "google_storage_bucket", "aws_security_group": "google_compute_firewall",
               "aws_instance": "google_compute_instance", "aws_db_instance": "google_sql_database_instance",
               "aws_sqs_queue": "google_pubsub_topic", "aws_sns_topic": "google_pubsub_topic",
               "aws_lambda_function": "google_cloudfunctions2_function", "aws_iam_role": "google_service_account",
               "aws_kinesis_stream": "google_pubsub_topic", "aws_dynamodb_table": "google_firestore_database",
               "aws_kms_key": "google_kms_crypto_key", "aws_ecr_repository": "google_artifact_registry_repository",
               "aws_secretsmanager_secret": "google_secret_manager_secret", "aws_eks_cluster": "google_container_cluster",
               "aws_redshift_cluster": "google_bigquery_dataset", "aws_elasticache_cluster": "google_redis_instance",
               "aws_cloudfront_distribution": "google_compute_backend_bucket"}


def _ref(expr: Any) -> Any:
    """Rewrite ${aws_x.name.attr} into a google reference expression."""
    if not isinstance(expr, str):
        return expr
    m = REF_RE.fullmatch(expr.strip())
    if m:
        t, n, attr = m.groups()
        g = TYPE_RENAME.get(t)
        if g:
            return Raw(f"{g}.{n}.{'id' if attr in ('id', 'arn') else attr}")
    return expr


def _name(s: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", s.lower()).strip("-")[:63]


def _labels(tags: dict | None) -> dict:
    return {re.sub(r"[^a-z0-9_-]", "_", k.lower()): re.sub(r"[^a-z0-9_-]", "_", str(v).lower()) for k, v in (tags or {}).items()}


def _first(x: Any) -> Any:
    return x[0] if isinstance(x, list) and x else x


class TfConverter:
    def __init__(self, region: str = "us-east-1", project_var: str = "var.project_id"):
        self.aws_region = region
        self.region = REGION_MAP.get(region, "us-central1")
        self.zone = f"{self.region}-b"
        self.project = Raw(project_var)
        self.sg_tags: dict[str, str] = {}

    def convert(self, rtype: str, rname: str, body: dict, subs: dict[str, dict], res: ConversionResult) -> list[Block]:
        fn = getattr(self, f"_c_{rtype}", None)
        res.constructs_total += 1
        if fn is None:
            res.unsupported.append(f"{rtype}.{rname}: no conversion rule – manual design required")
            return [Block("# TODO resource", [rtype, rname], {}, comment=f"UNSUPPORTED {rtype}.{rname}: map manually")]
        blocks = fn(rname, body, subs.get(rname, {}), res)
        res.constructs_converted += 1
        res.mapping_log.append({"source": f"{rtype}.{rname}", "target": ", ".join(f"{b.kind} {'.'.join(b.labels)}" for b in blocks)})
        return blocks

    # ---- networking
    def _c_aws_vpc(self, n, b, s, r):
        r.assumptions.append("AWS VPC is regional; GCP VPC is global – subnets carry the region")
        return [Block("resource", ["google_compute_network", n], {"name": _name((b.get("tags") or {}).get("Name", n)),
                                                                 "project": self.project, "auto_create_subnetworks": False,
                                                                 "routing_mode": "GLOBAL"})]

    def _c_aws_subnet(self, n, b, s, r):
        az = b.get("availability_zone")
        if az:
            r.assumptions.append(f"subnet {n}: AZ {az} has no subnet equivalent (GCP subnets are regional)")
        return [Block("resource", ["google_compute_subnetwork", n], {
            "name": _name(n), "project": self.project, "region": self.region, "ip_cidr_range": b.get("cidr_block"),
            "network": _ref(b.get("vpc_id")), "private_ip_google_access": True})]

    def _c_aws_security_group(self, n, b, s, r):
        tag = _name(b.get("name", n))
        self.sg_tags[n] = tag
        blocks = []
        for i, ing in enumerate(b.get("ingress", []) or []):
            ports = str(ing.get("from_port")) if ing.get("from_port") == ing.get("to_port") else f"{ing.get('from_port')}-{ing.get('to_port')}"
            proto = "all" if str(ing.get("protocol")) == "-1" else ing.get("protocol")
            allow_body = {"protocol": proto}
            if proto != "all":
                allow_body["ports"] = [ports]
            blocks.append(Block("resource", ["google_compute_firewall", f"{n}_ingress_{i}"], {
                "name": _name(f"{tag}-ingress-{i}"), "project": self.project, "network": _ref(b.get("vpc_id")),
                "direction": "INGRESS", "source_ranges": ing.get("cidr_blocks", []), "target_tags": [tag],
                "allow": nested("allow", allow_body)}))
            if "0.0.0.0/0" in (ing.get("cidr_blocks") or []):
                r.warnings.append(f"{n}: ingress from 0.0.0.0/0 preserved – front with a load balancer + Cloud Armor")
        r.assumptions.append(f"security group {n} -> network tag '{tag}' + firewall rules (GCP egress allowed by default)")
        return blocks

    # ---- storage
    def _c_aws_s3_bucket(self, n, b, s, r):
        body: dict[str, Any] = {"name": b.get("bucket", n), "project": self.project, "location": self.region.upper(),
                                "storage_class": "STANDARD", "uniform_bucket_level_access": True,
                                "public_access_prevention": "enforced", "labels": _labels(b.get("tags"))}
        ver = s.get("versioning", {})
        vc = _first(ver.get("versioning_configuration")) if ver else None
        if vc and vc.get("status") == "Enabled":
            body["versioning"] = nested("versioning", {"enabled": True})
        lc = s.get("lifecycle_configuration")
        rules = []
        for rule in (lc or {}).get("rule", []) or []:
            for t in rule.get("transition", []) or []:
                cls = {"GLACIER": "ARCHIVE", "DEEP_ARCHIVE": "ARCHIVE", "GLACIER_IR": "COLDLINE",
                       "STANDARD_IA": "NEARLINE", "ONEZONE_IA": "NEARLINE", "INTELLIGENT_TIERING": "AUTOCLASS"}.get(t.get("storage_class"), "NEARLINE")
                if cls == "AUTOCLASS":
                    body["autoclass"] = nested("autoclass", {"enabled": True})
                    continue
                rules.append(Block("lifecycle_rule", [], {"condition": nested("condition", {"age": t.get("days", 30)}),
                                                          "action": nested("action", {"type": "SetStorageClass", "storage_class": cls})}))
        if rules:
            body["_rules"] = rules
            r.assumptions.append(f"bucket {n}: S3 lifecycle transitions mapped to GCS storage classes (GLACIER->ARCHIVE)")
        r.manual_actions.append(f"bucket {n}: translate bucket policy/ACLs to IAM bindings; re-create event notifications as Pub/Sub notifications or Eventarc triggers")
        return [Block("resource", ["google_storage_bucket", n], body)]

    # ---- compute
    def _c_aws_instance(self, n, b, s, r):
        it = b.get("instance_type", "")
        mt = MACHINE_MAP.get(it)
        if not mt:
            mt = "n2-standard-2"
            r.warnings.append(f"{n}: instance type {it} not in machine map – defaulted to {mt}, right-size from utilisation")
        tags = [self.sg_tags.get(m.group(1), m.group(1)) for x in (b.get("vpc_security_group_ids") or [])
                for m in [re.search(r"aws_security_group\.([\w-]+)\.", str(x))] if m]
        r.manual_actions.append(f"{n}: AMI {b.get('ami')} -> replicate disk with Migrate to Virtual Machines or rebuild image (Packer)")
        return [Block("resource", ["google_compute_instance", n], {
            "name": _name((b.get("tags") or {}).get("Name", n)), "project": self.project, "zone": self.zone,
            "machine_type": mt, "tags": tags, "labels": _labels(b.get("tags")),
            "boot_disk": nested("boot_disk", {"initialize_params": nested("initialize_params", {"image": Raw("var.migrated_image_" + n.replace("-", "_"))})}),
            "network_interface": nested("network_interface", {"subnetwork": _ref(b.get("subnet_id"))}),
            "shielded_instance_config": nested("shielded_instance_config", {"enable_secure_boot": True})},
            comment=f"from aws_instance.{n} ({it} -> {mt})")]

    def _c_aws_db_instance(self, n, b, s, r):
        eng, ver = b.get("engine", "postgres"), str(b.get("engine_version", "15"))
        major = ver.split(".")[0]
        dbv = {"postgres": f"POSTGRES_{major}", "mysql": f"MYSQL_{ver.replace('.', '_')[:3]}",
               "sqlserver-se": "SQLSERVER_2019_STANDARD"}.get(eng)
        if not dbv:
            r.unsupported.append(f"aws_db_instance.{n}: engine {eng} -> needs Bare Metal Solution / Oracle@GC design")
            dbv = "POSTGRES_15"
        tier = DB_TIER_MAP.get(b.get("instance_class"), "db-custom-2-8192")
        return [Block("resource", ["google_sql_database_instance", n], {
            "name": _name(b.get("identifier", n)), "project": self.project, "region": self.region, "database_version": dbv,
            "deletion_protection": True,
            "settings": nested("settings", {
                "tier": tier, "availability_type": "REGIONAL" if b.get("multi_az") else "ZONAL",
                "disk_size": b.get("allocated_storage", 100), "disk_autoresize": True,
                "backup_configuration": nested("backup_configuration", {"enabled": True, "point_in_time_recovery_enabled": True}),
                "ip_configuration": nested("ip_configuration", {"ipv4_enabled": False, "private_network": Raw("var.private_network_id")})})},
            comment=f"from aws_db_instance.{n} ({b.get('instance_class')} -> {tier}); encryption at rest is default (CMEK optional)")]

    # ---- messaging / serverless
    def _c_aws_sqs_queue(self, n, b, s, r):
        r.assumptions.append(f"queue {n}: SQS -> Pub/Sub topic + pull subscription; visibility timeout -> ack deadline")
        ack = min(int(b.get("visibility_timeout_seconds", 10)), 600)
        ret = int(b.get("message_retention_seconds", 345600))
        if str(b.get("fifo_queue", "false")).lower() == "true":
            r.warnings.append(f"{n}: FIFO -> enable message ordering with ordering keys; exactly-once delivery on subscription")
        return [Block("resource", ["google_pubsub_topic", n], {"name": b.get("name", n), "project": self.project}),
                Block("resource", ["google_pubsub_subscription", f"{n}_sub"], {
                    "name": f"{b.get('name', n)}-sub", "project": self.project, "topic": Raw(f"google_pubsub_topic.{n}.id"),
                    "ack_deadline_seconds": ack, "message_retention_duration": f"{min(ret, 604800)}s"})]

    def _c_aws_sns_topic(self, n, b, s, r):
        return [Block("resource", ["google_pubsub_topic", n], {"name": b.get("name", n), "project": self.project})]

    def _c_aws_kinesis_stream(self, n, b, s, r):
        r.assumptions.append(f"stream {n}: shards are not provisioned on Pub/Sub; use ordering keys for per-key order")
        return [Block("resource", ["google_pubsub_topic", n], {"name": b.get("name", n), "project": self.project,
                                                               "message_retention_duration": "86400s"})]

    def _c_aws_dynamodb_table(self, n, b, s, r):
        r.warnings.append(f"{n}: DynamoDB table -> Firestore collection; collections are created by the app; "
                          "re-model GSIs as composite indexes")
        r.assumptions.append(f"{n}: Firestore location nam5 (US multi-region) assumed – confirm data residency")
        return [Block("resource", ["google_firestore_database", n], {
            "name": "(default)", "project": self.project, "location_id": "nam5", "type": "FIRESTORE_NATIVE"},
            comment=f"collection '{b.get('name', n)}' with document id = {b.get('hash_key')}")]

    def _c_aws_lambda_function(self, n, b, s, r):
        rt = str(b.get("runtime", "python3.12")).replace(".", "")
        mem = int(b.get("memory_size", 256))
        r.manual_actions.append(f"{n}: package converted source (see application conversion) to gs://<bucket>/{n}.zip")
        extra = []
        if not getattr(self, "_fn_sa", False):
            self._fn_sa = True
            extra.append(Block("resource", ["google_service_account", "function_sa"], {
                "account_id": "fn-runtime-sa", "project": self.project, "display_name": "Least-privilege runtime SA for functions"}))
        return extra + [Block("resource", ["google_cloudfunctions2_function", n], {
            "name": b.get("function_name", n), "project": self.project, "location": self.region,
            "build_config": nested("build_config", {"runtime": rt, "entry_point": "main",
                                                     "source": nested("source", {"storage_source": nested("storage_source", {
                                                         "bucket": Raw("var.source_bucket"), "object": f"{n}.zip"})})}),
            "service_config": nested("service_config", {"available_memory": f"{mem}M", "timeout_seconds": int(b.get("timeout", 60)),
                                                         "service_account_email": Raw("google_service_account.function_sa.email")})})]

    def _c_aws_iam_role(self, n, b, s, r):
        r.manual_actions.append(f"{n}: translate attached policies -> IAM role bindings (see IAM conversion)")
        return [Block("resource", ["google_service_account", n], {"account_id": _name(n)[:28], "project": self.project})]

    def _c_aws_cloudfront_distribution(self, n, b, s, r):
        r.warnings.append(f"{n}: CloudFront -> Cloud CDN on external Application LB; behaviours, Lambda@Edge, signed URLs need design")
        return [Block("resource", ["google_compute_backend_bucket", n], {
            "name": _name(n), "project": self.project, "bucket_name": Raw("var.static_bucket"), "enable_cdn": True})]

    def _c_aws_kms_key(self, n, b, s, r):
        r.warnings.append(f"{n}: KMS key material cannot be exported – data must be re-encrypted with the new key")
        return [Block("resource", ["google_kms_crypto_key", n], {"name": _name(n), "key_ring": Raw("var.key_ring_id"),
                                                                 "rotation_period": "7776000s"})]

    def _c_aws_secretsmanager_secret(self, n, b, s, r):
        return [Block("resource", ["google_secret_manager_secret", n], {"secret_id": _name(b.get("name", n)), "project": self.project,
                                                                        "replication": nested("replication", {"auto": nested("auto", {})})})]

    def _c_aws_ecr_repository(self, n, b, s, r):
        return [Block("resource", ["google_artifact_registry_repository", n], {"repository_id": _name(b.get("name", n)), "project": self.project,
                                                                               "location": self.region, "format": "DOCKER"})]

    def _c_aws_eks_cluster(self, n, b, s, r):
        r.manual_actions.append(f"{n}: migrate workloads with manifests/Helm; IRSA -> Workload Identity; storage classes")
        return [Block("resource", ["google_container_cluster", n], {"name": _name(b.get("name", n)), "project": self.project,
                                                                    "location": self.region, "enable_autopilot": True})]


def convert_terraform(path: Path) -> ConversionResult:
    import hcl2
    res = ConversionResult("terraform-aws-to-google", path.name)
    with path.open(encoding="utf-8") as f:
        doc = _unq(hcl2.load(f))
    region = "us-east-1"
    for p in doc.get("provider", []):
        if "aws" in p:
            region = p["aws"].get("region", region)
    conv = TfConverter(region)
    subs: dict[str, dict[str, dict]] = {}
    main: list[tuple[str, str, dict]] = []
    for block in doc.get("resource", []):
        for rtype, named in block.items():
            for rname, body in named.items():
                body = {k: v for k, v in body.items() if not k.startswith("__")}
                if rtype.startswith("aws_s3_bucket_"):
                    subs.setdefault(rname, {})[rtype.replace("aws_s3_bucket_", "")] = body
                else:
                    main.append((rtype, rname, body))
    # security groups first so instances can resolve network tags
    main.sort(key=lambda x: 0 if x[0] == "aws_security_group" else 1)
    blocks = []
    for rtype, rname, body in main:
        blocks += conv.convert(rtype, rname, body, subs, res)
    hcl = _render_module(blocks, conv)
    res.outputs["terraform/main.tf"] = hcl
    res.outputs["terraform/variables.tf"] = VARIABLES
    res.checks += [check_hcl(hcl, "main.tf"), check_policy(hcl, "main.tf")]
    return res


VARIABLES = """variable "project_id" { type = string }
variable "private_network_id" { type = string }
variable "source_bucket" { type = string }
variable "static_bucket" { type = string }
variable "key_ring_id" {
  type    = string
  default = ""
}
variable "migrated_image_customer_api" {
  type        = string
  description = "Image produced by Migrate to Virtual Machines / Packer"
}
"""


def _render_module(blocks: list[Block], conv: TfConverter) -> str:
    head = ("# GENERATED by AWS-to-GCP Migration Accelerator – REVIEW BEFORE USE. Never auto-applied.\n"
            "terraform {\n  required_providers {\n    google = {\n      source  = \"hashicorp/google\"\n      version = \"~> 6.0\"\n    }\n  }\n}\n\n"
            f"provider \"google\" {{\n  project = var.project_id\n  region  = \"{conv.region}\"\n}}\n\n")
    parts = []
    for b in blocks:
        if b.kind.startswith("# TODO"):
            parts.append(f"# TODO: {' '.join(b.labels)} – {b.comment}")
        else:
            parts.append(b.render())
    return head + "\n\n".join(parts) + "\n"


# --------------------------------------------------------------------------- CloudFormation
CFN_TO_TF = {"AWS::S3::Bucket": "aws_s3_bucket", "AWS::SNS::Topic": "aws_sns_topic", "AWS::SQS::Queue": "aws_sqs_queue",
             "AWS::DynamoDB::Table": "aws_dynamodb_table", "AWS::Lambda::Function": "aws_lambda_function",
             "AWS::IAM::Role": "aws_iam_role", "AWS::Kinesis::Stream": "aws_kinesis_stream", "AWS::EC2::Instance": "aws_instance",
             "AWS::RDS::DBInstance": "aws_db_instance", "AWS::KMS::Key": "aws_kms_key",
             "AWS::SecretsManager::Secret": "aws_secretsmanager_secret", "AWS::ECR::Repository": "aws_ecr_repository",
             "AWS::EKS::Cluster": "aws_eks_cluster"}


def _intrinsic(v: Any, params: dict, logical_types: dict) -> Any:
    if isinstance(v, dict):
        if "Ref" in v:
            ref = v["Ref"]
            if ref in params:
                return f"${{var.{ref.lower()}}}"
            t = CFN_TO_TF.get(logical_types.get(ref, ""))
            return f"${{{t}.{_snake(ref)}.id}}" if t else ref
        if "Fn::Sub" in v:
            s = v["Fn::Sub"] if isinstance(v["Fn::Sub"], str) else v["Fn::Sub"][0]
            return re.sub(r"\$\{(\w+)\}", lambda m: f"${{var.{m.group(1).lower()}}}", s)
        if "Fn::GetAtt" in v:
            tgt = v["Fn::GetAtt"]
            tgt = tgt.split(".")[0] if isinstance(tgt, str) else tgt[0]
            t = CFN_TO_TF.get(logical_types.get(tgt, ""))
            return f"${{{t}.{_snake(tgt)}.arn}}" if t else tgt
        return {k: _intrinsic(x, params, logical_types) for k, x in v.items()}
    if isinstance(v, list):
        return [_intrinsic(x, params, logical_types) for x in v]
    return v


def _snake(s: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


def _cfn_props_to_tf(tf_type: str, p: dict) -> tuple[dict, dict]:
    subs: dict = {}
    if tf_type == "aws_s3_bucket":
        body = {"bucket": p.get("BucketName")}
        if (p.get("VersioningConfiguration") or {}).get("Status") == "Enabled":
            subs["versioning"] = {"versioning_configuration": [{"status": "Enabled"}]}
        return body, subs
    if tf_type in ("aws_sns_topic",):
        return {"name": p.get("TopicName")}, subs
    if tf_type == "aws_sqs_queue":
        return {"name": p.get("QueueName"), "visibility_timeout_seconds": p.get("VisibilityTimeout", 30),
                "fifo_queue": str(p.get("FifoQueue", "false"))}, subs
    if tf_type == "aws_dynamodb_table":
        hk = next((k["AttributeName"] for k in p.get("KeySchema", []) if k.get("KeyType") == "HASH"), None)
        return {"name": p.get("TableName"), "hash_key": hk}, subs
    if tf_type == "aws_lambda_function":
        return {"function_name": p.get("FunctionName"), "runtime": p.get("Runtime"), "memory_size": p.get("MemorySize", 128),
                "timeout": p.get("Timeout", 3)}, subs
    if tf_type == "aws_kinesis_stream":
        return {"name": p.get("Name")}, subs
    return {k[0].lower() + k[1:]: v for k, v in p.items()}, subs


def convert_cloudformation(path: Path) -> ConversionResult:
    res = ConversionResult("cloudformation-to-terraform-google", path.name)
    doc = load_cfn(path)
    params = doc.get("Parameters") or {}
    resources = doc.get("Resources") or {}
    logical_types = {k: v.get("Type") for k, v in resources.items()}
    conv = TfConverter("us-east-1")
    blocks: list[Block] = []
    for lid, r in resources.items():
        rtype = r.get("Type", "")
        tf_type = CFN_TO_TF.get(rtype)
        if not tf_type:
            res.constructs_total += 1
            res.unsupported.append(f"{lid} ({rtype}): no rule – custom resources/macros need manual redesign")
            blocks.append(Block("# TODO resource", [rtype, lid], {}, comment=f"UNSUPPORTED {rtype}"))
            continue
        props = _intrinsic(r.get("Properties") or {}, params, logical_types)
        body, subs = _cfn_props_to_tf(tf_type, props)
        blocks += conv.convert(tf_type, _snake(lid), body, {_snake(lid): subs}, res)
    if "Outputs" in doc:
        res.manual_actions.append("CloudFormation Outputs/Exports -> Terraform outputs / remote state")
    var_block = "\n".join(f'variable "{k.lower()}" {{\n  type    = string\n  default = "{(v or {}).get("Default", "")}"\n}}'
                          for k, v in params.items())
    hcl = _render_module(blocks, conv) + "\n" + var_block + "\n"
    res.outputs["terraform/cfn_main.tf"] = hcl
    res.checks += [check_hcl(hcl, "cfn_main.tf"), check_policy(hcl, "cfn_main.tf")]
    return res
