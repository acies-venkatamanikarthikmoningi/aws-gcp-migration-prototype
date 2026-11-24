"""Deterministic extractors: inventory, Terraform, CloudFormation, code, SQL, documents and diagrams.

Every extractor returns a list of evidence dicts:
    {"entity_type": resource|relationship|owner|consumer|requirement|code_reference|secret_finding|needs_review,
     "data": {...}, "confidence": 0..1, "source_location": "file:line/sheet/xpath"}
No LLM is used here (blueprint: deterministic where possible).
"""
from __future__ import annotations

import ast
import csv
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import yaml

EXTRACTOR_VERSION = "extract-1.1.0"

# --------------------------------------------------------------------------- service normalization
SERVICE_ALIASES = {
    "ec2": "EC2", "amazon ec2": "EC2", "s3": "S3", "amazon s3": "S3", "rds": "RDS", "aurora": "Aurora",
    "dynamodb": "DynamoDB", "lambda": "Lambda", "glue": "Glue", "redshift": "Redshift", "athena": "Athena",
    "emr": "EMR", "kinesis": "Kinesis", "kinesis data streams": "Kinesis", "firehose": "Kinesis Firehose",
    "msk": "MSK", "sqs": "SQS", "sns": "SNS", "eventbridge": "EventBridge", "eks": "EKS", "ecs": "ECS",
    "fargate": "Fargate", "ecr": "ECR", "api gateway": "API Gateway", "apigateway": "API Gateway",
    "cloudfront": "CloudFront", "route 53": "Route 53", "route53": "Route 53", "vpc": "VPC", "iam": "IAM",
    "kms": "KMS", "secrets manager": "Secrets Manager", "secretsmanager": "Secrets Manager",
    "cloudwatch": "CloudWatch", "cloudtrail": "CloudTrail", "step functions": "Step Functions",
    "stepfunctions": "Step Functions", "mwaa": "MWAA", "elasticache": "ElastiCache", "efs": "EFS", "ebs": "EBS",
    "cognito": "Cognito", "sagemaker": "SageMaker", "iot core": "IoT Core", "ses": "SES", "qldb": "QLDB",
    "direct connect": "Direct Connect", "elb": "ELB", "alb": "ELB", "nlb": "ELB", "opensearch": "OpenSearch",
    "documentdb": "DocumentDB", "neptune": "Neptune", "bedrock": "Bedrock", "waf": "WAF",
    "codepipeline": "CodePipeline", "codebuild": "CodeBuild", "cloudformation": "CloudFormation",
}

TF_TYPE_MAP = {
    "aws_instance": "EC2", "aws_launch_template": "EC2", "aws_autoscaling_group": "EC2 Auto Scaling",
    "aws_s3_bucket": "S3", "aws_db_instance": "RDS", "aws_rds_cluster": "Aurora", "aws_dynamodb_table": "DynamoDB",
    "aws_lambda_function": "Lambda", "aws_sqs_queue": "SQS", "aws_sns_topic": "SNS", "aws_vpc": "VPC",
    "aws_subnet": "VPC", "aws_security_group": "VPC", "aws_nat_gateway": "NAT Gateway",
    "aws_cloudfront_distribution": "CloudFront", "aws_kinesis_stream": "Kinesis", "aws_eks_cluster": "EKS",
    "aws_ecs_service": "ECS", "aws_ecs_cluster": "ECS", "aws_ecr_repository": "ECR", "aws_iam_role": "IAM",
    "aws_iam_policy": "IAM", "aws_kms_key": "KMS", "aws_elasticache_cluster": "ElastiCache",
    "aws_elasticache_replication_group": "ElastiCache", "aws_msk_cluster": "MSK",
    "aws_redshift_cluster": "Redshift", "aws_glue_job": "Glue", "aws_glue_catalog_database": "Glue Data Catalog",
    "aws_sfn_state_machine": "Step Functions", "aws_api_gateway_rest_api": "API Gateway",
    "aws_apigatewayv2_api": "API Gateway", "aws_lb": "ELB", "aws_alb": "ELB", "aws_route53_zone": "Route 53",
    "aws_route53_record": "Route 53", "aws_secretsmanager_secret": "Secrets Manager", "aws_efs_file_system": "EFS",
    "aws_ebs_volume": "EBS", "aws_cloudwatch_log_group": "CloudWatch", "aws_cloudwatch_metric_alarm": "CloudWatch",
    "aws_mwaa_environment": "MWAA", "aws_emr_cluster": "EMR", "aws_athena_workgroup": "Athena",
    "aws_cognito_user_pool": "Cognito", "aws_wafv2_web_acl": "WAF", "aws_opensearch_domain": "OpenSearch",
    "aws_docdb_cluster": "DocumentDB", "aws_sagemaker_endpoint": "SageMaker", "aws_cloudtrail": "CloudTrail",
}
TF_SUBRESOURCE_PARENT = {  # sub-resources folded into parent configuration
    "aws_s3_bucket_versioning": "aws_s3_bucket", "aws_s3_bucket_lifecycle_configuration": "aws_s3_bucket",
    "aws_s3_bucket_policy": "aws_s3_bucket", "aws_s3_bucket_server_side_encryption_configuration": "aws_s3_bucket",
    "aws_s3_bucket_public_access_block": "aws_s3_bucket",
}

CFN_TYPE_MAP = {
    "AWS::S3::Bucket": "S3", "AWS::SNS::Topic": "SNS", "AWS::SQS::Queue": "SQS", "AWS::DynamoDB::Table": "DynamoDB",
    "AWS::Lambda::Function": "Lambda", "AWS::IAM::Role": "IAM", "AWS::Kinesis::Stream": "Kinesis",
    "AWS::EC2::Instance": "EC2", "AWS::RDS::DBInstance": "RDS", "AWS::RDS::DBCluster": "Aurora",
    "AWS::EKS::Cluster": "EKS", "AWS::ECS::Service": "ECS", "AWS::StepFunctions::StateMachine": "Step Functions",
    "AWS::Events::Rule": "EventBridge", "AWS::ApiGateway::RestApi": "API Gateway", "AWS::Glue::Job": "Glue",
    "AWS::Redshift::Cluster": "Redshift", "AWS::ElastiCache::CacheCluster": "ElastiCache", "AWS::KMS::Key": "KMS",
    "AWS::SecretsManager::Secret": "Secrets Manager", "AWS::CloudFront::Distribution": "CloudFront",
    "AWS::Logs::LogGroup": "CloudWatch", "AWS::EC2::VPC": "VPC", "AWS::Cognito::UserPool": "Cognito",
}

BOTO3_SERVICE = {
    "s3": "S3", "dynamodb": "DynamoDB", "sns": "SNS", "sqs": "SQS", "secretsmanager": "Secrets Manager",
    "kinesis": "Kinesis", "lambda": "Lambda", "glue": "Glue", "redshift-data": "Redshift", "athena": "Athena",
    "ssm": "Systems Manager", "events": "EventBridge", "stepfunctions": "Step Functions", "kms": "KMS",
    "cloudwatch": "CloudWatch", "logs": "CloudWatch", "sagemaker-runtime": "SageMaker", "bedrock-runtime": "Bedrock",
    "ses": "SES", "cognito-idp": "Cognito", "firehose": "Kinesis Firehose", "rekognition": "Rekognition",
    "textract": "Textract", "comprehend": "Comprehend", "sts": "IAM",
}

SECRET_PATTERNS = [
    ("aws_access_key_id", re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("aws_secret_access_key", re.compile(r"(?i)aws_secret_access_key\s*[=:]\s*['\"][A-Za-z0-9/+=]{40}['\"]")),
    ("private_key", re.compile(r"-----BEGIN (RSA |EC )?PRIVATE KEY-----")),
    ("password_literal", re.compile(r"(?i)password\s*=\s*['\"][^'\"\s]{4,}['\"]")),
]
ENDPOINT_PATTERNS = [
    ("rds_endpoint", re.compile(r"[a-z0-9-]+\.[a-z0-9]+\.[a-z0-9-]+\.rds\.amazonaws\.com")),
    ("sqs_url", re.compile(r"https://sqs\.[a-z0-9-]+\.amazonaws\.com/\d+/[A-Za-z0-9_-]+")),
    ("s3_uri", re.compile(r"s3://[a-z0-9.\-]+(?:/[^\s'\"]*)?")),
    ("arn", re.compile(r"arn:aws:[a-z0-9-]+:[a-z0-9-]*:\d{0,12}:[^\s'\"]+")),
]


def canon_service(raw: str) -> str:
    if not raw:
        return "UNKNOWN"
    return SERVICE_ALIASES.get(raw.strip().lower(), raw.strip())


def ev(entity_type: str, data: dict, confidence: float, loc: str) -> dict:
    return {"entity_type": entity_type, "data": data, "confidence": round(confidence, 2), "source_location": loc}


def redact(text: str) -> tuple[str, list[str]]:
    """Redact credentials before any content leaves the deterministic layer (e.g. to an LLM)."""
    found = []
    for name, pat in SECRET_PATTERNS:
        if pat.search(text):
            found.append(name)
            text = pat.sub(f"<REDACTED:{name}>", text)
    return text, found


# --------------------------------------------------------------------------- inventory
INV_COLS = {
    "resource_id": ["resource_id", "resourceid", "id", "arn"], "name": ["resource_name", "name"],
    "service": ["service", "resource_type", "type"], "region": ["region", "aws_region"],
    "application": ["application", "app"], "environment": ["environment", "env"],
    "criticality": ["criticality", "tier"],
}


def _pick(row: dict, keys: list[str]) -> Any:
    low = {k.lower(): v for k, v in row.items()}
    for k in keys:
        if k in low and low[k] not in (None, ""):
            return low[k]
    return None


def _inv_row(row: dict, loc: str) -> dict:
    core = {k: _pick(row, v) for k, v in INV_COLS.items()}
    core["service"] = canon_service(str(core["service"] or ""))
    config = {k: v for k, v in row.items() if k.lower() not in sum(INV_COLS.values(), []) and v not in (None, "")}
    return ev("resource", {**core, "configuration": config}, 0.95, loc)


def parse_inventory_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return [_inv_row(r, f"{path.name}:row{i + 2}") for i, r in enumerate(csv.DictReader(f))]


def parse_inventory_xlsx(path: Path) -> list[dict]:
    from openpyxl import load_workbook
    out = []
    wb = load_workbook(path, read_only=True, data_only=True)
    for ws in wb.worksheets:
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue
        hdr = [str(h).strip() if h is not None else "" for h in rows[0]]
        for i, r in enumerate(rows[1:]):
            d = dict(zip(hdr, r))
            if "source_resource" in hdr:
                out.append(ev("relationship", {"source": d.get("source_resource"), "target": d.get("target_resource"),
                                               "type": d.get("relationship_type"), "note": d.get("evidence")},
                              0.85, f"{path.name}:{ws.title}!row{i + 2}"))
            elif _pick(d, INV_COLS["resource_id"]):
                out.append(_inv_row(d, f"{path.name}:{ws.title}!row{i + 2}"))
    return out


def parse_inventory_json(path: Path) -> list[dict]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    items = doc.get("resources", doc) if isinstance(doc, dict) else doc
    out = []
    for i, it in enumerate(items):
        tags = it.get("Tags", {}) or {}
        if isinstance(tags, list):
            tags = {t.get("Key"): t.get("Value") for t in tags}
        row = {"resource_id": it.get("ResourceId") or it.get("Arn"), "resource_name": it.get("Name"),
               "service": it.get("Service"), "region": it.get("Region"), "application": tags.get("application"),
               "environment": tags.get("environment")}
        row.update({k: v for k, v in it.items() if k not in ("ResourceId", "Name", "Service", "Region", "Tags", "Arn")})
        out.append(_inv_row(row, f"{path.name}:resources[{i}]"))
    return out


def parse_dependency_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return [ev("relationship", {"source": r["source_resource"], "target": r["target_resource"],
                                    "type": r["relationship_type"]}, 0.95, f"{path.name}:row{i + 2}")
                for i, r in enumerate(csv.DictReader(f))]


def parse_ownership_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if rows and "owner_email" in rows[0]:
        return [ev("owner", r, 1.0, f"{path.name}:row{i + 2}") for i, r in enumerate(rows)]
    return [ev("consumer", r, 1.0, f"{path.name}:row{i + 2}") for i, r in enumerate(rows)]


# --------------------------------------------------------------------------- Terraform
def _unq(x: Any) -> Any:
    """python-hcl2 >= 5 keeps HCL quotes on keys/strings; normalize them away."""
    if isinstance(x, dict):
        return {_unq(k): _unq(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_unq(v) for v in x]
    if isinstance(x, str) and len(x) >= 2 and x[0] == x[-1] == '"':
        return x[1:-1]
    return x


def parse_terraform(path: Path) -> list[dict]:
    import hcl2
    with path.open(encoding="utf-8") as f:
        doc = _unq(hcl2.load(f))
    out, by_addr = [], {}
    provider_region = None
    for p in doc.get("provider", []):
        if "aws" in p:
            provider_region = p["aws"].get("region")
    subs = []
    for block in doc.get("resource", []):
        for rtype, named in block.items():
            for rname, body in named.items():
                body = {k: v for k, v in body.items() if not k.startswith("__")}
                if rtype in TF_SUBRESOURCE_PARENT:
                    subs.append((rtype, rname, body))
                    continue
                service = TF_TYPE_MAP.get(rtype, f"UNMAPPED:{rtype}")
                tags = body.get("tags") or {}
                name = (body.get("bucket") or body.get("identifier") or body.get("name") or body.get("function_name")
                        or tags.get("Name") or body.get("comment") or rname)
                e = ev("resource", {"resource_id": None, "name": name, "service": service,
                                    "region": provider_region, "application": tags.get("application"),
                                    "environment": tags.get("environment"), "iac_type": rtype, "iac_address": f"{rtype}.{rname}",
                                    "configuration": body}, 0.9, f"{path.name}:{rtype}.{rname}")
                by_addr[(rtype, rname)] = e
                out.append(e)
    for rtype, rname, body in subs:
        parent = by_addr.get((TF_SUBRESOURCE_PARENT[rtype], rname))
        if parent:
            parent["data"]["configuration"][rtype.replace("aws_s3_bucket_", "")] = body
    # references -> relationships (e.g. aws_subnet.app.id inside instance)
    for e in list(by_addr.values()):
        blob = json.dumps(e["data"]["configuration"])
        for (rt, rn), other in by_addr.items():
            if other is not e and f"{rt}.{rn}." in blob:
                out.append(ev("relationship", {"source": e["data"]["name"], "target": other["data"]["name"],
                                               "type": "IAC_REFERENCE"}, 0.8, e["source_location"]))
    return out


# --------------------------------------------------------------------------- CloudFormation
class _CfnLoader(yaml.SafeLoader):
    pass


def _cfn_tag(loader, tag_suffix, node):
    if isinstance(node, yaml.ScalarNode):
        val = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        val = loader.construct_sequence(node)
    else:
        val = loader.construct_mapping(node)
    return {f"Fn::{tag_suffix}" if tag_suffix != "Ref" else "Ref": val}


_CfnLoader.add_multi_constructor("!", _cfn_tag)


def load_cfn(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        return json.loads(text)
    return yaml.load(text, Loader=_CfnLoader)


def parse_cloudformation(path: Path) -> list[dict]:
    doc = load_cfn(path)
    out = []
    for lid, res in (doc.get("Resources") or {}).items():
        rtype = res.get("Type", "")
        props = res.get("Properties", {}) or {}
        if rtype.startswith("Custom::") or rtype == "AWS::CloudFormation::CustomResource":
            out.append(ev("needs_review", {"logical_id": lid, "type": rtype,
                                           "reason": "Custom resource backed by Lambda – behaviour not inferable"},
                          0.3, f"{path.name}:Resources.{lid}"))
            continue
        name = (props.get("BucketName") or props.get("TopicName") or props.get("QueueName") or props.get("TableName")
                or props.get("FunctionName") or props.get("Name") or lid)
        if isinstance(name, dict):
            name = name.get("Fn::Sub") if isinstance(name.get("Fn::Sub"), str) else json.dumps(name)
        out.append(ev("resource", {"resource_id": None, "name": name, "service": CFN_TYPE_MAP.get(rtype, f"UNMAPPED:{rtype}"),
                                   "region": None, "iac_type": rtype, "iac_address": lid, "configuration": props},
                      0.9, f"{path.name}:Resources.{lid}"))
        blob = json.dumps(props)
        for other in (doc.get("Resources") or {}):
            if other != lid and (f'"Ref": "{other}"' in blob or f'"{other}.' in blob or f'["{other}"' in blob
                                 or f'"Fn::GetAtt": "{other}' in blob):
                out.append(ev("relationship", {"source": lid, "target": other, "type": "IAC_REFERENCE"}, 0.8,
                              f"{path.name}:Resources.{lid}"))
    return out


# --------------------------------------------------------------------------- application code
def parse_python(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    out = []
    try:
        tree = ast.parse(text)
    except SyntaxError as e:
        return [ev("needs_review", {"reason": f"python syntax error: {e}"}, 0.2, f"{path.name}:{e.lineno}")]
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("client", "resource"):
            base = node.func.value
            if isinstance(base, ast.Name) and base.id == "boto3" and node.args and isinstance(node.args[0], ast.Constant):
                svc = node.args[0].value
                out.append(ev("code_reference", {"kind": "aws_sdk", "sdk_service": svc,
                                                 "service": BOTO3_SERVICE.get(svc, svc), "file": path.name},
                              0.95, f"{path.name}:{node.lineno}"))
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            mod = node.module if isinstance(node, ast.ImportFrom) else node.names[0].name
            if mod and mod.split(".")[0] in ("awsglue", "airflow", "psycopg2", "pymysql", "redshift_connector", "kafka"):
                out.append(ev("code_reference", {"kind": "framework", "module": mod, "file": path.name}, 0.95,
                              f"{path.name}:{node.lineno}"))
    for i, line in enumerate(text.splitlines(), 1):
        for kind, pat in ENDPOINT_PATTERNS:
            for m in pat.findall(line):
                out.append(ev("code_reference", {"kind": kind, "value": m, "file": path.name}, 0.9, f"{path.name}:{i}"))
        for kind, pat in SECRET_PATTERNS:
            if pat.search(line):
                out.append(ev("secret_finding", {"kind": kind, "file": path.name,
                                                 "action": "Redacted from AI context; rotate and move to Secret Manager"},
                              1.0, f"{path.name}:{i}"))
    return out


def parse_sql(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    out = []
    for m in re.finditer(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)", text, re.I):
        out.append(ev("code_reference", {"kind": "table_ddl", "value": m.group(1), "file": path.name}, 0.95,
                      f"{path.name}:{text[:m.start()].count(chr(10)) + 1}"))
    for m in ENDPOINT_PATTERNS[2][1].finditer(text):
        out.append(ev("code_reference", {"kind": "s3_uri", "value": m.group(0), "file": path.name}, 0.9,
                      f"{path.name}:{text[:m.start()].count(chr(10)) + 1}"))
    return out


def parse_json_code(path: Path, ftype: str) -> list[dict]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    out = []
    if ftype == "stepfunctions":
        for name, st in doc.get("States", {}).items():
            res = st.get("Resource", "")
            if res:
                out.append(ev("code_reference", {"kind": "state_task", "state": name, "value": res, "file": path.name},
                              0.95, f"{path.name}:States.{name}"))
    elif ftype == "iam_policy":
        for i, s in enumerate(doc.get("Statement", [])):
            acts = s.get("Action", [])
            acts = [acts] if isinstance(acts, str) else acts
            if s.get("Effect") == "Allow" and (any(a.endswith(":*") or a == "*" for a in acts) or s.get("Resource") == "*"):
                out.append(ev("secret_finding", {"kind": "over_privileged_policy", "sid": s.get("Sid"),
                                                 "action": "Narrow to least-privilege predefined/custom role on GCP"},
                              0.9, f"{path.name}:Statement[{i}]"))
    return out


# --------------------------------------------------------------------------- documents
REL_PHRASES = [
    (re.compile(r"([\w-]+)\s+(?:also\s+)?feeds\s+(?:the\s+)?([\w-]+(?:\s+[A-Z][\w-]*)*)"), "FEEDS"),
    (re.compile(r"([\w-]+)\s+depends\s+on\s+(?:the\s+)?([\w-]+)"), "DEPENDS_ON"),
]


def _doc_text_docx(path: Path) -> tuple[list[str], list[list[list[str]]]]:
    from docx import Document
    d = Document(path)
    paras = [p.text for p in d.paragraphs if p.text.strip()]
    tables = [[[c.text.strip() for c in r.cells] for r in t.rows] for t in d.tables]
    return paras, tables


def _doc_text_pdf(path: Path) -> list[str]:
    import pymupdf
    with pymupdf.open(path) as d:
        return [pg.get_text() for pg in d]


def _doc_text_pptx(path: Path) -> list[str]:
    from pptx import Presentation
    out = []
    for i, s in enumerate(Presentation(path).slides):
        out.append(" ".join(sh.text_frame.text for sh in s.shapes if sh.has_text_frame))
    return out


def parse_document(path: Path, ftype: str, known_names: set[str] | None = None) -> list[dict]:
    tables: list = []
    if ftype == "docx":
        paras, tables = _doc_text_docx(path)
    elif ftype == "pdf":
        paras = _doc_text_pdf(path)
    elif ftype == "pptx":
        paras = _doc_text_pptx(path)
    else:
        paras = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    out = []
    for i, p in enumerate(paras):
        loc = f"{path.name}:para{i + 1}"
        m = re.search(r"(\d+)\s+months?\s+of\s+([\w\s]+?)\s+history", p, re.I)
        if m:
            out.append(ev("requirement", {"kind": "retention_months", "value": int(m.group(1)), "object": m.group(2).strip(), "text": p[:200]}, 0.85, loc))
        for m in re.finditer(r"([\w\s-]+?)\s+(?:requires|tolerance[^\d]*)\s*(?:is\s+)?(near-zero downtime|\d+\s+hours?)", p, re.I):
            out.append(ev("requirement", {"kind": "downtime_tolerance", "value": m.group(2), "text": m.group(0).strip()[:200]}, 0.7, loc))
        if "not recorded" in p.lower() and "owner" in p.lower():
            out.append(ev("needs_review", {"reason": "Document states an owner is not recorded", "text": p[:200]}, 0.9, loc))
        for pat, rtype in REL_PHRASES:
            for m in pat.finditer(p):
                src, tgt = m.group(1).strip(), m.group(2).strip()
                if known_names is None or src in known_names:
                    out.append(ev("relationship", {"source": src, "target": tgt, "type": f"DOC_{rtype}", "text": m.group(0)[:160]},
                                  0.6, loc))
    for ti, t in enumerate(tables):
        if t and [c.lower() for c in t[0]][:2] == ["system", "depends on"]:
            for ri, r in enumerate(t[1:]):
                out.append(ev("relationship", {"source": r[1], "target": r[0], "type": r[2]}, 0.85,
                              f"{path.name}:table{ti + 1}!row{ri + 2}"))
    return out


# --------------------------------------------------------------------------- diagrams
LABEL_RE = re.compile(r"^\s*([\w.-]+)\s*(?:\(([^)]*)\))?")


def _label_to_resource(label: str) -> tuple[str, str]:
    m = LABEL_RE.match(label)
    name = m.group(1) if m else label
    svc_hint = (m.group(2) or "") if m else ""
    svc = "UNKNOWN"
    for k, v in sorted(SERVICE_ALIASES.items(), key=lambda kv: -len(kv[0])):
        if k in (svc_hint or label).lower():
            svc = v
            break
    return name, svc


def parse_drawio(path: Path) -> list[dict]:
    root = ET.parse(path).getroot()
    nodes, out = {}, []
    for c in root.iter("mxCell"):
        if c.get("vertex") == "1":
            label = re.sub("<[^>]+>", " ", c.get("value", "")).strip()
            name, svc = _label_to_resource(label)
            nodes[c.get("id")] = label if svc == "UNKNOWN" else name
            out.append(ev("resource" if svc != "UNKNOWN" else "consumer",
                          {"name": name if svc != "UNKNOWN" else label, "service": svc, "label": label,
                           "display_name": label} if svc == "UNKNOWN" else
                          {"resource_id": None, "name": name, "service": svc, "label": label, "configuration": {}},
                          0.7, f"{path.name}:cell[{c.get('id')}]"))
    for c in root.iter("mxCell"):
        if c.get("edge") == "1" and c.get("source") in nodes and c.get("target") in nodes:
            out.append(ev("relationship", {"source": nodes[c.get("source")], "target": nodes[c.get("target")],
                                           "type": "DIAGRAM_FLOW", "label": c.get("value", "")}, 0.7,
                          f"{path.name}:cell[{c.get('id')}]"))
    return out


def parse_mermaid(path: Path) -> list[dict]:
    out, labels = [], {}
    text = path.read_text(encoding="utf-8")
    for m in re.finditer(r"(\w+)\[([^\]]+)\]", text):
        nid, label = m.group(1), m.group(2)
        parts = label.split()
        name = parts[0]
        svc = canon_service(" ".join(parts[1:]))
        labels[nid] = name
        out.append(ev("resource", {"resource_id": None, "name": name, "service": svc, "label": label, "configuration": {}},
                      0.7, f"{path.name}:{nid}"))
    for i, line in enumerate(text.splitlines(), 1):
        m = re.match(r"\s*(\w+)(?:\[[^\]]*\])?\s*-->\s*(\w+)", line)
        if m and m.group(1) in labels and m.group(2) in labels:
            out.append(ev("relationship", {"source": labels[m.group(1)], "target": labels[m.group(2)], "type": "DIAGRAM_FLOW"},
                          0.7, f"{path.name}:{i}"))
    return out


def parse_image(path: Path) -> list[dict]:
    return [ev("needs_review", {"reason": "Raster diagram: OCR/vision extraction runs only via the approved AI model; "
                                          "no nodes are invented. Provide .drawio/.mmd/.vsdx export for deterministic parsing."},
               0.0, path.name)]


def parse_dynamodb_export(path: Path) -> list[dict]:
    n = sum(1 for _ in path.open(encoding="utf-8"))
    return [ev("code_reference", {"kind": "dynamodb_export", "items": n, "file": path.name}, 1.0, path.name)]


# --------------------------------------------------------------------------- router
def extract(path: Path, category: str, ftype: str, known_names: set[str] | None = None) -> list[dict]:
    if category == "inventory":
        return {"csv": parse_inventory_csv, "xlsx": parse_inventory_xlsx, "json": parse_inventory_json}[ftype](path)
    if category == "dependency":
        return parse_inventory_xlsx(path) if ftype == "xlsx" else parse_dependency_csv(path)
    if category == "ownership":
        return parse_ownership_csv(path)
    if category == "code":
        if ftype == "terraform":
            return parse_terraform(path)
        if ftype == "cloudformation":
            return parse_cloudformation(path)
        if ftype in ("python", "lambda_python", "glue_pyspark", "airflow_dag"):
            return parse_python(path)
        if ftype == "sql":
            return parse_sql(path)
        if ftype in ("stepfunctions", "iam_policy"):
            return parse_json_code(path, ftype)
        return []
    if category == "document":
        return parse_document(path, ftype, known_names)
    if category == "diagram":
        return {"drawio": parse_drawio, "mermaid": parse_mermaid}.get(ftype, parse_image)(path)
    if category == "data" and ftype == "dynamodb_export":
        return parse_dynamodb_export(path)
    return []
