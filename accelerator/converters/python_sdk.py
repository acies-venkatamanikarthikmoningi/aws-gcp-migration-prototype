"""AWS application code -> GCP (Lambda -> Cloud Run functions, boto3 apps -> Cloud Run).

Two-step pattern (both deterministic):
  1. Replatform with a compatibility adapter: boto3 clients are swapped for `gcp_compat` clients exposing the
     same call shapes on top of Cloud Storage / Firestore / Pub/Sub / Secret Manager. Code keeps working with
     minimal edits; the adapter is generated and reviewable.
  2. A refactor report lists each AWS call so the team can later move to native GCP SDK calls.
Hard-coded ARNs/URLs/endpoints become environment variables; secrets are redacted and flagged.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from ..parsers import ENDPOINT_PATTERNS, redact
from .common import ConversionResult, check_no_aws_refs, check_python

CLIENT_MAP = {  # (boto3 kind, service) -> compat factory
    ("client", "s3"): "gcp_compat.storage_client()",
    ("resource", "s3"): "gcp_compat.storage_client()",
    ("resource", "dynamodb"): "gcp_compat.firestore_resource()",
    ("client", "dynamodb"): "gcp_compat.firestore_resource()",
    ("client", "sns"): "gcp_compat.pubsub_publisher()",
    ("client", "sqs"): "gcp_compat.pubsub_publisher()",
    ("client", "secretsmanager"): "gcp_compat.secret_client()",
    ("client", "kinesis"): "gcp_compat.pubsub_publisher()",
}
SUPPORTED_CALLS = {
    "s3": {"get_object", "put_object", "generate_presigned_url", "list_objects_v2", "delete_object"},
    "dynamodb": {"Table", "put_item", "get_item", "delete_item"},
    "sns": {"publish"}, "sqs": {"send_message"}, "secretsmanager": {"get_secret_value"}, "kinesis": {"put_record"},
}

COMPAT = '''"""gcp_compat – GENERATED compatibility adapter (AWS SDK call shapes on Google Cloud SDKs).
Purpose: replatform quickly with minimal code change; replace with native calls during refactor.
Review required. Only the call shapes listed below are supported; anything else raises NotImplementedError.
"""
import io
import json
import os
from datetime import timedelta


def _topic_path(arn_or_url: str) -> str:
    # arn:aws:sns:region:acct:name | https://sqs.region.amazonaws.com/acct/name | projects/p/topics/name
    if arn_or_url.startswith("projects/"):
        return arn_or_url
    name = arn_or_url.rstrip("/").split(":")[-1].split("/")[-1]
    return f"projects/{os.environ['GCP_PROJECT']}/topics/{name}"


class _Storage:
    def __init__(self):
        from google.cloud import storage
        self._c = storage.Client()

    def get_object(self, Bucket, Key):
        data = self._c.bucket(Bucket).blob(Key).download_as_bytes()
        return {"Body": io.BytesIO(data), "ContentLength": len(data)}

    def put_object(self, Bucket, Key, Body, **kw):
        self._c.bucket(Bucket).blob(Key).upload_from_string(Body, content_type=kw.get("ContentType"))
        return {}

    def delete_object(self, Bucket, Key):
        self._c.bucket(Bucket).blob(Key).delete()
        return {}

    def list_objects_v2(self, Bucket, Prefix="", **kw):
        return {"Contents": [{"Key": b.name, "Size": b.size} for b in self._c.list_blobs(Bucket, prefix=Prefix)]}

    def generate_presigned_url(self, op, Params, ExpiresIn=3600):
        method = {"get_object": "GET", "put_object": "PUT"}[op]
        blob = self._c.bucket(Params["Bucket"]).blob(Params["Key"])
        return blob.generate_signed_url(version="v4", expiration=timedelta(seconds=ExpiresIn), method=method)


class _Table:
    def __init__(self, db, name, key):
        self._col, self._key = db.collection(name), key

    def put_item(self, Item):
        self._col.document(str(Item[self._key])).set(Item)
        return {}

    def get_item(self, Key):
        snap = self._col.document(str(Key[self._key])).get()
        return {"Item": snap.to_dict()} if snap.exists else {}

    def delete_item(self, Key):
        self._col.document(str(Key[self._key])).delete()
        return {}


class _Firestore:
    def __init__(self):
        from google.cloud import firestore
        self._db = firestore.Client()

    def Table(self, name):
        return _Table(self._db, name, os.environ.get("TABLE_KEY", "session_id"))


class _Publisher:
    def __init__(self):
        from google.cloud import pubsub_v1
        self._p = pubsub_v1.PublisherClient()

    def _pub(self, target, payload, **attrs):
        return self._p.publish(_topic_path(target), payload.encode() if isinstance(payload, str) else payload, **attrs).result()

    def publish(self, TopicArn, Message, **kw):
        return {"MessageId": self._pub(TopicArn, Message)}

    def send_message(self, QueueUrl, MessageBody, **kw):
        return {"MessageId": self._pub(QueueUrl, MessageBody)}

    def put_record(self, StreamName, Data, PartitionKey):
        return {"SequenceNumber": self._pub(StreamName, Data, ordering_key_hint=PartitionKey)}


class _Secrets:
    def __init__(self):
        from google.cloud import secretmanager
        self._c = secretmanager.SecretManagerServiceClient()

    def get_secret_value(self, SecretId):
        name = f"projects/{os.environ['GCP_PROJECT']}/secrets/{SecretId}/versions/latest"
        return {"SecretString": self._c.access_secret_version(name=name).payload.data.decode()}


def storage_client():
    return _Storage()


def firestore_resource():
    return _Firestore()


def pubsub_publisher():
    return _Publisher()


def secret_client():
    return _Secrets()


def s3_event_from_cloud_event(cloud_event) -> dict:
    """Adapt a Cloud Storage 'object finalized' CloudEvent to the S3 event shape the Lambda handler expects."""
    d = cloud_event.data
    return {"Records": [{"eventSource": "gcp:storage", "s3": {"bucket": {"name": d["bucket"]}, "object": {"key": d["name"], "size": int(d.get("size", 0))}}}]}
'''


class _Rewriter(ast.NodeTransformer):
    def __init__(self, res: ConversionResult, fname: str):
        self.res, self.fname = res, fname
        self.var_service: dict[str, str] = {}
        self.calls: list[dict] = []

    def visit_Assign(self, node: ast.Assign):
        v = node.value
        if (isinstance(v, ast.Call) and isinstance(v.func, ast.Attribute) and isinstance(v.func.value, ast.Name)
                and v.func.value.id == "boto3" and v.args and isinstance(v.args[0], ast.Constant)):
            kind, svc = v.func.attr, v.args[0].value
            repl = CLIENT_MAP.get((kind, svc))
            target = node.targets[0].id if isinstance(node.targets[0], ast.Name) else None
            self.res.constructs_total += 1
            if repl and target:
                self.var_service[target] = svc
                self.res.constructs_converted += 1
                self.res.mapping_log.append({"line": node.lineno, "source": f"boto3.{kind}('{svc}')", "target": repl})
                node.value = ast.parse(repl, mode="eval").body
            else:
                self.res.unsupported.append(f"{self.fname}:{node.lineno} boto3.{kind}('{svc}') – no adapter")
        return self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        f = node.func
        if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id in self.var_service:
            svc = self.var_service[f.value.id]
            ok = f.attr in SUPPORTED_CALLS.get(svc, set())
            self.calls.append({"line": node.lineno, "service": svc, "call": f.attr, "adapter": ok})
            if not ok:
                self.res.unsupported.append(f"{self.fname}:{node.lineno} {svc}.{f.attr} not covered by adapter")
        return self.generic_visit(node)


def _externalize(code: str, res: ConversionResult) -> tuple[str, dict[str, str]]:
    env: dict[str, str] = {}

    def sub(kind: str, value: str) -> str:
        if kind == "s3_uri":
            return value.replace("s3://", "gs://", 1)
        name = re.sub(r"[^A-Z0-9]", "_", value.rstrip("/").split(":")[-1].split("/")[-1].split(".")[0].upper()) + "_" + {
            "arn": "TOPIC", "sqs_url": "TOPIC", "rds_endpoint": "DB_HOST"}.get(kind, "REF")
        env[name] = {"arn": "projects/${GCP_PROJECT}/topics/" + value.split(":")[-1], "sqs_url": "projects/${GCP_PROJECT}/topics/" + value.rstrip("/").split("/")[-1],
                     "rds_endpoint": "127.0.0.1 (Cloud SQL Auth Proxy) or private IP"}.get(kind, value)
        return f'__ENV__{name}__'

    for kind, pat in ENDPOINT_PATTERNS:
        def repl(m, kind=kind):
            res.mapping_log.append({"source": m.group(0), "target": "env/gs mapping", "kind": kind})
            return sub(kind, m.group(0))
        code = pat.sub(repl, code)
    # "...__ENV__X__..." literal -> os.environ["X"]
    code = re.sub(r'"__ENV__(\w+)__"', r'os.environ["\1"]', code)
    code = re.sub(r"'__ENV__(\w+)__'", r'os.environ["\1"]', code)
    return code, env


def convert_python_app(path: Path, kind: str) -> ConversionResult:
    """kind: lambda_python | python (generic boto3 service, e.g. Flask API)."""
    res = ConversionResult("lambda-to-cloud-run-functions" if kind == "lambda_python" else "boto3-app-to-cloud-run", path.name)
    src = path.read_text(encoding="utf-8")
    redacted, found = redact(src)
    for f in found:
        res.warnings.append(f"{f} found in source – redacted; rotate credential and load from Secret Manager")
    tree = ast.parse(redacted)
    rw = _Rewriter(res, path.name)
    tree = rw.visit(tree)
    ast.fix_missing_locations(tree)
    code = ast.unparse(tree)
    code = code.replace("import boto3", "import gcp_compat")
    code, env = _externalize(code, res)
    code = re.sub(r"""(\w+)\s*=\s*['"]<REDACTED:aws_access_key_id>['"]""", r"# \1 removed: use the runtime service account identity", code)
    if "import os" not in code:
        code = "import os\n" + code
    reqs = ["google-cloud-storage>=2.16", "google-cloud-firestore>=2.16", "google-cloud-pubsub>=2.21",
            "google-cloud-secret-manager>=2.20"]
    if kind == "lambda_python":
        code = ("import functions_framework\n" + code +
                "\n\n\n@functions_framework.cloud_event\ndef main(cloud_event):\n"
                "    \"\"\"Cloud Run functions entry point (Eventarc: google.cloud.storage.object.v1.finalized).\"\"\"\n"
                "    return lambda_handler(gcp_compat.s3_event_from_cloud_event(cloud_event), None)\n")
        reqs.insert(0, "functions-framework>=3.5")
        res.assumptions.append("S3 ObjectCreated trigger -> Eventarc Cloud Storage finalized trigger; event adapted to S3 shape")
        res.manual_actions.append("Deploy: gcloud functions deploy customer-ingest-fn --gen2 --runtime=python312 "
                                  "--entry-point=main --trigger-bucket=customer-raw (after approval)")
        out_name = "app/lambda_to_cloud_run_function/main.py"
        base = "app/lambda_to_cloud_run_function/"
    else:
        reqs += ["flask>=3.0", "gunicorn>=22.0", "psycopg2-binary>=2.9", "cloud-sql-python-connector>=1.9"]
        if "psycopg2" in src:
            res.manual_actions.append("RDS endpoint -> Cloud SQL: use Cloud SQL Auth Proxy sidecar or Python Connector with IAM DB auth")
        base = "app/customer_api_cloud_run/"
        out_name = base + Path(path.name).name
        res.outputs[base + "Dockerfile"] = ("FROM python:3.12-slim\nWORKDIR /app\nCOPY requirements.txt .\n"
                                            "RUN pip install --no-cache-dir -r requirements.txt\nCOPY . .\n"
                                            "CMD exec gunicorn --bind :$PORT --workers 2 customer_api:app\n")
    header = (f"# GENERATED from {path.name} by AWS-to-GCP Migration Accelerator – review before deployment.\n"
              f"# Pattern: replatform via gcp_compat adapter; see refactor_report.md for native-SDK refactor.\n")
    res.outputs[out_name] = header + code + ("\n" if not code.endswith("\n") else "")
    res.outputs[base + "gcp_compat.py"] = COMPAT
    res.outputs[base + "requirements.txt"] = "\n".join(reqs) + "\n"
    res.outputs[base + ".env.example"] = "GCP_PROJECT=<project-id>\n" + "\n".join(f"{k}={v}" for k, v in env.items()) + "\n"
    report = ["# Refactor report – AWS SDK calls", "", "| line | service | call | adapter covers |", "|---|---|---|---|"]
    report += [f"| {c['line']} | {c['service']} | {c['call']} | {'yes' if c['adapter'] else 'NO'} |" for c in rw.calls]
    res.outputs[base + "refactor_report.md"] = "\n".join(report) + "\n"
    res.constructs_total += len(rw.calls)
    res.constructs_converted += sum(1 for c in rw.calls if c["adapter"])
    res.checks += [check_python(res.outputs[out_name], out_name), check_python(COMPAT, "gcp_compat.py"),
                   check_no_aws_refs(res.outputs[out_name], out_name)]
    return res
