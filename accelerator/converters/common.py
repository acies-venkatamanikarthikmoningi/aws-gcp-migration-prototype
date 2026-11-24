"""Shared conversion result model, HCL writer and static checks."""
from __future__ import annotations

import ast
import json
from dataclasses import asdict, dataclass, field
from typing import Any

import yaml

REGION_MAP = {"us-east-1": "us-east1", "us-east-2": "us-east4", "us-west-1": "us-west2", "us-west-2": "us-west1",
              "eu-west-1": "europe-west1", "eu-west-2": "europe-west2", "eu-central-1": "europe-west3",
              "ap-south-1": "asia-south1", "ap-southeast-1": "asia-southeast1", "ap-northeast-1": "asia-northeast1",
              "ap-southeast-2": "australia-southeast1", "ca-central-1": "northamerica-northeast1",
              "sa-east-1": "southamerica-east1"}
# vCPU/RAM-equivalent families (x86). Graviton (g) -> Arm (t2a/c4a) flagged for review.
MACHINE_MAP = {"t2.micro": "e2-micro", "t2.small": "e2-small", "t2.medium": "e2-medium", "t3.medium": "e2-medium",
               "t3.large": "e2-standard-2", "m5.large": "n2-standard-2", "m5.xlarge": "n2-standard-4",
               "m5.2xlarge": "n2-standard-8", "m5.4xlarge": "n2-standard-16", "c5.large": "c3-standard-4",
               "c5.xlarge": "c3-standard-4", "c5.2xlarge": "c3-standard-8", "r5.large": "n2-highmem-2",
               "r5.xlarge": "n2-highmem-4", "m6i.large": "n2-standard-2", "m6i.xlarge": "n2-standard-4"}
DB_TIER_MAP = {"db.t3.medium": "db-custom-2-4096", "db.r6g.large": "db-custom-2-16384", "db.r5.large": "db-custom-2-16384",
               "db.r6g.xlarge": "db-custom-4-32768", "db.m5.large": "db-custom-2-8192", "db.m5.2xlarge": "db-custom-8-32768"}


@dataclass
class ConversionResult:
    converter: str
    source_file: str
    outputs: dict[str, str] = field(default_factory=dict)  # relative path -> content
    warnings: list[str] = field(default_factory=list)
    unsupported: list[str] = field(default_factory=list)
    assumptions: list[str] = field(default_factory=list)
    manual_actions: list[str] = field(default_factory=list)
    mapping_log: list[dict] = field(default_factory=list)
    checks: list[dict] = field(default_factory=list)
    constructs_total: int = 0
    constructs_converted: int = 0

    @property
    def confidence(self) -> float:
        if not self.constructs_total:
            return 0.0
        base = self.constructs_converted / self.constructs_total
        penalty = 0.03 * len(self.warnings) + 0.05 * len(self.unsupported)
        failed = sum(1 for c in self.checks if c["status"] == "FAIL")
        return round(max(0.0, min(1.0, base - penalty - 0.2 * failed)), 2)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["confidence"] = self.confidence
        return d


# --------------------------------------------------------------------------- HCL writer
def _hcl_value(v: Any, indent: int) -> str:
    pad = "  " * indent
    if isinstance(v, Raw):
        return v.text
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return json.dumps(v)
    if isinstance(v, list):
        return "[" + ", ".join(_hcl_value(x, indent) for x in v) + "]"
    if isinstance(v, dict):
        inner = "\n".join(f"{pad}  {k} = {_hcl_value(x, indent + 1)}" for k, x in v.items())
        return "{\n" + inner + f"\n{pad}}}"
    raise TypeError(type(v))


class Raw:
    """Unquoted HCL expression, e.g. a reference."""

    def __init__(self, text: str):
        self.text = text


class Block:
    def __init__(self, kind: str, labels: list[str], body: dict[str, Any], comment: str | None = None):
        self.kind, self.labels, self.body, self.comment = kind, labels, body, comment

    def render(self, indent: int = 0) -> str:
        pad = "  " * indent
        lines = []
        if self.comment:
            lines += [f"{pad}# {c}" for c in self.comment.splitlines()]
        head = " ".join([self.kind] + [json.dumps(lbl) for lbl in self.labels])
        lines.append(f"{pad}{head} {{")
        for k, v in self.body.items():
            if isinstance(v, Block):
                lines.append(v.render(indent + 1))
            elif isinstance(v, list) and v and all(isinstance(x, Block) for x in v):
                lines += [x.render(indent + 1) for x in v]
            else:
                lines.append(f"{pad}  {k} = {_hcl_value(v, indent + 1)}")
        lines.append(f"{pad}}}")
        return "\n".join(lines)


def nested(kind: str, body: dict) -> Block:
    return Block(kind, [], body)


# --------------------------------------------------------------------------- static checks
def check_python(code: str, name: str) -> dict:
    try:
        ast.parse(code)
        return {"check": f"python syntax: {name}", "status": "PASS"}
    except SyntaxError as e:
        return {"check": f"python syntax: {name}", "status": "FAIL", "detail": str(e)}


def check_hcl(code: str, name: str) -> dict:
    import io

    import hcl2
    try:
        hcl2.load(io.StringIO(code))
        return {"check": f"terraform HCL parse: {name}", "status": "PASS"}
    except Exception as e:  # noqa: BLE001
        return {"check": f"terraform HCL parse: {name}", "status": "FAIL", "detail": str(e)[:300]}


def check_yaml(code: str, name: str) -> dict:
    try:
        yaml.safe_load(code)
        return {"check": f"yaml parse: {name}", "status": "PASS"}
    except yaml.YAMLError as e:
        return {"check": f"yaml parse: {name}", "status": "FAIL", "detail": str(e)[:300]}


def check_no_aws_refs(code: str, name: str) -> dict:
    hits = [t for t in ("boto3", "arn:aws:", "amazonaws.com", "s3://", "awsglue") if t in code]
    return {"check": f"no residual AWS references: {name}", "status": "PASS" if not hits else "WARN",
            "detail": ", ".join(hits) if hits else ""}


def check_policy(code: str, name: str) -> dict:
    """Policy guardrails on generated IaC (OPA/Conftest-like rules, deterministic)."""
    issues = []
    if '"0.0.0.0/0"' in code and "google_compute_firewall" in code:
        issues.append("firewall open to 0.0.0.0/0")
    if "google_storage_bucket" in code and "uniform_bucket_level_access = true" not in code:
        issues.append("bucket without uniform bucket-level access")
    if "google_sql_database_instance" in code and "deletion_protection = true" not in code:
        issues.append("Cloud SQL without deletion protection")
    return {"check": f"policy guardrails: {name}", "status": "WARN" if issues else "PASS", "detail": "; ".join(issues)}
