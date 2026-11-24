"""Mapping engine: GCP target recommendation + 7R strategy, driven by catalogue + workload facts.

Explainable: every recommendation returns the rules that fired. LLM (optional) only phrases explanations.
"""
from __future__ import annotations

from typing import Any

from .catalogue import CATALOGUE_VERSION, lookup

SEVEN_RS = {
    "Rehost": "Lift-and-shift with minimal change (e.g. EC2 -> Compute Engine).",
    "Replatform": "Move to managed equivalent with small changes (e.g. RDS -> Cloud SQL).",
    "Refactor": "Re-architect / rewrite code or data model for cloud-native target.",
    "Repurchase": "Replace with SaaS / Marketplace / different product.",
    "Relocate": "Move platform/hypervisor or connectivity without changing workloads.",
    "Retain": "Keep in AWS (or on-prem) for now – compliance, coupling or timing.",
    "Retire": "Decommission – unused, duplicated or end-of-support.",
}
EFFORT = {1: "S", 2: "M", 3: "L", 4: "XL", 5: "XXL"}


def _cfg(res: dict) -> dict:
    merged: dict[str, Any] = {}
    for _, c in (res.get("configuration") or {}).items():
        if isinstance(c, dict):
            merged.update(c)
    return merged


def _num(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _tags(cfg: dict) -> dict:
    raw = str(cfg.get("tags") or "")
    out = {}
    for part in raw.replace(";", ",").split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip().lower()] = v.strip().lower()
    return out


def recommend(res: dict) -> dict:
    cfg = _cfg(res)
    tags = _tags(cfg)
    engine = str(cfg.get("engine") or cfg.get("Engine") or "")
    entry = lookup(res["service"], engine)
    rules: list[str] = []
    if not entry:
        return {"resource_id": res["resource_id"], "aws_service": res["service"], "target_service": "UNKNOWN",
                "alternatives": [], "strategy": "Retain", "score": 0, "effort": "?", "constraints": [],
                "rationale": ["NEEDS REVIEW: service not in catalogue " + CATALOGUE_VERSION + "; no mapping invented"],
                "doc": None, "migration_tools": [], "gap": "Not in catalogue", "catalogue_version": CATALOGUE_VERSION}
    targets = [t["service"] for t in entry["targets"]]
    primary = targets[0]
    strategy = entry["default_strategy"]
    complexity = entry["complexity"]
    size_gb = _num(cfg.get("size_gb") or cfg.get("allocated_storage") or cfg.get("SizeGB"))
    rules.append(f"catalogue default: {entry['aws_service']} -> {primary} ({strategy})")

    svc = entry["aws_service"]
    # ---- workload-fact rules
    if svc == "RDS PostgreSQL":
        if size_gb >= 2000 or tags.get("workload") in ("htap", "analytics"):
            primary = "AlloyDB"
            rules.append(f"size {size_gb:.0f} GB / HTAP -> AlloyDB")
        else:
            rules.append(f"size {size_gb:.0f} GB OLTP -> Cloud SQL for PostgreSQL is sufficient; AlloyDB if perf tests demand")
            primary = "Cloud SQL for PostgreSQL"
    if svc == "RDS Oracle":
        if "byol" in str(cfg.get("tags", "")).lower():
            primary = "Bare Metal Solution"
            rules.append("BYOL Oracle licence -> keep Oracle on Bare Metal Solution / Oracle Database@Google Cloud")
        rules.append("Alternative: refactor to AlloyDB via DMS conversion workspace (PL/SQL rewrite effort)")
    if svc == "DynamoDB":
        pattern = tags.get("pattern", "")
        if size_gb > 1000 or tags.get("throughput") == "very-high":
            primary = "Bigtable"
            rules.append("very large / very high throughput -> Bigtable")
        elif tags.get("transactions") == "global":
            primary = "Spanner"
            rules.append("global transactions -> Spanner")
        else:
            primary = "Firestore"
            rules.append(f"{pattern or 'document'} access pattern, {size_gb:.0f} GB -> Firestore")
    if svc == "Lambda":
        timeout = _num(cfg.get("timeout") or cfg.get("Timeout"), 0)
        if timeout > 3600:
            primary = "Cloud Run"
            rules.append(f"timeout {timeout}s > 60 min -> Cloud Run jobs")
        else:
            rules.append(f"timeout {timeout or 'n/a'}s fits Cloud Run functions")
    if svc == "EC2":
        if _num(cfg.get("last_accessed_days")) > 180:
            strategy, primary = "Retire", "Decommission"
            rules.append(f"no access for {cfg.get('last_accessed_days')} days -> Retire candidate (owner confirmation required)")
        elif "pinned" in str(cfg.get("tags", "")).lower() or tags.get("compliance"):
            strategy, primary = "Retain", "Remain in AWS (interim)"
            rules.append("compliance-pinned connectivity -> Retain until dependency is re-homed")
    if svc == "Kinesis" and tags.get("replay") == "required":
        primary = "Managed Service for Apache Kafka"
        rules.append("consumers require replay/log semantics -> Managed Kafka")
    if svc == "CloudFront" and "lambda_edge=true" in str(cfg.get("tags", "")).lower():
        complexity = max(complexity, 4)
        rules.append("Lambda@Edge in use -> edge logic redesign (complexity raised)")
    if svc == "Cognito" and _num(tags.get("users")) > 0:
        rules.append(f"{tags['users']} users: password hashes not exportable -> plan lazy migration or reset")
    if entry["gap"]:
        rules.append(f"gap: {entry['gap']}")

    if res.get("conflicts"):
        rules.append(f"{len(res['conflicts'])} conflicting evidence item(s) must be resolved before approval")
    score = max(5, 100 - 12 * complexity - (15 if entry["gap"] else 0) - (10 if res.get("conflicts") else 0))
    alternatives = [{"service": t["service"], "when": t["when"]} for t in entry["targets"] if t["service"] != primary]
    if strategy not in ("Retire", "Retain") and svc == "EC2":
        alternatives.append({"service": "GKE / Cloud Run", "when": "Replatform: containerize the app"})
    constraints = [c.strip() for c in entry["considerations"].split(",") if c.strip()]
    return {"resource_id": res["resource_id"], "aws_service": svc, "target_service": primary,
            "alternatives": alternatives, "strategy": strategy, "score": score, "effort": EFFORT[complexity],
            "constraints": constraints, "rationale": rules, "doc": entry["doc"], "migration_tools": entry["migration_tools"],
            "gap": entry["gap"], "catalogue_version": CATALOGUE_VERSION}
