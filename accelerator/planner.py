"""Migration planning: data-movement method selection, cutover strategy, wave plan and backfill plan.

All arithmetic is deterministic. Transfer tool choice is a *recommendation*; blueprint 6.5 requires the final
selection after assessing source type, volume, network, RPO/RTO and customer standards.
"""
from __future__ import annotations

import math
from collections import defaultdict

import networkx as nx

DEFAULT_BANDWIDTH_MBPS = 1000
LINK_UTILISATION = 0.7

# service -> (bulk method, catch-up/CDC method, notes)
MOVEMENT = {
    "S3": ("Storage Transfer Service (S3 -> Cloud Storage)", "Storage Transfer Service incremental sync / event-driven",
           "Preserve object metadata; map storage classes; verify MD5/CRC32C."),
    "S3 Glacier": ("Glacier restore -> Storage Transfer Service", "n/a (cold)", "Plan AWS restore time and cost."),
    "Redshift": ("UNLOAD to S3 Parquet -> Storage Transfer Service -> BigQuery load (or BigQuery Data Transfer Service)",
                 "Watermark-based incremental extract or Datastream from upstream OLTP",
                 "Translate DDL/SQL with BigQuery Migration Service; DIST/SORT -> partition/cluster."),
    "RDS PostgreSQL": ("Database Migration Service initial load", "Database Migration Service continuous CDC (logical replication)",
                       "Requires rds.logical_replication=1; test extensions."),
    "RDS MySQL": ("Database Migration Service initial load", "Database Migration Service CDC (binlog)", "binlog_format=ROW."),
    "RDS SQL Server": ("Database Migration Service / native backup to S3", "DMS CDC", "Check edition features."),
    "RDS Oracle": ("Data Pump / DMS heterogeneous", "Datastream or GoldenGate CDC", "Schema conversion workspace required for AlloyDB."),
    "Aurora": ("Database Migration Service", "Database Migration Service CDC", "Aurora-only features need review."),
    "DynamoDB": ("Export to S3 (PITR) -> Storage Transfer -> Dataflow -> Firestore/Bigtable",
                 "DynamoDB Streams -> Lambda/Kinesis bridge -> Pub/Sub -> Dataflow", "Convert typed attribute JSON."),
    "MSK": ("MirrorMaker 2 topic replication", "MirrorMaker 2 continuous with offset sync", "Cut consumers over by group."),
    "Kinesis": ("n/a (streaming)", "Dual-publish producers to Pub/Sub; drain Kinesis", "Re-point consumers after lag = 0."),
    "EFS": ("Storage Transfer Service (POSIX agents)", "Incremental rsync passes", "Preserve permissions/ownership."),
    "EC2": ("Migrate to Virtual Machines replication", "Continuous replication until cutover", "Test-clone before cutover."),
    "ElastiCache": ("Rebuild cache (warm from source of truth)", "n/a", "Caches are rebuilt, not migrated, unless persistence required."),
    "EMR": ("DistCp / Storage Transfer for HDFS & S3 data", "Incremental DistCp", "Jobs re-pointed to gs://."),
    "SageMaker": ("Copy model artefacts to Cloud Storage; re-register in Vertex AI", "Retrain pipeline", "Re-validate model metrics."),
    "Secrets Manager": ("Scripted secret copy (no logging)", "Re-sync on rotation", "Rotate after cutover."),
    "Cognito": ("User export (profiles only – password hashes are not exportable) -> Identity Platform import",
                "Lazy migration: authenticate against Cognito on first login, then create user in Identity Platform",
                "Users who never log in need a password reset campaign."),
    "QLDB": ("Export journal to S3 -> Cloud Storage -> Spanner load", "n/a (service end-of-support)", "Re-implement verification digest."),
    "Kinesis Firehose": ("n/a (delivery)", "Re-point producers to Pub/Sub", ""),
}


def transfer_hours(size_gb: float, bandwidth_mbps: float = DEFAULT_BANDWIDTH_MBPS) -> float:
    if size_gb <= 0:
        return 0.0
    return round(size_gb * 8 * 1024 / (bandwidth_mbps * LINK_UTILISATION) / 3600, 2)


def parse_downtime(text: str | None) -> float | None:
    if not text:
        return None
    t = text.lower()
    if "zero" in t:
        return 0.25
    digits = "".join(ch for ch in t if ch.isdigit() or ch == ".")
    return float(digits) if digits else None


def choose_movement(service: str, size_gb: float, downtime_hours: float | None,
                    bandwidth_mbps: float = DEFAULT_BANDWIDTH_MBPS, change_rate_pct_day: float = 2.0) -> dict:
    bulk, cdc, notes = MOVEMENT.get(service, ("Export -> Cloud Storage landing -> load", "Watermark incremental", ""))
    hours = transfer_hours(size_gb, bandwidth_mbps)
    offline = size_gb >= 100 * 1024 or hours > 24 * 7
    rationale = [f"estimated online transfer {hours} h for {size_gb:.0f} GB at {bandwidth_mbps} Mbps x {LINK_UTILISATION:.0%}"]
    if offline:
        bulk = "Transfer Appliance (offline) + " + cdc
        rationale.append("> 100 TB or > 7 days online -> offline Transfer Appliance for bulk")
    # cutover strategy
    if service in ("Kinesis", "MSK"):
        cutover, pattern = "Parallel run (dual-publish / mirror) then consumer switchover", "Streaming replication"
    elif service == "Cognito":
        cutover, pattern = "Phased user migration (lazy migration + reset campaign)", "Identity migration"
    elif downtime_hours is None:
        cutover, pattern = "NEEDS REVIEW – downtime tolerance unknown", "Full backfill + incremental catch-up (default)"
        rationale.append("downtime tolerance not stated in any supplied artefact -> request from owner; nothing assumed")
    elif downtime_hours < 1:
        cutover, pattern = "Online CDC with near-zero-downtime switchover", "Full load + CDC"
        rationale.append(f"downtime tolerance {downtime_hours} h < 1 h -> CDC required")
    elif hours <= downtime_hours * 0.5:
        cutover, pattern = "Offline big-bang within window", "Full historical backfill"
        rationale.append(f"load {hours} h fits within 50% of {downtime_hours} h window")
    else:
        cutover, pattern = "Phased: partition backfill ahead of window + incremental catch-up at cutover", \
            "Partition backfill + incremental catch-up"
        rationale.append(f"load {hours} h exceeds window {downtime_hours} h -> pre-load history, catch up at cutover")
    daily_delta = size_gb * change_rate_pct_day / 100
    return {"service": service, "size_gb": size_gb, "bulk_method": bulk, "catch_up_method": cdc, "notes": notes,
            "estimated_transfer_hours": hours, "daily_change_gb": round(daily_delta, 2), "offline_recommended": offline,
            "cutover_strategy": cutover, "data_pattern": pattern, "rationale": rationale,
            "rollback": "Keep AWS source authoritative until sign-off; reverse-sync or re-point consumers back; never delete source automatically"}


FOUNDATION_SERVICES = {"VPC", "IAM", "KMS", "Direct Connect", "Secrets Manager", "CloudWatch", "CloudTrail", "Organizations"}


def plan_waves(resources: list[dict], relationships: list[dict], mappings: dict[str, dict],
               consumers: list[dict] | None = None) -> list[dict]:
    """Group applications into dependency-ordered waves. Wave 0 = landing zone/foundation.

    Application A must migrate no later than applications that consume it, unless a bridge is planned;
    cycles collapse into one wave (strongly connected components)."""
    app_of = {r["resource_id"]: (r.get("application") or "Unassigned") for r in resources}
    for c in consumers or []:  # downstream consumer systems pull their application into the ordering
        app_of.setdefault(c["node_id"], c.get("application") or "Unassigned")
    g = nx.DiGraph()
    excluded = []
    for r in resources:
        m = mappings.get(r["resource_id"], {})
        if m.get("strategy") in ("Retire", "Retain"):
            excluded.append({"resource_id": r["resource_id"], "strategy": m["strategy"], "reason": m["rationale"][-1]})
            continue
        if r["service"] in FOUNDATION_SERVICES:
            continue
        g.add_node(app_of[r["resource_id"]])
    for rel in relationships:
        a, b = app_of.get(rel["source"]), app_of.get(rel["target"])
        if a and b and a != b and a in g and b in g:
            g.add_edge(a, b)
    cond = nx.condensation(g)
    waves = [{"wave": 0, "name": "Foundation / landing zone",
              "applications": ["(shared)"],
              "resources": sorted(r["resource_id"] for r in resources if r["service"] in FOUNDATION_SERVICES),
              "notes": "Org/folders/projects, VPC + Interconnect/VPN to AWS, IAM + Workload Identity Federation, KMS, logging"}]
    for i, gen in enumerate(nx.topological_generations(cond), start=1):
        apps = sorted(a for c in gen for a in cond.nodes[c]["members"])
        res = sorted(r["resource_id"] for r in resources if app_of[r["resource_id"]] in apps
                     and r["service"] not in FOUNDATION_SERVICES
                     and mappings.get(r["resource_id"], {}).get("strategy") not in ("Retire", "Retain"))
        waves.append({"wave": i, "name": f"Wave {i}", "applications": apps, "resources": res,
                      "notes": "Upstream producers first; bridge/dual-run for consumers left in AWS"})
    waves.append({"wave": len(waves), "name": "Excluded (Retire / Retain)", "applications": [], "resources":
                  [e["resource_id"] for e in excluded], "notes": "; ".join(f"{e['resource_id']}: {e['strategy']}" for e in excluded)})
    return waves


def backfill_plan(source_object: str, target_object: str, partitions: list[str], retention_months: int | None,
                  load_mode: str = "REPLACE_PARTITION", batch_key_prefix: str = "B") -> dict:
    """Blueprint 6.1/6.3. Decide historical depth from retention requirement and build ordered batches."""
    parts = sorted(partitions)
    if retention_months and len(parts) > retention_months:
        in_scope = parts[-retention_months:]
        decision = f"Retention requirement {retention_months} months -> load last {retention_months} partitions"
    elif retention_months and len(parts) < retention_months:
        in_scope = parts
        decision = f"Only {len(parts)} partitions exist but {retention_months} required -> FLAG coverage gap"
    else:
        in_scope = parts
        decision = "Full historical backfill of all retained partitions"
    batches = [{"batch_key": f"{batch_key_prefix}{i + 1:03d}", "partition": p} for i, p in enumerate(in_scope)]
    return {"source_object": source_object, "target_object": target_object, "mode": "FULL_HISTORICAL_BACKFILL",
            "load_mode": load_mode, "range_start": in_scope[0] if in_scope else None,
            "range_end": in_scope[-1] if in_scope else None, "decision": decision, "batches": batches,
            "max_parallel_batches": 4, "retry_policy": {"max_attempts": 3, "backoff_seconds": [5, 25, 125]},
            "idempotency": "deterministic batch_key; target partition replaced atomically (DELETE partition + INSERT in one txn)",
            "validation_rules": ["row_count", "control_total:amount", "partition_hash", "pk_uniqueness", "null_check:customer_id"]}


def estimate_batches(rows: int, batch_rows: int = 500_000) -> int:
    return max(1, math.ceil(rows / batch_rows))


def summarize_by_strategy(mappings: list[dict]) -> dict:
    out = defaultdict(int)
    for m in mappings:
        out[m["strategy"]] += 1
    return dict(out)
