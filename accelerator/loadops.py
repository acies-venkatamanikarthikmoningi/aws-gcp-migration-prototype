"""Load controls and indicators for data movement (Round 7; blueprint load monitor).

Everything here is computed from what the engine actually wrote (batch manifests, batch timings, the simulated source and
target tables) or from the movement plan, plus an editable rate card (knowledge_base/cost_rates.json) for cost. Costs are
estimates at list price; the rate card names its sources and the date they were checked.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from .store import Store, now

RATES = Path(__file__).resolve().parent.parent / "knowledge_base" / "cost_rates.json"
MAX_PARALLEL = 8          # the prototype's simulated target serialises writes; more than this only adds contention
LINEAGE = {"_batch_key", "_load_job_id", "_loaded_at"}


def rates() -> dict:
    return json.loads(RATES.read_text(encoding="utf-8"))


def egress_cost(gb: float, r: dict, include_free: bool = False) -> float:
    """Tiered AWS internet egress. By default the account's free monthly allowance is NOT subtracted, because other
    traffic usually uses it up; that makes this an upper estimate."""
    e = r["aws_egress_internet"]
    left = max(0.0, gb - (e["free_gb_per_month"] if include_free else 0))
    cost, floor = 0.0, 0.0
    for cap, price in e["tiers"]:
        span = left if cap is None else max(0.0, min(left, cap - floor))
        cost += span * price
        left -= span
        if cap is not None:
            floor = cap
        if left <= 0:
            break
    return round(cost, 6)


def estimate(gb: float) -> dict:
    r = rates()
    bq = r["bigquery_active_logical_gb_month"]
    return {"gb": round(gb, 6),
            "aws_egress": egress_cost(gb, r),
            "gcs_landing_per_month": round(gb * r["gcs_standard_gb_month"]["rate"], 6),
            "bigquery_load": round(gb * r["bigquery_batch_load_gb"]["rate"], 6),
            "bigquery_storage_per_month": round(max(0.0, gb - bq["free_gb_per_month"]) * bq["rate"], 6),
            "checked": r["checked"], "currency": r["currency"],
            "basis": "list price; AWS free 100 GB/month not subtracted (upper estimate); BigQuery first 10 GB/month free subtracted; "
                     "landing copy assumed kept one month"}


def _ts(s: str | None) -> datetime | None:
    try:
        return datetime.fromisoformat(s) if s else None
    except ValueError:
        return None


def peak_parallel(batches: list[dict]) -> int | None:
    """Largest number of batches that were in flight at the same moment, from recorded start and end times."""
    spans = [(_ts(b.get("started_at")), _ts(b.get("ended_at"))) for b in batches]
    spans = [(a, b) for a, b in spans if a and b]
    if not spans:
        return None
    events = sorted([(a, 1) for a, _ in spans] + [(b, -1) for _, b in spans], key=lambda x: (x[0], x[1]))
    cur = best = 0
    for _, d in events:
        cur += d
        best = max(best, cur)
    return best


def run_indicators(run: dict, plan: dict | None) -> dict:
    bs = run.get("batches") or []
    by = [(b.get("checkpoint") or {}).get("manifest", {}).get("bytes") for b in bs]
    known = [x for x in by if x is not None]
    total_bytes = sum(known)
    rows = sum(b.get("extracted") or b.get("source_count") or 0 for b in bs)
    a, e = _ts(run.get("started_at")), _ts(run.get("ended_at"))
    secs = (e - a).total_seconds() if a and e else None
    limit = run.get("max_parallel") or ((plan or {}).get("config") or {}).get("max_parallel_batches")
    return {"rows": rows, "bytes": total_bytes, "bytes_known_for": len(known), "batches": len(bs),
            "seconds": secs, "max_parallel": limit, "peak_parallel": peak_parallel(bs),
            "cost": estimate(total_bytes / 1024 ** 3) if known else None}


def plan_indicators(movement: list[dict], bandwidth_mbps: float | None) -> dict:
    gb = sum(float(m.get("size_gb") or 0) for m in movement)
    hours = sum(float(m.get("estimated_transfer_hours") or 0) for m in movement)
    return {"objects": len(movement), "gb": round(gb, 2), "transfer_hours": round(hours, 2),
            "bandwidth_mbps": bandwidth_mbps, "cost": estimate(gb)}


def set_parallel(store: Store, pid: str, plan_id: str, n: int, by: str, note: str) -> dict:
    plan = store.get("migration_plans", plan_id)
    if not plan or plan["project_id"] != pid:
        raise KeyError(plan_id)
    if not isinstance(n, int) or not 1 <= n <= MAX_PARALLEL:
        raise ValueError(f"Batches at a time must be between 1 and {MAX_PARALLEL}.")
    by, note = (by or "").strip(), (note or "").strip()
    if len(by) < 2 or len(note) < 10:
        raise ValueError("Enter your name and a reason of at least 10 characters (for example the source system's load limit).")
    cfg = dict(plan["config"])
    old = cfg.get("max_parallel_batches")
    cfg["max_parallel_batches"] = n
    cfg.setdefault("control_history", []).append({"field": "max_parallel_batches", "from": old, "to": n, "by": by, "note": note, "at": now()})
    store.update("migration_plans", plan_id, config=cfg)
    store.audit(pid, by, "LOAD_CONTROL_CHANGED", "migration_plan", plan_id, {"max_parallel_batches": [old, n], "note": note})
    return store.get("migration_plans", plan_id)


def _cols(db: Path, table: str) -> list[tuple[str, str]]:
    if not db.exists():
        return []
    c = sqlite3.connect(db)
    try:
        return [(r[1], (r[2] or "").upper()) for r in c.execute(f"PRAGMA table_info({table})")]
    finally:
        c.close()


def schema_check(workdir: Path, plan: dict | None, expected: list[str]) -> dict | None:
    """Compares the columns the load plan expects with the simulated source table and the simulated target table.
    Lineage columns the loader adds to the target are expected and listed separately."""
    src = _cols(workdir / "aws_source_sim.db", "analytics_orders")
    tgt = _cols(workdir / "bigquery_sim.db", "analytics_orders")
    if not src and not tgt:
        return None
    snap = ((plan or {}).get("config") or {}).get("source_columns_at_plan")
    s_names = [c for c, _ in src if c != "order_month"]
    t_names = [c for c, _ in tgt if c not in LINEAGE and c != "order_month"]
    alerts = []
    for c in expected:
        if c not in s_names:
            alerts.append(f"source no longer has column {c}")
        if tgt and c not in t_names:
            alerts.append(f"target is missing column {c}")
    for c in s_names:
        if c not in expected:
            alerts.append(f"source has a new column {c} that the load does not copy")
    if snap and snap != s_names:
        alerts.append(f"source columns changed since the plan was made (then: {', '.join(snap)})")
    return {"expected": expected, "source": src, "target": tgt, "lineage": sorted(LINEAGE & {c for c, _ in tgt}),
            "alerts": alerts, "checked_at": now(), "snapshot": snap}
