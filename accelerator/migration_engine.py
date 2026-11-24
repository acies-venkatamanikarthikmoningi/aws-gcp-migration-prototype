"""Historical data migration / backfill engine (blueprint sections 4.7, 6.2-6.4).

Prototype execution is SIMULATED but real: data actually moves between two local stores.
  AWS side  : `aws_source_sim.db`  (stands in for Redshift/S3; read-only to the engine except for the demo's
               simulated business changes after baseline)
  Landing   : `gcs_landing/`       (stands in for the Cloud Storage landing bucket – immutable raw batches)
  GCP side  : `bigquery_sim.db`    (stands in for BigQuery staging + curated target)
Patterns: FULL_HISTORICAL_BACKFILL, PARTITION_BACKFILL, INCREMENTAL_CATCH_UP (watermark), CDC (ordered LSN log),
SELECTIVE_RELOAD. Controls: run/batch IDs, checkpoints, watermarks, idempotent REPLACE_PARTITION / MERGE,
bounded retry, quarantine of rejected rows, lineage columns, audit events. Source is never deleted or altered.
"""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .store import Store, new_id, now

COLUMNS = ["order_id", "customer_id", "order_date", "status", "amount", "currency", "updated_at"]
STATUSES = {"NEW", "PAID", "SHIPPED", "DELIVERED", "RETURNED", "CANCELLED"}


class TransferIntegrityError(Exception):
    pass


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


# --------------------------------------------------------------------------- simulated AWS source
class AwsSourceSim:
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.RLock()
        self.conn.execute("CREATE TABLE IF NOT EXISTS analytics_orders(order_id TEXT, customer_id TEXT, order_date TEXT,"
                          " status TEXT, amount TEXT, currency TEXT, updated_at TEXT, order_month TEXT)")
        self.conn.commit()

    def seed_from_partitions(self, root: Path) -> int:
        self.conn.execute("DELETE FROM analytics_orders")
        n = 0
        for f in sorted(root.glob("order_month=*/*.csv")):
            part = f.parent.name.split("=", 1)[1]
            with f.open(encoding="utf-8") as fh:
                rows = [[r[c] for c in COLUMNS] + [part] for r in csv.DictReader(fh)]
            self.conn.executemany("INSERT INTO analytics_orders VALUES(?,?,?,?,?,?,?,?)", rows)
            n += len(rows)
        self.conn.commit()
        return n

    def partitions(self) -> list[str]:
        return [r[0] for r in self.conn.execute("SELECT DISTINCT order_month FROM analytics_orders ORDER BY 1")]

    def rows(self, partition: str | None = None, updated_after: str | None = None) -> list[dict]:
        q, p = "SELECT order_id,customer_id,order_date,status,amount,currency,updated_at,order_month FROM analytics_orders", []
        cond = []
        if partition:
            cond.append("order_month=?")
            p.append(partition)
        if updated_after:
            cond.append("updated_at>?")
            p.append(updated_after)
        if cond:
            q += " WHERE " + " AND ".join(cond)
        with self.lock:
            return [dict(zip(COLUMNS + ["order_month"], r)) for r in self.conn.execute(q + " ORDER BY order_id", p)]

    def max_watermark(self) -> str:
        return self.conn.execute("SELECT MAX(updated_at) FROM analytics_orders").fetchone()[0]

    # --- simulated business activity on the source (NOT done by the migration engine)
    def simulate_changes(self, change_file: Path) -> int:
        with change_file.open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        for r in rows:
            part = r["order_date"][:7]
            self.conn.execute("DELETE FROM analytics_orders WHERE order_id=?", (r["order_id"],))
            self.conn.execute("INSERT INTO analytics_orders VALUES(?,?,?,?,?,?,?,?)", [r[c] for c in COLUMNS] + [part])
        self.conn.commit()
        return len(rows)

    def simulate_cdc(self, events: list[dict]) -> None:
        for e in sorted(events, key=lambda x: x["lsn"]):
            a = e["after"]
            if e["op"] == "D":
                self.conn.execute("DELETE FROM analytics_orders WHERE order_id=?", (str(a["order_id"]),))
            elif e["op"] == "I":
                self.conn.execute("INSERT INTO analytics_orders VALUES(?,?,?,?,?,?,?,?)",
                                  [None if a[c] is None else str(a[c]) for c in COLUMNS] + [a["order_date"][:7]])
            elif e["op"] == "U":
                sets = {c: str(a[c]) for c in COLUMNS if c != "order_id" and a.get(c) is not None}
                self.conn.execute(f"UPDATE analytics_orders SET {','.join(f'{k}=?' for k in sets)} WHERE order_id=?",
                                  [*sets.values(), str(a["order_id"])])
        self.conn.commit()


# --------------------------------------------------------------------------- simulated GCP target
class BigQuerySim:
    def __init__(self, path: Path):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.RLock()
        # no PK: BigQuery does not enforce keys – uniqueness must be validated, not assumed
        self.conn.execute("CREATE TABLE IF NOT EXISTS analytics_orders(order_id INTEGER, customer_id INTEGER, order_date TEXT,"
                          " status TEXT, amount TEXT, currency TEXT, updated_at TEXT, order_month TEXT,"
                          " _batch_key TEXT, _load_job_id TEXT, _loaded_at TEXT)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS _cdc_state(object TEXT PRIMARY KEY, applied_lsn INTEGER)")
        self.conn.commit()

    def replace_partition(self, partition: str, rows: list[dict], batch_key: str, job_id: str) -> int:
        """Idempotent: delete+insert in ONE transaction (BigQuery: WRITE_TRUNCATE on partition decorator)."""
        ts = now()
        with self.lock, self.conn:
            self.conn.execute("DELETE FROM analytics_orders WHERE order_month=?", (partition,))
            self.conn.executemany("INSERT INTO analytics_orders VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                                  [[r["order_id"], r["customer_id"], r["order_date"], r["status"], r["amount"], r["currency"],
                                    r["updated_at"], partition, batch_key, job_id, ts] for r in rows])
        return len(rows)

    def merge(self, rows: list[dict], batch_key: str, job_id: str) -> dict:
        """MERGE ON order_id: update if exists else insert (idempotent upsert)."""
        ts, ins, upd = now(), 0, 0
        with self.conn:
            for r in rows:
                cur = self.conn.execute("SELECT COUNT(*) FROM analytics_orders WHERE order_id=?", (r["order_id"],)).fetchone()[0]
                self.conn.execute("DELETE FROM analytics_orders WHERE order_id=?", (r["order_id"],))
                self.conn.execute("INSERT INTO analytics_orders VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                                  [r["order_id"], r["customer_id"], r["order_date"], r["status"], r["amount"], r["currency"],
                                   r["updated_at"], r["order_date"][:7], batch_key, job_id, ts])
                upd += 1 if cur else 0
                ins += 0 if cur else 1
        return {"inserted": ins, "updated": upd}

    def rows(self, partition: str | None = None) -> list[dict]:
        q = "SELECT order_id,customer_id,order_date,status,amount,currency,updated_at,order_month FROM analytics_orders"
        args: list = []
        if partition:
            q += " WHERE order_month=?"
            args.append(partition)
        return [dict(zip(COLUMNS + ["order_month"], r)) for r in self.conn.execute(q + " ORDER BY order_id", args)]

    def partitions(self) -> list[str]:
        return [r[0] for r in self.conn.execute("SELECT DISTINCT order_month FROM analytics_orders ORDER BY 1")]


# --------------------------------------------------------------------------- typing / quarantine
def type_row(r: dict) -> tuple[dict | None, str | None]:
    try:
        oid = int(r["order_id"])
        cid = int(r["customer_id"]) if r.get("customer_id") not in (None, "", "None") else None
        d = date.fromisoformat(r["order_date"]).isoformat()
        amt = Decimal(r["amount"]).quantize(Decimal("0.01"))
        if r["status"] not in STATUSES:
            return None, f"status '{r['status']}' not in domain"
        return {"order_id": oid, "customer_id": cid, "order_date": d, "status": r["status"], "amount": str(amt),
                "currency": r["currency"], "updated_at": r["updated_at"]}, None
    except (ValueError, InvalidOperation, TypeError) as e:
        return None, f"{type(e).__name__}: {e}"


# --------------------------------------------------------------------------- engine
class DataMigrationEngine:
    def __init__(self, store: Store, project_id: str, workdir: Path, actor: str = "migration-engine"):
        self.store, self.project_id, self.actor = store, project_id, actor
        self.workdir = workdir
        self.landing = workdir / "gcs_landing"
        self.landing.mkdir(parents=True, exist_ok=True)
        self.source = AwsSourceSim(workdir / "aws_source_sim.db")
        self.target = BigQuerySim(workdir / "bigquery_sim.db")
        self.fail_once: set[str] = set()  # batch keys whose FIRST transfer is corrupted (demo of failure recovery)

    # ---------------------------------------------------------- plan
    def create_plan(self, plan: dict) -> dict:
        # columns of the source at planning time, so later schema drift can be detected (Round 7)
        plan = {**plan, "source_columns_at_plan": [r[1] for r in self.source.conn.execute("PRAGMA table_info(analytics_orders)")
                                                   if r[1] != "order_month"]}
        row = self.store.insert("migration_plans", {
            "id": new_id("PLAN-"), "project_id": self.project_id, "source_object": plan["source_object"],
            "target_object": plan["target_object"], "mode": plan["mode"], "range_start": plan["range_start"],
            "range_end": plan["range_end"], "config": plan, "approval_status": "PENDING", "created_at": now()})
        self.store.audit(self.project_id, self.actor, "MIGRATION_PLAN_CREATED", "migration_plan", row["id"],
                         {"batches": len(plan["batches"]), "decision": plan["decision"]})
        return row

    def _require_approved(self, plan_id: str) -> dict:
        plan = self.store.get("migration_plans", plan_id)
        if not plan or plan["approval_status"] != "APPROVED":
            raise PermissionError(f"plan {plan_id} is not APPROVED – historical load blocked (human-in-the-loop gate)")
        return plan

    # ---------------------------------------------------------- stages
    def _extract(self, run_id: str, batch_key: str, partition: str, attempt: int) -> dict:
        rows = self.source.rows(partition)
        d = self.landing / run_id / batch_key / f"attempt-{attempt}"
        d.mkdir(parents=True, exist_ok=True)
        data = d / "part-000.csv"
        with data.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS)
            w.writeheader()
            w.writerows([{c: r[c] for c in COLUMNS} for r in rows])
        manifest = {"run_id": run_id, "batch_key": batch_key, "partition": partition, "source": "aws:redshift/analytics.orders",
                    "source_query": f"SELECT * FROM analytics.orders WHERE order_month='{partition}'",
                    "rows": len(rows), "sha256": _sha(data), "bytes": data.stat().st_size, "extracted_at": now(),
                    "format": "csv", "compression": "none (prototype)", "encryption": "TLS in transit / CMEK at rest (production)"}
        (d / "_manifest.json").write_text(json.dumps(manifest, indent=2))
        return manifest

    def _transfer(self, manifest: dict, batch_key: str, attempt: int) -> Path:
        src = self.landing / manifest["run_id"] / batch_key / f"attempt-{attempt}" / "part-000.csv"
        dst_dir = self.landing / "_transferred" / manifest["run_id"] / batch_key / f"attempt-{attempt}"
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "part-000.csv"
        shutil.copy2(src, dst)
        if batch_key in self.fail_once and attempt == 1:
            with dst.open("ab") as f:  # simulate corrupted/partial object on the wire
                f.write(b"\x00TRUNCATED")
        got = _sha(dst)
        if got != manifest["sha256"]:
            raise TransferIntegrityError(f"checksum mismatch after transfer: expected {manifest['sha256'][:12]} got {got[:12]}")
        return dst

    def _load(self, path: Path, partition: str, batch_key: str, job_id: str, run_id: str) -> dict:
        good, bad = [], []
        with path.open(encoding="utf-8") as f:
            for i, r in enumerate(csv.DictReader(f)):
                t, err = type_row(r)
                (good if t else bad).append(t if t else {**r, "_reject_reason": err, "_line": i + 2})
        if bad:
            q = self.landing / "_quarantine" / run_id
            q.mkdir(parents=True, exist_ok=True)
            with (q / f"{batch_key}_rejected.csv").open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(bad[0].keys()))
                w.writeheader()
                w.writerows(bad)
        written = self.target.replace_partition(partition, good, batch_key, job_id)
        return {"read": len(good) + len(bad), "written": written, "rejected": len(bad),
                "rejected_ref": f"gcs_landing/_quarantine/{run_id}/{batch_key}_rejected.csv" if bad else None}

    def _process_batch(self, run_id: str, b: dict) -> dict:
        attempt = (b.get("attempt") or 0) + 1
        cp = b.get("checkpoint") or {}
        upd: dict[str, Any] = {"attempt": attempt, "updated_at": now()}
        self.store.update("migration_batches", b["id"], started_at=datetime.now(timezone.utc).isoformat(timespec="milliseconds"))
        try:
            if cp.get("stage") in (None, "PLANNED"):
                m = self._extract(run_id, b["batch_key"], b["partition"], attempt)
                cp = {"stage": "EXTRACTED", "manifest": m, "attempt_dir": f"attempt-{attempt}"}
                upd.update(extracted=m["rows"], source_count=m["rows"], checksum=m["sha256"], checkpoint=cp, status="EXTRACTED")
                self.store.update("migration_batches", b["id"], **upd)
            m = cp["manifest"]
            m_attempt = int(cp["attempt_dir"].split("-")[1])
            if cp["stage"] == "EXTRACTED":
                # resume from checkpoint: re-transfer the SAME immutable extracted object
                if m_attempt != attempt:
                    src = self.landing / run_id / b["batch_key"] / cp["attempt_dir"]
                    dst = self.landing / run_id / b["batch_key"] / f"attempt-{attempt}"
                    if not dst.exists():
                        shutil.copytree(src, dst)
                path = self._transfer(m, b["batch_key"], attempt)
                cp = {**cp, "stage": "TRANSFERRED", "landing_uri": str(path.relative_to(self.workdir))}
                upd.update(checkpoint=cp, status="TRANSFERRED")
                self.store.update("migration_batches", b["id"], **upd)
            if cp["stage"] == "TRANSFERRED":
                job_id = f"bqjob_{run_id}_{b['batch_key']}_{attempt}"
                res = self._load(self.workdir / cp["landing_uri"], b["partition"], b["batch_key"], job_id, run_id)
                ok = res["written"] + res["rejected"] == m["rows"]
                cp = {**cp, "stage": "LOADED" if ok else "LOAD_MISMATCH", "load_job_id": job_id, **res}
                upd.update(checkpoint=cp, loaded=res["written"], rejected=res["rejected"],
                           status="LOADED" if ok else "FAILED", error=None if ok else "rows read != written + rejected")
                self.store.update("migration_batches", b["id"], **upd)
        except TransferIntegrityError as e:
            upd.update(status="FAILED", error=str(e), checkpoint=cp)
            self.store.update("migration_batches", b["id"], **upd)
            self.store.insert("validation_exceptions", {"id": new_id("EXC-"), "result_id": b["id"], "validation_run_id": run_id,
                                                        "severity": "HIGH", "details": {"batch": b["batch_key"], "error": str(e),
                                                                                        "checkpoint": cp.get("stage")},
                                                        "owner": "Data Platform Lead", "remediation_status": "OPEN"})
            self.store.audit(self.project_id, self.actor, "BATCH_FAILED", "migration_batch", b["id"], {"error": str(e), "checkpoint": cp.get("stage")})
        self.store.update("migration_batches", b["id"], ended_at=datetime.now(timezone.utc).isoformat(timespec="milliseconds"))
        return self.store.get("migration_batches", b["id"])

    # ---------------------------------------------------------- run types
    def start_run(self, plan_id: str, run_type: str = "FULL_HISTORICAL_BACKFILL") -> dict:
        plan = self._require_approved(plan_id)
        cfg = plan["config"]
        run_id = new_id("RUN-")
        baseline_wm = self.source.max_watermark()
        self.store.insert("migration_runs", {"id": run_id, "plan_id": plan_id, "run_type": run_type, "run_status": "RUNNING",
                                             "started_at": now(), "watermark": baseline_wm})
        self.store.audit(self.project_id, self.actor, "MIGRATION_RUN_STARTED", "migration_run", run_id,
                         {"plan": plan_id, "type": run_type, "watermark": baseline_wm})
        batches = []
        for bk in cfg["batches"]:
            batches.append(self.store.insert("migration_batches", {
                "id": new_id("BAT-"), "run_id": run_id, "batch_key": bk["batch_key"], "partition": bk["partition"],
                "status": "PLANNED", "attempt": 0, "checkpoint": {"stage": "PLANNED"}, "updated_at": now()}))
        # concurrency limit protects source/target; set on the plan (Load runs screen), capped for the simulated stores
        limit = max(1, min(int(cfg.get("max_parallel_batches") or 4), 8))
        self.store.update("migration_runs", run_id, max_parallel=limit)
        with ThreadPoolExecutor(max_workers=limit) as ex:
            list(ex.map(lambda b: self._process_batch(run_id, b), batches))
        return self._finish_run(run_id)

    def retry_failed(self, run_id: str) -> dict:
        failed = self.store.query("SELECT * FROM migration_batches WHERE run_id=? AND status='FAILED'", (run_id,))
        plan = self.store.get("migration_plans", self.store.get("migration_runs", run_id)["plan_id"])
        max_attempts = plan["config"]["retry_policy"]["max_attempts"]
        self.store.update("migration_runs", run_id, run_status="RETRYING")
        for b in failed:
            if (b["attempt"] or 0) >= max_attempts:
                continue
            self.store.audit(self.project_id, "operator", "BATCH_RETRY", "migration_batch", b["id"],
                             {"batch": b["batch_key"], "resume_from": (b["checkpoint"] or {}).get("stage")})
            self._process_batch(run_id, b)
        for ex in self.store.query("SELECT * FROM validation_exceptions WHERE validation_run_id=? AND remediation_status='OPEN'", (run_id,)):
            bat = self.store.get("migration_batches", ex["result_id"])
            if bat and bat["status"] == "LOADED":
                self.store.update("validation_exceptions", ex["id"], remediation_status="RESOLVED_BY_RETRY")
        return self._finish_run(run_id)

    def selective_reload(self, plan_id: str, partitions: list[str], reason: str) -> dict:
        """Re-run only affected partitions (blueprint 6.2 'Selective reload'); REPLACE_PARTITION => no duplicates."""
        self._require_approved(plan_id)
        run_id = new_id("RUN-")
        self.store.insert("migration_runs", {"id": run_id, "plan_id": plan_id, "run_type": "SELECTIVE_RELOAD", "run_status": "RUNNING",
                                             "started_at": now(), "watermark": self.source.max_watermark(),
                                             "error_summary": reason})
        self.store.audit(self.project_id, self.actor, "SELECTIVE_RELOAD_STARTED", "migration_run", run_id, {"partitions": partitions, "reason": reason})
        for i, p in enumerate(partitions):
            b = self.store.insert("migration_batches", {"id": new_id("BAT-"), "run_id": run_id, "batch_key": f"R{i + 1:03d}", "partition": p,
                                                        "status": "PLANNED", "attempt": 0, "checkpoint": {"stage": "PLANNED"}, "updated_at": now()})
            self._process_batch(run_id, b)
        return self._finish_run(run_id)

    def incremental_catch_up(self, plan_id: str, since_watermark: str) -> dict:
        self._require_approved(plan_id)
        run_id = new_id("RUN-")
        rows = self.source.rows(updated_after=since_watermark)
        new_wm = max([r["updated_at"] for r in rows], default=since_watermark)
        self.store.insert("migration_runs", {"id": run_id, "plan_id": plan_id, "run_type": "INCREMENTAL_CATCH_UP", "run_status": "RUNNING",
                                             "started_at": now(), "watermark": new_wm})
        b = self.store.insert("migration_batches", {"id": new_id("BAT-"), "run_id": run_id, "batch_key": "INC001",
                                                    "partition": f"updated_at > {since_watermark}", "status": "EXTRACTED", "attempt": 1,
                                                    "source_count": len(rows), "extracted": len(rows), "updated_at": now()})
        good, bad = [], []
        for r in rows:
            t, err = type_row(r)
            (good if t else bad).append(t or r)
        res = self.target.merge(good, "INC001", f"bqjob_{run_id}_INC001")
        self.store.update("migration_batches", b["id"], status="LOADED", loaded=len(good), rejected=len(bad),
                          checkpoint={"stage": "LOADED", "mode": "MERGE ON order_id", "from_watermark": since_watermark,
                                      "to_watermark": new_wm, **res}, updated_at=now())
        self.store.audit(self.project_id, self.actor, "INCREMENTAL_APPLIED", "migration_run", run_id, {"from": since_watermark, "to": new_wm, **res})
        return self._finish_run(run_id)

    def apply_cdc(self, plan_id: str, events: list[dict]) -> dict:
        """Apply ordered change events exactly once using an LSN high-water mark (replay-safe)."""
        self._require_approved(plan_id)
        run_id = new_id("RUN-")
        cur = self.target.conn.execute("SELECT applied_lsn FROM _cdc_state WHERE object='analytics_orders'").fetchone()
        hwm = cur[0] if cur else 0
        todo = [e for e in sorted(events, key=lambda x: x["lsn"]) if e["lsn"] > hwm]
        stats = {"I": 0, "U": 0, "D": 0, "skipped_already_applied": len(events) - len(todo)}
        with self.target.conn:
            for e in todo:
                a = e["after"]
                if e["op"] == "D":
                    self.target.conn.execute("DELETE FROM analytics_orders WHERE order_id=?", (int(a["order_id"]),))
                elif e["op"] == "I":
                    t, _ = type_row({k: ("" if v is None else str(v)) for k, v in a.items()})
                    self.target.conn.execute("DELETE FROM analytics_orders WHERE order_id=?", (t["order_id"],))
                    self.target.conn.execute("INSERT INTO analytics_orders VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                                             [t["order_id"], t["customer_id"], t["order_date"], t["status"], t["amount"], t["currency"],
                                              t["updated_at"], t["order_date"][:7], "CDC001", f"cdc_{run_id}", now()])
                else:
                    sets = {c: (str(Decimal(str(a[c])).quantize(Decimal('0.01'))) if c == "amount" else a[c])
                            for c in COLUMNS if c != "order_id" and a.get(c) is not None}
                    self.target.conn.execute(f"UPDATE analytics_orders SET {','.join(f'{k}=?' for k in sets)} WHERE order_id=?",
                                             [*sets.values(), int(a["order_id"])])
                stats[e["op"]] += 1
            last = todo[-1]["lsn"] if todo else hwm
            self.target.conn.execute("INSERT OR REPLACE INTO _cdc_state VALUES('analytics_orders', ?)", (last,))
        last_ts = max([e["commit_ts"] for e in events], default=None)
        self.store.insert("migration_runs", {"id": run_id, "plan_id": plan_id, "run_type": "CDC_APPLY", "run_status": "COMPLETED",
                                             "started_at": now(), "ended_at": now(), "watermark": f"lsn={last}; commit_ts={last_ts}",
                                             "totals": stats})
        self.store.insert("migration_batches", {"id": new_id("BAT-"), "run_id": run_id, "batch_key": "CDC001",
                                                "partition": f"lsn {hwm + 1}..{last}", "status": "LOADED", "attempt": 1,
                                                "source_count": len(todo), "loaded": len(todo), "rejected": 0,
                                                "checkpoint": {"stage": "LOADED", "applied_lsn": last, **stats}, "updated_at": now()})
        self.store.audit(self.project_id, self.actor, "CDC_APPLIED", "migration_run", run_id, stats)
        return self.store.get("migration_runs", run_id)

    def _finish_run(self, run_id: str) -> dict:
        bs = self.store.all("migration_batches", run_id=run_id)
        totals = {"batches": len(bs), "loaded": sum(1 for b in bs if b["status"] == "LOADED"),
                  "failed": sum(1 for b in bs if b["status"] == "FAILED"),
                  "rows_extracted": sum(b["extracted"] or 0 for b in bs), "rows_loaded": sum(b["loaded"] or 0 for b in bs),
                  "rows_rejected": sum(b["rejected"] or 0 for b in bs)}
        status = "COMPLETED" if not totals["failed"] else "COMPLETED_WITH_FAILURES"
        self.store.update("migration_runs", run_id, run_status=status, ended_at=now(), totals=totals,
                          error_summary=None if not totals["failed"] else f"{totals['failed']} batch(es) failed – retry eligible")
        self.store.audit(self.project_id, self.actor, "MIGRATION_RUN_FINISHED", "migration_run", run_id, {"status": status, **totals})
        return self.store.get("migration_runs", run_id)
