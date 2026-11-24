"""Metadata store + immutable audit trail.

Prototype uses SQLite (zero-install). The schema mirrors blueprint section 10 so it can be moved to
PostgreSQL (JSON columns -> JSONB) without changing service code: every service talks to `Store`.
Audit events are hash-chained (each event stores SHA-256 of previous event) so tampering is detectable.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects(id TEXT PRIMARY KEY, customer TEXT, name TEXT, source_cloud TEXT,
  target_cloud TEXT, aws_account TEXT, aws_region TEXT, gcp_project TEXT, gcp_region TEXT, status TEXT,
  owner TEXT, config TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS uploads(id TEXT PRIMARY KEY, project_id TEXT, filename TEXT, category TEXT,
  file_type TEXT, sha256 TEXT, size_bytes INTEGER, storage_path TEXT, status TEXT, error TEXT, uploaded_by TEXT,
  created_at TEXT);
CREATE TABLE IF NOT EXISTS extracted_evidence(id TEXT PRIMARY KEY, project_id TEXT, upload_id TEXT,
  entity_type TEXT, extracted_data TEXT, confidence REAL, source_location TEXT, extractor_version TEXT);
CREATE TABLE IF NOT EXISTS resources(id TEXT PRIMARY KEY, project_id TEXT, resource_id TEXT, name TEXT,
  service TEXT, category TEXT, region TEXT, application TEXT, environment TEXT, criticality TEXT,
  configuration TEXT, evidence TEXT, conflicts TEXT, confidence REAL, evidence_status TEXT);
CREATE TABLE IF NOT EXISTS resource_relationships(id TEXT PRIMARY KEY, project_id TEXT, source TEXT,
  target TEXT, relationship_type TEXT, confidence REAL, status TEXT, evidence TEXT);
CREATE TABLE IF NOT EXISTS service_mappings(id TEXT PRIMARY KEY, project_id TEXT, resource_id TEXT,
  aws_service TEXT, target_service TEXT, alternatives TEXT, strategy TEXT, score REAL, rationale TEXT,
  constraints TEXT, effort TEXT, catalogue_version TEXT, approval_status TEXT);
CREATE TABLE IF NOT EXISTS conversion_runs(id TEXT PRIMARY KEY, project_id TEXT, source_file TEXT,
  converter TEXT, converter_version TEXT, output_path TEXT, confidence REAL, warnings TEXT, unsupported TEXT,
  checks TEXT, status TEXT, approval_status TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS migration_plans(id TEXT PRIMARY KEY, project_id TEXT, source_object TEXT,
  target_object TEXT, mode TEXT, range_start TEXT, range_end TEXT, config TEXT, approval_status TEXT,
  created_at TEXT);
CREATE TABLE IF NOT EXISTS migration_runs(id TEXT PRIMARY KEY, plan_id TEXT, run_type TEXT, run_status TEXT,
  started_at TEXT, ended_at TEXT, watermark TEXT, totals TEXT, error_summary TEXT);
CREATE TABLE IF NOT EXISTS migration_batches(id TEXT PRIMARY KEY, run_id TEXT, batch_key TEXT, partition TEXT,
  status TEXT, attempt INTEGER, source_count INTEGER, extracted INTEGER, loaded INTEGER, rejected INTEGER,
  checksum TEXT, checkpoint TEXT, error TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS validation_runs(id TEXT PRIMARY KEY, project_id TEXT, run_type TEXT, object TEXT,
  source_snapshot TEXT, target_snapshot TEXT, status TEXT, summary TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS validation_results(id TEXT PRIMARY KEY, validation_run_id TEXT, object TEXT,
  partition TEXT, dimension TEXT, rule TEXT, source_value TEXT, target_value TEXT, tolerance TEXT, status TEXT,
  severity TEXT, details TEXT);
CREATE TABLE IF NOT EXISTS validation_exceptions(id TEXT PRIMARY KEY, result_id TEXT, validation_run_id TEXT,
  severity TEXT, details TEXT, owner TEXT, remediation_status TEXT);
CREATE TABLE IF NOT EXISTS risk_items(id TEXT PRIMARY KEY, project_id TEXT, application TEXT, dimension TEXT,
  score REAL, rationale TEXT, mitigation TEXT, owner TEXT, status TEXT, follow_up_note TEXT, follow_up_at TEXT, follow_up_by TEXT);
CREATE TABLE IF NOT EXISTS risk_followups(id TEXT PRIMARY KEY, project_id TEXT, application TEXT, dimension TEXT,
  tracking_status TEXT, owner TEXT, note TEXT, updated_by TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS stakeholder_overrides(id TEXT PRIMARY KEY, project_id TEXT, application TEXT, name TEXT,
  email TEXT, type TEXT, updated_by TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS signoffs(id TEXT PRIMARY KEY, project_id TEXT, application TEXT,
  business_status TEXT, business_approver TEXT, business_rationale TEXT, business_at TEXT,
  technical_status TEXT, technical_approver TEXT, technical_rationale TEXT, technical_at TEXT,
  gates_hash TEXT, status TEXT, created_at TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS stakeholders(id TEXT PRIMARY KEY, project_id TEXT, application TEXT, name TEXT,
  email TEXT, type TEXT, owner_source TEXT, verified INTEGER);
CREATE TABLE IF NOT EXISTS notifications(id TEXT PRIMARY KEY, project_id TEXT, stakeholder_id TEXT, event TEXT,
  recipient TEXT, subject TEXT, body TEXT, status TEXT, approval TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS approval_requests(id TEXT PRIMARY KEY, project_id TEXT, object_type TEXT,
  object_id TEXT, requested_by TEXT, approver TEXT, decision TEXT, rationale TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS agent_runs(id TEXT PRIMARY KEY, project_id TEXT, agent_name TEXT, input_ref TEXT,
  output_ref TEXT, model_version TEXT, status TEXT, started_at TEXT, ended_at TEXT);
CREATE TABLE IF NOT EXISTS audit_logs(seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT, project_id TEXT,
  actor TEXT, action TEXT, entity_type TEXT, entity_id TEXT, details TEXT, ts TEXT, prev_hash TEXT, hash TEXT);
CREATE TABLE IF NOT EXISTS availability_datasets(id TEXT PRIMARY KEY, project_id TEXT, name TEXT, application TEXT,
  source_access TEXT, date_column TEXT, key_column TEXT, range_start TEXT, range_end TEXT, source_path TEXT, target_path TEXT,
  source_label TEXT, target_label TEXT, owner_name TEXT, owner_email TEXT, result TEXT, confirmation TEXT, decision_rec TEXT,
  seeded TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS validation_objects(id TEXT PRIMARY KEY, project_id TEXT, application TEXT, name TEXT,
  key_column TEXT, pii_columns TEXT, tolerances TEXT, source_path TEXT, target_path TEXT, source_label TEXT, target_label TEXT,
  last_run_id TEXT, last_status TEXT, seeded TEXT, created_at TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS monitoring_notes(id TEXT PRIMARY KEY, project_id TEXT, application TEXT, note TEXT,
  updated_by TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS app_migration_status(id TEXT PRIMARY KEY, project_id TEXT, application TEXT, status TEXT,
  note TEXT, updated_by TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS dependency_confirmations(id TEXT PRIMARY KEY, project_id TEXT, kind TEXT, source TEXT, target TEXT,
  outcome TEXT, by_name TEXT, note TEXT, recorded_at TEXT);
CREATE INDEX IF NOT EXISTS ix_res_proj ON resources(project_id);
CREATE INDEX IF NOT EXISTS ix_batches_run ON migration_batches(run_id);
CREATE INDEX IF NOT EXISTS ix_vr_run ON validation_results(validation_run_id);
CREATE INDEX IF NOT EXISTS ix_audit_proj ON audit_logs(project_id);
"""

JSON_COLS = {"config", "extracted_data", "configuration", "evidence", "conflicts", "alternatives", "constraints",
             "warnings", "unsupported", "checks", "totals", "summary", "details", "source_snapshot",
             "target_snapshot", "checkpoint", "result", "confirmation", "decision_rec", "pii_columns", "tolerances",
             "edited", "approval_rec", "tracking", "outputs", "tests", "schema_history"}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_id(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}" if prefix else str(uuid.uuid4())


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._migrate()

    def _migrate(self) -> None:
        """Add columns introduced after a table already existed on disk. CREATE TABLE IF NOT EXISTS above
        leaves an existing project's database untouched, so a new column needs an explicit ALTER TABLE here."""
        added = {"validation_exceptions": {"note": "TEXT", "updated_by": "TEXT", "updated_at": "TEXT"},
                 "notifications": {"about_app": "TEXT", "edited": "TEXT", "approval_rec": "TEXT", "tracking": "TEXT",
                                   "updated_at": "TEXT"},
                 "conversion_runs": {"upload_id": "TEXT", "version": "TEXT", "kind": "TEXT", "outputs": "TEXT", "outputs_hash": "TEXT",
                                     "tests": "TEXT", "parent_id": "TEXT", "edited_by": "TEXT", "note": "TEXT",
                                     "approved_by": "TEXT", "approval_note": "TEXT", "approved_at": "TEXT"},
                 "migration_batches": {"started_at": "TEXT", "ended_at": "TEXT", "bytes": "INTEGER"},
                 "migration_runs": {"max_parallel": "INTEGER"},
                 "availability_datasets": {"schema_history": "TEXT"}}
        with self._lock:
            for table, cols in added.items():
                existing = {r[1] for r in self.conn.execute(f"PRAGMA table_info({table})")}
                for name, decl in cols.items():
                    if name not in existing:
                        self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
            self.conn.commit()

    # ------------------------------------------------------------ generic helpers
    def insert(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        row = dict(row)
        row.setdefault("id", new_id())
        enc = {k: (json.dumps(v, default=str) if k in JSON_COLS and v is not None and not isinstance(v, str) else v)
               for k, v in row.items()}
        cols = ",".join(enc)
        q = ",".join("?" for _ in enc)
        with self._lock:
            self.conn.execute(f"INSERT OR REPLACE INTO {table}({cols}) VALUES({q})", list(enc.values()))
            self.conn.commit()
        return row

    def update(self, table: str, row_id: str, **fields: Any) -> None:
        enc = {k: (json.dumps(v, default=str) if k in JSON_COLS and v is not None and not isinstance(v, str) else v)
               for k, v in fields.items()}
        sets = ",".join(f"{k}=?" for k in enc)
        with self._lock:
            self.conn.execute(f"UPDATE {table} SET {sets} WHERE id=?", [*enc.values(), row_id])
            self.conn.commit()

    def query(self, sql: str, params: tuple | list = ()) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            for k in d:
                if k in JSON_COLS and isinstance(d[k], str):
                    try:
                        d[k] = json.loads(d[k])
                    except json.JSONDecodeError:
                        pass
            out.append(d)
        return out

    def all(self, table: str, **where: Any) -> list[dict[str, Any]]:
        if where:
            cond = " AND ".join(f"{k}=?" for k in where)
            return self.query(f"SELECT * FROM {table} WHERE {cond}", list(where.values()))
        return self.query(f"SELECT * FROM {table}")

    def get(self, table: str, row_id: str) -> dict[str, Any] | None:
        r = self.query(f"SELECT * FROM {table} WHERE id=?", (row_id,))
        return r[0] if r else None

    def delete_where(self, table: str, **where: Any) -> None:
        cond = " AND ".join(f"{k}=?" for k in where)
        with self._lock:
            self.conn.execute(f"DELETE FROM {table} WHERE {cond}", list(where.values()))
            self.conn.commit()

    # ------------------------------------------------------------ audit
    def audit(self, project_id: str, actor: str, action: str, entity_type: str, entity_id: str,
              details: dict | None = None) -> str:
        with self._lock:
            last = self.conn.execute("SELECT hash FROM audit_logs ORDER BY seq DESC LIMIT 1").fetchone()
            prev = last["hash"] if last else "GENESIS"
            ts = now()
            payload = json.dumps([project_id, actor, action, entity_type, entity_id, details or {}, ts, prev],
                                 sort_keys=True, default=str)
            h = hashlib.sha256(payload.encode()).hexdigest()
            eid = new_id("AUD-")
            self.conn.execute(
                "INSERT INTO audit_logs(id,project_id,actor,action,entity_type,entity_id,details,ts,prev_hash,hash)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (eid, project_id, actor, action, entity_type, entity_id, json.dumps(details or {}, default=str), ts,
                 prev, h))
            self.conn.commit()
        return eid

    def verify_audit_chain(self) -> dict[str, Any]:
        prev = "GENESIS"
        rows = self.query("SELECT * FROM audit_logs ORDER BY seq")
        for r in rows:
            payload = json.dumps([r["project_id"], r["actor"], r["action"], r["entity_type"], r["entity_id"],
                                  r["details"], r["ts"], prev], sort_keys=True, default=str)
            if r["prev_hash"] != prev or hashlib.sha256(payload.encode()).hexdigest() != r["hash"]:
                return {"valid": False, "broken_at_seq": r["seq"], "events": len(rows)}
            prev = r["hash"]
        return {"valid": True, "events": len(rows), "head": prev}

    # ------------------------------------------------------------ approvals (human-in-the-loop)
    def record_approval(self, project_id: str, object_type: str, object_id: str, approver: str, decision: str,
                        rationale: str, requested_by: str = "orchestrator") -> dict[str, Any]:
        row = self.insert("approval_requests", {
            "id": new_id("APR-"), "project_id": project_id, "object_type": object_type, "object_id": object_id,
            "requested_by": requested_by, "approver": approver, "decision": decision, "rationale": rationale,
            "created_at": now()})
        self.audit(project_id, approver, f"APPROVAL_{decision}", object_type, object_id, {"rationale": rationale})
        return row
