"""Validation objects and exception tracking for a project's own data (project document, phase 7 / Round 5).

An "object" is a named thing a person wants reconciled: two small CSV files (source and target, or the
target's staging copy), a key column to match rows on, and an optional per-column numeric tolerance.
Running it reuses the same exact key-based reconciliation the sample project's post-migration validation
uses (`ValidationEngine.reconcile_exact`) — row count, coverage, per-column aggregate controls and value
differences — so a custom project is not limited to the pre-conversion availability check: it can produce
the same kind of evidence the readiness gate (MANDATORY_RECONCILIATION) actually asks for.

Re-running does not invent a new object: it reuses the stored file paths (or a newly chosen target) and
the current tolerances, and records a fresh validation_runs row. The previous run's results stay in history.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .store import Store, new_id, now
from .validation import ValidationEngine

MAX_ROWS = 200_000  # exact key-based comparison; the same prototype limit style as availability's CSV check


class ValidationObjects:
    TABLE = "validation_objects"

    def __init__(self, store: Store, project_id: str, actor: str = "console.user"):
        self.s, self.pid, self.actor = store, project_id, actor

    # ------------------------------------------------------------------ helpers
    def _upload_path(self, upload_id: str | None) -> tuple[str | None, str | None]:
        if not upload_id:
            return None, None
        u = self.s.get("uploads", upload_id)
        if not u or u.get("project_id") != self.pid or u.get("status") not in ("ACCEPTED", "DUPLICATE"):
            raise ValueError(f"Upload {upload_id} is not a usable file in this project.")
        if not str(u["filename"]).lower().endswith((".csv", ".tsv", ".txt")):
            raise ValueError(f"{u['filename']} is not a CSV file. The prototype compares CSV files.")
        return u["storage_path"], u["filename"]

    def _row(self, oid: str) -> dict:
        r = self.s.get(self.TABLE, oid)
        if not r or r["project_id"] != self.pid:
            raise KeyError(oid)
        return r

    @staticmethod
    def _parse_tolerances(raw: dict[str, Any] | None) -> dict[str, str]:
        out: dict[str, str] = {}
        for col, v in (raw or {}).items():
            col = (col or "").strip()
            if not col:
                continue
            try:
                out[col] = str(Decimal(str(v)))
            except (InvalidOperation, ValueError) as e:
                raise ValueError(f"Tolerance for '{col}' must be a number.") from e
        return out

    # ------------------------------------------------------------------ views
    def view(self, d: dict) -> dict:
        run = self.s.get("validation_runs", d["last_run_id"]) if d.get("last_run_id") else None
        exceptions = self.s.all("validation_exceptions", validation_run_id=d["last_run_id"]) if d.get("last_run_id") else []
        out = {k: d[k] for k in ("id", "name", "application", "key_column", "pii_columns", "tolerances",
                                 "source_label", "target_label", "seeded", "created_at", "updated_at")}
        out.update(last_run_id=d.get("last_run_id"), last_status=d.get("last_status") or "NOT_RUN",
                   summary=(run or {}).get("summary"),
                   open_exceptions=sum(1 for e in exceptions if e["remediation_status"] not in ("WAIVED", "RESOLVED", "RESOLVED_BY_RETRY")))
        return out

    def list(self) -> list[dict]:
        return [self.view(d) for d in sorted(self.s.all(self.TABLE, project_id=self.pid), key=lambda x: x["created_at"])]

    # ------------------------------------------------------------------ actions
    def add(self, *, name: str, key_column: str, application: str = "", source_upload: str | None = None,
            target_upload: str | None = None, source_path: str | None = None, target_path: str | None = None,
            source_label: str = "", target_label: str = "", pii_columns: list[str] | None = None,
            tolerances: dict[str, Any] | None = None, seeded: str = "") -> dict:
        name, key_column = (name or "").strip(), (key_column or "").strip()
        if len(name) < 2:
            raise ValueError("Give the object a name.")
        if len(key_column) < 1:
            raise ValueError("Name the column both files use to match rows (for example order_id).")
        tol = self._parse_tolerances(tolerances)
        sp, sl = (source_path, source_label) if source_path else self._upload_path(source_upload)
        if not sp:
            raise ValueError("Choose the source file.")
        tp, tl = (target_path, target_label) if target_path else self._upload_path(target_upload)
        if not tp:
            raise ValueError("Choose the target file.")
        row = self.s.insert(self.TABLE, {
            "id": new_id("DQO-"), "project_id": self.pid, "application": application, "name": name, "key_column": key_column,
            "pii_columns": sorted(set(pii_columns or [])), "tolerances": tol, "source_path": sp, "target_path": tp,
            "source_label": sl or source_label, "target_label": tl or target_label, "last_run_id": None, "last_status": "NOT_RUN",
            "seeded": seeded, "created_at": now(), "updated_at": now()})
        self.s.audit(self.pid, self.actor, "VALIDATION_OBJECT_ADDED", "validation_object", row["id"],
                     {"name": name, "application": application, "source": row["source_label"], "target": row["target_label"]})
        return self.run(row["id"])

    def set_target(self, oid: str, target_upload: str) -> dict:
        d = self._row(oid)
        tp, tl = self._upload_path(target_upload)
        self.s.update(self.TABLE, oid, target_path=tp, target_label=tl or "", updated_at=now())
        self.s.audit(self.pid, self.actor, "VALIDATION_OBJECT_TARGET_CHANGED", "validation_object", oid, {"target": tl})
        return self.view(self._row(oid))

    def set_tolerances(self, oid: str, tolerances: dict[str, Any]) -> dict:
        d = self._row(oid)
        tol = self._parse_tolerances(tolerances)
        self.s.update(self.TABLE, oid, tolerances=tol, updated_at=now())
        self.s.audit(self.pid, self.actor, "VALIDATION_TOLERANCE_SET", "validation_object", oid, {"tolerances": tol})
        return self.view(self.s.get(self.TABLE, oid))

    def run(self, oid: str) -> dict:
        d = self._row(oid)
        sp, tp = Path(d["source_path"]), Path(d["target_path"])
        if not sp.exists() or not tp.exists():
            raise ValueError("Source or target file is no longer on disk. Re-add the object with current files.")
        for p in (sp, tp):
            n = sum(1 for _ in p.open(encoding="utf-8-sig", errors="replace")) - 1
            if n > MAX_ROWS:
                raise ValueError(f"{p.name} has more than {MAX_ROWS:,} rows; this check compares small reference objects row by row.")
        eng = ValidationEngine(self.s, self.pid, actor=self.actor)
        tol = {k: Decimal(v) for k, v in (d.get("tolerances") or {}).items()}
        run = eng.reconcile_exact(d["application"] and f"{d['application']}.{d['name']}" or d["name"], d["key_column"], sp, tp,
                                  pii=set(d.get("pii_columns") or []), tolerances=tol, run_type="MANUAL_RECONCILIATION")
        self.s.update(self.TABLE, oid, last_run_id=run["id"], last_status=run["status"], updated_at=now())
        return self.view(self.s.get(self.TABLE, oid))

    def summary(self) -> dict:
        objs = self.list()
        from collections import Counter
        c = Counter(o["last_status"] for o in objs)
        return {"total": len(objs), **{k: c.get(k, 0) for k in ("PASS", "FAIL", "REVIEW", "NOT_RUN")},
                "open_exceptions": sum(o["open_exceptions"] for o in objs)}

    def by_app(self) -> dict[str, list[dict]]:
        """The latest run of every object, grouped by application — for feeding the risk/readiness gates."""
        out: dict[str, list[dict]] = {}
        for d in self.s.all(self.TABLE, project_id=self.pid):
            if not d.get("last_run_id"):
                continue
            run = self.s.get("validation_runs", d["last_run_id"])
            if run:
                out.setdefault(d["application"] or "Unassigned", []).append(run)
        return out


# ---------------------------------------------------------------- exceptions queue
REMEDIATION_STATUSES = ("OPEN", "IN_PROGRESS", "RESOLVED")


def all_exceptions(store: Store, project_id: str) -> list[dict]:
    """Every validation exception across every run in this project, newest first, with the owning run's
    object/application attached so they can be triaged in one place instead of run by run."""
    rows = store.query(
        "SELECT e.*, r.object, r.run_type, r.project_id AS run_project_id FROM validation_exceptions e "
        "JOIN validation_runs r ON r.id = e.validation_run_id WHERE r.project_id=? ORDER BY e.id DESC", (project_id,))
    for e in rows:
        res = store.get("validation_results", e["result_id"]) or {}
        e["partition"], e["dimension"], e["rule"] = res.get("partition"), res.get("dimension"), res.get("rule")
    return rows


def update_exception(store: Store, project_id: str, exc_id: str, remediation_status: str, owner: str, note: str, actor: str) -> dict:
    if remediation_status not in REMEDIATION_STATUSES:
        raise ValueError(f"Status must be one of {', '.join(REMEDIATION_STATUSES)}.")
    e = store.get("validation_exceptions", exc_id)
    if not e:
        raise ValueError("Exception not found.")
    run = store.get("validation_runs", e["validation_run_id"])
    if not run or run["project_id"] != project_id:
        raise ValueError("Exception not found in this project.")
    if e["remediation_status"] in ("WAIVED", "RESOLVED_BY_RETRY"):
        raise ValueError(f"This exception is already {e['remediation_status'].replace('_', ' ').lower()}; "
                         "it was closed automatically, not by tracking status.")
    store.update("validation_exceptions", exc_id, remediation_status=remediation_status, owner=(owner or "").strip(),
                note=(note or "").strip(), updated_by=actor, updated_at=now())
    store.audit(project_id, actor, "EXCEPTION_TRACKING_UPDATED", "validation_exception", exc_id,
               {"status": remediation_status, "owner": owner})
    return store.get("validation_exceptions", exc_id)


# ---------------------------------------------------------------- post-cutover monitoring note
def get_note(store: Store, project_id: str, application: str) -> dict:
    row = next((n for n in store.all("monitoring_notes", project_id=project_id, application=application)), None)
    return row or {"project_id": project_id, "application": application, "note": "", "updated_by": None, "updated_at": None}


def set_note(store: Store, project_id: str, application: str, note: str, actor: str) -> dict:
    existing = next((n for n in store.all("monitoring_notes", project_id=project_id, application=application)), None)
    row = store.insert("monitoring_notes", {"id": existing["id"] if existing else new_id("MON-"), "project_id": project_id,
                                            "application": application, "note": (note or "").strip(), "updated_by": actor, "updated_at": now()})
    store.audit(project_id, actor, "MONITORING_NOTE_SAVED", "monitoring_note", row["id"], {"application": application})
    return row
