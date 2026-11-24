"""Pre-conversion data availability check (project document, phase 3).

Question answered: is the data this application needs already in the target, and does it cover the same dates?
The check compares two CSV files that a person supplied for each dataset: the source side (or its staging copy)
and the target side. It reports counts by month, missing days, missing columns and, when a key column is given,
missing keys. It does NOT prove records are identical; counts and keys are evidence, not proof.

Limited access: when the real source is offline or owned by a customer, only the staging copy can be checked.
The result then says so, an owner-confirmation e-mail is drafted (never sent), and the dataset stays unconfirmed
until a person records the owner's reply. A person may also decide to proceed despite a discrepancy; that decision
is stored with a reason and goes stale automatically if the data changes afterwards.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

from .store import Store, new_id, now

ACCESS = {"DIRECT": "Direct access to the source",
          "STAGING_ONLY": "Only a staging copy (SFTP folder, S3 bucket) can be seen",
          "NONE": "No access to the source or a copy"}
_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
MAX_ROWS = 2_000_000


def _read(path: str | None, date_col: str, key_col: str) -> dict[str, Any] | None:
    if not path or not Path(path).exists():
        return None
    days: Counter = Counter()
    keys: set[str] = set()
    n = parsed = 0
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
        rd = csv.DictReader(f)
        cols = list(rd.fieldnames or [])
        for row in rd:
            n += 1
            if n > MAX_ROWS:
                raise ValueError(f"{Path(path).name} has more than {MAX_ROWS:,} rows; the prototype check stops there.")
            if date_col and date_col in cols:
                m = _DATE.match((row.get(date_col) or "").strip())
                if m:
                    parsed += 1
                    days["-".join(m.groups())] += 1
            if key_col and key_col in cols:
                keys.add((row.get(key_col) or "").strip())
    return {"rows": n, "columns": cols, "days": days, "keys": keys, "parsed": parsed,
            "min_date": min(days) if days else None, "max_date": max(days) if days else None}


def analyse(source_path: str | None, target_path: str | None, date_col: str = "", key_col: str = "",
            start: str = "", end: str = "") -> dict[str, Any]:
    """Compare the two sides and return a plain, JSON-safe result."""
    src = _read(source_path, date_col, key_col)
    if src is None:
        return {"status": "CANNOT_CHECK", "summary": "The source file is missing, so nothing can be compared.", "checked_at": now(),
                "evidence": "None", "issues": ["Source file not found."], "months": [], "missing_days": [], "missing_day_count": 0}
    tgt = _read(target_path, date_col, key_col)
    use_dates = bool(date_col) and date_col in src["columns"] and src["parsed"] >= 0.9 * max(1, src["rows"])
    issues: list[str] = []
    notes: list[str] = []
    if date_col and not use_dates:
        notes.append(f"Date column '{date_col}' was not found or could not be read in at least 90% of source rows, so only total counts are compared.")
    inrange = lambda d: (not start or d >= start) and (not end or d <= end)          # noqa: E731
    s_days = {d: c for d, c in src["days"].items() if inrange(d)} if use_dates else {}
    res: dict[str, Any] = {"checked_at": now(), "range": {"start": start or src["min_date"], "end": end or src["max_date"]},
                           "date_column": date_col if use_dates else "", "key_column": key_col if key_col in src["columns"] else "",
                           "source": {"rows": src["rows"], "columns": len(src["columns"]), "min_date": src["min_date"], "max_date": src["max_date"]},
                           "notes": notes}
    if tgt is None:
        res.update(status="MISSING", summary="No target data was found for this dataset.", evidence="None", target=None,
                   issues=["Target file not supplied or not found."], months=[], missing_days=sorted(s_days)[:31] if use_dates else [],
                   missing_day_count=len(s_days), columns={"source": list(src["columns"]), "target": []})
        return res
    t_days = {d: c for d, c in tgt["days"].items() if inrange(d)} if use_dates else {}
    res["target"] = {"rows": tgt["rows"], "columns": len(tgt["columns"]), "min_date": tgt["min_date"], "max_date": tgt["max_date"]}
    miss_cols = [c for c in src["columns"] if c not in tgt["columns"]]
    extra_cols = [c for c in tgt["columns"] if c not in src["columns"]]
    res["schema"] = {"missing_in_target": miss_cols, "extra_in_target": extra_cols}
    res["columns"] = {"source": list(src["columns"]), "target": list(tgt["columns"])}
    if miss_cols:
        issues.append(f"{len(miss_cols)} source column(s) are missing in the target: {', '.join(miss_cols[:8])}.")
    months: list[dict] = []
    if use_dates:
        by = {}
        for d, c in s_days.items():
            by.setdefault(d[:7], [0, 0])[0] += c
        for d, c in t_days.items():
            by.setdefault(d[:7], [0, 0])[1] += c
        months = [{"month": m, "source": a, "target": b, "difference": b - a} for m, (a, b) in sorted(by.items())]
        s_total, t_total = sum(s_days.values()), sum(t_days.values())
        missing_days = sorted(d for d in s_days if d not in t_days)
        diff_days = sorted(d for d in s_days if d in t_days and s_days[d] != t_days[d])
        res["missing_days"], res["missing_day_count"] = missing_days[:31], len(missing_days)
        res["differing_day_count"] = len(diff_days)
        res["extra_day_count"] = sum(1 for d in t_days if d not in s_days)
        if missing_days:
            issues.append(f"{len(missing_days)} day(s) with source rows have no rows in the target (first: {missing_days[0]}).")
        if diff_days:
            issues.append(f"{len(diff_days)} day(s) have a different row count (first: {diff_days[0]}).")
    else:
        s_total, t_total = src["rows"], tgt["rows"]
        res["missing_days"], res["missing_day_count"] = [], 0
    res["months"] = months
    res["totals"] = {"source": s_total, "target": t_total, "difference": t_total - s_total}
    if t_total != s_total:
        issues.append(f"Row count differs by {t_total - s_total:+,} ({s_total:,} in the source, {t_total:,} in the target).")
    evidence = "Total counts" if not use_dates else "Counts by day and month"
    if res["key_column"] and key_col in tgt["columns"]:
        miss = src["keys"] - tgt["keys"]
        extra = tgt["keys"] - src["keys"]
        res["keys"] = {"source": len(src["keys"]), "target": len(tgt["keys"]), "missing_in_target": len(miss),
                       "extra_in_target": len(extra), "sample_missing": sorted(miss)[:10]}
        evidence = "Key-level, plus counts"
        if miss:
            issues.append(f"{len(miss):,} key(s) in the source are not in the target.")
    elif key_col:
        notes.append(f"Key column '{key_col}' was not found on both sides, so keys were not compared.")
    res["evidence"] = evidence
    res["issues"] = issues
    res["status"] = "DISCREPANCY" if issues else "AVAILABLE"
    res["summary"] = ("Target covers the same dates and counts as the source." if not issues else f"{len(issues)} difference(s) found.")
    return res


def result_hash(r: dict | None) -> str:
    keep = {k: r.get(k) for k in ("status", "totals", "months", "missing_day_count", "differing_day_count", "keys", "schema")} if r else {}
    return hashlib.sha256(json.dumps(keep, sort_keys=True, default=str).encode()).hexdigest()[:16]


def clearance(d: dict) -> tuple[str, str]:
    """(code, plain-English reason). The code is what gates and screens use."""
    r, conf, dec = d.get("result"), d.get("confirmation") or {}, d.get("decision_rec") or {}
    if not r:
        return "NOT_RUN", "The check has not been run."
    limited = d["source_access"] != "DIRECT"
    if dec.get("decision") == "HOLD":
        return "HOLD", f"Held by {dec.get('approver')}: {dec.get('rationale')}"
    if r["status"] in ("DISCREPANCY", "MISSING", "CANNOT_CHECK"):
        if dec.get("decision") == "PROCEED" and dec.get("result_hash") == result_hash(r):
            return "PROCEED", f"{dec.get('approver')} accepted proceeding despite the difference: {dec.get('rationale')}"
        why = "A person must decide whether to proceed."
        if dec.get("decision") == "PROCEED":
            why = "The data changed after the earlier decision, so it needs a new one."
        return "NEEDS_DECISION", why
    if limited and conf.get("status") != "CONFIRMED":
        return "NEEDS_CONFIRMATION", ("The source cannot be seen directly. The owner has been asked to confirm every expected file was uploaded."
                                      if conf.get("status") == "REQUESTED" else
                                      "The source cannot be seen directly. Ask the source owner to confirm every expected file was uploaded.")
    return "CLEAR", "Available in the target and, where the source is limited, confirmed by its owner."


class Availability:
    TABLE = "availability_datasets"

    def __init__(self, store: Store, project_id: str, vault: Path | None = None):
        self.s, self.pid, self.vault = store, project_id, vault

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

    def _row(self, did: str) -> dict:
        r = self.s.get(self.TABLE, did)
        if not r or r["project_id"] != self.pid:
            raise KeyError(did)
        return r

    def view(self, d: dict) -> dict:
        code, why = clearance(d)
        out = {k: d[k] for k in ("id", "name", "application", "source_access", "date_column", "key_column", "range_start", "range_end",
                                 "source_label", "target_label", "owner_name", "owner_email", "result", "confirmation", "decision_rec",
                                 "seeded", "created_at")}
        out["decision"] = out.pop("decision_rec")
        out.update(clearance=code, clearance_reason=why, access_text=ACCESS[d["source_access"]])
        return out

    def list(self) -> list[dict]:
        return [self.view(d) for d in sorted(self.s.all(self.TABLE, project_id=self.pid), key=lambda x: x["created_at"])]

    # ------------------------------------------------------------------ actions
    def add(self, *, name: str, source_upload: str | None = None, target_upload: str | None = None, source_path: str | None = None,
            target_path: str | None = None, source_label: str = "", target_label: str = "", date_column: str = "", key_column: str = "",
            source_access: str = "DIRECT", owner_name: str = "", owner_email: str = "", application: str = "",
            range_start: str = "", range_end: str = "", seeded: str = "", actor: str = "console.user") -> dict:
        name = (name or "").strip()
        if len(name) < 2:
            raise ValueError("Give the dataset a name.")
        if source_access not in ACCESS:
            raise ValueError("Source access must be DIRECT, STAGING_ONLY or NONE.")
        if source_access == "NONE":
            raise ValueError("With no access to the source or a copy, nothing can be compared. Ask the owner for a staging copy first.")
        for lbl, v in (("Range start", range_start), ("Range end", range_end)):
            if v and not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
                raise ValueError(f"{lbl} must look like 2025-01-31.")
        if owner_email and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", owner_email):
            raise ValueError("Owner e-mail does not look like an address.")
        sp, sl = (source_path, source_label) if source_path else self._upload_path(source_upload)
        if not sp:
            raise ValueError("Choose the source file.")
        tp, tl = (target_path, target_label) if target_path else self._upload_path(target_upload)
        row = self.s.insert(self.TABLE, {
            "id": new_id("DAV-"), "project_id": self.pid, "name": name, "application": application, "source_access": source_access,
            "date_column": date_column.strip(), "key_column": key_column.strip(), "range_start": range_start, "range_end": range_end,
            "source_path": sp, "target_path": tp, "source_label": sl or source_label, "target_label": tl or target_label,
            "owner_name": owner_name.strip(), "owner_email": owner_email.strip(), "result": None, "confirmation": None,
            "decision_rec": None, "seeded": seeded, "created_at": now()})
        self.s.audit(self.pid, actor, "AVAILABILITY_DATASET_ADDED", "availability", row["id"],
                     {"name": name, "source_access": source_access, "source": row["source_label"], "target": row["target_label"]})
        return row

    def set_target(self, did: str, target_upload: str | None, actor: str = "console.user") -> dict:
        d = self._row(did)
        tp, tl = self._upload_path(target_upload)
        self.s.update(self.TABLE, did, target_path=tp, target_label=tl or "")
        self.s.audit(self.pid, actor, "AVAILABILITY_TARGET_CHANGED", "availability", did, {"target": tl})
        return self._row(did)

    def set_application(self, did: str, application: str, actor: str = "console.user") -> dict:
        d = self._row(did)
        self.s.update(self.TABLE, did, application=application)
        self.s.audit(self.pid, actor, "AVAILABILITY_APPLICATION_SET", "availability", did, {"from": d["application"], "to": application})
        return self._row(did)

    def set_source(self, did: str, source_upload: str, actor: str = "console.user") -> dict:
        d = self._row(did)
        sp, sl = self._upload_path(source_upload)
        if not sp:
            raise ValueError("Choose the source file.")
        self.s.update(self.TABLE, did, source_path=sp, source_label=sl or "")
        self.s.audit(self.pid, actor, "AVAILABILITY_SOURCE_CHANGED", "availability", did, {"source": sl})
        return self._row(did)

    def run(self, did: str, actor: str = "console.user") -> dict:
        d = self._row(did)
        try:
            res = analyse(d["source_path"], d["target_path"], d["date_column"], d["key_column"], d["range_start"], d["range_end"])
        except (ValueError, csv.Error, OSError) as e:
            res = {"status": "CANNOT_CHECK", "summary": f"The files could not be read: {e}", "issues": [str(e)], "evidence": "None",
                   "checked_at": now(), "months": [], "missing_days": [], "missing_day_count": 0}
        # schema drift (Round 7): compare the columns seen now with the columns seen at the previous check of this dataset
        hist = list(d.get("schema_history") or [])
        cols = res.get("columns")
        if cols:
            prev = hist[-1] if hist else None
            if prev:
                drift = {}
                for side in ("source", "target"):
                    a, b = prev["columns"].get(side) or [], cols.get(side) or []
                    added, removed = [c for c in b if c not in a], [c for c in a if c not in b]
                    if (added or removed) and a:
                        drift[side] = {"added": added, "removed": removed}
                if drift:
                    res["drift"] = {**drift, "since": prev["at"], "since_files": prev.get("files")}
            if not prev or prev["columns"] != cols:
                hist.append({"at": res.get("checked_at") or now(), "columns": cols,
                             "files": {"source": d["source_label"], "target": d["target_label"]}})
        self.s.update(self.TABLE, did, result=res, schema_history=hist[-20:])
        if res.get("drift"):
            self.s.audit(self.pid, actor, "SCHEMA_DRIFT_DETECTED", "availability", did, res["drift"])
        self.s.audit(self.pid, actor, "AVAILABILITY_CHECKED", "availability", did,
                     {"status": res["status"], "issues": len(res.get("issues", [])), "totals": res.get("totals")})
        return self._row(did)

    def run_all(self, actor: str = "console.user") -> int:
        ds = self.s.all(self.TABLE, project_id=self.pid)
        for d in ds:
            self.run(d["id"], actor)
        return len(ds)

    def request_confirmation(self, did: str, actor: str = "console.user") -> dict:
        d = self._row(did)
        if d["source_access"] == "DIRECT":
            raise ValueError("The source is directly accessible, so no owner confirmation is needed.")
        r = d.get("result") or {}
        rng = r.get("range") or {}
        body = (f"Hello {d['owner_name'] or 'source owner'},\n\n"
                f"We are checking whether the data for \"{d['name']}\" is complete before it is migrated to Google Cloud. "
                f"We can only see the staging copy ({d['source_label'] or 'uploaded files'}), not your original system, so we cannot tell "
                "whether a file is missing before it reached the staging location.\n\n"
                + (f"What we can see: {r['source']['rows']:,} rows covering {rng.get('start')} to {rng.get('end')}.\n\n" if r.get("source") else "")
                + "Please reply to confirm that every expected file for this period has been uploaded, or list any that are missing.\n\n"
                f"Reference: project {self.pid}, dataset {did}\n\nThis is a DRAFT generated by the Migration Accelerator and has not been sent.")
        status, recipient = ("DRAFT", d["owner_email"]) if d["owner_email"] else ("BLOCKED_OWNER_UNKNOWN", None)
        if status != "DRAFT":
            body = "[NOT GENERATED FOR SENDING] No e-mail address is recorded for the source owner. Add one to this dataset.\n\n" + body
        n = self.s.insert("notifications", {"id": new_id("NTF-"), "project_id": self.pid, "stakeholder_id": None,
                                            "event": "SOURCE_CONFIRMATION_REQUEST", "recipient": recipient,
                                            "subject": f"Please confirm all files were uploaded: {d['name']}", "body": body, "status": status,
                                            "approval": "PENDING", "created_at": now()})
        self.s.update(self.TABLE, did, confirmation={"status": "REQUESTED", "requested_at": now(), "notification_id": n["id"], "draft_status": status})
        self.s.audit(self.pid, actor, "SOURCE_CONFIRMATION_REQUESTED", "availability", did, {"notification": n["id"], "draft_status": status, "sent": False})
        return self._row(did)

    def confirm(self, did: str, confirmer: str, note: str, outcome: str = "CONFIRMED") -> dict:
        d = self._row(did)
        if d["source_access"] == "DIRECT":
            raise ValueError("The source is directly accessible, so no owner confirmation is needed.")
        if outcome not in ("CONFIRMED", "REPORTED_MISSING"):
            raise ValueError("Outcome must be CONFIRMED or REPORTED_MISSING.")
        if not (confirmer or "").strip() or not (note or "").strip():
            raise ValueError("Enter who confirmed and what they said.")
        prev = d.get("confirmation") or {}
        self.s.update(self.TABLE, did, confirmation={**prev, "status": outcome, "confirmed_by": confirmer.strip(), "note": note.strip(), "recorded_at": now()})
        self.s.audit(self.pid, confirmer.strip(), f"SOURCE_{outcome}", "availability", did, {"note": note.strip()})
        return self._row(did)

    def decide(self, did: str, approver: str, decision: str, rationale: str) -> dict:
        d = self._row(did)
        if decision not in ("PROCEED", "HOLD"):
            raise ValueError("Decision must be PROCEED or HOLD.")
        if not (approver or "").strip() or len((rationale or "").strip()) < 10:
            raise ValueError("Enter your name and a reason of at least 10 characters.")
        if not d.get("result"):
            raise ValueError("Run the check first.")
        rec = {"decision": decision, "approver": approver.strip(), "rationale": rationale.strip(), "at": now(), "result_hash": result_hash(d["result"])}
        self.s.update(self.TABLE, did, decision_rec=rec)
        self.s.record_approval(self.pid, "data_availability", did, approver.strip(), decision, rationale.strip(), requested_by="console.user")
        return self._row(did)

    def summary(self) -> dict:
        ds = self.list()
        c = Counter(d["clearance"] for d in ds)
        return {"total": len(ds), **{k: c.get(k, 0) for k in ("CLEAR", "NEEDS_CONFIRMATION", "NEEDS_DECISION", "PROCEED", "HOLD", "NOT_RUN")},
                "unresolved": sum(c.get(k, 0) for k in ("NEEDS_CONFIRMATION", "NEEDS_DECISION", "HOLD", "NOT_RUN"))}
