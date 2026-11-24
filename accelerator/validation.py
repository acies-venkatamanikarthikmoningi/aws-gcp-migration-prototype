"""Post-migration AWS-to-GCP data validation (blueprint section 7).

Deterministic: counts, Decimal aggregates and SHA-256 hashes are computed in code – never by an LLM.
Outcomes: PASS | FAIL | REVIEW | WAIVED | NOT_RUN. A mismatch is never silently accepted: tolerance must be
configured per rule, and REVIEW/WAIVED carry evidence, approver and expiry.
"""
from __future__ import annotations

import csv
import hashlib
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable

from .migration_engine import COLUMNS, type_row
from .store import Store, new_id, now

MANDATORY = {"coverage", "schema", "row_count", "uniqueness", "aggregate", "hash", "completeness", "freshness"}


def choose_strategy(rows: int, streaming: bool = False, sensitive: bool = False) -> dict:
    if streaming:
        s = "offset/watermark + event counts + lag + sampled event-level checks"
    elif rows <= 1_000:
        s = "exact key-based row comparison"
    elif rows <= 50_000_000:
        s = "partitioned counts/aggregates + key-level diff on mismatching partitions"
    else:
        s = "partition counts + deterministic partition hashes + aggregate controls + targeted drill-down"
    return {"rows": rows, "strategy": s, "sensitive_mode": "compare hashes/masked values only" if sensitive else "raw values allowed"}


def _canon(t: dict) -> str:
    return "|".join("" if t[c] is None else str(t[c]) for c in COLUMNS)


def profile_orders(rows: list[dict], typed: bool) -> dict:
    """Partition-level profile. Source rows are raw text: invalid rows are counted, not dropped silently."""
    parts: dict[str, dict] = defaultdict(lambda: {"count": 0, "invalid": 0, "valid_count": 0, "sum_amount": Decimal("0"),
                                                  "min_amount": None, "max_amount": None, "nulls": Counter(), "keys": Counter(),
                                                  "hashes": [], "status": Counter(), "max_updated_at": "", "min_date": None, "max_date": None})
    for r in rows:
        p = parts[r["order_month"]]
        p["count"] += 1
        t, err = (r, None) if typed else type_row(r)
        if typed:
            t = {**r, "amount": str(Decimal(str(r["amount"])).quantize(Decimal("0.01"))) if r["amount"] is not None else None}
        if t is None:
            p["invalid"] += 1
            continue
        p["valid_count"] += 1
        for c in COLUMNS:
            if t.get(c) in (None, ""):
                p["nulls"][c] += 1
        p["keys"][t["order_id"]] += 1
        if t["amount"] is not None:
            a = Decimal(t["amount"])
            p["sum_amount"] += a
            p["min_amount"] = a if p["min_amount"] is None else min(p["min_amount"], a)
            p["max_amount"] = a if p["max_amount"] is None else max(p["max_amount"], a)
        p["status"][t["status"]] += 1
        p["max_updated_at"] = max(p["max_updated_at"], t["updated_at"] or "")
        p["min_date"] = t["order_date"] if p["min_date"] is None else min(p["min_date"], t["order_date"])
        p["max_date"] = t["order_date"] if p["max_date"] is None else max(p["max_date"], t["order_date"])
        p["hashes"].append(hashlib.sha256(_canon(t).encode()).hexdigest())
    out = {}
    for k, p in sorted(parts.items()):
        out[k] = {"count": p["count"], "invalid": p["invalid"], "valid_count": p["valid_count"],
                  "sum_amount": str(p["sum_amount"]), "min_amount": str(p["min_amount"]), "max_amount": str(p["max_amount"]),
                  "nulls": dict(p["nulls"]), "duplicate_keys": sum(v - 1 for v in p["keys"].values() if v > 1),
                  "distinct_keys": len(p["keys"]), "status_distribution": dict(sorted(p["status"].items())),
                  "max_updated_at": p["max_updated_at"], "min_date": p["min_date"], "max_date": p["max_date"],
                  "partition_hash": hashlib.sha256("".join(sorted(p["hashes"])).encode()).hexdigest()}
    total = {"count": sum(v["count"] for v in out.values()), "valid_count": sum(v["valid_count"] for v in out.values()),
             "invalid": sum(v["invalid"] for v in out.values()),
             "sum_amount": str(sum(Decimal(v["sum_amount"]) for v in out.values())),
             "partitions": len(out), "max_updated_at": max((v["max_updated_at"] for v in out.values()), default="")}
    return {"partitions": out, "total": total, "profiled_at": now()}


SCHEMA_COMPAT = {("BIGINT", "INT64"): "PASS", ("INTEGER", "INT64"): "PASS", ("DATE", "DATE"): "PASS",
                 ("VARCHAR", "STRING"): "PASS", ("CHAR", "STRING"): "PASS", ("TIMESTAMP", "DATETIME"): "PASS",
                 ("TIMESTAMPTZ", "TIMESTAMP"): "PASS", ("DECIMAL", "NUMERIC"): "PASS", ("NUMERIC", "NUMERIC"): "PASS",
                 ("DOUBLE PRECISION", "FLOAT64"): "PASS", ("REAL", "FLOAT64"): "REVIEW", ("BOOLEAN", "BOOL"): "PASS",
                 ("SUPER", "JSON"): "REVIEW", ("DECIMAL", "FLOAT64"): "FAIL"}


def compare_schema(source: list[tuple[str, str, int | None, int | None]], target: list[tuple[str, str, int | None, int | None]]) -> list[dict]:
    """(name, base_type, precision, scale) per column."""
    res = []
    tmap = {c[0]: c for c in target}
    for name, st, sp, ss in source:
        t = tmap.get(name)
        if not t:
            res.append({"column": name, "status": "FAIL", "detail": "missing in target"})
            continue
        status = SCHEMA_COMPAT.get((st, t[1]), "REVIEW")
        detail = f"{st}{f'({sp},{ss})' if sp else ''} -> {t[1]}{f'({t[2]},{t[3]})' if t[2] else ''}"
        if status == "PASS" and sp and t[2] and (t[2] < sp or (t[3] or 0) < (ss or 0)):
            status, detail = "FAIL", detail + " precision/scale reduced"
        res.append({"column": name, "status": status, "detail": detail})
    for name in tmap:
        if name not in {c[0] for c in source}:
            res.append({"column": name, "status": "REVIEW", "detail": "extra column in target"})
    return res


ORDERS_SOURCE_SCHEMA = [("order_id", "BIGINT", None, None), ("customer_id", "BIGINT", None, None), ("order_date", "DATE", None, None),
                        ("status", "VARCHAR", None, None), ("amount", "NUMERIC", 18, 2), ("currency", "CHAR", None, None),
                        ("updated_at", "TIMESTAMP", None, None)]
ORDERS_TARGET_SCHEMA = [("order_id", "INT64", None, None), ("customer_id", "INT64", None, None), ("order_date", "DATE", None, None),
                        ("status", "STRING", None, None), ("amount", "NUMERIC", 18, 2), ("currency", "STRING", None, None),
                        ("updated_at", "DATETIME", None, None)]


class ValidationEngine:
    def __init__(self, store: Store, project_id: str, actor: str = "data-quality-agent"):
        self.store, self.project_id, self.actor = store, project_id, actor

    def baseline(self, object_name: str, profile: dict, snapshot_ref: str) -> dict:
        vid = new_id("VAL-")
        self.store.insert("validation_runs", {"id": vid, "project_id": self.project_id, "run_type": "SOURCE_BASELINE", "object": object_name,
                                              "source_snapshot": {"ref": snapshot_ref, "watermark": profile["total"]["max_updated_at"],
                                                                  "profile": profile},
                                              "status": "CAPTURED", "summary": profile["total"], "created_at": now()})
        self.store.audit(self.project_id, self.actor, "SOURCE_BASELINE_CAPTURED", "validation_run", vid, profile["total"])
        return self.store.get("validation_runs", vid)

    def _result(self, vid: str, obj: str, part: str | None, dim: str, rule: str, sv, tv, tol: str, status: str,
                sev: str = "HIGH", details: dict | None = None) -> dict:
        r = {"id": new_id("RES-"), "validation_run_id": vid, "object": obj, "partition": part, "dimension": dim, "rule": rule,
             "source_value": None if sv is None else str(sv), "target_value": None if tv is None else str(tv), "tolerance": tol,
             "status": status, "severity": sev if status in ("FAIL", "REVIEW") else "INFO", "details": details or {}}
        w = self._active_waiver(r) if status in ("FAIL", "REVIEW") else None
        if w:  # an approved, unexpired waiver with the SAME scope and values carries forward – nothing else does
            r["status"], r["details"] = "WAIVED", {**r["details"], "waiver": w, "previous_status": status}
        self.store.insert("validation_results", r)
        if r["status"] in ("FAIL", "REVIEW"):
            self.store.insert("validation_exceptions", {"id": new_id("EXC-"), "result_id": r["id"], "validation_run_id": vid,
                                                        "severity": r["severity"], "details": {"rule": rule, "partition": part, **(details or {})},
                                                        "owner": "Data Platform Lead" if status == "FAIL" else "Analytics Manager",
                                                        "remediation_status": "OPEN"})
        return r

    def reconcile_orders(self, run_type: str, label: str, src_profile: dict, tgt_profile: dict,
                         src_rows: Callable[[str], list[dict]], tgt_rows: Callable[[str], list[dict]],
                         freshness_tolerance_min: float = 0.0, parent_keys: set | None = None,
                         quarantined: dict[str, int] | None = None) -> dict:
        vid = new_id("VAL-")
        obj = "analytics.orders"
        self.store.insert("validation_runs", {"id": vid, "project_id": self.project_id, "run_type": run_type, "object": obj,
                                              "source_snapshot": {"label": label, "total": src_profile["total"]},
                                              "target_snapshot": {"total": tgt_profile["total"]}, "status": "RUNNING", "created_at": now()})
        S, T = src_profile["partitions"], tgt_profile["partitions"]
        quarantined = quarantined or {}
        strategy = choose_strategy(src_profile["total"]["count"])
        # schema
        for c in compare_schema(ORDERS_SOURCE_SCHEMA, ORDERS_TARGET_SCHEMA):
            self._result(vid, obj, None, "schema", f"column {c['column']}", "", "", "type compatibility", c["status"], "MEDIUM",
                         {"detail": c["detail"]})
        # coverage
        missing, extra = sorted(set(S) - set(T)), sorted(set(T) - set(S))
        self._result(vid, obj, None, "coverage", "expected partitions present", len(S), len(T) - len(extra), "exact",
                     "PASS" if not missing else "FAIL", "CRITICAL", {"missing": missing, "extra": extra})
        mismatched: list[str] = []
        for p in sorted(S):
            s, t = S[p], T.get(p)
            if t is None:
                mismatched.append(p)
                continue
            # row count – invalid source rows must be accounted for by quarantine evidence
            if s["count"] == t["count"]:
                st, note = "PASS", {}
            elif t["count"] == s["valid_count"] and s["invalid"] and quarantined.get(p) == s["invalid"]:
                st, note = "REVIEW", {"explained_by": f"{s['invalid']} invalid source rows quarantined (evidence: rejected file)",
                                      "action": "data owner to accept (WAIVE) or fix at source and reload"}
            else:
                st, note = "FAIL", {"difference": s["count"] - t["count"]}
            self._result(vid, obj, p, "row_count", "row count", s["count"], t["count"], "exact", st, "HIGH", note)
            # completeness
            st = "PASS" if s["nulls"] == t["nulls"] else "FAIL"
            self._result(vid, obj, p, "completeness", "null counts per column", s["nulls"], t["nulls"], "exact", st, "MEDIUM")
            # uniqueness
            st = "PASS" if s["duplicate_keys"] == t["duplicate_keys"] else "FAIL"
            self._result(vid, obj, p, "uniqueness", "duplicate order_id", s["duplicate_keys"], t["duplicate_keys"], "exact", st, "HIGH")
            # aggregates
            diff = abs(Decimal(s["sum_amount"]) - Decimal(t["sum_amount"]))
            st = "PASS" if diff <= Decimal("0.00") else "FAIL"
            self._result(vid, obj, p, "aggregate", "SUM(amount) control total", s["sum_amount"], t["sum_amount"], "abs <= 0.00", st,
                         "HIGH", {"difference": str(diff)} if st == "FAIL" else None)
            # business rule
            st = "PASS" if s["status_distribution"] == t["status_distribution"] else "FAIL"
            self._result(vid, obj, p, "business_rule", "order status distribution", s["status_distribution"], t["status_distribution"],
                         "exact", st, "MEDIUM")
            # hash
            st = "PASS" if s["partition_hash"] == t["partition_hash"] else "FAIL"
            self._result(vid, obj, p, "hash", "partition SHA-256 (order-independent)", s["partition_hash"][:16], t["partition_hash"][:16],
                         "exact", st, "HIGH")
            if st == "FAIL" or s["count"] != t["count"] and not (s["invalid"] and t["count"] == s["valid_count"]):
                mismatched.append(p)
        # drill-down on mismatching partitions (key-level diff) – the scalable pattern for large data
        for p in sorted(set(mismatched)):
            if p in missing:
                continue
            sv = {}
            for r in src_rows(p):
                t, _ = type_row(r)
                if t:
                    sv[t["order_id"]] = t
            tv = {}
            dup_t = Counter()
            for r in tgt_rows(p):
                dup_t[int(r["order_id"])] += 1
                tv[int(r["order_id"])] = {**r, "order_id": int(r["order_id"]),
                                          "amount": str(Decimal(str(r["amount"])).quantize(Decimal("0.01"))) if r["amount"] is not None else None}
            miss = sorted(set(sv) - set(tv))
            ext = sorted(set(tv) - set(sv))
            changed = []
            for k in sorted(set(sv) & set(tv)):
                cols = [c for c in COLUMNS if str(sv[k][c]) != str(tv[k][c])]
                if cols:
                    changed.append({"order_id": k, "columns": cols,
                                    "sample": {c: {"aws": sv[k][c], "gcp": tv[k][c]} for c in cols}})
            self._result(vid, obj, p, "row_diff", "key-level drill-down", len(sv), len(tv), "exact", "FAIL", "HIGH",
                         {"missing_in_gcp": miss[:20], "missing_count": len(miss), "extra_in_gcp": ext[:20], "extra_count": len(ext),
                          "duplicate_keys_in_gcp": [k for k, v in dup_t.items() if v > 1][:20],
                          "changed": changed[:10], "changed_count": len(changed)})
        # freshness / watermark
        sw, tw = src_profile["total"]["max_updated_at"], tgt_profile["total"]["max_updated_at"]
        lag = (datetime.fromisoformat(sw) - datetime.fromisoformat(tw)).total_seconds() / 60 if sw and tw else None
        st = "PASS" if lag is not None and lag <= freshness_tolerance_min else ("NOT_RUN" if lag is None else "FAIL")
        self._result(vid, obj, None, "freshness", "max(updated_at) lag minutes", sw, tw, f"<= {freshness_tolerance_min} min", st, "HIGH",
                     {"lag_minutes": lag})
        # overall control total
        diff = abs(Decimal(src_profile["total"]["sum_amount"]) - Decimal(tgt_profile["total"]["sum_amount"]))
        self._result(vid, obj, None, "aggregate", "overall SUM(amount)", src_profile["total"]["sum_amount"],
                     tgt_profile["total"]["sum_amount"], "abs <= 0.00", "PASS" if diff == 0 else "FAIL", "CRITICAL")
        # referential integrity
        if parent_keys is None:
            self._result(vid, obj, None, "referential_integrity", "orders.customer_id -> customer", None, None, "n/a", "NOT_RUN", "LOW",
                         {"reason": "customer master not in migration scope for this run – cannot evaluate; do not claim validated"})
        # transformation validation: NUMERIC scale preserved
        bad_scale = sum(1 for p in T for r in tgt_rows(p)
                        if r["amount"] is not None and len(str(r["amount"]).split(".")[-1]) != 2)
        self._result(vid, obj, None, "transformation", "amount NUMERIC(18,2) scale preserved", "0 rows off-scale",
                     f"{bad_scale} rows off-scale", "exact", "PASS" if bad_scale == 0 else "FAIL", "LOW")
        return self._finalize(vid, strategy)

    def reconcile_exact(self, obj: str, key: str, source_csv: Path, target_csv: Path, pii: set[str] | None = None,
                        tolerances: dict[str, Decimal] | None = None, run_type: str = "POST_LOAD_RECONCILIATION") -> dict:
        """Small datasets: exact key-based row comparison with column-level differences. PII values are hashed."""
        pii, tolerances = pii or set(), tolerances or {}
        vid = new_id("VAL-")
        read = lambda p: {r[key]: r for r in csv.DictReader(p.open(encoding="utf-8"))}  # noqa: E731
        s, t = read(source_csv), read(target_csv)
        self.store.insert("validation_runs", {"id": vid, "project_id": self.project_id, "run_type": run_type, "object": obj,
                                              "source_snapshot": {"file": source_csv.name, "rows": len(s)},
                                              "target_snapshot": {"file": target_csv.name, "rows": len(t)}, "status": "RUNNING", "created_at": now()})
        mask = lambda c, v: ("sha256:" + hashlib.sha256(str(v).encode()).hexdigest()[:10]) if c in pii else v  # noqa: E731
        self._result(vid, obj, None, "row_count", "row count", len(s), len(t), "exact", "PASS" if len(s) == len(t) else "FAIL", "HIGH")
        self._result(vid, obj, None, "coverage", "keys present", len(s), len(set(s) & set(t)), "exact",
                     "PASS" if set(s) == set(t) else "FAIL", "HIGH", {"missing": sorted(set(s) - set(t)), "extra": sorted(set(t) - set(s))})
        num_cols = [c for c in next(iter(s.values())).keys() if c != key and all(_isnum(r[c]) for r in s.values())]
        for c in num_cols:
            ss = sum(Decimal(r[c]) for r in s.values())
            ts = sum(Decimal(r[c]) for r in t.values() if _isnum(r[c]))
            tol = tolerances.get(c, Decimal("0"))
            self._result(vid, obj, None, "aggregate", f"SUM({c})", ss, ts, f"abs <= {tol}", "PASS" if abs(ss - ts) <= tol else "FAIL", "HIGH",
                         {"difference": str(ss - ts)})
        for k in sorted(set(s) & set(t)):
            for c in s[k]:
                if c == key:
                    continue
                a, b = s[k][c], t[k].get(c)
                if c in num_cols and _isnum(b):
                    ok = abs(Decimal(a) - Decimal(b)) <= tolerances.get(c, Decimal("0"))
                else:
                    ok = a == b
                if not ok:
                    self._result(vid, obj, f"{key}={k}", "value", f"column {c}", mask(c, a), mask(c, b), "exact", "FAIL", "HIGH",
                                 {"difference": str(Decimal(a) - Decimal(b)) if c in num_cols else "value differs", "pii_masked": c in pii})
        return self._finalize(vid, choose_strategy(len(s), sensitive=bool(pii)))

    def _finalize(self, vid: str, strategy: dict) -> dict:
        res = self.store.all("validation_results", validation_run_id=vid)
        c = Counter(r["status"] for r in res)
        mandatory_fail = [r for r in res if r["status"] == "FAIL" and r["dimension"] in MANDATORY | {"value", "row_diff"}]
        status = "FAIL" if mandatory_fail else "REVIEW" if c.get("REVIEW") else "PASS"
        summary = {"checks": len(res), **{k: c.get(k, 0) for k in ("PASS", "FAIL", "REVIEW", "WAIVED", "NOT_RUN")}, "strategy": strategy}
        self.store.update("validation_runs", vid, status=status, summary=summary)
        self.store.audit(self.project_id, self.actor, "VALIDATION_COMPLETED", "validation_run", vid, {"status": status, **summary})
        return self.store.get("validation_runs", vid)

    def _active_waiver(self, r: dict) -> dict | None:
        today = now()[:10]
        for w in self.store.query("SELECT details FROM validation_results WHERE status='WAIVED' AND object=? AND "
                                  "IFNULL(partition,'')=IFNULL(?,'') AND dimension=? AND rule=? AND source_value IS ? AND target_value IS ?",
                                  (r["object"], r["partition"], r["dimension"], r["rule"], r["source_value"], r["target_value"])):
            wv = (w["details"] or {}).get("waiver")
            if wv and wv.get("expiry", "9999") >= today and not wv.get("carried_from"):
                return {**wv, "carried_from": "prior approved waiver"}
            if wv and wv.get("carried_from") and wv.get("expiry", "9999") >= today:
                return wv
        return None

    def waive(self, result_id: str, approver: str, rationale: str, expiry: str) -> dict:
        r = self.store.get("validation_results", result_id)
        if r["status"] not in ("REVIEW", "FAIL"):
            raise ValueError("only REVIEW/FAIL results can be waived")
        self.store.update("validation_results", result_id, status="WAIVED",
                          details={**(r["details"] or {}), "waiver": {"approver": approver, "rationale": rationale, "expiry": expiry,
                                                                      "previous_status": r["status"]}})
        for ex in self.store.all("validation_exceptions", result_id=result_id):
            self.store.update("validation_exceptions", ex["id"], remediation_status="WAIVED")
        self.store.record_approval(self.project_id, "validation_result", result_id, approver, "WAIVED", f"{rationale} (expires {expiry})")
        vid = r["validation_run_id"]
        run = self.store.get("validation_runs", vid)
        return self._finalize(vid, (run["summary"] or {}).get("strategy", {}))


def _isnum(v) -> bool:
    try:
        Decimal(str(v))
        return True
    except Exception:  # noqa: BLE001
        return False
