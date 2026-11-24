"""Dependency intelligence, risk & readiness, stakeholder resolution and notification drafts.

Guardrail: owners and e-mail addresses come ONLY from supplied directories. Unknown -> 'Owner unknown'.
Risk = sum(weight x dimension score); hard gates override the aggregate score (blueprint section 13).
"""
from __future__ import annotations

import hashlib
from collections import defaultdict

import networkx as nx

from .store import Store, new_id, now


def _stable_id(prefix: str, *parts: str) -> str:
    """Deterministic id for a row that should be upserted in place instead of accumulating duplicates across runs."""
    return prefix + hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]

WEIGHTS = {"service_compatibility": 0.20, "application_complexity": 0.15, "data_quality": 0.20,
           "dependency_impact": 0.20, "security_compliance": 0.10, "testing_coverage": 0.10, "rollback_readiness": 0.05}


TRACKING_STATUSES = ("OPEN", "IN_PROGRESS", "MITIGATED", "ACCEPTED_LOW")


def set_risk_followup(store: Store, project_id: str, application: str, dimension: str, tracking_status: str, owner: str,
                      note: str, actor: str) -> dict:
    if tracking_status not in TRACKING_STATUSES:
        raise ValueError(f"Status must be one of {', '.join(TRACKING_STATUSES)}.")
    row = store.insert("risk_followups", {"id": _stable_id("RSKF-", project_id, application, dimension), "project_id": project_id,
                                          "application": application, "dimension": dimension, "tracking_status": tracking_status,
                                          "owner": (owner or "").strip(), "note": (note or "").strip(), "updated_by": actor, "updated_at": now()})
    store.audit(project_id, actor, "RISK_FOLLOWUP_UPDATED", "risk_followup", row["id"],
               {"application": application, "dimension": dimension, "status": tracking_status})
    return row


def classify(score: float) -> str:
    return "Low" if score < 30 else "Medium" if score < 50 else "High" if score < 75 else "Critical"


# --------------------------------------------------------------------------- dependency graph
def build_graph(resources: list[dict], relationships: list[dict], consumers: list[dict], mappings: dict[str, dict]) -> nx.DiGraph:
    g = nx.DiGraph()
    for r in resources:
        m = mappings.get(r["resource_id"], {})
        g.add_node(r["resource_id"], kind="aws_resource", label=r["name"], service=r["service"], application=r.get("application"),
                   gcp_target=m.get("target_service"), strategy=m.get("strategy"), criticality=r.get("criticality"))
    for c in consumers:
        g.add_node(c["node_id"], kind="consumer", label=c["display_name"], service=c.get("consumer_type"), application=c.get("application"))
    for rel in relationships:
        for n in (rel["source"], rel["target"]):
            if n not in g:
                g.add_node(n, kind="unresolved", label=n, service="?", application=None)
        g.add_edge(rel["source"], rel["target"], type=rel["relationship_type"], status=rel["status"], confidence=rel["confidence"],
                   evidence=len(rel["evidence"]))
    return g


def impact(g: nx.DiGraph, node: str) -> dict:
    down = nx.descendants(g, node) if node in g else set()
    up = nx.ancestors(g, node) if node in g else set()
    apps = sorted({g.nodes[n].get("application") for n in down if g.nodes[n].get("application")})
    return {"node": node, "upstream": sorted(up), "downstream": sorted(down), "impacted_applications": apps}


def graph_json(g: nx.DiGraph) -> dict:
    return {"nodes": [{"id": n, **d} for n, d in g.nodes(data=True)],
            "edges": [{"source": a, "target": b, **d} for a, b, d in g.edges(data=True)]}


# --------------------------------------------------------------------------- stakeholders
def set_owner_override(store: Store, project_id: str, application: str, name: str, email: str, type_: str, actor: str) -> dict:
    """Record a manually entered owner for an application. Resolved ahead of the ownership directory and survives
    every later re-assessment, until someone clears it."""
    import re
    name, email = (name or "").strip(), (email or "").strip()
    if len(name) < 2:
        raise ValueError("Enter the owner's name.")
    if email and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        raise ValueError("That does not look like an e-mail address.")
    row = store.insert("stakeholder_overrides", {"id": _stable_id("OVR-", project_id, application), "project_id": project_id,
                                                 "application": application, "name": name, "email": email, "type": type_ or "BUSINESS_OWNER",
                                                 "updated_by": actor, "updated_at": now()})
    store.audit(project_id, actor, "OWNER_ENTERED", "stakeholder_override", row["id"], {"application": application, "name": name})
    return row


def clear_owner_override(store: Store, project_id: str, application: str, actor: str) -> None:
    store.delete_where("stakeholder_overrides", project_id=project_id, application=application)
    store.audit(project_id, actor, "OWNER_OVERRIDE_CLEARED", "stakeholder_override", application, {"application": application})


def resolve_stakeholders(store: Store, project_id: str, owners: list[dict], g: nx.DiGraph) -> list[dict]:
    by_app = defaultdict(list)
    for o in owners:
        by_app[o["application"]].append(o)
    overrides = {o["application"]: o for o in store.all("stakeholder_overrides", project_id=project_id)}
    apps = {d.get("application") for _, d in g.nodes(data=True) if d.get("application")}
    out = []
    for app in sorted(apps):
        ov = overrides.get(app)
        if ov:
            row = store.insert("stakeholders", {"id": _stable_id("STK-", project_id, app, "override"), "project_id": project_id,
                                                "application": app, "name": ov["name"], "email": ov["email"] or None, "type": ov["type"],
                                                "owner_source": f"Entered manually by {ov['updated_by']}", "verified": 1})
            out.append(row)
        elif by_app.get(app):
            for i, o in enumerate(by_app[app]):
                row = store.insert("stakeholders", {"id": _stable_id("STK-", project_id, app, "dir", str(i)), "project_id": project_id,
                                                    "application": app, "name": o["owner_name"], "email": o["owner_email"], "type": o["owner_type"],
                                                    "owner_source": "ownership/application_owners.csv", "verified": 1})
                out.append(row)
        else:
            row = store.insert("stakeholders", {"id": _stable_id("STK-", project_id, app, "unknown"), "project_id": project_id,
                                                "application": app, "name": "Owner unknown", "email": None, "type": "UNKNOWN",
                                                "owner_source": "not found in any supplied directory", "verified": 0})
            out.append(row)
    return out


EVENTS = {
    "ASSESSMENT_COMPLETE": ("Assessment complete – review requested: {app}",
                           "Discovery and impact analysis for {app} is complete. {n_res} AWS resources were assessed; "
                           "{n_down} downstream consumers depend on them. Please review the mappings and dependencies marked for your application."),
    "MIGRATION_PLANNED": ("Planned migration window – {app}",
                          "A migration window is proposed for {app} (wave {wave}). Impacted datasets: {datasets}. Action: confirm the window and "
                          "any blackout dates."),
    "READINESS_REVIEW": ("Readiness review: confirm dependencies for {app}",
                         "Please confirm the upstream/downstream dependencies listed for {app}. Items marked INFERRED or NEEDS_REVIEW need your confirmation."),
    "VALIDATION_COMPLETE": ("Data validation complete – {app}: {status}",
                            "Reconciliation finished for {datasets}. Result: {status}. {exceptions} open exception(s). "
                            "Action: review exceptions in the Validation Workbench."),
    "SIGNOFF_REQUIRED": ("Sign-off required – {app}",
                         "Readiness gates were evaluated for {app}: {gate}. Formal business and technical sign-off is required before cutover."),
}


def draft_notifications(store: Store, project_id: str, stakeholders: list[dict], ctx: dict,
                        cutover: list[tuple] | None = None) -> list[dict]:
    """Draft (never send) the stakeholder e-mails. Rows are upserted under stable ids so that what a person did to a draft
    (edited, approved, recorded as sent, recorded a reply) survives every re-evaluation; see comms.upsert_drafts."""
    from . import comms
    drafts = []
    for s in stakeholders:
        app = s["application"]
        c = ctx.get(app)
        if not c:
            continue
        events = ["ASSESSMENT_COMPLETE", "READINESS_REVIEW"]
        if c.get("datasets"):
            events += ["MIGRATION_PLANNED", "VALIDATION_COMPLETE"]
        events.append("SIGNOFF_REQUIRED")
        for ev in events:
            subj, body = EVENTS[ev]
            fields = {"app": app, **c}
            if not s["verified"]:
                status, recipient = "BLOCKED_OWNER_UNKNOWN", None
                body_txt = f"[NOT GENERATED] No verified owner for {app}. Request owner enrichment from CMDB/application directory."
            else:
                status, recipient = "DRAFT", s["email"]
                body_txt = (f"Hello {s['name']},\n\n" + body.format(**fields) +
                            f"\n\nSupport contact: migration-office@example.com\nReference: project {project_id}\n\n"
                            "This is a DRAFT generated by the Migration Accelerator and has not been sent.")
            drafts.append({"stakeholder_id": s["id"], "event": ev, "about_app": app, "recipient": recipient,
                           "subject": subj.format(**fields), "body": body_txt, "status": status, "created_at": now()})
    for s, ev, about, subj, body in cutover or []:
        if not s["verified"]:
            status, recipient = "BLOCKED_OWNER_UNKNOWN", None
            body_txt = f"[NOT GENERATED] No verified owner for {s['application']}. Request owner enrichment from CMDB/application directory."
        else:
            status, recipient = "DRAFT", s["email"]
            body_txt = (f"Hello {s['name']},\n\n{body}\n\nSupport contact: migration-office@example.com\nReference: project {project_id}\n\n"
                        "This is a DRAFT generated by the Migration Accelerator and has not been sent.")
        drafts.append({"stakeholder_id": s["id"], "event": ev, "about_app": about, "recipient": recipient, "subject": subj,
                       "body": body_txt, "status": status, "created_at": now()})
    rows = comms.upsert_drafts(store, project_id, drafts)
    store.audit(project_id, "communication-agent", "NOTIFICATION_DRAFTS_CREATED", "notification", "*",
                {"drafts": sum(1 for d in rows if d["status"] == "DRAFT"),
                 "blocked_owner_unknown": sum(1 for d in rows if d["status"] != "DRAFT"), "sent": 0})
    return rows


# --------------------------------------------------------------------------- risk & readiness
def assess_risk(store: Store, project_id: str, resources: list[dict], mappings: dict[str, dict], g: nx.DiGraph,
                conversions: list[dict], validations_by_app: dict[str, list[dict]], findings: list[dict],
                stakeholders: list[dict]) -> dict:
    apps = defaultdict(list)
    for r in resources:
        apps[r.get("application") or "Unassigned"].append(r)
    owners_ok = {s["application"] for s in stakeholders if s["verified"]}
    out = {}
    for app, rs in sorted(apps.items()):
        ms = [mappings[r["resource_id"]] for r in rs if r["resource_id"] in mappings]
        active = [m for m in ms if m["strategy"] not in ("Retire", "Retain")]
        cx = {"S": 1, "M": 2, "L": 3, "XL": 4, "XXL": 5, "?": 5}
        dims: dict[str, tuple[float, str]] = {}
        avg_c = sum(cx[m["effort"]] for m in active) / max(1, len(active))
        gaps = [m["aws_service"] for m in active if m.get("gap")]
        dims["service_compatibility"] = (min(100, avg_c * 18 + 10 * len(gaps)),
                                         f"avg complexity {avg_c:.1f}/5; gaps: {', '.join(gaps) or 'none'}")
        n_rel = sum(1 for r in rs for _ in g.out_edges(r["resource_id"])) if rs else 0
        dims["application_complexity"] = (min(100, 10 + 2.5 * len(rs) + 2 * n_rel),
                                          f"{len(rs)} resources, {n_rel} outbound dependencies")
        vals = validations_by_app.get(app, [])
        has_data = any(r.get("category") in ("database", "analytics", "storage")
                       and r["service"] not in ("ElastiCache", "MemoryDB", "Athena") for r in rs)  # caches are rebuilt, not reconciled
        if vals:
            worst = "FAIL" if any(v["status"] == "FAIL" for v in vals) else "REVIEW" if any(v["status"] == "REVIEW" for v in vals) else "PASS"
            dq = {"FAIL": 90, "REVIEW": 50, "PASS": 10}[worst]
            dims["data_quality"] = (dq, f"latest validation status {worst} across {len(vals)} object(s)")
        else:
            dims["data_quality"] = (60 if has_data else 10, "NOT RUN – no validation evidence" if has_data else "no data objects")
        down = set()
        for r in rs:
            if r["resource_id"] in g:
                down |= {n for n in nx.descendants(g, r["resource_id"]) if g.nodes[n].get("application") != app}
        crit = sum(1 for r in rs if (r.get("criticality") or "").lower() == "high")
        dims["dependency_impact"] = (min(100, 15 * len(down) + 5 * crit), f"{len(down)} cross-application downstream nodes; {crit} high-criticality resources")
        app_find = [f for f in findings if f.get("application") in (app, None)]
        restricted = sum(1 for r in rs if str((r.get("configuration") or {}).get("inventory", {}).get("data_classification", "")).lower() in ("restricted", "confidential"))
        sec = min(100, 25 * sum(1 for f in app_find if f["kind"] in ("aws_access_key_id", "private_key", "password_literal"))
                  + 10 * sum(1 for f in app_find if f["kind"] == "over_privileged_policy") + 5 * restricted)
        dims["security_compliance"] = (sec, f"{len(app_find)} security finding(s); {restricted} confidential/restricted resources")
        app_conv = [c for c in conversions if c.get("application") in (app, None)]
        if app_conv:
            checks = [ch for c in app_conv for ch in c["checks"]]
            failed = sum(1 for ch in checks if ch["status"] in ("FAIL", "NOT_RUN"))
            dims["testing_coverage"] = (min(100, 40 + 10 * failed), f"{len(checks)} static checks; runtime/integration tests not yet executed")
        else:
            dims["testing_coverage"] = (70, "no converted artefacts / tests for this application")
        dims["rollback_readiness"] = (50, "AWS source retained as fallback; rollback rehearsal not yet executed")
        score = round(sum(WEIGHTS[k] * v[0] for k, v in dims.items()), 1)
        # ---- hard gates
        gates = []
        unapproved = [m["resource_id"] for m in active if m.get("approval_status") != "APPROVED"]
        if unapproved:
            gates.append({"gate": "TARGET_ARCHITECTURE_APPROVED", "status": "BLOCKED", "detail": f"{len(unapproved)} mapping(s) pending approval"})
        if any(v["status"] == "FAIL" for v in vals):
            gates.append({"gate": "MANDATORY_RECONCILIATION", "status": "BLOCKED",
                          "detail": "; ".join(f"{v['object']} {v['status']}" for v in vals if v["status"] == "FAIL")})
        elif has_data and not vals:
            gates.append({"gate": "MANDATORY_RECONCILIATION", "status": "BLOCKED", "detail": "validation NOT RUN for data objects"})
        if any(f["kind"] in ("aws_access_key_id", "private_key") for f in app_find):
            gates.append({"gate": "CRITICAL_SECURITY", "status": "BLOCKED", "detail": "hard-coded credential found – rotate before cutover"})
        if app not in owners_ok:
            gates.append({"gate": "OWNER_APPROVAL", "status": "BLOCKED", "detail": "no verified owner"})
        unknown_down = sorted({g.nodes[n].get("application") for n in down if g.nodes[n].get("application") not in owners_ok and g.nodes[n].get("application")})
        if unknown_down:
            gates.append({"gate": "DOWNSTREAM_OWNERS_KNOWN", "status": "BLOCKED", "detail": f"impacted consumers without owner: {', '.join(unknown_down)}"})
        gates.append({"gate": "ROLLBACK_TESTED", "status": "OPEN", "detail": "schedule rollback rehearsal"})
        blocked = any(x["status"] == "BLOCKED" for x in gates)
        readiness = "BLOCKED" if blocked else "READY_FOR_SIGNOFF" if score < 50 else "CONDITIONAL"
        mitig = {
            "service_compatibility": "Run PoC for gap services; confirm partner/product choice with architecture board",
            "application_complexity": "Split into sub-waves; strangler pattern for coupled components",
            "data_quality": "Resolve validation exceptions; agree tolerances with data owner; re-run reconciliation",
            "dependency_impact": "Confirm consumers with owners; plan bridge/dual-run for consumers that stay on AWS",
            "security_compliance": "Rotate exposed credentials; least-privilege IAM review; CMEK/VPC-SC per policy",
            "testing_coverage": "Execute unit/integration tests and BigQuery dry-runs in non-prod project",
            "rollback_readiness": "Rehearse rollback: re-point consumers to AWS; keep source authoritative until sign-off"}
        follow = {f["dimension"]: f for f in store.all("risk_followups", project_id=project_id, application=app)}
        default_owner = next((s["name"] for s in stakeholders if s["application"] == app and s["verified"]), "Owner unknown")
        for d, (sc, why) in dims.items():
            fu = follow.get(d)
            store.insert("risk_items", {"id": _stable_id("RSK-", project_id, app, d), "project_id": project_id, "application": app,
                                        "dimension": d, "score": sc, "rationale": why, "mitigation": mitig[d] if sc >= 50 else "Monitor",
                                        "owner": (fu["owner"] if fu and fu.get("owner") else default_owner),
                                        "status": (fu["tracking_status"] if fu else ("OPEN" if sc >= 50 else "ACCEPTED_LOW")),
                                        "follow_up_note": fu["note"] if fu else "", "follow_up_at": fu["updated_at"] if fu else None,
                                        "follow_up_by": fu["updated_by"] if fu else None})
        out[app] = {"application": app, "score": score, "classification": classify(score),
                    "dimensions": {k: {"score": v[0], "weight": WEIGHTS[k], "rationale": v[1]} for k, v in dims.items()},
                    "gates": gates, "readiness": readiness,
                    "items": {d: {k: v for k, v in store.get("risk_items", _stable_id("RSK-", project_id, app, d)).items()
                                  if k in ("dimension", "score", "rationale", "mitigation", "owner", "status", "follow_up_note",
                                           "follow_up_at", "follow_up_by")} for d in dims}}
    store.audit(project_id, "risk-agent", "RISK_ASSESSED", "risk_assessment", "*", {a: {"score": v["score"], "readiness": v["readiness"]} for a, v in out.items()})
    return out
