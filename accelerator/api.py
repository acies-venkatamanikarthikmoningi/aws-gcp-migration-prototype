"""FastAPI backend (blueprint 9.1). Serves the console UI and REST endpoints.

Run:  uvicorn accelerator.api:app --reload      (UI at http://localhost:8000)
Long-running work returns run IDs; the prototype executes synchronously in-process (production: Celery/Cloud Tasks).
"""
from __future__ import annotations

import json
import re
import shutil
import tempfile
import threading
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel

from . import catalogue, codecheck, codehistory, comms, dq, intelligence, loadops, parsers
from .ingestion import RejectedFile, check_content, classify, sha256_file
from .orchestrator import CONVERTERS, ROOT, Orchestrator
from .validation import ValidationEngine

import os

WORK = Path(
    os.environ.get(
        "ACCEL_WORKDIR",
        "/tmp/aws2gcp-migration-accelerator/runs/api"
    )
)

UI = ROOT / "ui" / "index.html"
app = FastAPI(title="AWS→GCP Migration Accelerator", version="1.1.0")
_lock = threading.Lock()
_orc: Orchestrator | None = None


_pool: dict[str, Orchestrator] = {}     # user projects loaded in this process, by project id


def _proj_root() -> Path:
    """User projects live beside (never inside) the demo workspace, so resetting the demo cannot touch them."""
    return WORK.parent / "projects"


def orc(pid: str | None = None) -> Orchestrator:
    """The sample project when no id is given, otherwise the named project (loaded from disk on first use)."""
    global _orc
    with _lock:
        if _orc is None:
            if WORK.exists():
                shutil.rmtree(WORK)
            _orc = Orchestrator(WORK, log=lambda m: None)
            _orc.run_all()
        if pid is None or pid == _orc.project_id:
            return _orc
        if pid in _pool:
            return _pool[pid]
        d = _proj_root() / pid
        if not (d / "ctx.json").exists():
            raise HTTPException(404, f"Project {pid} not found")
        _pool[pid] = Orchestrator.load(d, log=lambda m: None)
        return _pool[pid]


def _owner_of(table: str, rid: str) -> Orchestrator | None:
    """Find the project that holds a row, so id-only endpoints keep working across projects."""
    o = orc()
    if o.store.get(table, rid):
        return o
    if _proj_root().exists():
        for d in sorted(_proj_root().iterdir()):
            if (d / "ctx.json").exists():
                x = orc(json.loads((d / "ctx.json").read_text(encoding="utf-8")).get("project_id"))
                if x.store.get(table, rid):
                    return x
    return None


def _summary(o: Orchestrator) -> dict:
    pr = o.store.get("projects", o.project_id) or {}
    k = o.state()["kpis"] if o.ctx.get("assessed_at") else {}
    ups = o.store.all("uploads", project_id=o.project_id)
    return {**pr, "is_sample": bool((pr.get("config") or {}).get("sample")),
            "files": sum(1 for u in ups if u["status"] == "ACCEPTED"), "rejected": sum(1 for u in ups if u["status"] == "REJECTED"),
            "assessed_at": o.ctx.get("assessed_at"), "resources": k.get("resources_discovered", 0),
            "mappings_approved": k.get("mappings_approved", 0), "mappings_total": k.get("mappings_total", 0)}


def _all_projects() -> list[dict]:
    out = [_summary(orc())]
    if _proj_root().exists():
        for d in sorted(_proj_root().iterdir(), key=lambda x: x.stat().st_mtime):
            f = d / "ctx.json"
            if f.exists():
                out.append(_summary(orc(json.loads(f.read_text(encoding="utf-8")).get("project_id"))))
    return out


def _state(pid: str | None = None) -> dict:
    o = orc(pid)
    st = o.state()
    rd = o.workdir / "reports"
    st["reports"] = [str(p.relative_to(o.workdir)) for p in rd.glob("*")] if rd.exists() else []
    st["projects"] = [{k: v for k, v in x.items() if k in ("id", "name", "customer", "status", "is_sample", "files", "assessed_at")}
                      for x in _all_projects()]
    return st


class NewProject(BaseModel):
    name: str
    customer: str = ""
    application: str = ""
    owner: str = ""
    aws_account: str = ""
    aws_region: str = "us-east-1"
    gcp_project: str = ""
    gcp_region: str = "us-east1"
    environment: str = "Prod"
    criticality: str = "Medium"


_RULES = {"name": (r"^.{3,80}$", "Name must be 3 to 80 characters."),
          "aws_account": (r"^(\d{12})?$", "AWS account ID must be 12 digits (or left blank)."),
          "aws_region": (r"^[a-z]{2}-[a-z]+-\d$", "AWS region looks like us-east-1."),
          "gcp_project": (r"^([a-z][a-z0-9-]{4,28}[a-z0-9])?$", "GCP project ID is 6 to 30 lowercase letters, digits or hyphens, starting with a letter."),
          "gcp_region": (r"^[a-z]+-[a-z]+\d$", "GCP region looks like us-east1."),
          "application": (r"^([A-Za-z][A-Za-z0-9 _-]{1,60})?$", "Application name uses letters, digits, spaces, hyphens or underscores.")}


def _check(d: dict) -> dict:
    errs = {k: msg for k, (rx, msg) in _RULES.items() if k in d and not re.match(rx, str(d[k] or ""))}
    if d.get("environment") and d["environment"] not in ("Prod", "Non-prod", "Dev", "Test"):
        errs["environment"] = "Choose Prod, Non-prod, Dev or Test."
    if d.get("criticality") and d["criticality"] not in ("Low", "Medium", "High", "Critical"):
        errs["criticality"] = "Choose Low, Medium, High or Critical."
    if errs:
        raise HTTPException(422, {"errors": errs})
    return d


class Decision(BaseModel):
    approver: str
    decision: str = "APPROVED"
    rationale: str


class Waiver(BaseModel):
    approver: str
    rationale: str
    expiry: str


@app.get("/", response_class=HTMLResponse)
def ui() -> str:
    return UI.read_text(encoding="utf-8")


@app.get("/api/v1/state")
def state(project: str | None = Query(None)) -> JSONResponse:
    return JSONResponse(json.loads(json.dumps(_state(project), default=str)))


@app.post("/api/v1/demo/run")
def rerun() -> dict:
    global _orc
    with _lock:
        _orc = None
    o = orc()
    return {"project_id": o.project_id, "status": "COMPLETED"}


@app.get("/api/v1/projects")
def projects() -> list[dict]:
    return _all_projects()


@app.post("/api/v1/projects", status_code=201)
def create_project(d: NewProject) -> dict:
    d = NewProject(**_check(d.model_dump()))
    app_name = d.application.strip() or d.name.strip()
    o = Orchestrator(_proj_root() / "_new", log=lambda m: None)
    pid = o.create_project(name=d.name.strip(), customer=d.customer.strip() or "Not specified", owner=d.owner.strip() or "Not assigned",
                           aws_account=d.aws_account, aws_region=d.aws_region, gcp_project=d.gcp_project or "not set",
                           gcp_region=d.gcp_region, status="NEW", actor="console.user",
                           config={"environment": d.environment, "criticality": d.criticality, "application": app_name,
                                   "sample": False, "execution": "simulated"})
    o.store.conn.commit()
    o.store.conn.close()
    final = _proj_root() / pid
    if final.exists():
        raise HTTPException(409, "Project id clash, try again")
    shutil.move(str(_proj_root() / "_new"), str(final))
    o2 = Orchestrator.load(final, log=lambda m: None)
    o2.project_id, o2.app_name = pid, app_name
    o2.save_ctx()
    with _lock:
        _pool[pid] = o2
    return _summary(o2)


@app.get("/api/v1/projects/{pid}")
def project(pid: str) -> dict:
    o = orc(pid)
    return _summary(o)


@app.patch("/api/v1/projects/{pid}")
def update_project(pid: str, d: dict) -> dict:
    o = orc(pid)
    allowed = {"name", "customer", "owner", "aws_account", "aws_region", "gcp_project", "gcp_region", "application",
               "environment", "criticality"}
    bad = set(d) - allowed
    if bad:
        raise HTTPException(422, {"errors": {k: "This field cannot be changed." for k in bad}})
    _check(d)
    cfg = dict((o.store.get("projects", pid) or {}).get("config") or {})
    cols = {}
    for k, v in d.items():
        if k in ("environment", "criticality", "application"):
            cfg[k] = v
            if k == "application" and v:
                o.app_name = v
        else:
            cols[k] = v
    o.store.update("projects", pid, config=cfg, **cols)
    o.store.audit(pid, "console.user", "PROJECT_UPDATED", "project", pid, {"fields": sorted(d)})
    o.save_ctx()
    return _summary(o)


@app.post("/api/v1/projects/{pid}/uploads")
async def project_uploads(pid: str, files: list[UploadFile] = File(...)) -> dict:
    o = orc(pid)
    if (o.store.get("projects", pid).get("config") or {}).get("sample"):
        raise HTTPException(400, "The sample project is read-only. Create a project to upload your own files.")
    data = [(f.filename or "upload", await f.read()) for f in files]
    if sum(len(b) for _, b in data) > 50 * 1024 * 1024:
        raise HTTPException(413, "Upload is larger than 50 MB.")
    rows = o.add_files(data)
    return {"uploads": [{k: r.get(k) for k in ("id", "filename", "category", "file_type", "status", "error")} for r in rows],
            "accepted": sum(1 for r in rows if r.get("status") == "ACCEPTED"),
            "duplicates": sum(1 for r in rows if r.get("status") == "DUPLICATE"),
            "rejected": sum(1 for r in rows if r.get("status") == "REJECTED")}


@app.post("/api/v1/projects/{pid}/assess")
def project_assess(pid: str) -> dict:
    o = orc(pid)
    if (o.store.get("projects", pid).get("config") or {}).get("sample"):
        raise HTTPException(400, "The sample project is already assessed. Use Reset sample to rebuild it.")
    try:
        return o.run_assessment()
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@app.get("/api/v1/discovery/{pid}")
def discovery(pid: str) -> list[dict]:
    return orc(pid).store.all("resources", project_id=pid)


@app.get("/api/v1/mappings/{pid}")
def mappings(pid: str) -> list[dict]:
    return orc(pid).store.all("service_mappings", project_id=pid)


@app.post("/api/v1/mappings/{mid}/approve")
def approve_mapping(mid: str, d: Decision) -> dict:
    o = _owner_of("service_mappings", mid)
    if not o:
        raise HTTPException(404)
    m = o.store.get("service_mappings", mid)
    if d.decision not in ("APPROVED", "REJECTED"):
        raise HTTPException(400, "decision must be APPROVED or REJECTED")
    o.store.record_approval(m["project_id"], "service_mapping", mid, d.approver, d.decision, d.rationale)
    o.store.update("service_mappings", mid, approval_status=d.decision)
    for x in o.ctx["mappings"].values():
        if x.get("mapping_id") == mid:
            x["approval_status"] = d.decision
    o.intelligence()
    o.save_ctx()
    return o.store.get("service_mappings", mid)


class NewDataset(BaseModel):
    name: str
    source_upload: str = ""
    target_upload: str = ""
    date_column: str = ""
    key_column: str = ""
    source_access: str = "DIRECT"
    owner_name: str = ""
    owner_email: str = ""
    application: str | None = None      # None: let the tool choose; "": deliberately not linked
    range_start: str = ""
    range_end: str = ""


class TargetChange(BaseModel):
    target_upload: str = ""


class Confirmation(BaseModel):
    confirmer: str
    note: str
    outcome: str = "CONFIRMED"


class AvailDecision(BaseModel):
    approver: str
    decision: str
    rationale: str


class NewValidationObject(BaseModel):
    name: str
    key_column: str
    application: str = ""
    source_upload: str = ""
    target_upload: str = ""
    pii_columns: list[str] = []
    tolerances: dict[str, str] = {}


class Tolerances(BaseModel):
    tolerances: dict[str, str] = {}


class ExceptionUpdate(BaseModel):
    remediation_status: str
    owner: str = ""
    note: str = ""


class MonitoringNote(BaseModel):
    note: str


def _dq_do(oid: str, fn) -> dict:
    o = _owner_of("validation_objects", oid)
    if not o:
        raise HTTPException(404, "Validation object not found")
    try:
        result = fn(o)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    o.dq_changed()
    return result


@app.get("/api/v1/projects/{pid}/validation/objects")
def validation_objects(pid: str) -> dict:
    o = orc(pid)
    return {"objects": o.dq.list(), "summary": o.dq.summary()}


@app.post("/api/v1/projects/{pid}/validation/objects", status_code=201)
def add_validation_object(pid: str, d: NewValidationObject) -> dict:
    o = orc(pid)
    if (o.store.get("projects", pid).get("config") or {}).get("sample"):
        raise HTTPException(400, "The sample project is read-only. Create a project to add your own validation objects.")
    try:
        row = o.dq.add(**{**d.model_dump(), "source_upload": d.source_upload or None, "target_upload": d.target_upload or None,
                          "application": o.check_app(d.application)})
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    o.dq_changed()
    return row


@app.post("/api/v1/validation/objects/{oid}/target")
def dq_target(oid: str, t: TargetChange) -> dict:
    return _dq_do(oid, lambda o: o.dq.set_target(oid, t.target_upload))


@app.post("/api/v1/validation/objects/{oid}/tolerances")
def dq_tolerances(oid: str, t: Tolerances) -> dict:
    return _dq_do(oid, lambda o: o.dq.set_tolerances(oid, t.tolerances))


@app.post("/api/v1/validation/objects/{oid}/rerun")
def dq_rerun(oid: str) -> dict:
    return _dq_do(oid, lambda o: o.dq.run(oid))


@app.get("/api/v1/projects/{pid}/validation/exceptions")
def exceptions_queue(pid: str) -> list[dict]:
    return dq.all_exceptions(orc(pid).store, pid)


@app.post("/api/v1/projects/{pid}/validation/exceptions/{eid}")
def update_exception(pid: str, eid: str, u: ExceptionUpdate) -> dict:
    o = orc(pid)
    try:
        return dq.update_exception(o.store, pid, eid, u.remediation_status, u.owner, u.note, "console.user")
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@app.get("/api/v1/projects/{pid}/applications/{app}/monitoring-note")
def get_monitoring_note(pid: str, app: str) -> dict:
    return dq.get_note(orc(pid).store, pid, app)


@app.post("/api/v1/projects/{pid}/applications/{app}/monitoring-note")
def set_monitoring_note(pid: str, app: str, n: MonitoringNote) -> dict:
    return dq.set_note(orc(pid).store, pid, app, n.note, "console.user")


def _avail_do(did: str, fn) -> dict:
    o = _owner_of("availability_datasets", did)
    if not o:
        raise HTTPException(404, "Dataset not found")
    try:
        fn(o)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    o.availability_changed()
    return next(d for d in o.avail.list() if d["id"] == did)


@app.get("/api/v1/projects/{pid}/availability")
def availability(pid: str) -> dict:
    o = orc(pid)
    return {"datasets": o.avail.list(), "summary": o.avail.summary()}


@app.post("/api/v1/projects/{pid}/availability/datasets", status_code=201)
def add_dataset(pid: str, d: NewDataset) -> dict:
    o = orc(pid)
    if (o.store.get("projects", pid).get("config") or {}).get("sample"):
        raise HTTPException(400, "The sample project is read-only. Create a project to add your own datasets.")
    try:
        row = o.avail.add(**{**d.model_dump(), "source_upload": d.source_upload or None, "target_upload": d.target_upload or None,
                             "application": o.default_app() if d.application is None else o.check_app(d.application)})
        o.avail.run(row["id"])
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    o.availability_changed()
    return next(x for x in o.avail.list() if x["id"] == row["id"])


@app.post("/api/v1/projects/{pid}/availability/check")
def check_all(pid: str) -> dict:
    o = orc(pid)
    n = o.avail.run_all()
    o.availability_changed()
    return {"checked": n, "summary": o.avail.summary()}


@app.post("/api/v1/availability/{did}/check")
def check_one(did: str) -> dict:
    return _avail_do(did, lambda o: o.avail.run(did))


@app.post("/api/v1/availability/{did}/target")
def change_target(did: str, t: TargetChange) -> dict:
    def go(o):
        o.avail.set_target(did, t.target_upload or None)
        o.avail.run(did)
    return _avail_do(did, go)


class AppLink(BaseModel):
    application: str = ""


@app.post("/api/v1/availability/{did}/application")
def link_dataset(did: str, a: AppLink) -> dict:
    def go(o):
        if (o.store.get("projects", o.project_id).get("config") or {}).get("sample"):
            raise ValueError("The sample project is read-only.")
        o.avail.set_application(did, o.check_app(a.application))
    return _avail_do(did, go)


@app.post("/api/v1/availability/{did}/request-confirmation")
def ask_owner(did: str) -> dict:
    return _avail_do(did, lambda o: o.avail.request_confirmation(did))


@app.post("/api/v1/availability/{did}/confirm")
def confirm_source(did: str, c: Confirmation) -> dict:
    return _avail_do(did, lambda o: o.avail.confirm(did, c.confirmer, c.note, c.outcome))


@app.post("/api/v1/availability/{did}/decision")
def decide_availability(did: str, d: AvailDecision) -> dict:
    return _avail_do(did, lambda o: o.avail.decide(did, d.approver, d.decision, d.rationale))


@app.get("/api/v1/conversions/{cid}")
def conversion(cid: str) -> dict:
    c = next((c for o in [orc(), *_pool.values()] for c in o.ctx["conversions"] if c["id"] == cid), None)
    if not c:
        raise HTTPException(404)
    return c


@app.get("/api/v1/migration-runs/{rid}")
def run(rid: str) -> dict:
    o = _owner_of("migration_runs", rid) or orc()
    r = o.store.get("migration_runs", rid)
    if not r:
        raise HTTPException(404)
    r["batches"] = o.store.all("migration_batches", run_id=rid)
    return r


@app.get("/api/v1/validation/results/{vid}")
def validation(vid: str) -> dict:
    o = _owner_of("validation_runs", vid) or orc()
    v = o.store.get("validation_runs", vid)
    if not v:
        raise HTTPException(404)
    v["results"] = o.store.all("validation_results", validation_run_id=vid)
    v["exceptions"] = o.store.all("validation_exceptions", validation_run_id=vid)
    return v


@app.post("/api/v1/validation/results/{rid}/waive")
def waive(rid: str, w: Waiver) -> dict:
    o = orc()
    try:
        return ValidationEngine(o.store, o.project_id).waive(rid, w.approver, w.rationale, w.expiry)
    except (ValueError, TypeError) as e:
        raise HTTPException(400, str(e)) from e


class Signoff(BaseModel):
    role: str
    approver: str
    rationale: str


class RiskFollowup(BaseModel):
    tracking_status: str
    owner: str = ""
    note: str = ""


class OwnerOverride(BaseModel):
    name: str
    email: str = ""
    type: str = "BUSINESS_OWNER"


@app.post("/api/v1/projects/{pid}/applications/{app}/signoff")
def signoff(pid: str, app: str, s: Signoff) -> dict:
    o = orc(pid)
    try:
        row = o.record_signoff(app, s.role, s.approver, s.rationale)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    o.intelligence()
    o.save_ctx()
    return row


@app.post("/api/v1/projects/{pid}/applications/{app}/risk/{dimension}/followup")
def risk_followup(pid: str, app: str, dimension: str, f: RiskFollowup) -> dict:
    o = orc(pid)
    try:
        row = intelligence.set_risk_followup(o.store, pid, app, dimension, f.tracking_status, f.owner, f.note, "console.user")
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    o.intelligence()
    o.save_ctx()
    return row


@app.post("/api/v1/projects/{pid}/applications/{app}/owner")
def set_owner(pid: str, app: str, o_: OwnerOverride) -> dict:
    o = orc(pid)
    try:
        intelligence.set_owner_override(o.store, pid, app, o_.name, o_.email, o_.type, "console.user")
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    o.intelligence()
    o.save_ctx()
    return next(s for s in o.ctx["stakeholders"] if s["application"] == app)


@app.delete("/api/v1/projects/{pid}/applications/{app}/owner")
def clear_owner(pid: str, app: str) -> dict:
    o = orc(pid)
    intelligence.clear_owner_override(o.store, pid, app, "console.user")
    o.intelligence()
    o.save_ctx()
    return next((s for s in o.ctx["stakeholders"] if s["application"] == app), {})


# --------------------------------------------------------------------------- Round 6: communications and dependencies
class AppStatus(BaseModel):
    status: str
    by: str = ""
    note: str = ""


class EdgeConfirm(BaseModel):
    source: str
    target: str
    outcome: str
    by: str = ""
    note: str = ""


class Bridge(BaseModel):
    by: str = ""
    note: str = ""


class NoteApproval(BaseModel):
    approver: str = ""
    decision: str = "APPROVED"
    note: str = ""


class NoteEdit(BaseModel):
    subject: str = ""
    body: str = ""
    editor: str = ""


class NoteTrack(BaseModel):
    action: str
    by: str = ""
    note: str = ""
    outcome: str = ""
    channel: str = ""


def _reeval(o) -> None:
    if o.ctx.get("assessed_at"):
        o.intelligence()
        o.save_ctx()


@app.post("/api/v1/projects/{pid}/applications/{app}/migration-status")
def migration_status(pid: str, app: str, a: AppStatus) -> dict:
    o = orc(pid)
    if not o.ctx.get("assessed_at"):
        raise HTTPException(400, "Run the assessment first.")
    known = {n.get("application") for n in o.ctx["graph"]["nodes"] if n.get("application")}
    if app not in known:
        raise HTTPException(404, f"{app} is not an application in this project's dependency graph.")
    rk = o.ctx["risk"].get(app)
    try:
        row = comms.set_app_status(o.store, pid, app, a.status, a.by, a.note, rk["readiness"] if rk else None)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    _reeval(o)
    return {**row, "label": comms.APP_LABEL[row["status"]]}


@app.post("/api/v1/projects/{pid}/dependencies/confirm")
def confirm_dependency(pid: str, c: EdgeConfirm) -> dict:
    o = orc(pid)
    edges = {(e["source"], e["target"]) for e in o.ctx["graph"]["edges"]}
    try:
        row = comms.confirm_edge(o.store, pid, c.source, c.target, c.outcome, c.by, c.note, edges)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    _reeval(o)
    return row


@app.post("/api/v1/projects/{pid}/applications/{app}/bridges/{downstream}")
def agree_bridge(pid: str, app: str, downstream: str, b: Bridge) -> dict:
    o = orc(pid)
    if downstream not in o.ctx.get("downstream_apps", {}).get(app, []):
        raise HTTPException(400, f"{downstream} is not downstream of {app} in the dependency graph.")
    try:
        row = comms.agree_bridge(o.store, pid, app, downstream, b.by, b.note)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    _reeval(o)
    return row


@app.delete("/api/v1/projects/{pid}/applications/{app}/bridges/{downstream}")
def clear_bridge(pid: str, app: str, downstream: str) -> dict:
    o = orc(pid)
    row = comms.agree_bridge(o.store, pid, app, downstream, "console.user", "", clear=True)
    _reeval(o)
    return row


def _note_do(pid: str, fn) -> dict:
    o = orc(pid)
    try:
        return fn(o)
    except KeyError as e:
        raise HTTPException(404, "Draft not found") from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@app.post("/api/v1/projects/{pid}/notifications/{nid}/approve")
def approve_notification(pid: str, nid: str, a: NoteApproval) -> dict:
    return _note_do(pid, lambda o: comms.approve(o.store, pid, nid, a.approver, a.decision, a.note))


@app.post("/api/v1/projects/{pid}/notifications/{nid}/edit")
def edit_notification(pid: str, nid: str, e: NoteEdit) -> dict:
    return _note_do(pid, lambda o: comms.edit(o.store, pid, nid, e.subject, e.body, e.editor))


@app.post("/api/v1/projects/{pid}/notifications/{nid}/track")
def track_notification(pid: str, nid: str, t: NoteTrack) -> dict:
    return _note_do(pid, lambda o: comms.track(o.store, pid, nid, t.action, t.by, t.note, t.outcome, t.channel))


# --------------------------------------------------------------------------- Round 7: load controls and conversion extras
class Review(BaseModel):
    approver: str = ""
    decision: str = "APPROVED"
    note: str = ""


class CodeEdit(BaseModel):
    path: str
    content: str
    by: str = ""
    note: str = ""


class LoadControl(BaseModel):
    plan_id: str
    max_parallel: int
    by: str = ""
    note: str = ""


class SourceChange(BaseModel):
    source_upload: str = ""
    target_upload: str | None = None     # set both in one step, so the check runs once and drift is measured once


def _conv_do(pid: str, fn):
    o = orc(pid)
    try:
        out = fn(o)
    except KeyError as e:
        raise HTTPException(404, "Conversion version not found") from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    _reeval(o)
    return out


@app.post("/api/v1/projects/{pid}/conversions/{cid}/review")
def review_conversion(pid: str, cid: str, r: Review) -> dict:
    return _conv_do(pid, lambda o: codehistory.view(next(v for v in codehistory.versions(
        o.store, pid, codehistory.decide(o.store, pid, cid, r.approver, r.decision, r.note)["source_file"]) if v["id"] == cid)))


@app.post("/api/v1/projects/{pid}/conversions/{cid}/edit")
def edit_conversion(pid: str, cid: str, e: CodeEdit) -> dict:
    def go(o):
        row = codehistory.edit(o.store, pid, cid, e.path, e.content, e.by, e.note, o.workdir)
        base = next((c for c in o.ctx["conversions"] if c["source_file"] == row["source_file"]), {})
        tests, files = codecheck.run_tests({**base, "outputs": row["outputs"], "source_file": row["source_file"]})
        o.store.update("conversion_runs", row["id"], tests={"results": tests, "files": files})
        return {"id": row["id"], "tests": codecheck.summary(tests)}
    return _conv_do(pid, go)


@app.get("/api/v1/projects/{pid}/conversions/{a}/diff/{b}")
def diff_conversion(pid: str, a: str, b: str) -> dict:
    o = orc(pid)
    try:
        return codehistory.diff(o.store, pid, a, b)
    except KeyError as e:
        raise HTTPException(404, "Conversion version not found") from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@app.post("/api/v1/projects/{pid}/load-controls")
def load_controls(pid: str, c: LoadControl) -> dict:
    o = orc(pid)
    try:
        row = loadops.set_parallel(o.store, pid, c.plan_id, c.max_parallel, c.by, c.note)
    except KeyError as e:
        raise HTTPException(404, "Plan not found") from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"plan_id": row["id"], "max_parallel": row["config"]["max_parallel_batches"]}


@app.post("/api/v1/availability/{did}/source")
def change_source(did: str, t: SourceChange) -> dict:
    def go(o):
        if (o.store.get("projects", o.project_id).get("config") or {}).get("sample"):
            raise ValueError("The sample project is read-only.")
        o.avail.set_source(did, t.source_upload)
        if t.target_upload is not None:
            o.avail.set_target(did, t.target_upload or None)
        o.avail.run(did)
    return _avail_do(did, go)


@app.get("/api/v1/dependencies/{pid}")
def deps(pid: str) -> dict:
    return orc(pid).ctx["graph"]


@app.get("/api/v1/risks/{pid}")
def risks(pid: str) -> dict:
    return orc(pid).ctx["risk"]


@app.get("/api/v1/stakeholders/{pid}")
def stakeholders(pid: str) -> list[dict]:
    return orc(pid).store.all("stakeholders", project_id=pid)


@app.get("/api/v1/catalogue")
def cat() -> list[dict]:
    return catalogue.catalogue()


@app.get("/api/v1/audit/verify")
def verify(project: str | None = Query(None)) -> dict:
    return orc(project).store.verify_audit_chain()


@app.get("/api/v1/reports/{name}")
def report(name: str, project: str | None = Query(None)):
    o = orc(project)
    p = (o.workdir / "reports" / name).resolve()
    if not str(p).startswith(str((o.workdir / "reports").resolve())) or not p.exists():
        raise HTTPException(404)
    return FileResponse(p)


@app.post("/api/v1/analyze")
async def analyze(file: UploadFile = File(...)) -> dict:
    """Live demo: drop ANY supported artefact -> security checks, deterministic extraction, and conversion preview."""
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / Path(file.filename or "upload").name
        p.write_bytes(await file.read())
        try:
            cat_, ftype = classify(p)
            check_content(p, ftype)
        except RejectedFile as e:
            return {"filename": p.name, "status": "REJECTED", "reason": str(e)}
        out = {"filename": p.name, "status": "ACCEPTED", "category": cat_, "file_type": ftype, "sha256": sha256_file(p)}
        try:
            out["evidence"] = parsers.extract(p, cat_, ftype)[:200]
        except Exception as e:  # noqa: BLE001
            out["evidence_error"] = str(e)
        if ftype in CONVERTERS:
            try:
                out["conversion"] = CONVERTERS[ftype](p).to_dict()
            except Exception as e:  # noqa: BLE001
                out["conversion_error"] = str(e)
        return out
