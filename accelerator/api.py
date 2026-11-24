"""FastAPI backend (blueprint 9.1). Serves the console UI and REST endpoints.

Run:  uvicorn accelerator.api:app --reload      (UI at http://localhost:8000)
Long-running work returns run IDs; the prototype executes synchronously in-process (production: Celery/Cloud Tasks).
"""
from __future__ import annotations

import json
import shutil
import tempfile
import threading
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel

from . import catalogue, parsers
from .ingestion import RejectedFile, check_content, classify, sha256_file
from .orchestrator import CONVERTERS, ROOT, Orchestrator
from .validation import ValidationEngine

WORK = ROOT / "runs" / "api"
UI = ROOT / "ui" / "index.html"
app = FastAPI(title="AWS→GCP Migration Accelerator", version="1.1.0")
_lock = threading.Lock()
_orc: Orchestrator | None = None


def orc() -> Orchestrator:
    global _orc
    with _lock:
        if _orc is None:
            if WORK.exists():
                shutil.rmtree(WORK)
            _orc = Orchestrator(WORK, log=lambda m: None)
            _orc.run_all()
        return _orc


def _state() -> dict:
    o = orc()
    st = o.state()
    st["reports"] = [str(p.relative_to(o.workdir)) for p in (o.workdir / "reports").glob("*")]
    return st


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
def state() -> JSONResponse:
    return JSONResponse(json.loads(json.dumps(_state(), default=str)))


@app.post("/api/v1/demo/run")
def rerun() -> dict:
    global _orc
    with _lock:
        _orc = None
    o = orc()
    return {"project_id": o.project_id, "status": "COMPLETED"}


@app.get("/api/v1/projects")
def projects() -> list[dict]:
    return orc().store.all("projects")


@app.get("/api/v1/projects/{pid}")
def project(pid: str) -> dict:
    p = orc().store.get("projects", pid)
    if not p:
        raise HTTPException(404)
    return p


@app.get("/api/v1/discovery/{pid}")
def discovery(pid: str) -> list[dict]:
    return orc().store.all("resources", project_id=pid)


@app.get("/api/v1/mappings/{pid}")
def mappings(pid: str) -> list[dict]:
    return orc().store.all("service_mappings", project_id=pid)


@app.post("/api/v1/mappings/{mid}/approve")
def approve_mapping(mid: str, d: Decision) -> dict:
    o = orc()
    m = o.store.get("service_mappings", mid)
    if not m:
        raise HTTPException(404)
    if d.decision not in ("APPROVED", "REJECTED"):
        raise HTTPException(400, "decision must be APPROVED or REJECTED")
    o.store.record_approval(m["project_id"], "service_mapping", mid, d.approver, d.decision, d.rationale)
    o.store.update("service_mappings", mid, approval_status=d.decision)
    for x in o.ctx["mappings"].values():
        if x.get("mapping_id") == mid:
            x["approval_status"] = d.decision
    o.intelligence()
    return o.store.get("service_mappings", mid)


@app.get("/api/v1/conversions/{cid}")
def conversion(cid: str) -> dict:
    c = next((c for c in orc().ctx["conversions"] if c["id"] == cid), None)
    if not c:
        raise HTTPException(404)
    return c


@app.get("/api/v1/migration-runs/{rid}")
def run(rid: str) -> dict:
    o = orc()
    r = o.store.get("migration_runs", rid)
    if not r:
        raise HTTPException(404)
    r["batches"] = o.store.all("migration_batches", run_id=rid)
    return r


@app.get("/api/v1/validation/results/{vid}")
def validation(vid: str) -> dict:
    o = orc()
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


@app.get("/api/v1/dependencies/{pid}")
def deps(pid: str) -> dict:
    return orc().ctx["graph"]


@app.get("/api/v1/risks/{pid}")
def risks(pid: str) -> dict:
    return orc().ctx["risk"]


@app.get("/api/v1/stakeholders/{pid}")
def stakeholders(pid: str) -> list[dict]:
    return orc().store.all("stakeholders", project_id=pid)


@app.get("/api/v1/catalogue")
def cat() -> list[dict]:
    return catalogue.catalogue()


@app.get("/api/v1/audit/verify")
def verify() -> dict:
    return orc().store.verify_audit_chain()


@app.get("/api/v1/reports/{name}")
def report(name: str):
    p = (orc().workdir / "reports" / name).resolve()
    if not str(p).startswith(str((orc().workdir / "reports").resolve())) or not p.exists():
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
