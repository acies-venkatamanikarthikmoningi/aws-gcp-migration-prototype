"""Migration Orchestrator – runs the end-to-end journey (blueprint 11.1) with specialised agents.

Agents are deterministic services; each run is recorded in agent_runs. Approvals in the demo are SIMULATED and
labelled as such – in production they are captured from authorised users through the UI/API.
"""
from __future__ import annotations

import json
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from . import catalogue, intelligence, mapping, normalize, parsers, planner
from .ai import provider
from .converters import pipelines, python_sdk, sql_redshift, terraform
from . import dq
from .availability import Availability
from .dq import ValidationObjects
from . import codecheck, codehistory, comms, loadops
from .ingestion import classify as classify_file, ingest_tree
from .migration_engine import DataMigrationEngine
from .store import Store, new_id, now
from .validation import ValidationEngine, profile_orders

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "sample_data"
SIMULATED = " (SIMULATED demo approval)"

CONVERTERS: dict[str, Callable[[Path], Any]] = {
    "terraform": terraform.convert_terraform,
    "cloudformation": terraform.convert_cloudformation,
    "sql": sql_redshift.convert_sql,
    "lambda_python": lambda p: python_sdk.convert_python_app(p, "lambda_python"),
    "python": lambda p: python_sdk.convert_python_app(p, "python"),
    "glue_pyspark": pipelines.convert_glue,
    "airflow_dag": pipelines.convert_airflow,
    "stepfunctions": pipelines.convert_step_functions,
    "iam_policy": pipelines.convert_iam_policy,
    "dynamodb_export": pipelines.convert_dynamodb_export,
}


class Orchestrator:
    def __init__(self, workdir: Path, log: Callable[[str], None] = print):
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.store = Store(self.workdir / "metadata.db")
        self.log = log
        self.llm = provider()
        self.project_id: str | None = None
        self.app_name = "CustomerAnalytics"
        self.ctx: dict[str, Any] = self.empty_ctx()

    # ---------------------------------------------------------------- persistence (one workspace folder per project)
    @staticmethod
    def empty_ctx() -> dict[str, Any]:
        """Pipeline state of a project that has not been assessed yet. Every screen can render from this."""
        return {"uploads": [], "resources": [], "relationships": [], "consumers": [], "evidence": [], "owners": [],
                "requirements": [], "findings": [], "needs_review": [], "mappings": {}, "conversions": [],
                "discovery_stats": {"resources": 0, "relationships": 0, "consumers": 0, "conflicts": 0,
                                    "by_status": {"CORROBORATED": 0, "INVENTORY_ONLY": 0, "IAC_ONLY": 0,
                                                  "DIAGRAM_OR_DOC_ONLY": 0, "CONFLICT": 0}},
                "plan": {"retention_months": None, "movement": [], "waves": [], "strategy_summary": {},
                         "seven_rs": mapping.SEVEN_RS},
                "migration": {"plan_id": None, "backfill_plan": None, "story": [], "validations": {}},
                "graph": {"nodes": [], "edges": []}, "stakeholders": [], "risk": {}, "notifications": [], "impacts": {},
                "assessed_at": None}

    @property
    def avail(self) -> Availability:
        return Availability(self.store, self.project_id, self.workdir / "vault")

    @property
    def dq(self) -> ValidationObjects:
        return ValidationObjects(self.store, self.project_id)

    def availability_changed(self) -> None:
        """Re-evaluate readiness gates after any availability action, and persist."""
        if self.ctx.get("assessed_at"):
            self.intelligence()
            self.save_ctx()

    def discovered_apps(self) -> list[str]:
        """Application names carried by the discovered resources (IaC tags or inference). These, not the project's own
        name, are the keys every readiness gate is computed against."""
        return sorted({r.get("application") for r in self.ctx.get("resources", []) if r.get("application")})

    def default_app(self) -> str:
        """Which application a new dataset or validation object belongs to when the person does not say: the project's
        configured application if the assessment found it, else the only application found, else none (left unlinked
        rather than guessed)."""
        apps = self.discovered_apps()
        if self.app_name in apps:
            return self.app_name
        return apps[0] if len(apps) == 1 else ""

    def check_app(self, application: str) -> str:
        application = (application or "").strip()
        apps = self.discovered_apps()
        if application and apps and application not in apps:
            raise ValueError(f"{application} is not one of the applications the assessment found: {', '.join(apps)}.")
        return application

    def dq_changed(self) -> None:
        """Re-evaluate readiness gates after a validation object is added, re-run or edited, and persist."""
        if self.ctx.get("assessed_at"):
            self.intelligence()
            self.save_ctx()
        self.save_ctx()

    def seed_availability(self) -> None:
        """Sample project only: three datasets that show the three outcomes. The target copies are built here, on purpose,
        with planted gaps, and every dataset says so."""
        import csv as _csv
        d = self.workdir / "availability_sample"
        d.mkdir(exist_ok=True)
        rows = []
        for part in sorted((SAMPLE / "data" / "source_orders").glob("order_month=*/*.csv")):
            with open(part, newline="") as f:
                rows += list(_csv.DictReader(f))
        cols = list(rows[0])
        def dump(name, rs):
            with open(d / name, "w", newline="") as f:
                w = _csv.DictWriter(f, fieldnames=cols)
                w.writeheader()
                w.writerows(rs)
            return str(d / name)
        src = dump("orders_staging_copy.csv", rows)
        tgt = dump("orders_in_bigquery.csv", [r for r in rows if not r["order_date"].startswith("2025-04")
                                              and not (r["order_date"].startswith("2025-09") and int(r["order_id"]) % 40 == 0)])
        note = "Sample: the target copy was built with a missing month and some missing rows so the check has something to find."
        a = self.avail
        a.add(name="analytics.orders (store files)", source_path=src, target_path=tgt, source_label="Store uploads, S3 staging bucket (sample)",
              target_label="BigQuery export (sample)", date_column="order_date", key_column="order_id", source_access="STAGING_ONLY",
              owner_name="Retail Operations (sample)", owner_email="retail-ops@example.com", application=self.app_name, seeded=note)
        a.add(name="analytics.customer", source_path=str(SAMPLE / "data" / "source_customer.csv"), target_path=str(SAMPLE / "data" / "target_customer.csv"),
              source_label="source_customer.csv (sample)", target_label="target_customer.csv (sample)", key_column="customer_id", source_access="DIRECT",
              application=self.app_name, seeded="Sample customer files.")
        a.add(name="analytics.orders incremental changes", source_path=str(SAMPLE / "data" / "incremental" / "orders_changes_after_watermark.csv"),
              target_path=None, source_label="orders_changes_after_watermark.csv (sample)", target_label="", date_column="updated_at",
              key_column="order_id", source_access="DIRECT", application=self.app_name,
              seeded="Sample: no target copy exists yet, so this dataset shows the missing outcome.")
        a.run_all("orchestrator")

    def save_ctx(self) -> None:
        """Write the pipeline state next to the database so a restart loses nothing. Atomic replace."""
        tmp = self.workdir / "ctx.json.tmp"
        tmp.write_text(json.dumps({"project_id": self.project_id, "app_name": self.app_name, "ctx": self.ctx}, default=str),
                       encoding="utf-8")
        tmp.replace(self.workdir / "ctx.json")

    @classmethod
    def load(cls, workdir: Path, log: Callable[[str], None] = lambda m: None) -> "Orchestrator":
        o = cls(workdir, log=log)
        f = o.workdir / "ctx.json"
        if f.exists():
            d = json.loads(f.read_text(encoding="utf-8"))
            o.project_id, o.app_name = d.get("project_id"), d.get("app_name", o.app_name)
            o.ctx = {**cls.empty_ctx(), **d.get("ctx", {})}
        else:
            row = o.store.all("projects")
            o.project_id = row[0]["id"] if row else None
        return o

    # ---------------------------------------------------------------- helpers
    def _agent(self, name: str, fn: Callable[[], Any], input_ref: str = "") -> Any:
        rid = new_id("AGT-")
        self.store.insert("agent_runs", {"id": rid, "project_id": self.project_id, "agent_name": name, "input_ref": input_ref,
                                         "model_version": f"deterministic rules; llm={self.llm.name}", "status": "RUNNING",
                                         "started_at": now()})
        try:
            out = fn()
            self.store.update("agent_runs", rid, status="COMPLETED", ended_at=now(),
                              output_ref=f"{type(out).__name__}:{len(out) if hasattr(out, '__len__') else 1}")
            self.log(f"  ✓ {name}")
            return out
        except Exception as e:
            self.store.update("agent_runs", rid, status="FAILED", ended_at=now(), output_ref=str(e)[:200])
            raise

    # ---------------------------------------------------------------- phase 1
    def create_project(self, **kw) -> str:
        """Create the project row. With no arguments this is the built-in sample project."""
        pid = new_id("PRJ-")
        self.project_id = pid
        cfg = {"environment": "Prod", "criticality": "High", "wave_plan": "auto", "bandwidth_mbps": 1000,
               "application": "CustomerAnalytics", "sample": True, "execution": "simulated"}
        cfg.update(kw.get("config") or {})
        self.app_name = cfg.get("application") or kw.get("name") or self.app_name
        self.store.insert("projects", {"id": pid, "customer": kw.get("customer", "Carrier Global (prototype)"),
                                       "name": kw.get("name", "Customer Analytics – AWS to GCP"), "source_cloud": "AWS", "target_cloud": "GCP",
                                       "aws_account": kw.get("aws_account", "111122223333 (synthetic)"),
                                       "aws_region": kw.get("aws_region", "us-east-1"),
                                       "gcp_project": kw.get("gcp_project", "cg-migration-proto"),
                                       "gcp_region": kw.get("gcp_region", "us-east1"), "status": kw.get("status", "ASSESSING"),
                                       "owner": kw.get("owner", "Acies Global – Data Foundation"), "config": cfg,
                                       "created_at": now()})
        self.store.audit(pid, kw.get("actor", "demo.user"), "PROJECT_CREATED", "project", pid,
                         {"name": kw.get("name", "Customer Analytics – AWS to GCP")})
        return pid

    # ---------------------------------------------------------------- user projects: add files, then assess
    def add_files(self, files: list[tuple[str, bytes]], user: str = "console.user") -> list[dict]:
        """Validate and vault uploaded files. Archives are unpacked safely. Returns one upload row per file."""
        import tempfile
        vault = self.workdir / "vault"
        rows: list[dict] = []
        with tempfile.TemporaryDirectory() as td:
            for i, (name, data) in enumerate(files):
                d = Path(td) / str(i)
                d.mkdir()
                (d / (Path(name).name or f"upload-{i}")).write_bytes(data)
            rows = ingest_tree(self.store, self.project_id, Path(td), vault, user)
        for r in rows:
            r.pop("original_path", None)
        self.ctx["uploads"] = self.store.all("uploads", project_id=self.project_id)
        self.save_ctx()
        return rows

    def _reset_assessment(self) -> dict:
        """Clear everything derived from the uploads so the assessment can be re-run. Uploads, approvals and the audit
        log stay. Returns the earlier mapping decisions so they can be carried over."""
        S, pid = self.store, self.project_id
        prev = {}
        for m in S.all("service_mappings", project_id=pid):
            if m["approval_status"] in ("APPROVED", "REJECTED"):
                a = [x for x in S.all("approval_requests", project_id=pid, object_id=m["id"])]
                last = a[-1] if a else {}
                prev[(m["resource_id"], m["target_service"])] = {"id": m["id"], "status": m["approval_status"],
                                                                 "approver": last.get("approver", "unknown"),
                                                                 "rationale": last.get("rationale", "")}
        for t in ("resources", "resource_relationships", "extracted_evidence", "service_mappings", "stakeholders", "risk_items"):
            S.delete_where(t, project_id=pid)
        S.conn.execute("DELETE FROM notifications WHERE project_id=? AND event!='SOURCE_CONFIRMATION_REQUEST' "
                       "AND tracking IS NULL AND approval_rec IS NULL AND edited IS NULL", (pid,))
        S.conn.commit()
        keep = {k: self.ctx[k] for k in ("uploads",)}
        self.ctx = {**self.empty_ctx(), **keep}
        return prev

    def run_assessment(self) -> dict:
        """Discovery, mapping, conversion, planning, dependencies, risk and drafts for the uploaded files.
        No cloud data is touched, nothing is approved by the tool, and no data load or validation is invented."""
        S, pid = self.store, self.project_id
        if not [u for u in S.all("uploads", project_id=pid) if u["status"] == "ACCEPTED"]:
            raise ValueError("Add at least one accepted file before running the assessment.")
        prev = self._reset_assessment()
        self.ctx["uploads"] = S.all("uploads", project_id=pid)
        self._agent("Discovery Agent + Architecture Agent (parallel extraction, normalization)", self.extract_and_normalize)
        self._agent("Mapping Agent", lambda: self.map_services(demo_approve=False))
        carried = 0
        for m in self.ctx["mappings"].values():
            old = prev.get((m["resource_id"], m["target_service"]))
            if old:
                S.record_approval(pid, "service_mapping", m["mapping_id"], old["approver"], old["status"],
                                  f"Carried over from {old['id']} after re-assessment (same resource and target). {old['rationale']}".strip())
                S.update("service_mappings", m["mapping_id"], approval_status=old["status"])
                m["approval_status"] = old["status"]
                carried += 1
        self._agent("Conversion Agent", self.convert)
        self._agent("Migration Planning Agent", self.plan)
        self._agent("Dependency Agent + Risk Agent + Communication Agent", self.intelligence)
        self.ctx["assessed_at"] = now()
        S.update("projects", pid, status="ASSESSED")
        S.audit(pid, "orchestrator", "ASSESSMENT_COMPLETED", "project", pid,
                {"resources": len(self.ctx["resources"]), "mappings": len(self.ctx["mappings"]), "decisions_carried_over": carried})
        st = self.state()
        from .reporting import write_reports
        try:
            paths = self._agent("Reporting", lambda: write_reports(st, self.workdir / "reports"))
            self.ctx["report_files"] = [str(p.relative_to(self.workdir)) for p in paths]
        except Exception as e:  # noqa: BLE001  reports must never block an assessment
            self.ctx["report_files"] = []
            self.log(f"reports skipped: {e}")
        self.save_ctx()
        return {"resources": len(self.ctx["resources"]), "mappings": len(self.ctx["mappings"]),
                "conversions": len(self.ctx["conversions"]), "decisions_carried_over": carried}

    def _negative_samples(self) -> Path:
        d = self.workdir / "negative_tests"
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
        (d / "installer.exe").write_bytes(b"MZ\x90\x00fake")
        with zipfile.ZipFile(d / "traversal.zip", "w") as z:
            z.writestr("../../etc/passwd_overwrite.txt", "x")
        (d / "fake_inventory.xlsx").write_text("this is not a spreadsheet")
        (d / "malware_sample.txt").write_text("harmless demo payload ACCEL-DEMO-MALWARE-SIGNATURE")
        return d

    def ingest(self, source_dir: Path = SAMPLE) -> list[dict]:
        vault = self.workdir / "vault"
        rows = ingest_tree(self.store, self.project_id, source_dir, vault)
        rows += ingest_tree(self.store, self.project_id, self._negative_samples(), vault)
        self.ctx["uploads"] = rows
        return rows

    # ---------------------------------------------------------------- phase 2-3
    def extract_and_normalize(self) -> dict:
        ups = [u for u in self.store.all("uploads", project_id=self.project_id) if u["status"] == "ACCEPTED"]
        names: set[str] = set()

        for i, u in enumerate(ups):
            # uploads classified before CSV headers were checked: a data file read as an AWS inventory is re-labelled here
            if u["file_type"] == "csv" and u["category"] == "inventory":
                try:
                    cat, _ = classify_file(Path(u["storage_path"]))
                except Exception:  # noqa: BLE001
                    cat = u["category"]
                if cat != u["category"]:
                    self.store.update("uploads", u["id"], category=cat)
                    ups[i] = {**u, "category": cat}

        def run(u):
            if u["category"] == "data" and u["file_type"] != "dynamodb_export":
                return u, []
            path = Path(u["storage_path"])
            try:
                return u, parsers.extract(path, u["category"], u["file_type"], None)
            except Exception as e:  # noqa: BLE001
                return u, [parsers.ev("needs_review", {"reason": f"extraction failed: {e}"}, 0.0, u["filename"])]

        with ThreadPoolExecutor(max_workers=6) as ex:  # blueprint: extraction runs in parallel
            results = list(ex.map(run, ups))
        evidence = []
        for u, evs in results:
            for e in evs:
                e.update(category=u["category"], upload_id=u["id"])
                self.store.insert("extracted_evidence", {"id": new_id("EVD-"), "project_id": self.project_id, "upload_id": u["id"],
                                                         "entity_type": e["entity_type"], "extracted_data": e["data"],
                                                         "confidence": e["confidence"], "source_location": e["source_location"],
                                                         "extractor_version": parsers.EXTRACTOR_VERSION})
                evidence.append(e)
        resources, rels, extra = normalize.resolve(evidence)
        for r in resources:
            self.store.insert("resources", {"id": new_id("RES-"), "project_id": self.project_id, "resource_id": r["resource_id"], "name": r["name"],
                                            "service": r["service"], "category": r["category"], "region": r["region"],
                                            "application": r.get("application"), "environment": r.get("environment"),
                                            "criticality": r.get("criticality"), "configuration": r["configuration"],
                                            "evidence": r["evidence"], "conflicts": r["conflicts"], "confidence": r["confidence"],
                                            "evidence_status": r["evidence_status"]})
            names.add(r["name"])
        for rel in rels:
            self.store.insert("resource_relationships", {"id": new_id("REL-"), "project_id": self.project_id, "source": rel["source"],
                                                         "target": rel["target"], "relationship_type": rel["relationship_type"],
                                                         "confidence": rel["confidence"], "status": rel["status"], "evidence": rel})
        self.ctx.update(resources=resources, relationships=rels, consumers=extra["consumers"], evidence=evidence,
                        discovery_stats=extra["stats"])
        self.ctx["owners"] = [e["data"] for e in evidence if e["entity_type"] == "owner"]
        self.ctx["requirements"] = [e for e in evidence if e["entity_type"] == "requirement"]
        self.ctx["findings"] = [{**e["data"], "location": e["source_location"], "application": self.app_name}
                                for e in evidence if e["entity_type"] == "secret_finding"]
        self.ctx["needs_review"] = [e for e in evidence if e["entity_type"] == "needs_review"]
        self.store.audit(self.project_id, "discovery-agent", "DISCOVERY_COMPLETED", "project", self.project_id, extra["stats"])
        return extra["stats"]

    # ---------------------------------------------------------------- phase 4
    def map_services(self, demo_approve: bool = True) -> list[dict]:
        out = []
        for r in self.ctx["resources"]:
            m = mapping.recommend(r)
            if self.llm.name.startswith("vertex"):
                m["ai_explanation"] = self.llm.explain(json.dumps({"resource": r["name"], "mapping": m}))
            row = self.store.insert("service_mappings", {"id": new_id("MAP-"), "project_id": self.project_id, "resource_id": r["resource_id"],
                                                         "aws_service": m["aws_service"], "target_service": m["target_service"],
                                                         "alternatives": m["alternatives"], "strategy": m["strategy"], "score": m["score"],
                                                         "rationale": json.dumps(m["rationale"]), "constraints": m["constraints"],
                                                         "effort": m["effort"], "catalogue_version": m["catalogue_version"],
                                                         "approval_status": "PENDING"})
            m["mapping_id"], m["approval_status"] = row["id"], "PENDING"
            out.append(m)
        if demo_approve:
            # Business decisions intentionally left PENDING: they need customer input (licensing, partner selection)
            hold = {"RDS Oracle": "Oracle licensing/target decision (BMS vs Oracle@GC vs AlloyDB) needs customer architecture board",
                    "IoT Core": "Partner MQTT broker selection required (no first-party GCP IoT Core)"}
            res_by_id = {r["resource_id"]: r for r in self.ctx["resources"]}
            for m in out:
                if m["aws_service"] in hold:
                    m["hold_reason"] = hold[m["aws_service"]]
                    continue
                if m["target_service"] == "UNKNOWN":
                    continue
                r = res_by_id[m["resource_id"]]
                why = "Recommended target accepted"
                if r["conflicts"]:
                    why = ("Conflict resolved: IaC (Terraform) declared value treated as intended state; inventory refresh requested – "
                           + "; ".join(c["attribute"] for c in r["conflicts"]))
                self.store.record_approval(self.project_id, "service_mapping", m["mapping_id"], "Solution Architect" + SIMULATED, "APPROVED", why)
                self.store.update("service_mappings", m["mapping_id"], approval_status="APPROVED")
                m["approval_status"] = "APPROVED"
        self.ctx["mappings"] = {m["resource_id"]: m for m in out}
        return out

    # ---------------------------------------------------------------- phase 5
    def convert(self) -> list[dict]:
        ver = f"v{len(self.store.all('conversion_runs', project_id=self.project_id)) + 1}"
        outdir = self.workdir / "output" / "conversions" / ver
        approved = {m["aws_service"] for m in self.ctx["mappings"].values() if m["approval_status"] == "APPROVED"}
        results = []
        for u in self.store.all("uploads", project_id=self.project_id):
            if u["status"] != "ACCEPTED" or u["file_type"] not in CONVERTERS:
                continue
            if u["file_type"] == "python" and "boto3" not in Path(u["storage_path"]).read_text(encoding="utf-8", errors="ignore"):
                continue
            tmp = Path(u["storage_path"])  # converters only READ the vaulted original; outputs go to a versioned area
            res = CONVERTERS[u["file_type"]](tmp)
            for rel, content in res.outputs.items():
                p = outdir / rel
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(content, encoding="utf-8")
            d = res.to_dict()
            d.update(source_text=tmp.read_text(encoding="utf-8", errors="ignore")[:20000] if u["file_type"] != "dynamodb_export" else "",
                     upload_id=u["id"], file_type=u["file_type"], version=ver, application=self.app_name,
                     gated_by="approved mappings: " + ", ".join(sorted(approved & {"Redshift", "S3", "EC2", "Lambda", "Glue", "Step Functions",
                                                                                     "MWAA", "IAM", "DynamoDB", "RDS PostgreSQL", "SQS"})))
            tests, test_files = codecheck.run_tests({**d, "source_file": u["filename"]})
            row = self.store.insert("conversion_runs", {"id": new_id("CNV-"), "project_id": self.project_id, "source_file": u["filename"],
                                                        "converter": res.converter, "converter_version": "conv-1.1.0", "output_path": str(outdir),
                                                        "confidence": res.confidence, "warnings": res.warnings, "unsupported": res.unsupported,
                                                        "checks": res.checks, "status": "GENERATED", "approval_status": "PENDING_REVIEW",
                                                        "created_at": now(), "upload_id": u["id"], "version": ver, "kind": "GENERATED",
                                                        "outputs": res.outputs, "outputs_hash": codehistory.outputs_hash(res.outputs),
                                                        "tests": {"results": tests, "files": test_files}})
            codehistory.carry_approval(self.store, self.project_id, row)
            d["id"] = row["id"]
            self.store.audit(self.project_id, "conversion-agent", "CODE_GENERATED", "conversion_run", row["id"],
                             {"file": u["filename"], "confidence": res.confidence, "auto_deployed": False})
            results.append(d)
        self.ctx["conversions"] = results
        return results

    # ---------------------------------------------------------------- phase 6
    def plan(self) -> dict:
        reqs = self.ctx["requirements"]
        retention = next((r["data"]["value"] for r in reqs if r["data"]["kind"] == "retention_months"), None)
        downtime = {r["data"]["text"].split()[0].lower(): planner.parse_downtime(r["data"]["value"])
                    for r in reqs if r["data"]["kind"] == "downtime_tolerance"}
        wh_dt = next((v for k, v in downtime.items() if k in ("downtime",)), None)
        movement = []
        for r in self.ctx["resources"]:
            m = self.ctx["mappings"].get(r["resource_id"], {})
            if r["category"] not in ("storage", "database", "analytics", "streaming", "compute", "ai_ml", "security") or m.get("strategy") in ("Retire", "Retain"):
                continue
            if r["service"] in ("Athena", "IAM", "KMS", "VPC"):
                continue
            cfg = r["configuration"].get("inventory", {}) | r["configuration"].get("inventory_json", {})
            size = float(cfg.get("size_gb") or cfg.get("SizeGB") or 0)
            dt = downtime.get(r["name"].lower()) if r["name"].lower() in downtime else wh_dt if r["service"] == "Redshift" else None
            svc = m.get("aws_service", r["service"])
            mv = planner.choose_movement(svc, size, dt)
            mv.update(resource_id=r["resource_id"], name=r["name"], downtime_hours=dt,
                      downtime_source="application_specification.docx" if dt is not None else "UNKNOWN – request from owner")
            movement.append(mv)
        waves = planner.plan_waves(self.ctx["resources"], self.ctx["relationships"], self.ctx["mappings"], self.ctx["consumers"])
        self.ctx["plan"] = {"retention_months": retention, "movement": movement, "waves": waves,
                            "strategy_summary": planner.summarize_by_strategy(list(self.ctx["mappings"].values())),
                            "seven_rs": mapping.SEVEN_RS}
        return self.ctx["plan"]

    # ---------------------------------------------------------------- phase 7
    def migrate_and_validate(self) -> dict:
        eng = DataMigrationEngine(self.store, self.project_id, self.workdir)
        val = ValidationEngine(self.store, self.project_id)
        story: list[dict] = []

        def note(step, **kw):
            story.append({"step": step, "at": now(), **kw})
            self.log(f"    · {step}")

        eng.source.seed_from_partitions(SAMPLE / "data" / "source_orders")
        src_prof = profile_orders(eng.source.rows(), typed=False)
        base = val.baseline("analytics.orders", src_prof, "aws:redshift snapshot rs:customer-warehouse-2026-11-20")
        note("PROFILE & BASELINE", validation_run=base["id"], rows=src_prof["total"]["count"], partitions=src_prof["total"]["partitions"],
             watermark=src_prof["total"]["max_updated_at"])
        bp = planner.backfill_plan("aws:redshift/analytics.orders", "gcp:bigquery/analytics.orders", eng.source.partitions(),
                                   self.ctx["plan"]["retention_months"])
        plan_row = eng.create_plan(bp)
        note("PLAN", plan=plan_row["id"], decision=bp["decision"], batches=len(bp["batches"]))
        try:
            eng.start_run(plan_row["id"])
        except PermissionError as e:
            note("GATE ENFORCED", detail=str(e))
        self.store.record_approval(self.project_id, "migration_plan", plan_row["id"], "Data Platform Lead" + SIMULATED, "APPROVED",
                                   "Scope 24 months, REPLACE_PARTITION, acceptance: exact counts/totals/hashes")
        self.store.update("migration_plans", plan_row["id"], approval_status="APPROVED")
        note("APPROVE", plan=plan_row["id"])

        last_batch = bp["batches"][-1]["batch_key"]
        eng.fail_once = {last_batch}
        run = eng.start_run(plan_row["id"])
        note("EXTRACT → TRANSFER → LOAD", run=run["id"], status=run["run_status"], totals=run["totals"],
             failed_batch=last_batch)
        run = eng.retry_failed(run["id"])
        note("RETRY failed batch from checkpoint", run=run["id"], status=run["run_status"], totals=run["totals"])
        quarantine = {b["partition"]: b["rejected"] for b in self.store.all("migration_batches", run_id=run["id"]) if b["rejected"]}

        # seeded defects: simulate a faulty downstream job corrupting the target (demonstrates detection + selective reload)
        t = eng.target.conn
        with t:
            t.execute("DELETE FROM analytics_orders WHERE order_month='2025-04'")
            t.execute("UPDATE analytics_orders SET amount = CAST(CAST(amount AS REAL) * 0.9 AS TEXT) WHERE order_month='2026-02' AND order_id % 17 = 0")
            t.execute("INSERT INTO analytics_orders SELECT * FROM analytics_orders WHERE order_month='2025-09' AND order_id % 25 = 0")
        note("SEEDED DEFECTS injected into GCP target", partitions=["2025-04 (deleted)", "2026-02 (amount -10% on some rows)", "2025-09 (duplicates)"])
        v1 = val.reconcile_orders("POST_LOAD_RECONCILIATION", "after historical load", profile_orders(eng.source.rows(), False),
                                  profile_orders(eng.target.rows(), True), eng.source.rows, eng.target.rows, quarantined=quarantine)
        note("VALIDATE (post-load)", validation_run=v1["id"], status=v1["status"], summary=v1["summary"])
        bad_parts = sorted({r["partition"] for r in self.store.all("validation_results", validation_run_id=v1["id"])
                            if r["status"] == "FAIL" and r["partition"]} | set(
            next((r["details"].get("missing", []) for r in self.store.all("validation_results", validation_run_id=v1["id"])
                  if r["dimension"] == "coverage"), [])))
        rr = eng.selective_reload(plan_row["id"], bad_parts, f"remediate {v1['id']}")
        note("REMEDIATE & RE-RUN (selective reload)", run=rr["id"], partitions=bad_parts, totals=rr["totals"])
        v2 = val.reconcile_orders("POST_LOAD_RECONCILIATION", "after selective reload", profile_orders(eng.source.rows(), False),
                                  profile_orders(eng.target.rows(), True), eng.source.rows, eng.target.rows, quarantined=quarantine)
        note("RE-VALIDATE", validation_run=v2["id"], status=v2["status"], summary=v2["summary"])
        for r in self.store.all("validation_results", validation_run_id=v2["id"]):
            if r["status"] == "REVIEW":
                v2 = val.waive(r["id"], "Analytics Manager" + SIMULATED,
                               "2 malformed source rows (invalid amount/date) quarantined; to be corrected at source post-cutover", "2027-01-31")
                note("WAIVER recorded", result=r["id"], partition=r["partition"], new_status=v2["status"])

        # business keeps changing the AWS source after the baseline -> catch-up needed
        baseline_wm = run["watermark"]
        n = eng.source.simulate_changes(SAMPLE / "data" / "incremental" / "orders_changes_after_watermark.csv")
        v3 = val.reconcile_orders("INCREMENTAL_VALIDATION", "before catch-up (lag check)", profile_orders(eng.source.rows(), False),
                                  profile_orders(eng.target.rows(), True), eng.source.rows, eng.target.rows,
                                  freshness_tolerance_min=60, quarantined=quarantine)
        note("SOURCE CHANGED after baseline – lag detected", changes=n, validation_run=v3["id"], status=v3["status"])
        inc = eng.incremental_catch_up(plan_row["id"], baseline_wm)
        note("CATCH UP (incremental, watermark MERGE)", run=inc["id"], watermark=inc["watermark"], totals=inc["totals"])
        events = [json.loads(x) for x in (SAMPLE / "data" / "cdc" / "orders_cdc.jsonl").read_text().splitlines() if x.strip()]
        eng.source.simulate_cdc(events)
        cdc = eng.apply_cdc(plan_row["id"], events)
        replay = eng.apply_cdc(plan_row["id"], events)
        note("CDC apply + replay safety", run=cdc["id"], totals=cdc["totals"], replay_totals=replay["totals"])
        v4 = val.reconcile_orders("FINAL_CUTOVER_VALIDATION", "final cutover watermark", profile_orders(eng.source.rows(), False),
                                  profile_orders(eng.target.rows(), True), eng.source.rows, eng.target.rows, quarantined=quarantine)
        note("FINAL CUTOVER VALIDATION", validation_run=v4["id"], status=v4["status"], summary=v4["summary"])
        vc = val.reconcile_exact("analytics.customer", "customer_id", SAMPLE / "data" / "source_customer.csv",
                                 SAMPLE / "data" / "target_customer.csv", pii={"customer_name"})
        note("CUSTOMER exact reconciliation", validation_run=vc["id"], status=vc["status"], summary=vc["summary"])
        self.ctx["migration"] = {"plan_id": plan_row["id"], "backfill_plan": bp, "story": story,
                                 "validations": {"orders_final": v4, "customer": vc}}
        return self.ctx["migration"]

    # ---------------------------------------------------------------- phase 8-10
    def intelligence(self) -> dict:
        g_all = intelligence.build_graph(self.ctx["resources"], self.ctx["relationships"], self.ctx["consumers"], self.ctx["mappings"])
        # owner confirmations: a link an owner says is not real stops counting for impact and gates, but stays visible
        outcomes = comms.edge_outcomes(self.store, self.project_id)
        statuses = comms.app_statuses(self.store, self.project_id)
        for a, b, d in g_all.edges(data=True):
            oc = outcomes.get((a, b))
            d["owner_status"] = oc["outcome"] if oc else None
            d["owner_by"] = oc["by_name"] if oc else None
            d["owner_note"] = oc["note"] if oc else None
            d["flow_default"] = "REVERSE" if d.get("type") in comms.REVERSE_TYPES else "FORWARD"
            d["flow"] = comms.flow(d)
        for n, d in g_all.nodes(data=True):
            d["migration_status"] = comms.status_of(statuses, d["application"]) if d.get("application") else None
        # g: the graph every impact list and risk score uses. Arrows follow the direction data flows (READS_FROM points the
        # other way when stored), an owner's answer overrides the default, and a link an owner said is not real is dropped.
        import networkx as nx
        g = nx.DiGraph()
        g.add_nodes_from(g_all.nodes(data=True))
        for a, b, d in g_all.edges(data=True):
            f = d["flow"] if d["flow"] != "UNCONFIRMED" else d["flow_default"]
            if f == "FORWARD":
                g.add_edge(a, b, **d)
            elif f == "REVERSE":
                g.add_edge(b, a, **d)
        stakeholders = intelligence.resolve_stakeholders(self.store, self.project_id, self.ctx["owners"], g)
        v = self.ctx["migration"].get("validations") or {}
        vals_by_app = {self.app_name: [v["orders_final"], v["customer"]]} if v else {}
        for app, runs in self.dq.by_app().items():
            vals_by_app.setdefault(app, []).extend(runs)
        convs = [{**c, "checks": c["checks"] + [{"status": "FAIL", "check": t["name"]} for t in c["tests"] if t["status"] == "FAIL"]}
                 for c in self.current_conversions()]
        risk = intelligence.assess_risk(self.store, self.project_id, self.ctx["resources"], self.ctx["mappings"], g,
                                        convs, vals_by_app, self.ctx["findings"], stakeholders)
        self._availability_gates(risk)
        downstream_of = self._downstream_gates(risk, g_all, statuses)
        self._signoff_gates(risk)
        wave_of = {}
        for w in self.ctx["plan"]["waves"]:
            for a in w["applications"]:
                wave_of[a] = w["wave"]
        nctx = {}
        for app in {s["application"] for s in stakeholders}:
            rs = [r for r in self.ctx["resources"] if r.get("application") == app]
            down = set()
            for r in rs:
                down |= set(intelligence.impact(g, r["resource_id"])["downstream"])
            ds = "analytics.orders, analytics.customer" if (v and app == self.app_name) else ""
            vs = vals_by_app.get(app, [])
            rk = risk.get(app, {})
            nctx[app] = {"n_res": len(rs), "n_down": len(down), "wave": wave_of.get(app, "TBD"), "datasets": ds,
                         "status": ", ".join(f"{x['object']}={x['status']}" for x in vs) or "n/a",
                         "exceptions": sum(1 for x in vs for _ in self.store.query(
                             "SELECT 1 FROM validation_exceptions WHERE validation_run_id=? AND remediation_status IN ('OPEN','IN_PROGRESS')", (x["id"],))),
                         "gate": rk.get("readiness", "n/a")}
            if v and app in ("SalesReporting", "CustomerPortal", "FinanceOps"):
                nctx[app].update(datasets="analytics.orders (downstream consumer)", status="see CustomerAnalytics validation")
        cut = comms.cutover_drafts(statuses, stakeholders, downstream_of, comms.bridges(self.store, self.project_id))
        notes = intelligence.draft_notifications(self.store, self.project_id, stakeholders, nctx, cut)
        self.ctx.update(graph=intelligence.graph_json(g_all), stakeholders=stakeholders, risk=risk, notifications=notes,
                        downstream_apps={k: sorted(v) for k, v in downstream_of.items()},
                        impacts={r["resource_id"]: intelligence.impact(g, r["resource_id"]) for r in self.ctx["resources"]})
        return risk

    def current_conversions(self) -> list[dict]:
        """ctx conversions overlaid with their latest version (which may be a person's edit), its review decision, its
        generated tests and the full version history."""
        out = []
        for c in self.ctx["conversions"]:
            hist = codehistory.versions(self.store, self.project_id, c["source_file"])
            cur = dict(c)
            if hist:
                last = hist[-1]
                cur.update(id=last["id"], approval_status=last.get("approval_status") or "PENDING_REVIEW", approved_by=last.get("approved_by"),
                           approval_note=last.get("approval_note"), approved_at=last.get("approved_at"), current_n=last["n"],
                           current_kind=last.get("kind") or "GENERATED", edited_by=last.get("edited_by"), edit_note=last.get("note"),
                           generated_id=c["id"])
                if last.get("outputs"):
                    cur["outputs"] = last["outputs"]
                t = last.get("tests") or {}
                cur["tests"], cur["test_files"] = t.get("results") or [], t.get("files") or {}
            else:
                cur.update(current_n=1, current_kind="GENERATED", tests=[], test_files={})
            cur["test_summary"] = codecheck.summary(cur["tests"])
            cur["history"] = [codehistory.view(v) for v in hist]
            out.append(cur)
        return out

    def _downstream_gates(self, risk: dict, g_all, statuses: dict) -> dict[str, set]:
        """Project document phase 8: an application should not be cut over while an application that reads its data is still
        on AWS, unless that application's owner agreed a bridge. Direction comes from the link type or, for inferred links,
        from an owner's answer; an inferred link nobody has confirmed leaves the gate open rather than guessing.
        Returns, for every application, the other applications downstream of it."""
        import networkx as nx
        F = nx.DiGraph()
        F.add_nodes_from(g_all.nodes(data=True))
        unsure: dict[str, set] = {}
        for a, b, d in g_all.edges(data=True):
            f = d.get("flow")
            if f == "FORWARD":
                F.add_edge(a, b)
            elif f == "REVERSE":
                F.add_edge(b, a)
            elif f == "UNCONFIRMED":
                x, y = g_all.nodes[a].get("application"), g_all.nodes[b].get("application")
                if x and y and x != y:
                    unsure.setdefault(x, set()).add(y)
                    unsure.setdefault(y, set()).add(x)
        downstream_of: dict[str, set] = {}
        for n, d in F.nodes(data=True):
            app = d.get("application")
            if not app:
                continue
            downstream_of.setdefault(app, set())
            for m in nx.descendants(F, n):
                other = F.nodes[m].get("application")
                if other and other != app:
                    downstream_of[app].add(other)
        brid = comms.bridges(self.store, self.project_id)
        for app, r in risk.items():
            downs = sorted(downstream_of.get(app, set()))
            maybe = sorted(unsure.get(app, set()) - set(downs))
            waiting = [d for d in downs if comms.status_of(statuses, d) != "MIGRATED" and (app, d) not in brid]
            if waiting:
                r["gates"].append({"gate": "DOWNSTREAM_READY", "status": "BLOCKED",
                                   "detail": f"{', '.join(waiting)} read{'s' if len(waiting) == 1 else ''} its data and "
                                             f"{'is' if len(waiting) == 1 else 'are'} still on AWS with no bridge agreed"
                                             + (f"; inferred link(s) with {', '.join(maybe)} also need an owner's answer" if maybe else "")})
                r["readiness"] = "BLOCKED"
            elif maybe:
                r["gates"].append({"gate": "DOWNSTREAM_READY", "status": "OPEN",
                                   "detail": f"inferred link(s) with {', '.join(maybe)} need an owner to confirm which way data flows (Dependencies)"})
            elif downs:
                done = [d for d in downs if comms.status_of(statuses, d) == "MIGRATED"]
                parts = ([f"on Google Cloud: {', '.join(done)}"] if done else []) + \
                        ([f"bridge agreed: {', '.join(d for d in downs if d not in done)}"] if len(done) < len(downs) else [])
                r["gates"].append({"gate": "DOWNSTREAM_READY", "status": "PASS", "detail": "; ".join(parts)})
        return downstream_of

    @property
    def signoffs(self) -> dict[str, dict]:
        return {r["application"]: r for r in self.store.all("signoffs", project_id=self.project_id)}

    @staticmethod
    def _gates_fingerprint(gates: list[dict]) -> str:
        """Identifies the set of open/blocked gates a sign-off was recorded against, FORMAL_SIGNOFF itself excluded.
        If this changes, whatever was signed off no longer matches the current state."""
        import hashlib
        rest = sorted(f"{g['gate']}:{g['status']}" for g in gates if g["gate"] != "FORMAL_SIGNOFF")
        return hashlib.sha256("|".join(rest).encode()).hexdigest()[:16]

    def record_signoff(self, application: str, role: str, approver: str, rationale: str, actor: str = "console.user") -> dict:
        if role not in ("business", "technical"):
            raise ValueError("Role must be business or technical.")
        if not (approver or "").strip() or len((rationale or "").strip()) < 10:
            raise ValueError("Enter your name and a reason of at least 10 characters.")
        r = self.ctx["risk"].get(application)
        if not r:
            raise ValueError(f"No risk assessment for {application} yet.")
        other_blocked = [g for g in r["gates"] if g["gate"] != "FORMAL_SIGNOFF" and g["status"] == "BLOCKED"]
        if other_blocked:
            raise ValueError(f"{len(other_blocked)} other gate(s) are still blocked: "
                             + ", ".join(g["gate"] for g in other_blocked) + ". Clear those before signing off.")
        row = self.store.get("signoffs", new_id_for := self._stable_signoff_id(application)) or {}
        fp = self._gates_fingerprint(r["gates"])
        fields = {"id": new_id_for, "project_id": self.project_id, "application": application, "gates_hash": fp,
                  "business_status": row.get("business_status", "PENDING"), "business_approver": row.get("business_approver"),
                  "business_rationale": row.get("business_rationale"), "business_at": row.get("business_at"),
                  "technical_status": row.get("technical_status", "PENDING"), "technical_approver": row.get("technical_approver"),
                  "technical_rationale": row.get("technical_rationale"), "technical_at": row.get("technical_at"),
                  "created_at": row.get("created_at", now())}
        fields[f"{role}_status"], fields[f"{role}_approver"] = "SIGNED", approver.strip()
        fields[f"{role}_rationale"], fields[f"{role}_at"] = rationale.strip(), now()
        fields["status"] = "SIGNED_OFF" if fields["business_status"] == "SIGNED" and fields["technical_status"] == "SIGNED" else f"{role.upper()}_ONLY"
        fields["updated_at"] = now()
        self.store.insert("signoffs", fields)
        self.store.record_approval(self.project_id, "signoff", fields["id"], approver.strip(), f"{role.upper()}_SIGNED", rationale.strip(),
                                   requested_by=actor)
        self.save_ctx()
        return self.store.get("signoffs", fields["id"])

    def _stable_signoff_id(self, application: str) -> str:
        from .intelligence import _stable_id
        return _stable_id("SGN-", self.project_id, application)

    def _signoff_gates(self, risk: dict) -> None:
        sg = self.signoffs
        for app, r in risk.items():
            other_blocked = any(g["status"] == "BLOCKED" for g in r["gates"])
            row = sg.get(app)
            cur_fp = self._gates_fingerprint(r["gates"])
            if other_blocked:
                gate = {"gate": "FORMAL_SIGNOFF", "status": "OPEN", "detail": "Other gates must clear first."}
                r["readiness"] = "BLOCKED"
            elif not row or row["status"] == "PENDING":
                gate = {"gate": "FORMAL_SIGNOFF", "status": "OPEN", "detail": "Awaiting business and technical sign-off."}
            elif row["status"] == "SIGNED_OFF" and row["gates_hash"] == cur_fp:
                gate = {"gate": "FORMAL_SIGNOFF", "status": "PASS",
                       "detail": f"Signed off by {row['business_approver']} (business) and {row['technical_approver']} (technical)."}
            elif row["status"] == "SIGNED_OFF":
                gate = {"gate": "FORMAL_SIGNOFF", "status": "BLOCKED", "detail": "Other gates changed since this was signed off. Sign off again."}
            else:
                waiting = "technical" if row["status"] == "BUSINESS_ONLY" else "business"
                gate = {"gate": "FORMAL_SIGNOFF", "status": "OPEN", "detail": f"Awaiting {waiting} sign-off."}
            r["gates"] = [g for g in r["gates"] if g["gate"] != "FORMAL_SIGNOFF"] + [gate]
            if gate["status"] == "BLOCKED":
                r["readiness"] = "BLOCKED"
            elif gate["status"] == "PASS":
                r["readiness"] = "SIGNED_OFF"

    def _availability_gates(self, risk: dict) -> None:
        """Add the data-availability gate. A discrepancy does not stop a migration for good: it blocks sign-off until a person
        decides. No datasets at all is shown as an open item, not a pass."""
        ds = self.avail.list()
        known = set(risk)
        unlinked = [d for d in ds if d["application"] not in known]
        for app, r in risk.items():
            mine = [d for d in ds if d["application"] == app]
            if not mine and unlinked and len(known) == 1:
                mine = unlinked            # one application only: nothing to be ambiguous about
            if not mine:
                gate = {"gate": "DATA_AVAILABILITY", "status": "OPEN",
                        "detail": (f"{len(unlinked)} dataset(s) are not linked to an application yet; link them on Data availability"
                                   if unlinked else "no datasets added to the availability check yet")}
            else:
                bad = [d for d in mine if d["clearance"] not in ("CLEAR", "PROCEED")]
                gate = ({"gate": "DATA_AVAILABILITY", "status": "BLOCKED",
                         "detail": f"{len(bad)} of {len(mine)} dataset(s) need a check, a confirmation or a decision: " + ", ".join(d["name"] for d in bad[:4])}
                        if bad else {"gate": "DATA_AVAILABILITY", "status": "PASS",
                                     "detail": f"{len(mine)} dataset(s) available or accepted by a person ({sum(1 for d in mine if d['clearance']=='PROCEED')} accepted despite a difference)"})
            r["gates"] = [g for g in r["gates"] if g["gate"] != "DATA_AVAILABILITY"] + [gate]
            if any(g["status"] == "BLOCKED" for g in r["gates"]):
                r["readiness"] = "BLOCKED"

    # ---------------------------------------------------------------- full run
    def run_all(self) -> dict:
        self.log("▶ AWS→GCP Migration Accelerator – end-to-end demo run")
        self.create_project()
        self._agent("Ingestion", self.ingest, str(SAMPLE))
        self._agent("Discovery Agent + Architecture Agent (parallel extraction, normalization)", self.extract_and_normalize)
        self._agent("Mapping Agent", self.map_services)
        self._agent("Conversion Agent", self.convert)
        self._agent("Migration Planning Agent", self.plan)
        self._agent("Data Migration Engine + Data Quality Agent", self.migrate_and_validate)
        self.seed_availability()
        self._agent("Dependency Agent + Risk Agent + Communication Agent", self.intelligence)
        st = self.state()
        (self.workdir / "state.json").write_text(json.dumps(st, indent=1, default=str))
        from .reporting import write_reports
        paths = self._agent("Reporting", lambda: write_reports(st, self.workdir / "reports"))
        st["reports"] = [str(p.relative_to(self.workdir)) for p in paths]
        (self.workdir / "state.json").write_text(json.dumps(st, indent=1, default=str))
        self.ctx["assessed_at"] = now()
        self.ctx["report_files"] = st["reports"]
        self.save_ctx()
        self.log(f"✔ done – state: {self.workdir / 'state.json'}")
        return st

    # ---------------------------------------------------------------- state for UI / reports
    def state(self) -> dict:
        S, pid = self.store, self.project_id
        runs = S.query("SELECT r.* FROM migration_runs r JOIN migration_plans p ON p.id=r.plan_id WHERE p.project_id=? ORDER BY r.started_at", (pid,))
        plans = {p["id"]: p for p in S.all("migration_plans", project_id=pid)}
        for r in runs:
            r["batches"] = S.query("SELECT * FROM migration_batches WHERE run_id=? ORDER BY batch_key", (r["id"],))
            r["indicators"] = loadops.run_indicators(r, plans.get(r["plan_id"]))
        from .migration_engine import COLUMNS as _LOAD_COLUMNS
        load_plan = plans.get(self.ctx["migration"].get("plan_id")) if self.ctx["migration"].get("plan_id") else None
        load_controls = {"plan_id": load_plan["id"] if load_plan else None,
                         "max_parallel": ((load_plan or {}).get("config") or {}).get("max_parallel_batches"),
                         "limit_cap": loadops.MAX_PARALLEL, "history": ((load_plan or {}).get("config") or {}).get("control_history", []),
                         "schema": loadops.schema_check(self.workdir, load_plan, _LOAD_COLUMNS) if load_plan else None,
                         "plan": loadops.plan_indicators(self.ctx["plan"].get("movement", []),
                                                         ((S.get("projects", pid) or {}).get("config") or {}).get("bandwidth_mbps")),
                         "rates": loadops.rates()}
        conversions = self.current_conversions()
        vals = S.all("validation_runs", project_id=pid)
        for v in vals:
            v["results"] = S.query("SELECT * FROM validation_results WHERE validation_run_id=?", (v["id"],))
            v["exceptions"] = S.query("SELECT * FROM validation_exceptions WHERE validation_run_id=?", (v["id"],))
            if v["run_type"] == "SOURCE_BASELINE":
                v["source_snapshot"] = {k: x for k, x in (v["source_snapshot"] or {}).items() if k != "profile"} | {
                    "partitions": {p: {k: x for k, x in d.items() if k in ("count", "invalid", "sum_amount", "partition_hash")}
                                   for p, d in (v["source_snapshot"] or {}).get("profile", {}).get("partitions", {}).items()}}
        uploads = S.all("uploads", project_id=pid)
        mappings = list(self.ctx["mappings"].values())
        res = self.ctx["resources"]
        notes = comms.all_notifications(S, pid)
        val_final = self.ctx["migration"].get("validations") or {}
        kpis = {
            "artefacts_uploaded": len(uploads), "artefacts_rejected": sum(1 for u in uploads if u["status"] == "REJECTED"),
            "resources_discovered": len(res), "relationships": len(self.ctx["relationships"]),
            "conflicts": sum(1 for r in res if r["conflicts"]),
            "mappings_approved": sum(1 for m in mappings if m["approval_status"] == "APPROVED"), "mappings_total": len(mappings),
            "conversions": len(self.ctx["conversions"]),
            "avg_conversion_confidence": round(sum(c["confidence"] for c in self.ctx["conversions"]) / max(1, len(self.ctx["conversions"])), 2),
            "batches_loaded": sum(1 for r in runs for b in r["batches"] if b["status"] == "LOADED"),
            "validation_orders": (val_final.get("orders_final") or {}).get("status", "NOT_RUN"),
            "validation_customer": (val_final.get("customer") or {}).get("status", "NOT_RUN"),
            "apps_blocked": sum(1 for a in self.ctx["risk"].values() if a["readiness"] == "BLOCKED"),
            "apps_total": len(self.ctx["risk"]),
            "pending_approvals": sum(1 for m in mappings if m["approval_status"] != "APPROVED") +
                                 sum(1 for c in conversions if c.get("approval_status") != "APPROVED") +
                                 sum(1 for n in notes if n["status"] == "DRAFT" and n["approval"] in ("PENDING", "CHANGED_SINCE_APPROVAL")),
        }
        return {
            "generated_at": now(), "project": S.get("projects", pid), "kpis": kpis,
            "stages": {"files": sum(1 for u in uploads if u["status"] == "ACCEPTED"), "assessed": bool(self.ctx.get("assessed_at")),
                       "data_migration": bool(self.ctx["migration"].get("story")),
                       "sample": bool(((S.get("projects", pid) or {}).get("config") or {}).get("sample"))},
            "catalogue": {"version": catalogue.CATALOGUE_VERSION, "entries": len(catalogue.catalogue()),
                          "services": sorted(e["aws_service"] for e in catalogue.catalogue())},
            "uploads": uploads, "discovery": {"stats": self.ctx["discovery_stats"], "resources": res,
                                              "relationships": self.ctx["relationships"], "consumers": self.ctx["consumers"],
                                              "needs_review": self.ctx["needs_review"], "requirements": self.ctx["requirements"],
                                              "findings": self.ctx["findings"]},
            "mappings": mappings, "conversions": conversions, "plan": self.ctx["plan"],
            "migration": {**self.ctx["migration"], "runs": runs}, "validations": vals, "load_controls": load_controls,
            "graph": self.ctx["graph"], "impacts": self.ctx["impacts"], "stakeholders": self.ctx["stakeholders"],
            "notifications": notes, "notification_summary": comms.summary(notes),
            "app_status": {a: {**r, "label": comms.APP_LABEL[r["status"]]} for a, r in comms.app_statuses(S, pid).items()},
            "downstream_apps": self.ctx.get("downstream_apps", {}),
            "bridges": [c for c in comms.confirmations(S, pid) if c["kind"] == "BRIDGE"],
            "availability": {"datasets": self.avail.list(), "summary": self.avail.summary()}, "risk": self.ctx["risk"],
            "signoffs": self.signoffs,
            "dq_objects": self.dq.list(), "dq_summary": self.dq.summary(), "exceptions": dq.all_exceptions(S, pid),
            "monitoring_notes": S.all("monitoring_notes", project_id=pid),
            "approvals": S.all("approval_requests", project_id=pid), "agent_runs": S.all("agent_runs", project_id=pid),
            "audit": {"chain": S.verify_audit_chain(), "events": S.query("SELECT * FROM audit_logs WHERE project_id=? ORDER BY seq", (pid,))},
        }
