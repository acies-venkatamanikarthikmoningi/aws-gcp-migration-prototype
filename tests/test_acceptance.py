"""Prototype acceptance tests – blueprint section 19 / 19.1 quality targets."""
from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import pytest

from accelerator import catalogue, mapping
from accelerator.converters import pipelines, python_sdk, sql_redshift, terraform
from accelerator.ingestion import RejectedFile, classify, check_content, safe_extract
from accelerator.orchestrator import SAMPLE, Orchestrator


@pytest.fixture(scope="session")
def run(tmp_path_factory):
    o = Orchestrator(tmp_path_factory.mktemp("e2e"), log=lambda m: None)
    st = o.run_all()
    return o, st


# ---------------------------------------------------------------- ingestion & security
def test_multi_source_ingestion_all_categories(run):
    _, st = run
    cats = {u["category"] for u in st["uploads"] if u["status"] == "ACCEPTED"}
    assert {"inventory", "code", "document", "diagram", "dependency", "ownership"} <= cats


def test_unsafe_files_rejected(run):
    _, st = run
    rej = {u["filename"]: u["error"] for u in st["uploads"] if u["status"] == "REJECTED"}
    assert "installer.exe" in rej and "traversal.zip" in rej and "fake_inventory.xlsx" in rej and "malware_sample.txt" in rej


def test_zip_traversal_blocked(tmp_path):
    z = tmp_path / "x.zip"
    with zipfile.ZipFile(z, "w") as f:
        f.writestr("../evil.txt", "x")
    with pytest.raises(RejectedFile):
        safe_extract(z, tmp_path / "out")


def test_magic_mismatch(tmp_path):
    p = tmp_path / "a.pdf"
    p.write_text("not a pdf")
    with pytest.raises(RejectedFile):
        check_content(p, classify(p)[1])


# ---------------------------------------------------------------- discovery
def test_all_inventory_resources_discovered(run):
    _, st = run
    with (SAMPLE / "aws_inventory" / "aws_resources.csv").open() as f:
        ids = {r["resource_id"] for r in csv.DictReader(f)}
    found = {r["resource_id"] for r in st["discovery"]["resources"]}
    assert ids <= found  # 100% of curated inventory


def test_cross_source_correlation_and_conflicts(run):
    _, st = run
    res = {r["resource_id"]: r for r in st["discovery"]["resources"]}
    assert res["s3-1001"]["evidence_status"] == "CORROBORATED"
    assert {e["category"] for e in res["s3-1001"]["evidence"]} >= {"inventory", "code"}
    conflict = res["i-1001"]["conflicts"][0]
    assert conflict["attribute"] == "instance_type"
    assert {o["value"] for o in conflict["observations"]} == {"m5.large", "m5.xlarge"}


def test_secret_detected(run):
    _, st = run
    assert any(f["kind"] == "aws_access_key_id" for f in st["discovery"]["findings"])


# ---------------------------------------------------------------- mapping
def test_mapping_has_alternatives_and_rationale(run):
    _, st = run
    m = {x["resource_id"]: x for x in st["mappings"]}
    assert m["rs-1001"]["target_service"] == "BigQuery"
    assert m["s3-1001"]["target_service"] == "Cloud Storage"
    assert m["ddb-1001"]["alternatives"] and m["ddb-1001"]["rationale"]
    assert m["ec2-2001"]["strategy"] == "Retire" and m["ec2-2002"]["strategy"] == "Retain"
    assert m["iot-1001"]["approval_status"] == "PENDING"  # business decision left to humans


def test_unknown_service_is_not_invented():
    r = mapping.recommend({"resource_id": "x", "service": "TotallyNewService", "configuration": {}, "conflicts": []})
    assert r["target_service"] == "UNKNOWN" and "NEEDS REVIEW" in r["rationale"][0]


def test_catalogue_breadth():
    assert len(catalogue.catalogue()) >= 100


# ---------------------------------------------------------------- conversion
def test_terraform_conversion_parses():
    r = terraform.convert_terraform(SAMPLE / "infrastructure" / "main.tf")
    assert all(c["status"] != "FAIL" for c in r.checks)
    tf = r.outputs["terraform/main.tf"]
    assert 'resource "google_storage_bucket" "customer_raw"' in tf and "ARCHIVE" in tf and "aws_" not in tf.split("provider")[1]


def test_cfn_custom_resource_reported():
    r = terraform.convert_cloudformation(SAMPLE / "infrastructure" / "cloudformation.yaml")
    assert any("LegacyCustomResource" in u for u in r.unsupported)


def test_redshift_sql_conversion():
    r = sql_redshift.convert_sql(SAMPLE / "sql" / "redshift_customer.sql")
    out = r.outputs["sql/bigquery_customer.sql"]
    assert "PARTITION BY order_date CLUSTER BY customer_id" in out
    assert "DISTKEY" not in out and "ENCODE" not in out and "LOAD DATA INTO" in out and "EXPORT DATA" in out
    assert next(c for c in r.checks if c["check"] == "BigQuery dialect re-parse")["status"] == "PASS"


def test_lambda_conversion_removes_aws_and_secret():
    r = python_sdk.convert_python_app(SAMPLE / "application_code" / "lambda_function.py", "lambda_python")
    main = r.outputs["app/lambda_to_cloud_run_function/main.py"]
    assert "boto3" not in main and "AKIA" not in main and "functions_framework.cloud_event" in main
    assert all(c["status"] == "PASS" for c in r.checks)


def test_stepfunctions_and_glue():
    w = pipelines.convert_step_functions(SAMPLE / "orchestration" / "customer_state_machine.json")
    assert "googleapis.dataproc.v1" in w.outputs["orchestration/customer_workflow.yaml"]
    g = pipelines.convert_glue(SAMPLE / "etl" / "customer_etl.py")
    assert "awsglue" not in g.outputs["etl/customer_etl_dataproc.py"]


def test_generated_code_never_deployed(run):
    o, st = run
    assert all(c["approval_status"] == "PENDING_REVIEW" for c in o.store.all("conversion_runs", project_id=o.project_id))
    assert all(e["details"].get("auto_deployed") is False for e in st["audit"]["events"] if e["action"] == "CODE_GENERATED")


# ---------------------------------------------------------------- backfill
def test_plan_gate_enforced(run):
    _, st = run
    assert any(s["step"] == "GATE ENFORCED" for s in st["migration"]["story"])


def test_all_partitions_in_manifest_and_checkpoints(run):
    _, st = run
    full = next(r for r in st["migration"]["runs"] if r["run_type"] == "FULL_HISTORICAL_BACKFILL")
    assert len(full["batches"]) == 24 and all(b["status"] == "LOADED" and b["checksum"] for b in full["batches"])
    assert all(b["checkpoint"]["stage"] == "LOADED" for b in full["batches"])


def test_failed_batch_retried_without_duplicates(run):
    o, st = run
    full = next(r for r in st["migration"]["runs"] if r["run_type"] == "FULL_HISTORICAL_BACKFILL")
    b24 = next(b for b in full["batches"] if b["batch_key"] == "B024")
    assert b24["attempt"] == 2
    tgt = o.workdir / "bigquery_sim.db"
    import sqlite3
    dup = sqlite3.connect(tgt).execute("SELECT COUNT(*) FROM (SELECT order_id FROM analytics_orders GROUP BY 1 HAVING COUNT(*)>1)").fetchone()[0]
    assert dup == 0


def test_cdc_replay_is_idempotent(run):
    _, st = run
    cdc = [r for r in st["migration"]["runs"] if r["run_type"] == "CDC_APPLY"]
    assert cdc[-1]["totals"]["skipped_already_applied"] == 5


def test_incremental_and_cdc_loads_are_recorded_for_the_console(run):
    """Round 8: the Load runs screen shows the incremental loads from what the engine recorded, so that record must exist."""
    _, st = run
    runs = st["migration"]["runs"]
    cp = lambda r: r["batches"][0]["checkpoint"]  # noqa: E731
    inc = [r for r in runs if r["run_type"] == "INCREMENTAL_CATCH_UP"]
    assert len(inc) == 1 and inc[0]["run_status"] == "COMPLETED"
    c = cp(inc[0])
    assert c["from_watermark"] < c["to_watermark"] and c["inserted"] + c["updated"] == inc[0]["batches"][0]["loaded"] == 13
    cdc = [r for r in runs if r["run_type"] == "CDC_APPLY"]
    first, replay = cdc[0], cdc[-1]
    f = cp(first)
    assert (f["lsn_from"], f["lsn_to"], f["applied_lsn"], f["previous_applied_lsn"], f["events_received"]) == (9001, 9005, 9005, 0, 5)
    assert first["totals"]["I"] + first["totals"]["U"] + first["totals"]["D"] == 5
    r = cp(replay)  # the same events delivered again: nothing applied, position unchanged, label no longer a backwards range
    assert r["events_received"] == 5 and replay["totals"]["skipped_already_applied"] == 5 and r["applied_lsn"] == 9005
    assert r["lsn_from"] is None and replay["batches"][0]["partition"].startswith("no new events")
    # every one of these runs has its audit entry, which the screen quotes
    acts = {(e["entity_id"], e["action"]) for e in st["audit"]["events"]}
    assert (inc[0]["id"], "INCREMENTAL_APPLIED") in acts and (first["id"], "CDC_APPLIED") in acts and (replay["id"], "CDC_APPLIED") in acts
    # the freshness evidence the screen quotes: behind before catch-up, level after
    fr = lambda rt: [x for v in st["validations"] if v["run_type"] == rt for x in v["results"] if x["dimension"] == "freshness"][-1]  # noqa: E731
    assert fr("INCREMENTAL_VALIDATION")["status"] == "FAIL" and fr("INCREMENTAL_VALIDATION")["details"]["lag_minutes"] > 0
    assert fr("FINAL_CUTOVER_VALIDATION")["status"] == "PASS" and fr("FINAL_CUTOVER_VALIDATION")["details"]["lag_minutes"] == 0


def test_rejected_rows_quarantined(run):
    o, _ = run
    q = list((o.workdir / "gcs_landing" / "_quarantine").rglob("*_rejected.csv"))
    assert q and sum(1 for _ in q[0].open()) - 1 == 2


# ---------------------------------------------------------------- validation
def test_seeded_mismatches_all_detected(run):
    o, st = run
    v1 = next(v for v in st["validations"] if (v["source_snapshot"] or {}).get("label") == "after historical load")
    fails = {(r["dimension"], r["partition"]) for r in v1["results"] if r["status"] == "FAIL"}
    assert ("coverage", None) in fails                    # missing 2025-04 partition
    assert ("uniqueness", "2025-09") in fails             # duplicates
    assert ("aggregate", "2026-02") in fails              # value change
    assert ("hash", "2026-02") in fails


def test_customer_value_mismatch_detected(run):
    _, st = run
    c = next(v for v in st["validations"] if v["object"] == "analytics.customer")
    r = next(r for r in c["results"] if r["dimension"] == "value")
    assert r["partition"] == "customer_id=1003" and r["details"]["difference"] == "500"


def test_final_validation_pass_with_recorded_waiver(run):
    _, st = run
    final = next(v for v in st["validations"] if (v["source_snapshot"] or {}).get("label") == "final cutover watermark")
    assert final["status"] == "PASS"
    w = [r for r in final["results"] if r["status"] == "WAIVED"]
    assert w and w[0]["details"]["waiver"]["approver"] and w[0]["details"]["waiver"]["expiry"]
    assert any(r["status"] == "NOT_RUN" for r in final["results"])  # never represented as validated


def test_every_result_has_run_id(run):
    _, st = run
    assert all(r["validation_run_id"] for v in st["validations"] for r in v.get("results", []))


# ---------------------------------------------------------------- dependencies, owners, risk, comms
def test_dependency_graph(run):
    _, st = run
    edges = {(e["source"], e["target"]) for e in st["graph"]["edges"]}
    assert ("rs-1001", "powerbi-sales") in edges and ("glue-1001", "rs-1001") in edges and ("rs-1001", "finance-extract") in edges


def test_no_invented_owner_or_email(run):
    _, st = run
    allowed = {"lead@example.com", "analytics@example.com", "app@example.com", "bi@example.com", "billing@example.com"}
    assert {s["email"] for s in st["stakeholders"] if s["email"]} <= allowed
    assert any(s["application"] == "FinanceOps" and not s["verified"] for s in st["stakeholders"])
    assert all(n["recipient"] in allowed or n["recipient"] is None for n in st["notifications"])


def test_signoff_blocked_on_mandatory_failure(run):
    _, st = run
    ca = st["risk"]["CustomerAnalytics"]
    assert ca["readiness"] == "BLOCKED"
    assert any(g["gate"] == "MANDATORY_RECONCILIATION" and g["status"] == "BLOCKED" for g in ca["gates"])
    assert all(d["rationale"] for d in ca["dimensions"].values())


def test_audit_chain_and_reports(run):
    o, st = run
    assert st["audit"]["chain"]["valid"]
    assert (o.workdir / "reports" / "migration_report.xlsx").exists()
    assert (o.workdir / "reports" / "migration_readiness_report.html").exists()


def test_audit_tamper_detected(run):
    o, _ = run
    o.store.conn.execute("UPDATE audit_logs SET action='TAMPERED' WHERE seq=3")
    assert not o.store.verify_audit_chain()["valid"]


def test_api_smoke(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    import accelerator.api as api
    monkeypatch.setattr(api, "WORK", tmp_path / "api")
    api._orc = None
    c = TestClient(api.app)
    st = c.get("/api/v1/state").json()
    assert st["kpis"]["resources_discovered"] >= 35
    pending = next(m for m in st["mappings"] if m["approval_status"] == "PENDING")
    r = c.post(f"/api/v1/mappings/{pending['mapping_id']}/approve",
               json={"approver": "Test Architect", "decision": "APPROVED", "rationale": "test"})
    assert r.json()["approval_status"] == "APPROVED"
    tf = (SAMPLE / "infrastructure" / "main.tf").read_bytes()
    a = c.post("/api/v1/analyze", files={"file": ("main.tf", io.BytesIO(tf))}).json()
    assert a["status"] == "ACCEPTED" and a["conversion"]["converter"] == "terraform-aws-to-google"
    bad = c.post("/api/v1/analyze", files={"file": ("x.exe", io.BytesIO(b"MZ"))}).json()
    assert bad["status"] == "REJECTED"
    assert c.get("/").status_code == 200
    api._orc = None


# --------------------------------------------------------------------------- Round 2: projects
@pytest.fixture()
def client(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    import accelerator.api as api
    monkeypatch.setattr(api, "WORK", tmp_path / "api")
    api._orc = None
    api._pool.clear()
    yield TestClient(api.app)
    api._orc = None
    api._pool.clear()


def _files():
    return [("files", ("main.tf", (SAMPLE / "infrastructure" / "main.tf").read_bytes())),
            ("files", ("bad.exe", b"MZ"))]


def test_project_create_validates(client):
    r = client.post("/api/v1/projects", json={"name": "ab"})
    assert r.status_code == 422 and "name" in r.json()["detail"]["errors"]
    r = client.post("/api/v1/projects", json={"name": "Valid name", "gcp_project": "Bad_ID"})
    assert r.status_code == 422 and "gcp_project" in r.json()["detail"]["errors"]
    r = client.post("/api/v1/projects", json={"name": "Retail Orders", "application": "RetailOrders", "aws_account": "123456789012"})
    assert r.status_code == 201 and r.json()["status"] == "NEW" and not r.json()["is_sample"]
    assert len(client.get("/api/v1/projects").json()) == 2


def test_new_project_state_is_empty_not_sample(client):
    pid = client.post("/api/v1/projects", json={"name": "Empty one"}).json()["id"]
    st = client.get(f"/api/v1/state?project={pid}").json()
    assert st["kpis"]["resources_discovered"] == 0 and st["stages"]["assessed"] is False
    assert st["kpis"]["validation_orders"] == "NOT_RUN"
    assert client.post(f"/api/v1/projects/{pid}/assess").status_code == 400   # nothing uploaded yet


def test_upload_assess_persist_and_carry_over(client):
    import accelerator.api as api
    pid = client.post("/api/v1/projects", json={"name": "Upload flow", "application": "UploadFlow"}).json()["id"]
    u = client.post(f"/api/v1/projects/{pid}/uploads", files=_files()).json()
    assert u["accepted"] == 1 and u["rejected"] == 1
    a = client.post(f"/api/v1/projects/{pid}/assess").json()
    assert a["resources"] > 0
    api._pool.clear()                                                         # simulate a restart
    st = client.get(f"/api/v1/state?project={pid}").json()
    assert st["stages"]["assessed"] and st["stages"]["data_migration"] is False
    assert all(m["approval_status"] == "PENDING" for m in st["mappings"])      # the tool never approves on its own
    m = st["mappings"][0]
    client.post(f"/api/v1/mappings/{m['mapping_id']}/approve", json={"approver": "T", "decision": "APPROVED", "rationale": "ok"})
    assert client.post(f"/api/v1/projects/{pid}/assess").json()["decisions_carried_over"] == 1
    assert client.get(f"/api/v1/audit/verify?project={pid}").json()["valid"]


def test_sample_project_read_only_and_isolated(client):
    sid = client.get("/api/v1/projects").json()[0]["id"]
    assert client.post(f"/api/v1/projects/{sid}/uploads", files=_files()).status_code == 400
    pid = client.post("/api/v1/projects", json={"name": "Isolated"}).json()["id"]
    assert client.get(f"/api/v1/state?project={pid}").json()["project"]["id"] == pid
    assert client.get("/api/v1/state").json()["project"]["id"] == sid
    assert client.get("/api/v1/state?project=PRJ-nope").status_code == 404


# --------------------------------------------------------------------------- Round 3: data availability
def _csv(rows, header="id,dt,amt"):
    return (header + "\n" + "\n".join(rows)).encode()


def _proj_with_files(client, src_rows, tgt_rows=None):
    pid = client.post("/api/v1/projects", json={"name": "Availability test"}).json()["id"]
    files = [("files", ("src.csv", _csv(src_rows)))]
    if tgt_rows is not None:
        files.append(("files", ("tgt.csv", _csv(tgt_rows))))
    client.post(f"/api/v1/projects/{pid}/uploads", files=files)
    ups = {u["filename"]: u["id"] for u in client.get("/api/v1/state?project=" + pid).json()["uploads"]}
    return pid, ups


def _days(n, skip=()):
    return [f"{i},2025-01-{(i % 28) + 1:02d},{i}" for i in range(n) if i not in skip]


def test_availability_engine_outcomes():
    from accelerator.availability import analyse
    import tempfile, os
    d = tempfile.mkdtemp()
    def w(name, rows):
        p = os.path.join(d, name)
        open(p, "wb").write(_csv(rows))
        return p
    src = w("s.csv", _days(100))
    assert analyse(src, w("t_same.csv", _days(100)), "dt", "id")["status"] == "AVAILABLE"
    r = analyse(src, w("t_short.csv", _days(100, skip=range(0, 100, 20))), "dt", "id")
    assert r["status"] == "DISCREPANCY" and r["totals"]["difference"] == -5 and r["keys"]["missing_in_target"] == 5
    assert analyse(src, None, "dt")["status"] == "MISSING"
    assert analyse(None, None)["status"] == "CANNOT_CHECK"
    only_total = analyse(src, w("t_nodate.csv", _days(100)), "nope", "")
    assert only_total["status"] == "AVAILABLE" and only_total["evidence"] == "Total counts" and only_total["notes"]


def test_availability_decision_flow_and_staleness(client):
    pid, up = _proj_with_files(client, _days(100), _days(100, skip=range(0, 100, 20)))
    base = f"/api/v1/projects/{pid}/availability/datasets"
    assert client.post(base, json={"name": "x", "source_upload": up["src.csv"], "source_access": "NONE"}).status_code == 400
    d = client.post(base, json={"name": "Orders", "source_upload": up["src.csv"], "target_upload": up["tgt.csv"], "date_column": "dt",
                                "key_column": "id", "source_access": "STAGING_ONLY", "owner_name": "Pat", "owner_email": "pat@example.com"}).json()
    assert d["result"]["status"] == "DISCREPANCY" and d["clearance"] == "NEEDS_DECISION"
    bad = client.post(f"/api/v1/availability/{d['id']}/decision", json={"approver": "A", "decision": "PROCEED", "rationale": "short"})
    assert bad.status_code == 400
    ok = client.post(f"/api/v1/availability/{d['id']}/decision", json={"approver": "Ana Lead", "decision": "PROCEED",
                                                                       "rationale": "Customer confirmed the 5 rows are test data."}).json()
    assert ok["clearance"] == "PROCEED"
    # data changes -> the decision goes stale
    client.post(f"/api/v1/projects/{pid}/uploads", files=[("files", ("tgt2.csv", _csv(_days(100, skip=[1, 2]))))])
    ups = {u["filename"]: u["id"] for u in client.get("/api/v1/state?project=" + pid).json()["uploads"]}
    again = client.post(f"/api/v1/availability/{d['id']}/target", json={"target_upload": ups["tgt2.csv"]}).json()
    assert again["clearance"] == "NEEDS_DECISION" and "changed" in again["clearance_reason"]
    assert client.get(f"/api/v1/audit/verify?project={pid}").json()["valid"]


def test_limited_access_needs_owner_confirmation(client):
    pid, up = _proj_with_files(client, _days(50), _days(50))
    d = client.post(f"/api/v1/projects/{pid}/availability/datasets",
                    json={"name": "Store files", "source_upload": up["src.csv"], "target_upload": up["tgt.csv"], "date_column": "dt",
                          "source_access": "STAGING_ONLY", "owner_name": "Pat", "owner_email": "pat@example.com"}).json()
    assert d["result"]["status"] == "AVAILABLE" and d["clearance"] == "NEEDS_CONFIRMATION"   # matching counts are not enough
    r = client.post(f"/api/v1/availability/{d['id']}/request-confirmation").json()
    assert r["confirmation"]["status"] == "REQUESTED" and r["confirmation"]["draft_status"] == "DRAFT"
    st = client.get(f"/api/v1/state?project={pid}").json()
    n = [x for x in st["notifications"] if x["event"] == "SOURCE_CONFIRMATION_REQUEST"]
    assert len(n) == 1 and n[0]["status"] == "DRAFT" and "has not been sent" in n[0]["body"]
    r = client.post(f"/api/v1/availability/{d['id']}/confirm", json={"confirmer": "Pat", "note": "All 30 daily files are uploaded."}).json()
    assert r["clearance"] == "CLEAR"
    direct = client.post(f"/api/v1/projects/{pid}/availability/datasets",
                         json={"name": "Direct", "source_upload": up["src.csv"], "target_upload": up["tgt.csv"]}).json()
    assert client.post(f"/api/v1/availability/{direct['id']}/request-confirmation").status_code == 400


def test_availability_gate_in_readiness(client):
    sid = client.get("/api/v1/projects").json()[0]["id"]
    st = client.get("/api/v1/state").json()
    ds = {d["name"]: d for d in st["availability"]["datasets"]}
    assert ds["analytics.orders (store files)"]["result"]["status"] == "DISCREPANCY"
    assert ds["analytics.orders incremental changes"]["result"]["status"] == "MISSING"
    gate = next(g for g in st["risk"]["CustomerAnalytics"]["gates"] if g["gate"] == "DATA_AVAILABILITY")
    assert gate["status"] == "BLOCKED"
    for d in st["availability"]["datasets"]:
        if d["clearance"] == "NEEDS_DECISION":
            client.post(f"/api/v1/availability/{d['id']}/decision", json={"approver": "Test Lead", "decision": "PROCEED", "rationale": "Accepted for the test run."})
        if d["clearance"] == "NEEDS_CONFIRMATION":
            client.post(f"/api/v1/availability/{d['id']}/confirm", json={"confirmer": "Retail ops", "note": "All files uploaded."})
    st = client.get("/api/v1/state").json()
    gate = next(g for g in st["risk"]["CustomerAnalytics"]["gates"] if g["gate"] == "DATA_AVAILABILITY")
    assert gate["status"] == "PASS" and "accepted despite a difference" in gate["detail"]
    assert client.post(f"/api/v1/projects/{sid}/availability/datasets", json={"name": "nope", "source_upload": "x"}).status_code == 400


# --------------------------------------------------------------------------- Round 4: sign-off, risk follow-up, owner enrichment
def test_risk_items_do_not_duplicate_across_reruns(client):
    import accelerator.api as api
    client.get("/api/v1/state")
    before = api.orc().store.all("risk_items", project_id=api.orc().project_id)
    st = client.get("/api/v1/state").json()
    pending = next(m for m in st["mappings"] if m["approval_status"] == "PENDING")
    client.post(f"/api/v1/mappings/{pending['mapping_id']}/approve", json={"approver": "A", "decision": "APPROVED", "rationale": "x"})
    client.post(f"/api/v1/mappings/{pending['mapping_id']}/approve", json={"approver": "A", "decision": "REJECTED", "rationale": "y"})
    after = api.orc().store.all("risk_items", project_id=api.orc().project_id)
    assert len(after) == len(before) == len(set(r["id"] for r in after)) and len(after) > 0


def test_owner_override_persists_and_clears(client):
    st = client.get("/api/v1/state").json()
    unverified = next(s["application"] for s in st["stakeholders"] if not s["verified"])
    r = client.post(f"/api/v1/projects/{st['project']['id']}/applications/{unverified}/owner",
                    json={"name": "Pat Lee", "email": "pat@example.com"}).json()
    assert r["verified"] and r["name"] == "Pat Lee" and "manually" in r["owner_source"]
    bad = client.post(f"/api/v1/projects/{st['project']['id']}/applications/{unverified}/owner", json={"name": "P", "email": "bad"})
    assert bad.status_code == 400
    st2 = client.get("/api/v1/state").json()
    assert sum(1 for s in st2["stakeholders"] if s["application"] == unverified) == 1   # no duplicate row
    r = client.delete(f"/api/v1/projects/{st['project']['id']}/applications/{unverified}/owner").json()
    assert r["name"] == "Owner unknown"


def test_risk_followup_updates_item_and_survives_rerun(client):
    st = client.get("/api/v1/state").json()
    pid, app = st["project"]["id"], list(st["risk"])[0]
    dim = list(st["risk"][app]["dimensions"])[0]
    r = client.post(f"/api/v1/projects/{pid}/applications/{app}/risk/{dim}/followup",
                    json={"tracking_status": "IN_PROGRESS", "owner": "Sec Lead", "note": "Working on it."}).json()
    assert r["tracking_status"] == "IN_PROGRESS" and r["note"] == "Working on it."
    bad = client.post(f"/api/v1/projects/{pid}/applications/{app}/risk/{dim}/followup", json={"tracking_status": "NOT_A_STATUS"})
    assert bad.status_code == 400
    client.post(f"/api/v1/projects/{pid}/applications/{app}/risk/{dim}/followup", json={"tracking_status": "MITIGATED", "owner": "Sec Lead", "note": "Fixed."})
    st2 = client.get("/api/v1/state").json()
    item = st2["risk"][app]["items"][dim]
    assert item["status"] == "MITIGATED" and item["follow_up_note"] == "Fixed."


def test_signoff_refused_while_blocked_and_recorded_when_clear():
    import tempfile
    from accelerator.orchestrator import Orchestrator
    o = Orchestrator(Path(tempfile.mkdtemp()), log=lambda m: None)
    o.create_project(name="Unit", config={"sample": False})
    app = "UnitApp"
    o.ctx["risk"] = {app: {"application": app, "score": 10, "classification": "Low", "dimensions": {},
                          "gates": [{"gate": "ROLLBACK_TESTED", "status": "OPEN", "detail": "x"},
                                    {"gate": "OWNER_APPROVAL", "status": "BLOCKED", "detail": "no verified owner"}], "readiness": "BLOCKED"}}
    with pytest.raises(ValueError, match="OWNER_APPROVAL"):
        o.record_signoff(app, "business", "Biz Lead", "Business reason, long enough")
    with pytest.raises(ValueError, match="at least 10"):
        o.ctx["risk"][app]["gates"] = [g for g in o.ctx["risk"][app]["gates"] if g["gate"] != "OWNER_APPROVAL"]
        o.record_signoff(app, "business", "Biz Lead", "short")
    row = o.record_signoff(app, "business", "Biz Lead", "Business reason, long enough")
    assert row["status"] == "BUSINESS_ONLY"
    row = o.record_signoff(app, "technical", "Tech Lead", "Technical reason, long enough")
    assert row["status"] == "SIGNED_OFF"
    o._signoff_gates(o.ctx["risk"])
    gate = next(g for g in o.ctx["risk"][app]["gates"] if g["gate"] == "FORMAL_SIGNOFF")
    assert gate["status"] == "PASS" and o.ctx["risk"][app]["readiness"] == "SIGNED_OFF"
    assert o.store.verify_audit_chain()["valid"]
    # a new blocker makes the recorded sign-off stale
    o.ctx["risk"][app]["gates"] = [g for g in o.ctx["risk"][app]["gates"] if g["gate"] != "FORMAL_SIGNOFF"] + [
        {"gate": "OWNER_APPROVAL", "status": "BLOCKED", "detail": "owner left"}]
    o._signoff_gates(o.ctx["risk"])
    assert o.ctx["risk"][app]["readiness"] == "BLOCKED"
    # clearing the blocker but leaving the rest of the gate picture different from what was signed: still stale
    o.ctx["risk"][app]["gates"] = [g for g in o.ctx["risk"][app]["gates"] if g["gate"] not in ("FORMAL_SIGNOFF", "OWNER_APPROVAL")] + [
        {"gate": "ROLLBACK_TESTED", "status": "PASS", "detail": "rehearsed"}]
    o._signoff_gates(o.ctx["risk"])
    gate = next(g for g in o.ctx["risk"][app]["gates"] if g["gate"] == "FORMAL_SIGNOFF")
    assert gate["status"] == "BLOCKED" and "again" in gate["detail"]


def test_signoff_api_refused_while_blocked(client):
    st = client.get("/api/v1/state").json()
    app = list(st["risk"])[0]
    r = client.post(f"/api/v1/projects/{st['project']['id']}/applications/{app}/signoff",
                    json={"role": "business", "approver": "Biz Lead", "rationale": "Trying to sign off early"})
    assert r.status_code == 400 and "blocked" in r.json()["detail"].lower()


# --------------------------------------------------------------------------- Round 5: custom-project data validation
def _dqrows(n, miss=0, amt=lambda i: i * 2.5):
    return [f"{i},{amt(i):.2f},Cust{i}" for i in range(1, n + 1 - miss)]


DQ_APP = "CustomerAnalytics"   # the application tag carried by SAMPLE's main.tf, reused by _files()


def _proj_with_dq_files(client, src_rows, tgt_rows, header="order_id,amount,customer_name", assess=False):
    pid = client.post("/api/v1/projects", json={"name": "DQ test"}).json()["id"]
    files = [("files", ("src.csv", _csv(src_rows, header=header))), ("files", ("tgt.csv", _csv(tgt_rows, header=header)))]
    if assess:
        files += _files()
    client.post(f"/api/v1/projects/{pid}/uploads", files=files)
    if assess:
        client.post(f"/api/v1/projects/{pid}/assess")
    ups = {u["filename"]: u["id"] for u in client.get("/api/v1/state?project=" + pid).json()["uploads"]}
    return pid, ups


def test_sample_project_rejects_new_validation_objects(client):
    st = client.get("/api/v1/state").json()
    r = client.post(f"/api/v1/projects/{st['project']['id']}/validation/objects",
                    json={"name": "x", "key_column": "id", "source_upload": "a", "target_upload": "b"})
    assert r.status_code == 400 and "read-only" in r.json()["detail"].lower()


def test_validation_object_matching_data_clears_mandatory_reconciliation(client):
    pid, up = _proj_with_dq_files(client, _dqrows(20), _dqrows(20))
    r = client.post(f"/api/v1/projects/{pid}/validation/objects",
                    json={"name": "orders", "key_column": "order_id", "application": "App",
                          "source_upload": up["src.csv"], "target_upload": up["tgt.csv"], "pii_columns": ["customer_name"]})
    assert r.status_code == 201
    obj = r.json()
    assert obj["last_status"] == "PASS" and obj["open_exceptions"] == 0
    st = client.get(f"/api/v1/state?project={pid}").json()
    assert st["dq_summary"]["total"] == 1 and st["dq_summary"]["PASS"] == 1
    gates = [g for app_risk in st["risk"].values() for g in app_risk["gates"]]
    assert not any(g["gate"] == "MANDATORY_RECONCILIATION" for g in gates)


def test_validation_object_mismatch_blocks_gate_and_lists_exceptions(client):
    pid, up = _proj_with_dq_files(client, _dqrows(20), _dqrows(20, miss=2), assess=True)
    r = client.post(f"/api/v1/projects/{pid}/validation/objects",
                    json={"name": "orders", "key_column": "order_id", "application": DQ_APP,
                          "source_upload": up["src.csv"], "target_upload": up["tgt.csv"]})
    obj = r.json()
    assert obj["last_status"] == "FAIL" and obj["open_exceptions"] > 0
    st = client.get(f"/api/v1/state?project={pid}").json()
    gate = next(g for g in st["risk"][DQ_APP]["gates"] if g["gate"] == "MANDATORY_RECONCILIATION")
    assert gate["status"] == "BLOCKED" and "FAIL" in gate["detail"]
    assert len(st["exceptions"]) == obj["open_exceptions"]


def test_validation_object_bad_tolerance_rejected(client):
    pid, up = _proj_with_dq_files(client, _dqrows(20), _dqrows(20))
    r = client.post(f"/api/v1/projects/{pid}/validation/objects",
                    json={"name": "orders", "key_column": "order_id", "source_upload": up["src.csv"],
                          "target_upload": up["tgt.csv"], "tolerances": {"amount": "not-a-number"}})
    assert r.status_code == 400 and "amount" in r.json()["detail"]


def test_validation_object_rerun_flips_fail_to_pass(client):
    pid, up = _proj_with_dq_files(client, _dqrows(20), _dqrows(20, miss=2), assess=True)
    obj = client.post(f"/api/v1/projects/{pid}/validation/objects",
                      json={"name": "orders", "key_column": "order_id", "application": DQ_APP,
                            "source_upload": up["src.csv"], "target_upload": up["tgt.csv"]}).json()
    assert obj["last_status"] == "FAIL"
    client.post(f"/api/v1/projects/{pid}/uploads", files=[("files", ("tgt2.csv", _csv(_dqrows(20), header="order_id,amount,customer_name")))])
    fixed_up = next(u["id"] for u in client.get(f"/api/v1/state?project={pid}").json()["uploads"] if u["filename"] == "tgt2.csv")
    assert client.post(f"/api/v1/validation/objects/{obj['id']}/target", json={"target_upload": fixed_up}).status_code == 200
    r = client.post(f"/api/v1/validation/objects/{obj['id']}/rerun")
    assert r.status_code == 200 and r.json()["last_status"] == "PASS"
    st = client.get(f"/api/v1/state?project={pid}").json()
    assert not any(g["gate"] == "MANDATORY_RECONCILIATION" for g in st["risk"][DQ_APP]["gates"])


def test_exceptions_queue_update_rules(client):
    pid, up = _proj_with_dq_files(client, _dqrows(20), _dqrows(20, miss=2))
    client.post(f"/api/v1/projects/{pid}/validation/objects",
               json={"name": "orders", "key_column": "order_id", "application": "App",
                     "source_upload": up["src.csv"], "target_upload": up["tgt.csv"]})
    st = client.get(f"/api/v1/state?project={pid}").json()
    exc = st["exceptions"][0]
    bad = client.post(f"/api/v1/projects/{pid}/validation/exceptions/{exc['id']}", json={"remediation_status": "NOT_A_STATUS"})
    assert bad.status_code == 400
    ok = client.post(f"/api/v1/projects/{pid}/validation/exceptions/{exc['id']}",
                     json={"remediation_status": "IN_PROGRESS", "owner": "DQ Lead", "note": "Looking into it."})
    assert ok.status_code == 200 and ok.json()["remediation_status"] == "IN_PROGRESS"


def test_exceptions_queue_blocks_editing_system_closed_states(client):
    st = client.get("/api/v1/state").json()
    pid = st["project"]["id"]
    closed = next((e for e in st["exceptions"] if e["remediation_status"] in ("WAIVED", "RESOLVED_BY_RETRY")), None)
    assert closed is not None, "sample project should carry at least one system-closed exception"
    r = client.post(f"/api/v1/projects/{pid}/validation/exceptions/{closed['id']}", json={"remediation_status": "IN_PROGRESS"})
    assert r.status_code == 400


def test_monitoring_note_round_trips(client):
    pid, up = _proj_with_dq_files(client, _dqrows(20), _dqrows(20))
    client.post(f"/api/v1/projects/{pid}/validation/objects",
               json={"name": "orders", "key_column": "order_id", "application": "App",
                     "source_upload": up["src.csv"], "target_upload": up["tgt.csv"]})
    empty = client.get(f"/api/v1/projects/{pid}/applications/App/monitoring-note").json()
    assert empty.get("note", "") == ""
    r = client.post(f"/api/v1/projects/{pid}/applications/App/monitoring-note", json={"note": "Watch row counts for two weeks."})
    assert r.status_code == 200 and r.json()["note"] == "Watch row counts for two weeks."
    again = client.get(f"/api/v1/projects/{pid}/applications/App/monitoring-note").json()
    assert again["note"] == "Watch row counts for two weeks."


# --------------------------------------------------------------------------- availability: dataset linked to the right application
def test_availability_dataset_defaults_to_discovered_application(client):
    pid = client.post("/api/v1/projects", json={"name": "Named differently"}).json()["id"]
    client.post(f"/api/v1/projects/{pid}/uploads", files=_files() + [("files", ("src.csv", _csv(_days(30)))), ("files", ("tgt.csv", _csv(_days(30))))])
    client.post(f"/api/v1/projects/{pid}/assess")
    up = {u["filename"]: u["id"] for u in client.get(f"/api/v1/state?project={pid}").json()["uploads"]}
    base = f"/api/v1/projects/{pid}/availability/datasets"
    bad = client.post(base, json={"name": "Orders", "source_upload": up["src.csv"], "target_upload": up["tgt.csv"], "application": "NoSuchApp"})
    assert bad.status_code == 400 and "CustomerAnalytics" in bad.json()["detail"]
    d = client.post(base, json={"name": "Orders", "source_upload": up["src.csv"], "target_upload": up["tgt.csv"], "date_column": "dt"}).json()
    assert d["application"] == DQ_APP                       # not the project's own name
    gate = next(g for g in client.get(f"/api/v1/state?project={pid}").json()["risk"][DQ_APP]["gates"] if g["gate"] == "DATA_AVAILABILITY")
    assert gate["status"] == "PASS"


def test_availability_dataset_relink_and_unlinked_gate_detail(client):
    pid = client.post("/api/v1/projects", json={"name": "Relink"}).json()["id"]
    other = b'resource "aws_s3_bucket" "other" {\n  bucket = "other-app-data"\n  tags = { application = "OtherApp" }\n}\n'
    client.post(f"/api/v1/projects/{pid}/uploads", files=_files() + [("files", ("other.tf", other)),
                                                                     ("files", ("src.csv", _csv(_days(30)))), ("files", ("tgt.csv", _csv(_days(30))))])
    client.post(f"/api/v1/projects/{pid}/assess")
    up = {u["filename"]: u["id"] for u in client.get(f"/api/v1/state?project={pid}").json()["uploads"]}
    d = client.post(f"/api/v1/projects/{pid}/availability/datasets",
                    json={"name": "Orders", "source_upload": up["src.csv"], "target_upload": up["tgt.csv"]}).json()
    r = client.post(f"/api/v1/availability/{d['id']}/application", json={"application": ""}).json()
    assert r["application"] == ""
    st = client.get(f"/api/v1/state?project={pid}").json()
    assert {"OtherApp", DQ_APP} <= set(st["risk"])        # two applications: an unlinked dataset counts for neither
    gate = next(g for g in st["risk"][DQ_APP]["gates"] if g["gate"] == "DATA_AVAILABILITY")
    assert gate["status"] == "OPEN" and "not linked" in gate["detail"]
    assert client.post(f"/api/v1/availability/{d['id']}/application", json={"application": "Nope"}).status_code == 400
    r = client.post(f"/api/v1/availability/{d['id']}/application", json={"application": DQ_APP}).json()
    assert r["application"] == DQ_APP
    sid = client.get("/api/v1/projects").json()[0]["id"]
    sd = client.get("/api/v1/state").json()["availability"]["datasets"][0]
    assert client.post(f"/api/v1/availability/{sd['id']}/application", json={"application": DQ_APP}).status_code == 400


# --------------------------------------------------------------------------- Round 6: communications and dependencies
_PRODUCER_CONSUMER_TF = b'''resource "aws_s3_bucket" "raw" {
  bucket = "orders-raw"
  tags = { application = "Producer" }
}
resource "aws_lambda_function" "reader" {
  function_name = "orders-reader"
  runtime = "python3.12"
  handler = "main.handler"
  environment {
    variables = { BUCKET = aws_s3_bucket.raw.bucket }
  }
  tags = { application = "Consumer" }
}
'''


def _two_app_project(client):
    pid = client.post("/api/v1/projects", json={"name": "Cutover flow"}).json()["id"]
    rows = [f"{i},{i * 2.5:.2f}" for i in range(1, 30)]
    client.post(f"/api/v1/projects/{pid}/uploads", files=[("files", ("x.tf", _PRODUCER_CONSUMER_TF)),
                                                          ("files", ("s.csv", _csv(rows, header="id,amt"))),
                                                          ("files", ("t.csv", _csv(rows, header="id,amt")))])
    client.post(f"/api/v1/projects/{pid}/assess")
    return pid


def _gate(st, app, name):
    return next((g for g in st["risk"][app]["gates"] if g["gate"] == name), None)


def test_data_csvs_are_not_read_as_aws_inventory(client):
    pid = _two_app_project(client)
    st = client.get(f"/api/v1/state?project={pid}").json()
    assert {u["filename"]: u["category"] for u in st["uploads"]}["s.csv"] == "data"
    assert len(st["discovery"]["resources"]) == 2           # the bucket and the function, not 29 CSV rows


def test_notification_drafts_do_not_accumulate_and_keep_tracking(client):
    import accelerator.api as api
    st = client.get("/api/v1/state").json()
    pid, o = st["project"]["id"], api.orc()
    n0 = len(o.store.all("notifications", project_id=pid))
    n = next(x for x in st["notifications"] if x["status"] == "DRAFT")
    assert client.post(f"/api/v1/projects/{pid}/notifications/{n['id']}/track", json={"action": "SENT", "by": "Me"}).status_code == 400
    assert client.post(f"/api/v1/projects/{pid}/notifications/{n['id']}/approve", json={"approver": "Lead"}).json()["approval"] == "APPROVED"
    e = client.post(f"/api/v1/projects/{pid}/notifications/{n['id']}/edit",
                    json={"subject": "Reworded subject", "body": "A reworded body that is long enough.", "editor": "Lead"}).json()
    assert e["approval"] == "CHANGED_SINCE_APPROVAL"
    r = client.post(f"/api/v1/projects/{pid}/notifications/{n['id']}/track", json={"action": "SENT", "by": "Me"})
    assert r.status_code == 400 and "changed" in r.json()["detail"]
    client.post(f"/api/v1/projects/{pid}/notifications/{n['id']}/approve", json={"approver": "Lead"})
    assert client.post(f"/api/v1/projects/{pid}/notifications/{n['id']}/track",
                       json={"action": "SENT", "by": "Me", "channel": "Outlook"}).json()["delivery"] == "SENT"
    assert client.post(f"/api/v1/projects/{pid}/notifications/{n['id']}/edit",
                       json={"subject": "Too late now", "body": "Edits after sending are refused.", "editor": "Lead"}).status_code == 400
    r = client.post(f"/api/v1/projects/{pid}/notifications/{n['id']}/track",
                    json={"action": "REPLIED", "by": "Owner", "outcome": "OBJECTION", "note": "Blackout week"}).json()
    assert r["delivery"] == "REPLIED" and r["tracking"]["sent_snapshot"]["subject"] == "Reworded subject"
    for decision in ("APPROVED", "REJECTED"):                 # two re-evaluations
        pend = next(m for m in client.get("/api/v1/state").json()["mappings"] if m["approval_status"] == "PENDING")
        client.post(f"/api/v1/mappings/{pend['mapping_id']}/approve", json={"approver": "A", "decision": decision, "rationale": "x"})
    st = client.get("/api/v1/state").json()
    assert len(o.store.all("notifications", project_id=pid)) == n0
    kept = next(x for x in st["notifications"] if x["id"] == n["id"])
    assert kept["delivery"] == "REPLIED" and st["notification_summary"]["objections"] == 1
    assert client.get(f"/api/v1/audit/verify?project={pid}").json()["valid"]


def test_inferred_link_needs_owner_direction_before_downstream_gate(client):
    pid = _two_app_project(client)
    st = client.get(f"/api/v1/state?project={pid}").json()
    assert _gate(st, "Producer", "DOWNSTREAM_READY")["status"] == "OPEN"      # unconfirmed: not guessed either way
    e = st["graph"]["edges"][0]
    assert e["flow"] == "UNCONFIRMED"
    bad = client.post(f"/api/v1/projects/{pid}/dependencies/confirm", json={"source": "x", "target": "y", "outcome": "OWNER_CONFIRMED", "by": "A", "note": "hello"})
    assert bad.status_code == 400
    client.post(f"/api/v1/projects/{pid}/dependencies/confirm",
                json={"source": e["source"], "target": e["target"], "outcome": "OWNER_REVERSED", "by": "Consumer owner", "note": "the function reads the bucket"})
    st = client.get(f"/api/v1/state?project={pid}").json()
    assert st["downstream_apps"]["Producer"] == ["Consumer"]
    g = _gate(st, "Producer", "DOWNSTREAM_READY")
    assert g["status"] == "BLOCKED" and "Consumer" in g["detail"]
    client.post(f"/api/v1/projects/{pid}/dependencies/confirm",
                json={"source": e["source"], "target": e["target"], "outcome": "OWNER_REJECTED", "by": "Consumer owner", "note": "not used any more"})
    st = client.get(f"/api/v1/state?project={pid}").json()
    assert _gate(st, "Producer", "DOWNSTREAM_READY") is None and st["downstream_apps"]["Producer"] == []


def test_cutover_needs_signoff_and_drafts_started_completed_to_downstream_owners(client):
    pid = _two_app_project(client)
    st = client.get(f"/api/v1/state?project={pid}").json()
    e = st["graph"]["edges"][0]
    client.post(f"/api/v1/projects/{pid}/dependencies/confirm",
                json={"source": e["source"], "target": e["target"], "outcome": "OWNER_REVERSED", "by": "Consumer owner", "note": "the function reads the bucket"})
    for m in st["mappings"]:
        client.post(f"/api/v1/mappings/{m['mapping_id']}/approve", json={"approver": "Arch", "decision": "APPROVED", "rationale": "ok"})
    for app in ("Producer", "Consumer"):
        client.post(f"/api/v1/projects/{pid}/applications/{app}/owner", json={"name": f"{app} Owner", "email": f"{app.lower()}@example.com"})
    up = {u["filename"]: u["id"] for u in client.get(f"/api/v1/state?project={pid}").json()["uploads"]}
    client.post(f"/api/v1/projects/{pid}/validation/objects",
                json={"name": "orders", "key_column": "id", "application": "Producer", "source_upload": up["s.csv"], "target_upload": up["t.csv"]})
    r = client.post(f"/api/v1/projects/{pid}/applications/Producer/migration-status", json={"status": "IN_PROGRESS", "by": "Lead", "note": "Opening the window"})
    assert r.status_code == 400 and "signed off" in r.json()["detail"]
    assert client.post(f"/api/v1/projects/{pid}/applications/Producer/bridges/Nobody", json={"by": "X", "note": "not downstream"}).status_code == 400
    client.post(f"/api/v1/projects/{pid}/applications/Producer/bridges/Consumer",
                json={"by": "Consumer owner", "note": "reads a replicated copy on AWS for two weeks"})
    assert _gate(client.get(f"/api/v1/state?project={pid}").json(), "Producer", "DOWNSTREAM_READY")["status"] == "PASS"
    for role in ("business", "technical"):
        assert client.post(f"/api/v1/projects/{pid}/applications/Producer/signoff",
                           json={"role": role, "approver": f"{role} lead", "rationale": "All gates reviewed and clear"}).status_code == 200
    assert client.post(f"/api/v1/projects/{pid}/applications/Producer/migration-status",
                       json={"status": "MIGRATED", "by": "Lead", "note": "Skipping the start"}).status_code == 400
    assert client.post(f"/api/v1/projects/{pid}/applications/Producer/migration-status",
                       json={"status": "IN_PROGRESS", "by": "Lead", "note": "short"}).status_code == 400
    assert client.post(f"/api/v1/projects/{pid}/applications/Producer/migration-status",
                       json={"status": "IN_PROGRESS", "by": "Lead", "note": "Cutover window opened 22:00"}).status_code == 200
    client.post(f"/api/v1/projects/{pid}/applications/Producer/migration-status",
                json={"status": "MIGRATED", "by": "Lead", "note": "Cutover finished, checks green"})
    st = client.get(f"/api/v1/state?project={pid}").json()
    sent_to = {(n["event"], n["recipient"]) for n in st["notifications"] if n["event"].startswith("MIGRATION_")}
    assert sent_to == {(ev, who) for ev in ("MIGRATION_STARTED", "MIGRATION_COMPLETED")
                       for who in ("producer@example.com", "consumer@example.com")}
    down = next(n for n in st["notifications"] if n["event"] == "MIGRATION_STARTED" and n["recipient"] == "consumer@example.com")
    assert "replicated copy" in down["body"] and down["approval"] == "PENDING" and down["delivery"] == "NOT_SENT"
    assert st["app_status"]["Producer"]["status"] == "MIGRATED"
    assert {n["id"]: n["migration_status"] for n in st["graph"]["nodes"]}["cod:orders-raw"] == "MIGRATED"
    # an application outside the assessment can be recorded as migrated (its team says so) without sign-off here
    assert client.post(f"/api/v1/projects/{pid}/applications/NotHere/migration-status",
                       json={"status": "MIGRATED", "by": "x", "note": "not in the graph at all"}).status_code == 404


# --------------------------------------------------------------------------- Round 7: load controls and conversion extras
def test_generated_tests_catch_what_syntax_checks_miss():
    from accelerator import codecheck
    glue = {"file_type": "glue_pyspark", "source_file": "job.py", "source_text": "",
            "outputs": {"etl/job.py": "from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\n"
                                      "df = glueContext.create_dynamic_frame()\nx = Filter.apply(frame=df)\n"}}
    res, files = codecheck.run_tests(glue)
    und = next(r for r in res if "every name used is defined" in r["name"])
    assert und["status"] == "FAIL" and "glueContext" in und["detail"] and "Filter" in und["detail"]
    assert "tests/test_job_generated.py" in files and "def test_compiles_and_names_defined" in files["tests/test_job_generated.py"]
    lam = {"file_type": "lambda_python", "source_file": "h.py", "source_text": "def lambda_handler(e, c):\n    return 1\n",
           "outputs": {"app/main.py": "import boto3\ndef handler(request):\n    return gcp_compat.x()\n"}}
    res, _ = codecheck.run_tests(lam)
    assert {r["name"].split(": ")[-1]: r["status"] for r in res}["no AWS SDK imports"] == "FAIL"
    assert next(r for r in res if r["name"] == "functions kept from the source")["status"] == "REVIEW"
    tf = {"file_type": "terraform", "source_file": "m.tf", "source_text": "",
          "outputs": {"terraform/main.tf": 'resource "google_compute_instance" "w" { image = var.migrated_image_web }\n'
                                           'resource "aws_lb" "front" {}\n', "terraform/variables.tf": 'variable "project_id" {}\n'}}
    res, files = codecheck.run_tests(tf)
    s = {r["name"]: r for r in res}
    assert s["terraform: every var.* is declared"]["status"] == "FAIL" and "migrated_image_web" in s["terraform: every var.* is declared"]["detail"]
    assert s["terraform: no AWS resources left"]["status"] == "FAIL"
    assert s["terraform validate / plan in a GCP project"]["status"] == "NOT_RUN" and s["terraform validate / plan in a GCP project"]["ran"] == "not run"
    sql = {"file_type": "sql", "source_file": "q.sql", "source_text": "SELECT a FROM sales.fact;",
           "outputs": {"sql/q.sql": "SELECT a FROM `ds.fact`;"}}
    res, files = codecheck.run_tests(sql)
    assert "NOT RUN" in files["tests/parity_q.sql"] and "fact" in files["tests/parity_q.sql"]


def test_sample_conversion_tests_flag_undeclared_cloudformation_variables(client):
    st = client.get("/api/v1/state").json()
    cf = next(c for c in st["conversions"] if c["file_type"] == "cloudformation")
    t = next(x for x in cf["tests"] if x["name"] == "terraform: every var.* is declared")
    assert t["status"] == "FAIL" and "project_id" in t["detail"]
    assert cf["history"] and cf["current_n"] == 1 and cf["approval_status"] == "PENDING_REVIEW"


def test_code_versions_edit_review_diff_and_carry_over(client):
    st = client.get("/api/v1/state").json()
    pid = st["project"]["id"]
    cf = next(c for c in st["conversions"] if c["file_type"] == "cloudformation")
    path = next(k for k in cf["outputs"] if k.endswith(".tf"))
    assert client.post(f"/api/v1/projects/{pid}/conversions/{cf['id']}/review", json={"approver": "Eng", "note": "short"}).status_code == 400
    fixed = 'variable "project_id" { type = string }\nvariable "source_bucket" { type = string }\n' + cf["outputs"][path]
    r = client.post(f"/api/v1/projects/{pid}/conversions/{cf['id']}/edit", json={"path": path, "content": fixed, "by": "Eng", "note": "declare variables"}).json()
    assert r["tests"]["FAIL"] == 0
    assert client.post(f"/api/v1/projects/{pid}/conversions/{cf['id']}/edit",
                       json={"path": path, "content": "x", "by": "Eng", "note": "again"}).status_code == 400   # not the latest version
    cur = next(c for c in client.get("/api/v1/state").json()["conversions"] if c["file_type"] == "cloudformation")
    assert cur["current_n"] == 2 and cur["current_kind"] == "EDITED" and cur["outputs"][path] == fixed
    d = client.get(f"/api/v1/projects/{pid}/conversions/{cf['id']}/diff/{cur['id']}").json()
    assert list(d["changed"]) == [path] and '+variable "project_id"' in d["changed"][path]
    assert client.post(f"/api/v1/projects/{pid}/conversions/{cf['id']}/review",
                       json={"approver": "Eng", "decision": "APPROVED", "note": "reviewed the old one"}).status_code == 400
    r = client.post(f"/api/v1/projects/{pid}/conversions/{cur['id']}/review", json={"approver": "Eng", "decision": "APPROVED", "note": "variables declared now"})
    assert r.status_code == 200 and r.json()["approval_status"] == "APPROVED"
    # a regenerated version with identical content keeps the approval (custom project, assessed twice)
    pid2 = client.post("/api/v1/projects", json={"name": "Versions twice"}).json()["id"]
    client.post(f"/api/v1/projects/{pid2}/uploads", files=_files())
    client.post(f"/api/v1/projects/{pid2}/assess")
    c1 = client.get(f"/api/v1/state?project={pid2}").json()["conversions"][0]
    client.post(f"/api/v1/projects/{pid2}/conversions/{c1['id']}/review", json={"approver": "Eng", "decision": "APPROVED", "note": "first look is fine"})
    client.post(f"/api/v1/projects/{pid2}/assess")
    c2 = client.get(f"/api/v1/state?project={pid2}").json()["conversions"][0]
    assert c2["current_n"] == 2 and c2["approval_status"] == "APPROVED" and "Carried over" in c2["approval_note"]


def test_load_indicators_controls_and_schema_check(client):
    from accelerator import loadops
    st = client.get("/api/v1/state").json()
    pid, lc = st["project"]["id"], st["load_controls"]
    full = next(r for r in st["migration"]["runs"] if r["run_type"] == "FULL_HISTORICAL_BACKFILL" and r["indicators"]["peak_parallel"])
    i = full["indicators"]
    assert i["bytes"] > 0 and i["rows"] > 0 and 1 <= i["peak_parallel"] <= i["max_parallel"] == 4
    assert lc["schema"]["alerts"] == [] and set(lc["schema"]["lineage"]) == loadops.LINEAGE
    assert lc["plan"]["gb"] > 0 and lc["plan"]["cost"]["aws_egress"] > 0 and lc["plan"]["cost"]["bigquery_load"] == 0
    assert loadops.egress_cost(20000, loadops.rates()) == round(10240 * 0.09 + 9760 * 0.085, 4)
    assert client.post(f"/api/v1/projects/{pid}/load-controls", json={"plan_id": lc["plan_id"], "max_parallel": 12, "by": "DBA", "note": "far too many"}).status_code == 400
    assert client.post(f"/api/v1/projects/{pid}/load-controls", json={"plan_id": lc["plan_id"], "max_parallel": 2, "by": "DBA", "note": "short"}).status_code == 400
    r = client.post(f"/api/v1/projects/{pid}/load-controls", json={"plan_id": lc["plan_id"], "max_parallel": 2, "by": "DBA", "note": "source takes two exports at most"})
    assert r.json()["max_parallel"] == 2
    st = client.get("/api/v1/state").json()
    assert st["load_controls"]["max_parallel"] == 2 and st["load_controls"]["history"][-1]["by"] == "DBA"
    # schema drift: a column added to the source after the plan was made is reported
    import sqlite3
    import accelerator.api as api
    db = api.orc().workdir / "aws_source_sim.db"
    con = sqlite3.connect(db)
    con.execute("ALTER TABLE analytics_orders ADD COLUMN channel TEXT")
    con.commit()
    con.close()
    alerts = client.get("/api/v1/state").json()["load_controls"]["schema"]["alerts"]
    assert any("channel" in a for a in alerts) and any("changed since the plan" in a for a in alerts)


def test_availability_schema_drift_between_checks(client):
    pid, up = _proj_with_files(client, _days(30), _days(30))
    d = client.post(f"/api/v1/projects/{pid}/availability/datasets",
                    json={"name": "Orders", "source_upload": up["src.csv"], "target_upload": up["tgt.csv"], "date_column": "dt"}).json()
    assert not d["result"].get("drift")
    client.post(f"/api/v1/projects/{pid}/uploads", files=[("files", ("src2.csv", _csv([r + ",web" for r in _days(30)], header="id,dt,amt,channel")))])
    src2 = next(u["id"] for u in client.get(f"/api/v1/state?project={pid}").json()["uploads"] if u["filename"] == "src2.csv")
    r = client.post(f"/api/v1/availability/{d['id']}/source", json={"source_upload": src2, "target_upload": up["tgt.csv"]}).json()
    assert r["result"]["drift"]["source"]["added"] == ["channel"] and r["source_label"] == "src2.csv"
    assert "channel" in " ".join(r["result"]["issues"])          # and the target is now missing that column
