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
