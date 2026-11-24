"""Data-pipeline, orchestration and identity converters.

  * Glue PySpark      -> Dataproc Serverless PySpark (BigQuery + GCS connectors)
  * Step Functions    -> Google Cloud Workflows YAML
  * MWAA Airflow DAG  -> Cloud Composer DAG (Google provider operators)
  * IAM policy JSON   -> GCP IAM role bindings (Terraform) with least-privilege flags
  * DynamoDB export   -> Firestore/BigQuery-ready plain JSON (typed attribute unwrapping)
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml

from .common import ConversionResult, check_hcl, check_no_aws_refs, check_python, check_yaml

S3 = re.compile(r"s3://")


# --------------------------------------------------------------------------- Glue
def convert_glue(path: Path) -> ConversionResult:
    res = ConversionResult("glue-to-dataproc-serverless", path.name)
    src = path.read_text(encoding="utf-8")
    rules = [
        (r"^from awsglue[^\n]*\n", "", "awsglue imports removed"),
        (r"^from pyspark\.context import SparkContext\n", "", "SparkContext import not needed"),
        (r"^import sys\n", "import argparse\n", None),
        (r'args = getResolvedOptions\(sys\.argv, \[([^\]]*)\]\)',
         lambda m: "_p = argparse.ArgumentParser()\n" + "\n".join(
             f'_p.add_argument("--{a.strip().strip(chr(34))}")' for a in m.group(1).split(",")) +
         "\nargs = vars(_p.parse_args())", "getResolvedOptions -> argparse (Dataproc batch args)"),
        (r"sc = SparkContext\(\)\n", "", None),
        (r"glueContext = GlueContext\(sc\)\n", "", "GlueContext removed"),
        (r"spark = glueContext\.spark_session",
         'from pyspark.sql import SparkSession\nfrom pyspark.sql import functions as F\n'
         'spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()\n'
         'PROJECT = spark.conf.get("spark.migration.project", "PROJECT_ID")', "spark_session -> SparkSession"),
        (r"job = Job\(glueContext\)\njob\.init\([^)]*\)\n", "", "Job.init removed (bookmarks -> explicit watermark)"),
        (r'glueContext\.create_dynamic_frame\.from_catalog\(database="(\w+)", table_name="(\w+)"\)',
         r'spark.read.format("bigquery").option("table", f"{PROJECT}.\1.\2").load()', "catalog table -> BigQuery table"),
        (r'glueContext\.create_dynamic_frame\.from_options\(\s*connection_type="s3",\s*connection_options=\{"paths": \[([^\]]*)\]\},\s*format="(\w+)",?\s*\)',
         r'spark.read.format("\2").load([\1])', "from_options(s3) -> spark.read (gs://)"),
        (r"ApplyMapping\.apply\(frame=(\w+), mappings=\[(.*?)\]\)",
         lambda m: f"{m.group(1)}.select(" + ", ".join(
             f'F.col("{a}").cast("{t}").alias("{b}")' for a, _, b, t in
             re.findall(r'\("(\w+)",\s*"([\w(),]+)",\s*"(\w+)",\s*"([\w(),]+)"\)', m.group(2))) + ")",
         "ApplyMapping -> select/cast"),
        (r"\.toDF\(\)", "", "DynamicFrame.toDF removed (already DataFrame)"),
        (r"DynamicFrame\.fromDF\((\w+), glueContext, \"\w+\"\)", r"\1", "DynamicFrame.fromDF removed"),
        (r'glueContext\.write_dynamic_frame\.from_options\(\s*frame=(\w+),\s*connection_type="s3",\s*connection_options=\{"path": "([^"]+)", "partitionKeys": \[([^\]]*)\]\},\s*format="(\w+)",?\s*\)',
         r'\1.write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy(\3).format("\4").save("\2")',
         "write_dynamic_frame -> DataFrame.write (dynamic partition overwrite = idempotent)"),
        (r"job\.commit\(\)", "spark.stop()", "job.commit -> spark.stop"),
    ]
    code = src
    for pat, rep, note in rules:
        new = re.sub(pat, rep, code, flags=re.M | re.S)
        res.constructs_total += 1 if note else 0
        if new != code and note:
            res.constructs_converted += 1
            res.mapping_log.append({"rule": note})
        elif note and re.search(pat.split("(")[0][:15].replace("\\", ""), src):
            res.warnings.append(f"rule not applied: {note}")
        code = new
    code = S3.sub("gs://", code)
    if "awsglue" in code or "glueContext" in code:
        res.unsupported.append("residual Glue API usage – manual rewrite")
    if "job.init" in src:
        res.warnings.append("Glue job bookmarks replaced by explicit watermark: pass --run_date and filter source by it")
    res.constructs_total = max(res.constructs_total, 1)
    res.outputs["etl/customer_etl_dataproc.py"] = "# GENERATED: Glue -> Dataproc Serverless PySpark. Review before run.\n" + code
    res.outputs["etl/submit_batch.sh"] = (
        "#!/usr/bin/env bash\n# Requires approval. Never executed by the accelerator.\n"
        "gcloud dataproc batches submit pyspark etl/customer_etl_dataproc.py \\\n"
        "  --region=us-east1 --version=2.2 \\\n"
        "  --properties=spark.migration.project=${PROJECT_ID} \\\n"
        "  -- --JOB_NAME customer-etl --run_date ${RUN_DATE}\n")
    res.assumptions.append("Glue Data Catalog database 'customer_raw' -> BigQuery dataset of same name (or BigLake Metastore)")
    res.checks += [check_python(res.outputs["etl/customer_etl_dataproc.py"], "customer_etl_dataproc.py"),
                   check_no_aws_refs(res.outputs["etl/customer_etl_dataproc.py"], "customer_etl_dataproc.py")]
    return res


# --------------------------------------------------------------------------- Step Functions -> Workflows
def _sf_task(name: str, st: dict, res: ConversionResult) -> list[dict]:
    r = st.get("Resource", "")
    if r.startswith("arn:aws:lambda") or ":function:" in r:
        fn = r.split(":")[-1]
        call = {"call": "http.post", "args": {"url": f"${{\"https://\" + sys.get_env(\"GOOGLE_CLOUD_LOCATION\") + \"-\" + sys.get_env(\"GOOGLE_CLOUD_PROJECT_ID\") + \".cloudfunctions.net/{fn}\"}}",
                                                 "auth": {"type": "OIDC"}, "body": "${input}"}, "result": f"{name}_result"}
        res.mapping_log.append({"state": name, "from": "Lambda invoke", "to": "http.post (OIDC) to Cloud Run function"})
    elif "glue:startJobRun" in r:
        call = {"call": "googleapis.dataproc.v1.projects.locations.batches.create",
                "args": {"parent": "${\"projects/\" + sys.get_env(\"GOOGLE_CLOUD_PROJECT_ID\") + \"/locations/us-east1\"}",
                         "body": {"pysparkBatch": {"mainPythonFileUri": "gs://${source_bucket}/etl/customer_etl_dataproc.py"}}},
                "result": f"{name}_result"}
        res.mapping_log.append({"state": name, "from": "glue:startJobRun.sync", "to": "Dataproc batches.create (connector waits for LRO)"})
    elif "sns:publish" in r:
        topic = (st.get("Parameters") or {}).get("TopicArn", "").split(":")[-1]
        call = {"call": "googleapis.pubsub.v1.projects.topics.publish",
                "args": {"topic": f"${{\"projects/\" + sys.get_env(\"GOOGLE_CLOUD_PROJECT_ID\") + \"/topics/{topic}\"}}",
                         "body": {"messages": [{"data": "${base64.encode(text.encode(\"done\"))}"}]}}}
        res.mapping_log.append({"state": name, "from": "sns:publish", "to": "Pub/Sub publish connector"})
    else:
        res.unsupported.append(f"{name}: Task resource {r} has no connector rule")
        return [{name: {"raise": f"UNSUPPORTED task {r}"}}]
    step: dict[str, Any] = {name: call}
    if st.get("Retry"):
        rt = st["Retry"][0]
        step = {name: {"try": {"steps": [{f"{name}_call": call}]},
                       "retry": {"predicate": "${http.default_retry_predicate}", "max_retries": rt.get("MaxAttempts", 3),
                                 "backoff": {"initial_delay": rt.get("IntervalSeconds", 1), "max_delay": 300,
                                             "multiplier": rt.get("BackoffRate", 2.0)}}}}
        res.mapping_log.append({"state": name, "from": "Retry", "to": "try/retry with backoff"})
    return [step]


def _sf_condition(c: dict) -> str:
    var = c["Variable"].replace("$.", "input.")
    for op, sym in (("NumericGreaterThan", ">"), ("NumericLessThan", "<"), ("NumericEquals", "=="),
                    ("StringEquals", "=="), ("BooleanEquals", "==")):
        if op in c:
            v = c[op]
            return f"${{{var} {sym} {json.dumps(v)}}}"
    return "${false}"


def _sf_states(states: dict, start: str, res: ConversionResult) -> list[dict]:
    steps, seen, cur = [], set(), start
    order = []
    while cur and cur not in seen:  # linearise main path; branches referenced via next
        seen.add(cur)
        order.append(cur)
        st = states[cur]
        cur = st.get("Next") or (st.get("Default") if st.get("Type") == "Choice" else None)
    order += [s for s in states if s not in seen]
    for name in order:
        st = states[name]
        t = st["Type"]
        res.constructs_total += 1
        if t == "Task":
            steps += _sf_task(name, st, res)
        elif t == "Choice":
            steps.append({name: {"switch": [{"condition": _sf_condition(c), "next": c["Next"]} for c in st["Choices"]],
                                 "next": st.get("Default", "end")}})
        elif t == "Wait":
            steps.append({name: {"call": "sys.sleep", "args": {"seconds": st.get("Seconds", 1)}}})
        elif t == "Pass":
            steps.append({name: {"assign": [{"pass_result": st.get("Result", "${input}")}]}})
        elif t == "Parallel":
            branches = [{f"branch_{i}": {"steps": _sf_states(b["States"], b["StartAt"], res)}}
                        for i, b in enumerate(st["Branches"])]
            steps.append({name: {"parallel": {"branches": branches}}})
        elif t == "Map":
            it = st.get("ItemsPath", "$.items").replace("$.", "input.")
            steps.append({name: {"parallel": {"for": {"value": "item", "in": f"${{{it}}}",
                                                       "steps": _sf_states(st["Iterator"]["States"], st["Iterator"]["StartAt"], res)}}}})
        elif t == "Succeed":
            steps.append({name: {"return": "${input}"}})
        elif t == "Fail":
            steps.append({name: {"raise": st.get("Error", "Failed")}})
        else:
            res.unsupported.append(f"{name}: state type {t}")
            continue
        res.constructs_converted += 1
        nxt = st.get("Next")
        last = steps[-1][name]
        if nxt and isinstance(last, dict) and "switch" not in last:
            last["next"] = nxt
        if st.get("End") and isinstance(last, dict):
            last.setdefault("next", "end")
    return steps


def convert_step_functions(path: Path) -> ConversionResult:
    res = ConversionResult("stepfunctions-to-workflows", path.name)
    asl = json.loads(path.read_text(encoding="utf-8"))
    wf = {"main": {"params": ["input"], "steps": _sf_states(asl["States"], asl["StartAt"], res)}}
    text = "# GENERATED: Step Functions (ASL) -> Google Cloud Workflows. Review before deploy.\n" + \
           yaml.safe_dump(wf, sort_keys=False, width=140)
    res.outputs["orchestration/customer_workflow.yaml"] = text
    res.assumptions.append("JSONPath ($.x) -> Workflows expressions (input.x); ResultPath/OutputPath need review")
    res.checks += [check_yaml(text, "customer_workflow.yaml"), check_no_aws_refs(text, "customer_workflow.yaml")]
    return res


# --------------------------------------------------------------------------- Airflow (MWAA -> Composer)
AIRFLOW_RULES = [
    ("from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor",
     "from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor"),
    ("from airflow.providers.amazon.aws.operators.glue import GlueJobOperator",
     "from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator"),
    ("from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator",
     "from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator"),
    ("from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator",
     "from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator"),
]


def convert_airflow(path: Path) -> ConversionResult:
    res = ConversionResult("mwaa-to-cloud-composer", path.name)
    code = path.read_text(encoding="utf-8")
    for a, b in AIRFLOW_RULES:
        res.constructs_total += 1
        if a in code:
            code = code.replace(a, b)
            res.constructs_converted += 1
            res.mapping_log.append({"from": a.split()[-1], "to": b.split()[-1]})
    code = re.sub(r'S3KeySensor\(task_id="(\w+)", bucket_name="([\w-]+)", bucket_key="([^"]+)"\)',
                  r'GCSObjectExistenceSensor(task_id="\1", bucket="\2", object="\3")', code)
    code = re.sub(r'GlueJobOperator\(task_id="(\w+)", job_name="([\w-]+)"\)',
                  r'DataprocCreateBatchOperator(task_id="\1", region="us-east1", batch_id="\2-{{ ds_nodash }}", '
                  r'batch={"pyspark_batch": {"main_python_file_uri": "gs://customer-code/etl/customer_etl_dataproc.py", '
                  r'"args": ["--JOB_NAME", "\2", "--run_date", "{{ ds }}"]}})', code)
    code = re.sub(r'S3ToRedshiftOperator\(task_id="(\w+)", s3_bucket="([\w-]+)", s3_key="([\w/-]+)",\s*schema="(\w+)", table="(\w+)",[^)]*\]\)',
                  r'GCSToBigQueryOperator(task_id="\1", bucket="\2", source_objects=["\3/*"], source_format="PARQUET", '
                  r'destination_project_dataset_table="\4.\5", write_disposition="WRITE_APPEND")', code, flags=re.S)
    code = re.sub(r'RedshiftSQLOperator\(task_id="(\w+)", sql="CALL ([\w.]+)\(\);"\)',
                  r'BigQueryInsertJobOperator(task_id="\1", configuration={"query": {"query": "CALL \2();", "useLegacySql": False}})', code)
    if "amazon" in code:
        res.unsupported.append("residual amazon provider operators")
    if "CALL " in code:
        res.manual_actions.append("Stored procedure analytics.build_customer_marts must be re-implemented in BigQuery scripting")
    res.warnings.append("Airflow Connections/Variables (aws_default, redshift_default) -> google_cloud_default + Secret Manager backend")
    res.outputs["orchestration/customer_daily_dag_composer.py"] = "# GENERATED: MWAA -> Cloud Composer 2/3 DAG. Review before deploy.\n" + code
    res.checks += [check_python(code, "customer_daily_dag_composer.py"), check_no_aws_refs(code, "customer_daily_dag_composer.py")]
    return res


# --------------------------------------------------------------------------- IAM
IAM_ACTION_ROLE = [
    (r"^s3:(Get|List)", "roles/storage.objectViewer", "bucket"),
    (r"^s3:Put", "roles/storage.objectCreator", "bucket"),
    (r"^s3:Delete", "roles/storage.objectAdmin", "bucket"),
    (r"^s3:\*", "roles/storage.admin", "bucket"),
    (r"^dynamodb:(Get|Query|Scan|BatchGet)", "roles/datastore.viewer", "project"),
    (r"^dynamodb:(Put|Update|Delete|BatchWrite)", "roles/datastore.user", "project"),
    (r"^secretsmanager:GetSecretValue", "roles/secretmanager.secretAccessor", "secret"),
    (r"^sqs:SendMessage|^sns:Publish", "roles/pubsub.publisher", "topic"),
    (r"^sqs:(Receive|Delete)", "roles/pubsub.subscriber", "subscription"),
    (r"^glue:\*", "roles/dataproc.editor", "project"),
    (r"^glue:", "roles/dataproc.editor", "project"),
    (r"^kms:(Decrypt|Encrypt|GenerateDataKey)", "roles/cloudkms.cryptoKeyEncrypterDecrypter", "key"),
    (r"^logs:", "roles/logging.logWriter", "project"),
    (r"^redshift-data:|^redshift:", "roles/bigquery.jobUser", "project"),
]


def convert_iam_policy(path: Path, sa_name: str = "customer-etl-sa") -> ConversionResult:
    res = ConversionResult("iam-policy-to-gcp-iam", path.name)
    pol = json.loads(path.read_text(encoding="utf-8"))
    blocks = [f'resource "google_service_account" "etl" {{\n  account_id   = "{sa_name}"\n  display_name = "Converted from customer-etl-role"\n}}']
    seen = set()
    for i, st in enumerate(pol.get("Statement", [])):
        res.constructs_total += 1
        acts = st.get("Action", [])
        acts = [acts] if isinstance(acts, str) else acts
        resources = st.get("Resource", [])
        resources = [resources] if isinstance(resources, str) else resources
        if st.get("Effect") == "Deny":
            res.manual_actions.append(f"Statement {st.get('Sid', i)}: Deny{' with condition' if st.get('Condition') else ''} "
                                      "-> IAM deny policy or Organization Policy; conditions -> IAM Conditions/tags")
            continue
        for a in acts:
            role, scope = next(((r, s) for p, r, s in IAM_ACTION_ROLE if re.search(p, a)), (None, None))
            if not role:
                res.unsupported.append(f"{st.get('Sid', i)}: action {a} has no role rule")
                continue
            buckets = sorted({r.split(":::")[1].split("/")[0] for r in resources if r.startswith("arn:aws:s3:::")})
            targets = buckets if scope == "bucket" and buckets else ["*"]
            for t in targets:
                key = (role, scope, t)
                if key in seen:
                    continue
                seen.add(key)
                if scope == "bucket" and t != "*":
                    rn = re.sub(r"[^a-z0-9_]", "_", f"{t}_{role.split('.')[-1]}".lower())
                    blocks.append(f'resource "google_storage_bucket_iam_member" "{rn}" {{\n  bucket = "{t}"\n  role   = "{role}"\n'
                                  f'  member = "serviceAccount:${{google_service_account.etl.email}}"\n}}')
                else:
                    rn = re.sub(r"[^a-z0-9_]", "_", role.split("/")[-1].lower())
                    blocks.append(f'resource "google_project_iam_member" "{rn}" {{\n  project = var.project_id\n  role    = "{role}"\n'
                                  f'  member  = "serviceAccount:${{google_service_account.etl.email}}"\n}}')
                    if "*" in resources or a.endswith(":*"):
                        res.warnings.append(f"{st.get('Sid', i)}: '{a}' on '{','.join(resources)}' is broad -> granted {role} at "
                                            f"project level; narrow to resource-level binding")
                res.mapping_log.append({"action": a, "role": role, "scope": scope, "target": t})
        res.constructs_converted += 1
    hcl = "# GENERATED: AWS IAM policy -> GCP IAM bindings. Least-privilege review required.\n" + "\n\n".join(blocks) + \
          '\n\nvariable "project_id" { type = string }\n'
    res.outputs["security/iam_bindings.tf"] = hcl
    res.checks.append(check_hcl(hcl, "iam_bindings.tf"))
    return res


# --------------------------------------------------------------------------- DynamoDB export
def _unwrap(v: dict) -> Any:
    (t, x), = v.items()
    if t == "S":
        return x
    if t == "N":
        return int(x) if re.fullmatch(r"-?\d+", x) else float(x)
    if t == "BOOL":
        return x
    if t == "NULL":
        return None
    if t in ("SS", "NS"):
        return [(_unwrap({t[0]: i})) for i in x]
    if t == "L":
        return [_unwrap(i) for i in x]
    if t == "M":
        return {k: _unwrap(i) for k, i in x.items()}
    if t == "B":
        return {"__bytes_b64": x}
    raise ValueError(t)


def convert_dynamodb_export(path: Path) -> ConversionResult:
    res = ConversionResult("dynamodb-export-to-firestore-json", path.name)
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        res.constructs_total += 1
        item = json.loads(line)["Item"]
        out.append({k: _unwrap(v) for k, v in item.items()})
        res.constructs_converted += 1
    res.outputs["data/customer_sessions_firestore.jsonl"] = "\n".join(json.dumps(o) for o in out) + "\n"
    res.assumptions.append("Sets (SS/NS) -> arrays (uniqueness no longer enforced by store)")
    res.manual_actions.append("Bulk import via Dataflow template / batched writes; document ID = session_id")
    res.checks.append({"check": "item count preserved", "status": "PASS" if len(out) == res.constructs_total else "FAIL",
                       "detail": f"{len(out)} items"})
    return res
