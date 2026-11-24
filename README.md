# AWS → GCP Migration Accelerator — working prototype (v1.1)

This is a runnable implementation of the *AWS-to-GCP Migration Accelerator Blueprint v1.1* (Acies Global, built for the Carrier Global prototype discussion).
It runs the whole lifecycle end to end on a synthetic Customer Analytics estate: **discovery → assessment → mapping (7R) → conversion → historical backfill → CDC catch-up → post-migration validation → dependency impact → risk gates → stakeholder drafts → reports and audit**.

> Everything runs locally on synthetic data. Cloud execution is **simulated but real**: data actually moves between a stand-in "AWS" store, a "GCS landing" folder and a stand-in "BigQuery" store. Nothing is deployed and no email is sent. Approvals in the demo run are labelled `(SIMULATED demo approval)`.

## Quick start

```bash
pip install -r requirements.txt
python scripts/generate_samples.py          # synthetic estate (already included)

# Option A – live console with API (approve mappings, record waivers, drop in your own files)
uvicorn accelerator.api:app --port 8000     # open http://localhost:8000

# Option B – one-shot run + self-contained console (no server needed)
python -m accelerator.cli run               # writes runs/demo/{state.json, console.html, reports/, output/}
open runs/demo/console.html

pytest -q                                   # 31 acceptance tests (blueprint §19)
docker compose -f deployment/docker-compose.yml up --build
```

## Migration possibilities covered

| Area | Coverage |
|---|---|
| **Strategy (7R)** | Rehost, Replatform, Refactor, Repurchase, Relocate, Retain, Retire. Retire is detected from idle resources and Retain from compliance-pinned ones. |
| **Service catalogue** | 127 AWS services: compute, containers, serverless, storage, every RDS engine (including Oracle), DynamoDB/DocumentDB/Neptune/Keyspaces/Timestream/QLDB, Redshift/Athena/EMR/Glue/Lake Formation, Kinesis/MSK/SQS/SNS/EventBridge/MQ, networking (VPC, TGW, PrivateLink, Direct Connect, ELB, CloudFront), security (IAM, KMS, Cognito, WAF, GuardDuty…), observability, DevOps, AI/ML (SageMaker, Bedrock, Textract…), IoT. Known gaps with no first-party GCP equivalent are flagged explicitly (IoT Core, SES, Transfer Family, WorkSpaces, Lambda@Edge). |
| **Workload-fact rules** | Cloud SQL vs AlloyDB by size/HTAP; Firestore vs Bigtable vs Spanner by access pattern; Cloud Run functions vs Cloud Run by timeout; Oracle BYOL → Bare Metal Solution / Oracle@GC; Kinesis replay → Managed Kafka; Lambda@Edge complexity |
| **Data movement** | Storage Transfer Service, Transfer Appliance (>100 TB or >7 days online), BigQuery DTS + SQL translation, DMS CDC (homogeneous and heterogeneous), Datastream, MirrorMaker 2, dual-publish streaming, DynamoDB export → Dataflow, Migrate to VMs, cache rebuild, Cognito lazy migration |
| **Data patterns** | Full historical backfill, partition backfill, incremental watermark MERGE, CDC (ordered LSN, exactly-once), selective reload |
| **Cutover** | Offline big-bang inside window; phased pre-load + catch-up; online CDC near-zero downtime; parallel run / dual publish; NEEDS REVIEW when downtime tolerance is unknown |
| **Waves** | Dependency-ordered waves from the graph (foundation wave 0, producers before consumers, cycles collapsed) |
| **Conversion** | Terraform AWS → google provider; CloudFormation → Terraform; Redshift SQL → BigQuery (DIST/SORT → partition/cluster, COPY/UNLOAD, IDENTITY…); Lambda → Cloud Run functions; boto3 app → Cloud Run (compatibility adapter + refactor report); Glue → Dataproc Serverless; Step Functions → Workflows; MWAA → Composer; IAM policy → IAM bindings; DynamoDB JSON → Firestore JSON |
| **Validation** | Coverage, schema, row counts, completeness, uniqueness, aggregates, partition hashes, key-level drill-down, business rules, freshness/lag, transformation, referential integrity; outcomes PASS / FAIL / REVIEW / WAIVED / NOT_RUN; waivers record approver, rationale and expiry |

## Demo script (15–20 min, blueprint §20)
1. **Upload centre**: 51 artefacts ingested; 4 unsafe files rejected (`.exe`, zip path traversal, fake xlsx, EICAR).
2. **Discovery**: 40 resources correlated; conflicts shown (EC2 `m5.large` vs `m5.xlarge`; Lambda runtime 3.11 vs 3.12); hard-coded AWS key found.
3. **Mapping**: Redshift → BigQuery, S3 → Cloud Storage with rationale. Oracle and IoT Core stay *pending* because they need a business decision.
4. **Code conversion**: side-by-side source and output, static checks, confidence.
5. **Load monitor**: B024 fails on a checksum mismatch → retried from its checkpoint → no duplicates.
6. **Validation**: seeded defects detected (missing 2025-04, duplicates in 2025-09, value drift in 2026-02) → selective reload → PASS. Waiver on 2 quarantined rows. Customer 1003 revenue mismatch (−500) stays FAIL.
7. **Risk & readiness**: CustomerAnalytics is **BLOCKED** by gates even though most checks pass, and the tool refuses sign-off.
8. **Comms**: drafts only for verified owners; FinanceOps shows "Owner unknown".

## Layout
```
accelerator/   store (metadata + hash-chained audit), ingestion, parsers, normalize, catalogue, mapping,
               planner, converters/, migration_engine, validation, intelligence (deps/risk/comms),
               reporting, orchestrator, api, cli, ai (optional Vertex Gemini, explanations only)
sample_data/   synthetic estate (inventory, IaC, code, SQL, ETL, orchestration, IAM, diagrams, docs, data)
knowledge_base/aws_gcp_mapping.json   exported catalogue
ui/index.html  console (works live against the API or as a static export)
tests/         acceptance tests
```

## Prototype vs production
| Prototype | Production path |
|---|---|
| SQLite metadata (schema mirrors blueprint §10) | Cloud SQL for PostgreSQL (JSON → JSONB) |
| Simulated AWS/GCS/BigQuery stores | Read-only AWS roles; Storage Transfer, BigQuery DTS, DMS, Datastream |
| In-process execution | Celery/Cloud Tasks + Workflows/Composer; job IDs already returned |
| Demo approvals | Authenticated approvers via UI/API with RBAC |
| Raster diagrams flagged NEEDS REVIEW | Vision extraction through an approved model (never invents nodes) |
| No LLM required | `ACCEL_LLM=vertex` adds Gemini explanations only; inputs redacted first |

Catalogue product names and documentation links are as of catalogue version `aws-gcp-catalogue-2026.11.1`. Re-verify against current Google Cloud documentation before you rely on them.
