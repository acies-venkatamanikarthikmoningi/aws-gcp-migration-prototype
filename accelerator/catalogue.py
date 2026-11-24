"""Curated, versioned AWS -> GCP mapping catalogue (blueprint section 5, extended).

Each entry: candidate targets (primary first), default 7R strategy, complexity 1-5, decision considerations,
known gaps, and the data/workload movement tools that apply. Candidates are NOT guaranteed equivalents;
the mapping engine applies workload facts and every mapping needs human approval.
Product names reflect the catalogue as of its version date – re-verify against current GCP docs before use.
"""
from __future__ import annotations

import json
from pathlib import Path

CATALOGUE_VERSION = "aws-gcp-catalogue-2026.11.1"
GCP = "https://cloud.google.com/"

# (aws_service, category, [(target, when)], default_strategy, complexity, considerations, gap_note, tools, doc)
_C = [
    # ---------------- compute
    ("EC2", "compute", [("Compute Engine", "lift-and-shift VMs"), ("GKE", "containerize"), ("Cloud Run", "stateless HTTP app")],
     "Rehost", 2, "VM image/OS, instance family, licensing, network, sole-tenancy", "", ["Migrate to Virtual Machines"], "compute"),
    ("EC2 Auto Scaling", "compute", [("Managed Instance Groups", "autoscaled VM fleet")], "Replatform", 2,
     "Scaling policies, health checks, launch templates -> instance templates", "", [], "compute/docs/instance-groups"),
    ("Lightsail", "compute", [("Compute Engine", "small VMs"), ("Cloud Run", "simple web app")], "Rehost", 1, "Bundled networking/DNS", "", ["Migrate to Virtual Machines"], "compute"),
    ("Elastic Beanstalk", "compute", [("Cloud Run", "container/web app"), ("App Engine", "PaaS parity")], "Replatform", 2,
     "Platform runtime, env config, worker tiers", "", [], "run"),
    ("App Runner", "compute", [("Cloud Run", "container service")], "Replatform", 1, "Concurrency, VPC connector", "", [], "run"),
    ("Batch", "compute", [("Batch", "managed batch jobs"), ("GKE", "k8s jobs")], "Replatform", 2, "Job queues, compute envs, spot usage", "", [], "batch"),
    ("Outposts", "compute", [("Google Distributed Cloud", "on-prem/edge")], "Relocate", 4, "Hardware, connectivity, sovereignty", "Hardware programme, not a software port", [], "distributed-cloud"),
    # ---------------- containers & serverless
    ("EKS", "container", [("GKE", "Kubernetes"), ("GKE Autopilot", "less node ops")], "Replatform", 3,
     "K8s version, add-ons, ingress, IRSA -> Workload Identity, CSI storage", "", ["Migrate to Containers", "Velero/Backup for GKE"], "kubernetes-engine"),
    ("ECS", "container", [("Cloud Run", "service/job pattern"), ("GKE", "complex orchestration")], "Replatform", 3,
     "Task definitions, service discovery, sidecars", "", [], "run"),
    ("Fargate", "container", [("Cloud Run", "serverless containers"), ("GKE Autopilot", "k8s semantics")], "Replatform", 2, "Task sizing, networking", "", [], "run"),
    ("ECR", "container", [("Artifact Registry", "images and packages")], "Replatform", 1, "Image scanning, retention, IAM", "", ["crane/gcrane copy"], "artifact-registry"),
    ("Lambda", "serverless", [("Cloud Run functions", "event function"), ("Cloud Run", "longer/containers")], "Refactor", 3,
     "Runtime, event source mapping, timeout (Lambda 15 min), concurrency, layers", "Event payload shapes differ (S3 event vs CloudEvents)", [], "functions"),
    ("Lambda@Edge", "serverless", [("Cloud CDN + Cloud Run", "origin logic"), ("Media CDN edge extensions", "edge code")], "Refactor", 4,
     "Viewer/origin triggers", "No 1:1 edge compute; redesign edge logic", [], "cdn"),
    # ---------------- storage
    ("S3", "storage", [("Cloud Storage", "object storage")], "Replatform", 2,
     "Object layout, lifecycle, versioning, encryption, bucket policies -> IAM, event notifications", "",
     ["Storage Transfer Service", "Transfer Appliance (>100 TB offline)", "gcloud storage rsync"], "storage"),
    ("S3 Glacier", "storage", [("Cloud Storage Archive class", "cold archive")], "Replatform", 2, "Retrieval SLAs, restore costs on AWS side", "Glacier restore needed before transfer", ["Storage Transfer Service"], "storage/docs/storage-classes"),
    ("EBS", "storage", [("Persistent Disk", "general"), ("Hyperdisk", "high IOPS/throughput")], "Rehost", 2, "IOPS, throughput, snapshots", "", ["Migrate to Virtual Machines"], "compute/docs/disks"),
    ("EFS", "storage", [("Filestore", "NFS")], "Replatform", 2, "NFS semantics, throughput tiers, capacity", "", ["Storage Transfer Service (file systems)", "rsync"], "filestore"),
    ("FSx for NetApp ONTAP", "storage", [("Google Cloud NetApp Volumes", "ONTAP features")], "Replatform", 3, "SnapMirror, protocols", "", ["SnapMirror"], "netapp"),
    ("FSx for Windows", "storage", [("Google Cloud NetApp Volumes (SMB)", "SMB shares"), ("Compute Engine + Windows File Server", "full parity")], "Replatform", 3, "AD integration, DFS", "", ["Robocopy"], "netapp"),
    ("FSx for Lustre", "storage", [("Managed Lustre", "HPC scratch")], "Replatform", 3, "Throughput, S3 linkage", "", [], "managed-lustre"),
    ("Storage Gateway", "storage", [("Cloud Storage + partner gateway", "hybrid file")], "Repurchase", 3, "On-prem caching", "No first-party equivalent", [], "storage"),
    ("Backup", "storage", [("Backup and DR Service", "centralized backup")], "Replatform", 2, "Policies, vaults, retention", "", [], "backup-disaster-recovery"),
    ("Snowball", "storage", [("Transfer Appliance", "offline bulk")], "Retain", 1, "Used for transfer only", "", ["Transfer Appliance"], "transfer-appliance"),
    ("DataSync", "storage", [("Storage Transfer Service", "online transfer")], "Replatform", 1, "Agents, schedules", "", ["Storage Transfer Service"], "storage-transfer-service"),
    ("Transfer Family", "storage", [("Partner SFTP on Cloud Storage", "managed SFTP"), ("Compute Engine SFTP", "self-managed")], "Repurchase", 3, "Partners' SFTP, identity", "No first-party managed SFTP", [], "storage"),
    # ---------------- databases
    ("RDS MySQL", "database", [("Cloud SQL for MySQL", "managed MySQL")], "Replatform", 2, "Version, extensions, HA, read replicas", "", ["Database Migration Service (CDC)"], "sql"),
    ("RDS MariaDB", "database", [("Cloud SQL for MySQL", "MySQL-compatible")], "Replatform", 3, "MariaDB-specific features", "No managed MariaDB; test compatibility", ["Database Migration Service", "mysqldump"], "sql"),
    ("RDS PostgreSQL", "database", [("Cloud SQL for PostgreSQL", "standard OLTP"), ("AlloyDB", "high perf/HTAP")], "Replatform", 2,
     "Extensions, version, HA, logical replication", "", ["Database Migration Service (CDC)", "pg_dump/pg_restore"], "sql"),
    ("RDS SQL Server", "database", [("Cloud SQL for SQL Server", "managed"), ("Compute Engine", "full feature/licensing")], "Replatform", 3,
     "Edition, SSIS/SSRS, linked servers, licensing", "", ["Database Migration Service", "native backup/restore"], "sql/sqlserver"),
    ("RDS Oracle", "database", [("Oracle Database@Google Cloud", "keep Oracle"), ("Bare Metal Solution", "BYOL Oracle"), ("AlloyDB", "re-architect to PostgreSQL")], "Replatform", 5,
     "Licensing (BYOL), PL/SQL, RAC, partitioning", "Heterogeneous move needs schema+code conversion", ["Database Migration Service (Oracle->PostgreSQL)", "Data Pump", "GoldenGate"], "oracle"),
    ("Aurora", "database", [("AlloyDB", "PostgreSQL-compatible high perf"), ("Cloud SQL", "standard")], "Replatform", 3,
     "Aurora-only features (Global DB, backtrack, serverless v2)", "", ["Database Migration Service"], "alloydb"),
    ("Aurora Serverless", "database", [("AlloyDB", "autoscaling reads"), ("Cloud SQL", "small/variable")], "Replatform", 3, "Scale-to-zero expectations", "", ["Database Migration Service"], "alloydb"),
    ("DynamoDB", "database", [("Firestore", "document/key-value, serverless"), ("Bigtable", "very high throughput wide-column"), ("Spanner", "relational + global consistency")], "Refactor", 4,
     "Access patterns, GSIs, streams, TTL, transactions, item size", "Data model redesign; no wire compatibility", ["DynamoDB export to S3 -> Dataflow"], "firestore"),
    ("DocumentDB", "database", [("Firestore with MongoDB compatibility", "Mongo API"), ("MongoDB Atlas on GCP", "full Mongo")], "Replatform", 3, "Mongo API version coverage", "", ["mongodump/mongorestore", "Atlas Live Migrate"], "firestore"),
    ("Neptune", "database", [("Spanner Graph", "graph on Spanner"), ("Neo4j on Marketplace", "Cypher parity")], "Refactor", 4, "Gremlin/SPARQL vs GQL", "Query language change", [], "spanner"),
    ("Keyspaces", "database", [("Bigtable (Cassandra-compatible tooling)", "wide-column"), ("Astra/Cassandra on GKE", "CQL parity")], "Refactor", 4, "CQL features", "", [], "bigtable"),
    ("Timestream", "database", [("Bigtable", "high-rate time series"), ("BigQuery", "analytics")], "Refactor", 3, "Retention tiers, query patterns", "", ["Dataflow"], "bigtable"),
    ("QLDB", "database", [("Spanner + Cloud Audit Logs", "verifiable history pattern")], "Refactor", 4, "QLDB is end-of-support; redesign ledger semantics", "Service end-of-support on AWS – migrate pattern, not product", [], "spanner"),
    ("ElastiCache", "database", [("Memorystore for Redis / Valkey", "Redis-compatible"), ("Memorystore for Memcached", "Memcached")], "Replatform", 2,
     "Engine/version, cluster mode, persistence", "", ["RIOT / dump-restore"], "memorystore"),
    ("MemoryDB", "database", [("Memorystore for Redis Cluster / Valkey", "durable Redis")], "Replatform", 3, "Durability expectations", "", [], "memorystore"),
    ("OpenSearch", "analytics", [("Elastic Cloud on GCP (Marketplace)", "Elasticsearch parity"), ("Vertex AI Search", "managed search")], "Repurchase", 3, "Plugins, ISM, dashboards", "No first-party OpenSearch", ["snapshot/restore to GCS"], "vertex-ai-search-and-conversation"),
    # ---------------- analytics
    ("Redshift", "analytics", [("BigQuery", "serverless warehouse")], "Replatform", 3,
     "SQL dialect, DIST/SORT keys -> partition/cluster, stored procedures, WLM -> reservations", "",
     ["BigQuery Data Transfer Service (Redshift)", "BigQuery Migration Service (SQL translation)"], "bigquery/docs/migration/redshift-overview"),
    ("Redshift Serverless", "analytics", [("BigQuery", "on-demand/editions")], "Replatform", 3, "RPU sizing vs slots", "", ["BigQuery Data Transfer Service"], "bigquery"),
    ("Athena", "analytics", [("BigQuery (BigLake external tables)", "query lake in place")], "Replatform", 2, "Catalog, formats, partition projection", "", ["Storage Transfer Service"], "bigquery"),
    ("Glue", "etl", [("Dataproc Serverless", "Spark jobs"), ("Dataflow", "Beam/streaming"), ("BigQuery SQL / Dataform", "SQL-only transforms")], "Refactor", 3,
     "DynamicFrame APIs, job bookmarks, catalog", "Glue-specific APIs must be rewritten", [], "dataproc-serverless"),
    ("Glue Data Catalog", "governance", [("BigLake Metastore / Dataplex Universal Catalog", "metadata & governance")], "Replatform", 3, "Hive metastore compatibility", "", [], "dataplex"),
    ("Glue DataBrew", "etl", [("Dataform / BigQuery data preparation", "SQL prep"), ("Dataproc", "code")], "Refactor", 3, "No-code recipes", "Recipe conversion is manual", [], "bigquery"),
    ("Lake Formation", "governance", [("Dataplex + BigLake", "lake governance")], "Refactor", 3, "Row/column permissions", "", [], "dataplex"),
    ("EMR", "analytics", [("Dataproc", "Hadoop/Spark clusters"), ("Dataproc Serverless", "ephemeral Spark")], "Replatform", 3,
     "Hadoop/Spark versions, bootstrap actions, EMRFS -> GCS connector", "", ["DistCp", "Storage Transfer Service"], "dataproc"),
    ("EMR Serverless", "analytics", [("Dataproc Serverless", "serverless Spark")], "Replatform", 2, "Spark version", "", [], "dataproc-serverless"),
    ("QuickSight", "bi", [("Looker", "governed semantic layer"), ("Looker Studio", "self-service dashboards")], "Repurchase", 3, "SPICE datasets, row-level security", "Dashboards rebuilt", [], "looker"),
    ("Data Exchange", "analytics", [("Analytics Hub", "data sharing")], "Replatform", 2, "Subscriptions", "", [], "analytics-hub"),
    # ---------------- streaming & integration
    ("Kinesis", "streaming", [("Pub/Sub", "event ingestion"), ("Managed Service for Apache Kafka", "log semantics")], "Refactor", 3,
     "Shards vs throughput, ordering keys, retention, replay", "", ["Dual-publish bridge", "Dataflow"], "pubsub"),
    ("Kinesis Firehose", "streaming", [("Pub/Sub BigQuery/Cloud Storage subscriptions", "delivery"), ("Dataflow", "transform")], "Refactor", 2, "Buffering, transforms", "", [], "pubsub"),
    ("Kinesis Video Streams", "streaming", [("Vertex AI Vision", "video analytics")], "Refactor", 4, "Ingestion SDKs", "No direct equivalent", [], "vertex-ai"),
    ("MSK", "streaming", [("Managed Service for Apache Kafka", "Kafka API"), ("Pub/Sub", "re-architect")], "Replatform", 2,
     "Kafka version, consumer groups, connectors, ACLs", "", ["MirrorMaker 2", "Cluster Linking (Confluent)"], "managed-service-for-apache-kafka"),
    ("SQS", "integration", [("Pub/Sub", "pub/sub queueing"), ("Cloud Tasks", "task dispatch with rate control")], "Refactor", 2,
     "FIFO, visibility timeout -> ack deadline, DLQ", "", ["Drain-and-switch"], "pubsub"),
    ("SNS", "integration", [("Pub/Sub", "fan-out"), ("Firebase Cloud Messaging", "mobile push")], "Refactor", 2, "Filter policies, SMS/email endpoints", "SMS/email delivery needs partner", [], "pubsub"),
    ("EventBridge", "integration", [("Eventarc", "event routing"), ("Pub/Sub", "bus")], "Refactor", 3, "Rules, schema registry, partner sources", "", [], "eventarc"),
    ("EventBridge Scheduler", "integration", [("Cloud Scheduler", "cron")], "Replatform", 1, "Time zones, targets", "", [], "scheduler"),
    ("Step Functions", "orchestration", [("Workflows", "serverless orchestration"), ("Cloud Composer", "data pipelines")], "Refactor", 3,
     "State machine semantics, retries, service integrations, Map state", "", [], "workflows"),
    ("MWAA", "orchestration", [("Cloud Composer", "managed Airflow")], "Replatform", 2, "Airflow version, providers/operators, connections", "", [], "composer"),
    ("AppFlow", "integration", [("Application Integration / Integration Connectors", "SaaS integration")], "Refactor", 3, "Connectors", "", [], "application-integration"),
    ("MQ", "integration", [("Pub/Sub", "re-architect"), ("RabbitMQ/ActiveMQ on GKE", "protocol parity")], "Replatform", 3, "AMQP/JMS protocols", "No managed broker equivalent", [], "pubsub"),
    ("AppSync", "integration", [("Apigee + Cloud Run GraphQL", "GraphQL API"), ("Firebase Data Connect", "app backends")], "Refactor", 4, "Resolvers, subscriptions", "", [], "apigee"),
    ("API Gateway", "networking", [("API Gateway", "simple APIs"), ("Apigee", "full API management")], "Replatform", 2, "Authorizers, usage plans, stages", "", [], "api-gateway"),
    # ---------------- networking
    ("VPC", "networking", [("VPC network", "global VPC")], "Replatform", 3, "CIDRs, regional vs global VPC, firewall rules vs SGs/NACLs, peering", "", [], "vpc"),
    ("NAT Gateway", "networking", [("Cloud NAT", "egress NAT")], "Replatform", 1, "IP allocation", "", [], "nat"),
    ("Transit Gateway", "networking", [("Network Connectivity Center", "hub-and-spoke")], "Replatform", 3, "Route tables, attachments", "", [], "network-connectivity-center"),
    ("PrivateLink", "networking", [("Private Service Connect", "private endpoints")], "Replatform", 2, "Endpoint services", "", [], "vpc/docs/private-service-connect"),
    ("Direct Connect", "networking", [("Cloud Interconnect", "dedicated/partner"), ("Cross-Cloud Interconnect", "AWS<->GCP during migration")], "Relocate", 3,
     "Bandwidth, redundancy, lead time", "Physical provisioning lead time (weeks)", [], "network-connectivity/docs/interconnect"),
    ("Site-to-Site VPN", "networking", [("Cloud VPN (HA VPN)", "IPsec")], "Replatform", 1, "BGP, tunnels", "", [], "network-connectivity/docs/vpn"),
    ("ELB", "networking", [("Cloud Load Balancing", "ALB->Application LB, NLB->Network LB")], "Replatform", 2, "Listener rules, TLS, stickiness", "", [], "load-balancing"),
    ("Global Accelerator", "networking", [("Global external Application/Network LB", "anycast")], "Replatform", 2, "Static anycast IPs", "", [], "load-balancing"),
    ("CloudFront", "networking", [("Cloud CDN", "web CDN"), ("Media CDN", "large media")], "Replatform", 2, "Origins, cache behaviours, WAF, signed URLs", "", [], "cdn"),
    ("Route 53", "networking", [("Cloud DNS", "DNS")], "Replatform", 1, "Routing policies, health checks", "Latency/geo routing uses Cloud DNS routing policies – verify parity", [], "dns"),
    # ---------------- identity & security
    ("IAM", "security", [("Cloud IAM", "roles/service accounts"), ("Workload Identity Federation", "AWS/CI identities")], "Refactor", 4,
     "Policy language, role trust, least privilege, permission boundaries", "Policy model differs; no auto 1:1", [], "iam"),
    ("Organizations", "security", [("Resource Manager (org/folders/projects) + Org Policy", "hierarchy")], "Refactor", 3, "SCPs -> org policies", "", [], "resource-manager"),
    ("Control Tower", "security", [("Landing zone (Cloud Foundation Fabric / Setup checklist)", "baseline")], "Refactor", 3, "Guardrails", "", [], "architecture/landing-zones"),
    ("IAM Identity Center", "security", [("Cloud Identity / Workforce Identity Federation", "workforce SSO")], "Refactor", 3, "IdP integration", "", [], "iam/docs/workforce-identity-federation"),
    ("Cognito", "security", [("Identity Platform", "customer identity")], "Refactor", 4, "User pool export (password hashes not exportable)", "Users may need password reset or lazy migration", ["Lazy migration via auth trigger"], "identity-platform"),
    ("KMS", "security", [("Cloud KMS", "keys"), ("Cloud EKM", "external keys")], "Replatform", 3, "Key material not exportable; re-encrypt data", "Data must be re-encrypted with new keys", [], "kms"),
    ("CloudHSM", "security", [("Cloud HSM", "FIPS 140-2 L3")], "Replatform", 3, "Key ceremony", "", [], "kms/docs/hsm"),
    ("Secrets Manager", "security", [("Secret Manager", "secrets")], "Replatform", 1, "Rotation lambdas, versions", "", ["Scripted copy (never via logs)"], "secret-manager"),
    ("Systems Manager", "security", [("VM Manager", "patch/config"), ("Parameter Manager", "parameters")], "Replatform", 2, "Run Command, Parameter Store", "", [], "compute/docs/vm-manager"),
    ("ACM", "security", [("Certificate Manager", "TLS certs")], "Replatform", 1, "Private CA -> Certificate Authority Service", "", [], "certificate-manager"),
    ("WAF", "security", [("Cloud Armor", "WAF/DDoS")], "Replatform", 2, "Managed rule groups -> preconfigured rules", "", [], "armor"),
    ("Shield", "security", [("Cloud Armor (Enterprise)", "DDoS")], "Replatform", 2, "Response team", "", [], "armor"),
    ("GuardDuty", "security", [("Security Command Center (threat detection)", "threats")], "Repurchase", 2, "Findings integration", "", [], "security-command-center"),
    ("Security Hub", "security", [("Security Command Center", "posture")], "Repurchase", 2, "Standards", "", [], "security-command-center"),
    ("Inspector", "security", [("Security Command Center / Artifact Analysis", "vuln scanning")], "Repurchase", 2, "", "", [], "security-command-center"),
    ("Macie", "security", [("Sensitive Data Protection", "PII discovery")], "Repurchase", 2, "", "", [], "sensitive-data-protection"),
    ("Config", "governance", [("Cloud Asset Inventory + Org Policy", "config history/compliance")], "Repurchase", 2, "Rules", "", [], "asset-inventory"),
    # ---------------- observability & ops
    ("CloudWatch", "observability", [("Cloud Monitoring + Cloud Logging", "metrics/logs/alerts")], "Replatform", 2, "Metrics, log retention, dashboards, alarms", "", [], "monitoring"),
    ("CloudTrail", "observability", [("Cloud Audit Logs", "audit")], "Replatform", 1, "Retention/export to BigQuery", "Historic CloudTrail stays in AWS or is archived", [], "logging/docs/audit"),
    ("X-Ray", "observability", [("Cloud Trace", "tracing")], "Refactor", 2, "Prefer OpenTelemetry", "", [], "trace"),
    ("Managed Prometheus", "observability", [("Managed Service for Prometheus", "PromQL")], "Replatform", 1, "", "", [], "stackdriver/docs/managed-prometheus"),
    ("Managed Grafana", "observability", [("Grafana on GKE / Cloud Monitoring dashboards", "dashboards")], "Replatform", 2, "", "", [], "monitoring"),
    ("Trusted Advisor", "governance", [("Active Assist Recommender", "recommendations")], "Repurchase", 1, "", "", [], "recommender"),
    ("Cost Explorer", "governance", [("Cloud Billing reports + BigQuery export", "FinOps")], "Repurchase", 1, "", "", [], "billing"),
    # ---------------- devops
    ("CloudFormation", "devops", [("Terraform (google provider)", "IaC standard"), ("Infrastructure Manager", "managed Terraform")], "Refactor", 3, "Intrinsics, custom resources, nested stacks", "", [], "infrastructure-manager"),
    ("CDK", "devops", [("CDK for Terraform / Pulumi", "code-first IaC")], "Refactor", 3, "Constructs", "", [], "infrastructure-manager"),
    ("CodePipeline", "devops", [("Cloud Build + Cloud Deploy", "CI/CD")], "Refactor", 2, "Approvals, stages", "", [], "deploy"),
    ("CodeBuild", "devops", [("Cloud Build", "builds")], "Replatform", 1, "buildspec -> cloudbuild.yaml", "", [], "build"),
    ("CodeDeploy", "devops", [("Cloud Deploy", "progressive delivery")], "Replatform", 2, "Blue/green, canary", "", [], "deploy"),
    ("CodeCommit", "devops", [("Secure Source Manager", "managed git"), ("GitHub/GitLab", "SaaS git")], "Repurchase", 1, "", "", ["git mirror"], "secure-source-manager"),
    ("Cloud9", "devops", [("Cloud Workstations", "cloud IDE")], "Repurchase", 1, "", "", [], "workstations"),
    ("Amplify", "devops", [("Firebase App Hosting / Hosting", "web/mobile backends")], "Refactor", 3, "", "", [], "firebase"),
    # ---------------- AI/ML
    ("SageMaker", "ai_ml", [("Vertex AI", "training/endpoints/pipelines")], "Replatform", 3,
     "Containers, endpoints, feature store, pipelines, model registry", "", ["Model artifacts copy via Storage Transfer"], "vertex-ai"),
    ("Bedrock", "ai_ml", [("Vertex AI (Gemini, Model Garden)", "foundation models")], "Refactor", 3, "Model choice, guardrails, agents, KBs", "Prompts/evals must be re-validated per model", [], "vertex-ai"),
    ("Comprehend", "ai_ml", [("Natural Language API", "NLP"), ("Gemini on Vertex AI", "LLM")], "Repurchase", 2, "", "", [], "natural-language"),
    ("Rekognition", "ai_ml", [("Vision API", "images"), ("Video Intelligence API", "video")], "Repurchase", 2, "", "Face features differ by policy", [], "vision"),
    ("Textract", "ai_ml", [("Document AI", "documents")], "Repurchase", 2, "", "", [], "document-ai"),
    ("Translate", "ai_ml", [("Cloud Translation", "MT")], "Repurchase", 1, "", "", [], "translate"),
    ("Polly", "ai_ml", [("Text-to-Speech", "TTS")], "Repurchase", 1, "", "", [], "text-to-speech"),
    ("Transcribe", "ai_ml", [("Speech-to-Text", "ASR")], "Repurchase", 1, "", "", [], "speech-to-text"),
    ("Lex", "ai_ml", [("Dialogflow CX / Conversational Agents", "bots")], "Refactor", 3, "Intents/slots", "", [], "dialogflow"),
    ("Kendra", "ai_ml", [("Vertex AI Search", "enterprise search")], "Repurchase", 2, "", "", [], "enterprise-search"),
    ("Personalize", "ai_ml", [("Vertex AI Search for commerce", "recommendations")], "Repurchase", 3, "", "", [], "retail"),
    ("Forecast", "ai_ml", [("BigQuery ML / Vertex AI forecasting", "forecasting")], "Refactor", 2, "", "Amazon Forecast closed to new customers", [], "bigquery/docs/bqml-introduction"),
    # ---------------- IoT, messaging, misc
    ("IoT Core", "iot", [("Partner MQTT broker (Marketplace) + Pub/Sub", "device connectivity")], "Repurchase", 5,
     "Device registry, certificates, MQTT topics, shadows", "Google Cloud IoT Core retired (Aug 2023) – partner broker required", [], "solutions/iot"),
    ("SES", "integration", [("Partner email API (e.g. SendGrid via Marketplace)", "transactional email")], "Repurchase", 2, "Sender reputation, DKIM", "No first-party email sending service", [], "marketplace"),
    ("Pinpoint", "integration", [("Firebase Cloud Messaging + partner", "engagement")], "Repurchase", 3, "", "", [], "firebase"),
    ("Connect", "integration", [("Contact Center AI Platform", "contact centre")], "Repurchase", 4, "", "", [], "solutions/contact-center"),
    ("WorkSpaces", "enduser", [("Partner VDI (e.g. Citrix/VMware on GCP)", "VDI")], "Repurchase", 3, "", "No first-party VDI", [], "solutions"),
    ("Simple Workflow (SWF)", "orchestration", [("Workflows", "orchestration")], "Retire", 3, "Legacy service", "Consider retiring/replacing", [], "workflows"),
    ("Database Migration Service (AWS DMS)", "migration", [("Database Migration Service / Datastream", "replication")], "Retire", 1, "Tool, not a workload", "", [], "database-migration"),
]

DATA_TRANSFER_TOOLS = {
    "Storage Transfer Service": "Managed online S3->GCS transfer; agentless for S3; incremental sync and scheduling.",
    "Transfer Appliance": "Offline bulk for very large datasets or constrained bandwidth.",
    "BigQuery Data Transfer Service (Redshift)": "Schema + data migration from Redshift via S3 unload to BigQuery.",
    "BigQuery Migration Service (SQL translation)": "Batch/interactive Redshift SQL -> GoogleSQL translation.",
    "Database Migration Service (CDC)": "Homogeneous MySQL/PostgreSQL/SQL Server -> Cloud SQL/AlloyDB with continuous replication.",
    "Database Migration Service (Oracle->PostgreSQL)": "Heterogeneous Oracle -> AlloyDB/Cloud SQL PG with conversion workspace.",
    "Datastream": "Serverless CDC from MySQL/PostgreSQL/Oracle/SQL Server into BigQuery/GCS.",
    "MirrorMaker 2": "Kafka topic replication MSK -> Managed Kafka with offset translation.",
    "Dataflow": "Custom/large-scale transforms, DynamoDB export conversion, dual-run bridges.",
    "Migrate to Virtual Machines": "VM replication and cutover for EC2 -> Compute Engine.",
    "Migrate to Containers": "VM -> container artefact modernization.",
}


def catalogue() -> list[dict]:
    out = []
    for aws, cat, targets, strat, cx, cons, gap, tools, doc in _C:
        out.append({"aws_service": aws, "category": cat,
                    "targets": [{"service": t, "fit": "primary" if i == 0 else "alternative", "when": w}
                                for i, (t, w) in enumerate(targets)],
                    "default_strategy": strat, "complexity": cx, "considerations": cons, "gap": gap,
                    "migration_tools": tools, "doc": GCP + doc, "version": CATALOGUE_VERSION})
    return out


_INDEX = {e["aws_service"].lower(): e for e in catalogue()}


def lookup(service: str, engine: str | None = None) -> dict | None:
    s = (service or "").strip()
    if s.upper() == "RDS":
        eng = (engine or "").lower()
        s = ("RDS Oracle" if "oracle" in eng else "RDS SQL Server" if "sqlserver" in eng or "sql server" in eng
             else "RDS MySQL" if "mysql" in eng else "RDS MariaDB" if "mariadb" in eng else "RDS PostgreSQL")
    return _INDEX.get(s.lower())


def category_of(service: str) -> str:
    e = lookup(service)
    return e["category"] if e else "unknown"


def export(path: Path) -> None:
    path.write_text(json.dumps({"version": CATALOGUE_VERSION, "entries": catalogue(),
                                "data_transfer_tools": DATA_TRANSFER_TOOLS}, indent=2))
