from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

with DAG("customer_daily", start_date=datetime(2025, 1, 1), schedule="@daily", catchup=False) as dag:
    wait = S3KeySensor(task_id="wait_for_orders", bucket_name="customer-raw", bucket_key="orders/{{ ds }}/_SUCCESS")
    etl = GlueJobOperator(task_id="run_glue", job_name="customer-etl")
    load = S3ToRedshiftOperator(task_id="load_orders", s3_bucket="customer-curated", s3_key="orders_enriched",
                                schema="analytics", table="orders", copy_options=["FORMAT AS PARQUET"])
    agg = RedshiftSQLOperator(task_id="build_marts", sql="CALL analytics.build_customer_marts();")
    wait >> etl >> load >> agg
