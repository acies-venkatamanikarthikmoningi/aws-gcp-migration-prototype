import json
import os

import boto3
import psycopg2
from flask import Flask, jsonify

app = Flask(__name__)
secrets = boto3.client("secretsmanager", region_name="us-east-1")
sqs = boto3.client("sqs", region_name="us-east-1")
s3 = boto3.client("s3")
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/111122223333/customer-events-q"
DB_HOST = "customer-db.abc123xyz.us-east-1.rds.amazonaws.com"


def db_conn():
    creds = json.loads(secrets.get_secret_value(SecretId="customer-db-credentials")["SecretString"])
    return psycopg2.connect(host=DB_HOST, user=creds["username"], password=creds["password"], dbname="customer")


@app.get("/customers/<int:cid>")
def get_customer(cid):
    with db_conn() as c, c.cursor() as cur:
        cur.execute("SELECT customer_id, customer_name, country FROM customer WHERE customer_id = %s", (cid,))
        row = cur.fetchone()
    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps({"event": "viewed", "customer_id": cid}))
    return jsonify(row)


@app.get("/reports/<name>")
def report(name):
    url = s3.generate_presigned_url("get_object", Params={"Bucket": "customer-raw", "Key": f"reports/{name}"}, ExpiresIn=900)
    return jsonify({"url": url})
