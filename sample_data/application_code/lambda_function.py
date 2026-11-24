import json
import os
import urllib.parse

import boto3

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

TABLE_NAME = os.environ.get("TABLE_NAME", "customer-sessions")
TOPIC_ARN = os.environ.get("TOPIC_ARN", "arn:aws:sns:us-east-1:111122223333:customer-alerts")
# legacy hard-coded credential left by a developer (must be redacted before any LLM call)
AWS_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"


def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    processed = 0
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        obj = s3.get_object(Bucket=bucket, Key=key)
        payload = json.loads(obj["Body"].read())
        table.put_item(Item={"session_id": payload["session_id"], "customer_id": payload["customer_id"]})
        processed += 1
    sns.publish(TopicArn=TOPIC_ARN, Message=json.dumps({"processed": processed}))
    return {"statusCode": 200, "body": json.dumps({"processed": processed})}
