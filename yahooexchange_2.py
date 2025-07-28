import boto3
import json
import os
import csv
import snowflake.connector
from io import StringIO

sqs = boto3.client('sqs')
s3 = boto3.client('s3')

# Env vars
QUEUE_URL = 'https://sqs.amazonaws.com//yahoo-finance-queue'



SF_CONFIG = {
    'user': SF_USER,
    'password': SF_PASSWORD,
    'account': SF_ACCOUNT,
    'warehouse': SF_WAREHOUSE,
    'database': SF_DATABASE,
    'schema': SF_SCHEMA
}

TABLE_NAME = 'STOCK_DATA'

def parse_and_insert_csv(s3_bucket, s3_key, source_value):
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    csv_data = response['Body'].read().decode('utf-8')
    reader = csv.DictReader(StringIO(csv_data))
    rows = [
        (
            row['Datetime'],
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Volume'],
            row['symbol'],
            source_value,  # Set source from SNS or message attribute
            row['ingest_timestamp'],
            row['status']
        )
        for row in reader if row.get('status') == 'success'
    ]

    conn = snowflake.connector.connect(**SF_CONFIG)
    cs = conn.cursor()
    try:
        for row in rows:
            cs.execute(f"""
                INSERT INTO {TABLE_NAME}
                (datetime, open, high, low, close, volume, symbol, source, ingest_timestamp, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, row)
    finally:
        cs.close()
        conn.close()

def lambda_handler(event, context):
    messages = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1
    ).get("Messages", [])

    print(f"📥 Received {len(messages)} messages from SQS.")

    for msg in messages:
        try:
            print("🔎 Raw SQS Message Body:")
            print(msg["Body"])

            body = json.loads(msg["Body"])
            sns_msg = json.loads(body["Message"])
            bucket = sns_msg["bucket"]
            key = sns_msg["key"]
            source = sns_msg.get("source") or body.get("MessageAttributes", {}).get("source", {}).get("Value", "unknown")

            print(f"📦 Processing: s3://{bucket}/{key}")
            print(f"🔖 Source: {source}")

            parse_and_insert_csv(bucket, key, source)

            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg['ReceiptHandle'])
            print("✅ Message processed and deleted.")
        except Exception as e:
            print(f"❌ Error processing message: {e}")

    return {"status": "done"}
