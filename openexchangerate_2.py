import json
import boto3
import pyodbc
import csv
from io import StringIO
from datetime import datetime

# AWS clients
s3 = boto3.client("s3")
sqs = boto3.client("sqs")

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/open-exchange-rate-queue"

SQL_SERVER_CONFIG = {
    'server': '',
    'user': 'lambda_user',
    'password': '',
    'database': 'exchangerates'
}

def get_sql_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER_CONFIG['server']};"
        f"DATABASE={SQL_SERVER_CONFIG['database']};"
        f"UID={SQL_SERVER_CONFIG['user']};"
        f"PWD={SQL_SERVER_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str, timeout=10)

def insert_csv_to_sql(rows):
    conn = get_sql_connection()
    cursor = conn.cursor()

    for row in rows:
        try:
            cursor.execute("""
                INSERT INTO ExchangeRates (
                    currency, rate, base, exchange_timestamp,
                    ingest_timestamp, source, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row['currency'],
                float(row['rate']),
                row['base'],
                datetime.fromisoformat(row['exchange_timestamp']),
                datetime.fromisoformat(row['ingest_timestamp']),
                row['source'],
                row['status']
            ))
        except Exception as e:
            print(f"❌ Failed to insert row: {row} | Error: {e}")

    conn.commit()
    cursor.close()
    conn.close()

def lambda_handler(event, context):
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1
    )
    messages = response.get('Messages', [])
    print(f"📩 Received {len(messages)} messages from SQS.")

    for msg in messages:
        try:
            print("🔎 Raw message body:")
            print(msg["Body"])

            body = json.loads(msg["Body"])
            sns_msg = json.loads(body["Message"])
            bucket = sns_msg["bucket"]
            key = sns_msg["key"]
            print(f"📦 Fetching CSV from: s3://{bucket}/{key}")

            # Get CSV file from S3
            response = s3.get_object(Bucket=bucket, Key=key)
            csv_data = response['Body'].read().decode('utf-8')
            reader = csv.DictReader(StringIO(csv_data))
            rows = [row for row in reader if row.get("status") == "success"]

            insert_csv_to_sql(rows)

            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg['ReceiptHandle'])
            print("✅ Message processed and deleted.")

        except Exception as e:
            print(f"❌ Error processing message: {e}")

    return {
        'statusCode': 200,
        'body': f"Processed {len(messages)} message(s)"
    }
