import boto3
import json
import csv
from io import StringIO
from datetime import datetime

sqs = boto3.client("sqs")
s3 = boto3.client("s3")

QUEUE_URL = "https://sqs.amazonaws.com//coin-market-cap-queue"
DEST_BUCKET = "cde-hackathon-smit-rsa"  # or another bucket name
DEST_PREFIX = "processed/coinmarketcap/"

def transform_data(row):
    # Add your transformation logic here
    row['processed_at'] = datetime.utcnow().isoformat()
    return row

def lambda_handler(event, context):
    messages = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1
    ).get("Messages", [])

    print(f"📥 Received {len(messages)} messages.")

    for msg in messages:
        try:
            print("🔍 Raw SQS message body:")
            print(msg["Body"])
            body = json.loads(msg["Body"])
            sns_msg = json.loads(body["Message"])
            bucket = sns_msg["bucket"]
            key = sns_msg["key"]

            print(f"📂 Fetching file: s3://{bucket}/{key}")
            obj = s3.get_object(Bucket=bucket, Key=key)
            csv_data = obj['Body'].read().decode('utf-8')
            reader = csv.DictReader(StringIO(csv_data))

            processed_rows = [transform_data(row) for row in reader]

            # Prepare new CSV
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=processed_rows[0].keys())
            writer.writeheader()
            writer.writerows(processed_rows)

            new_key = f"{DEST_PREFIX}{datetime.utcnow().strftime('%Y/%m/%d/%H%M')}.csv"
            print(f"🚀 Uploading to s3://{DEST_BUCKET}/{new_key}")
            s3.put_object(Bucket=DEST_BUCKET, Key=new_key, Body=output.getvalue())

            # ✅ Delete message
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg['ReceiptHandle'])
            print("✅ Done.")

        except Exception as e:
            print(f"❌ Error: {e}")

    return {"status": "done"}
