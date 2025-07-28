import requests
import pandas as pd
import boto3
from datetime import datetime
import pytz
import io
import os

# Initialize S3 client
s3 = boto3.client('s3')

# Environment variables (set in Lambda console)
BUCKET = 'cde-hackathon-smit-rsa'         
SOURCE = 'openexchangerates'
APP_ID = os.getenv('APIKEY')      

def lambda_handler(event, context):
    now = datetime.now(pytz.UTC)
    date_folder = now.strftime(f"raw/{SOURCE}/%Y/%m/%d")
    timestamp_str = now.strftime("%H%M")
    file_name = f"{timestamp_str}.csv"
    s3_key = f"{date_folder}/{file_name}"

    print(f"[INFO] Fetching exchange rates from Open Exchange Rates...")

    try:
        url = f"https://openexchangerates.org/api/latest.json?app_id={APP_ID}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[ERROR] Failed to fetch exchange rates: {e}")
        return {
            "statusCode": 500,
            "body": f"Failed to fetch exchange rate data: {e}"
        }

    try:
        base = data.get('base', 'USD')
        timestamp = datetime.utcfromtimestamp(data.get('timestamp')).isoformat()
        rates = data.get('rates', {})

        df = pd.DataFrame(list(rates.items()), columns=['currency', 'rate'])
        df['base'] = base
        df['exchange_timestamp'] = timestamp
        df['ingest_timestamp'] = now.isoformat()
        df['source'] = SOURCE
        df['status'] = 'success'

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        s3.put_object(Bucket=BUCKET, Key=s3_key, Body=csv_buffer.getvalue())
        print(f"[INFO] Uploaded exchange rates to s3://{BUCKET}/{s3_key}")

        return {
            "statusCode": 200,
            "body": f"Exchange rate data uploaded to s3://{BUCKET}/{s3_key}"
        }

    except Exception as e:
        print(f"[ERROR] Failed to process or upload data: {e}")
        return {
            "statusCode": 500,
            "body": f"Processing or upload failed: {e}"
        }
