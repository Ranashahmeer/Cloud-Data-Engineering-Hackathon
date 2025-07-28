import yfinance as yf
import pandas as pd
import boto3
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
import io
import time
import os

# AWS S3 client
s3 = boto3.client('s3')

# Environment variables (set in Lambda console)
BUCKET = 'cde-hackathon-smit-rsa'

# ✅ Step 1: Get limited S&P 500 symbols from Wikipedia
def get_sp500_symbols(limit=6):
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"id": "constituents"})
        symbols = [
            row.find_all("td")[0].text.strip()
            for row in table.tbody.find_all("tr")[1:]
        ]
        return symbols[:limit]
    except Exception as e:
        print(f"[ERROR] Failed to fetch S&P 500 symbols: {str(e)}")
        return []

# ✅ Step 2: Lambda handler
def lambda_handler(event, context):
    now = datetime.now(pytz.UTC)
    date_prefix = now.strftime(f"raw/{'yahoofinance'}/%Y/%m/%d")
    timestamp_str = now.strftime("%H%M")
    s3_key = f"{date_prefix}/{timestamp_str}.csv"

    print(f"[INFO] Starting Yahoo Finance Ingestion at {now.isoformat()}")
    print(f"[INFO] Writing to S3 key: {s3_key}")

    symbols = get_sp500_symbols(limit=6)
    if not symbols:
        return {
            "statusCode": 500,
            "body": "Failed to retrieve S&P 500 symbols"
        }

    print(f"[INFO] Fetching minute-level data for symbols: {symbols}")

    all_dataframes = []

    for symbol in symbols:
        try:
            print(f"[INFO] Fetching data for: {symbol}")
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="1d", interval="1m")
            if df.empty:
                raise Exception("Empty DataFrame")
            df.reset_index(inplace=True)
            df["symbol"] = symbol
            df["yahoofinance"] = 'yahoofinance'
            df["ingest_timestamp"] = now.isoformat()
            df["status"] = "success"
            all_dataframes.append(df)
        except Exception as e:
            print(f"[WARN] Error for {symbol}: {str(e)}")
            error_df = pd.DataFrame([{
                "Datetime": now,
                "Open": None,
                "High": None,
                "Low": None,
                "Close": None,
                "Volume": None,
                "symbol": symbol,
                "yahoofinance": 'yahoofinance',
                "ingest_timestamp": now.isoformat(),
                "status": f"error: {str(e)}"
            }])
            all_dataframes.append(error_df)

        time.sleep(1)  # Avoid being rate-limited by Yahoo

    # ✅ Step 3: Save to CSV and upload to S3
    if all_dataframes:
        final_df = pd.concat(all_dataframes)
        csv_buffer = io.StringIO()
        final_df.to_csv(csv_buffer, index=False)

        try:
            s3.put_object(
                Bucket=BUCKET,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            print(f"[INFO] Uploaded to s3://{BUCKET}/{s3_key}")
            return {
                "statusCode": 200,
                "body": f"Data saved to s3://{BUCKET}/{s3_key}"
            }
        except Exception as e:
            print(f"[ERROR] Failed to upload to S3: {str(e)}")
            return {
                "statusCode": 500,
                "body": f"S3 upload failed: {str(e)}"
            }

    else:
        print("[WARN] No data to upload.")
        return {
            "statusCode": 204,
            "body": "No data was collected."
        }
