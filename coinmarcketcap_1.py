import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import pytz
import boto3
from io import StringIO
import os

s3 = boto3.client("s3")
BUCKET_NAME = 'cde-hackathon-smit-rsa'
SOURCE = "coinmarketcap"

def lambda_handler(event, context):
    now = datetime.now(pytz.UTC)
    date_folder = now.strftime(f"raw/{SOURCE}/%Y/%m/%d")
    timestamp_str = now.strftime("%H%M")
    s3_key = f"{date_folder}/{timestamp_str}.csv"

    print(f"[INFO] Starting CoinMarketCap scrape at {now.isoformat()}")
    print(f"[INFO] Target S3 path: s3://{BUCKET_NAME}/{s3_key}")

    url = "https://coinmarketcap.com/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
    except Exception as e:
        print(f"[ERROR] Failed to fetch or parse CoinMarketCap page: {e}")
        return {
            "statusCode": 500,
            "body": f"Failed to fetch CoinMarketCap data: {e}"
        }

    # Parse the crypto table
    tbody = soup.find("tbody")
    if not tbody:
        return {
            "statusCode": 500,
            "body": "Failed: Could not find table body on CoinMarketCap page"
        }

    rows = tbody.find_all("tr")
    if len(rows) < 10:
        return {
            "statusCode": 500,
            "body": "Failed: Less than 10 rows found"
        }

    data = []
    for row in rows[:10]:
        try:
            cols = row.find_all("td")
            name_cell = cols[2]
            name_tags = name_cell.find_all("p")
            name = name_tags[0].text.strip() if len(name_tags) > 0 else ""
            symbol = name_tags[1].text.strip() if len(name_tags) > 1 else ""

            price = cols[3].text.strip()
            change_24h = cols[4].text.strip()
            change_7d = cols[5].text.strip()
            market_cap = cols[6].text.strip()

            data.append({
                "name": name,
                "symbol": symbol,
                "price": price,
                "change_24h": change_24h,
                "change_7d": change_7d,
                "market_cap": market_cap,
                "timestamp": now.isoformat(),
                "source": SOURCE,
                "status": "success"
            })
        except Exception as e:
            print(f"[WARN] Skipping row due to error: {e}")
            continue

    if not data:
        return {
            "statusCode": 204,
            "body": "No crypto data found"
        }

    df = pd.DataFrame(data)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )
        print(f"[INFO] Upload successful: s3://{BUCKET_NAME}/{s3_key}")
        return {
            "statusCode": 200,
            "body": f"Top 10 cryptos uploaded to s3://{BUCKET_NAME}/{s3_key}"
        }
    except Exception as e:
        print(f"[ERROR] Failed to upload to S3: {e}")
        return {
            "statusCode": 500,
            "body": f"Upload to S3 failed: {e}"
        }
