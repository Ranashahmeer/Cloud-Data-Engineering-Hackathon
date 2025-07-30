# Cloud-Data-Engineering-Hackathon

This repository showcases the project I developed as part of the **SMIT Cloud Data Engineering Course**.  
It features a fully serverless, real-time data ingestion and processing architecture using AWS services, handling data from multiple live sources and integrating it into cloud data stores.

---

## 🚀 Project Overview

A robust, production-grade pipeline for ingesting, processing, and storing:

- 📈 Minute-level stock data from **Yahoo Finance**
- 💰 Live cryptocurrency snapshots from **CoinMarketCap**
- 💱 Real-time forex rates from **Open Exchange Rates**

All workflows are designed to be **automated**, **scalable**, and **cloud-native**, simulating real-world big data platforms.

---

## 📌 Project Objective

Build a real-time event-driven data engineering solution that:

- Supports multi-source ingestion on a 1-minute schedule
- Stores raw and processed data in S3, Snowflake, and SQL Server
- Uses Lambda functions for transformation logic
- Uses FIFO queues for guaranteed ordering
- Runs entirely in a serverless AWS environment

---

## 🛠️ Technologies Used

| Category        | Tools / Services |
|----------------|------------------|
| Cloud Platform | AWS (Lambda, S3, SQS, SNS, EventBridge, IAM) |
| Programming    | Python (boto3, yfinance, BeautifulSoup, requests) |
| Storage        | Amazon S3, SQL Server, ❄️ Snowflake |
| Messaging      | Amazon SQS (FIFO), Amazon SNS |
| Scheduling     | Amazon EventBridge (1-minute triggers) |
| Deployment     | Ngrok (for exposing local SQL Server) |
| Packaging      | AWS Lambda Layers (for native Python dependencies) |

---

## 🧱 System Architecture

### 📥 1. Ingestion Layer

Each data source has a dedicated AWS Lambda function triggered every minute via EventBridge.

| Source             | Description |
|--------------------|-------------|
| Yahoo Finance      | Uses `yfinance` to fetch OHLCV for S&P 500 |
| CoinMarketCap      | Scrapes top 10 cryptocurrencies using BeautifulSoup |
| Open Exchange Rates| Pulls forex rates via secure API |

Data is stored in this structured format on S3:
s3://<bucket>/raw/<source>/YYYY/MM/DD/HHMM.csv

---

### 📨 2. Event Notifications

When new files are added to S3:

- An **SNS** notification is published
- **SNS** uses message attributes (like `source`) to fan-out to:
  - `yahoo-finance-queue.fifo`
  - `coinmarketcap-queue.fifo`
  - `openexchangerates-queue.fifo`

---

### 🔄 3. Processing Layer

Each queue has a dedicated Lambda consumer which:

- Downloads the respective file from S3
- Parses and filters the data (e.g., status == success)
- Transforms the records
- Inserts into appropriate data sinks:

| Source             | Target Sink   |
|--------------------|---------------|
| Yahoo Finance      | ❄️ Snowflake |
| CoinMarketCap      | 🗂️ Processed S3 |
| Open Exchange Rates| 🧮 SQL Server |

---

## 🔒 SQL Server Access via Ngrok

Since AWS Lambda functions cannot access local services by default, I used ngrok to publicly expose a locally hosted SQL Server database. This allowed real-time insertions from Lambda without needing complex VPC setups.

---

## 📦 Lambda Layer Packaging

Creating Lambda Layers was one of the more challenging parts due to native Python package dependencies. Here's how it was done:

### 🧪 Steps

1. Launch an **EC2 instance** using Amazon Linux 2023.
2. Install required packages using `pip`, including:
   - `pandas`
   - `beautifulsoup4`
   - `yfinance`
3. Navigate to the `site-packages` directory inside the virtual environment.
4. Zip the contents of `site-packages`:
   ```bash
   cd <path-to-site-packages>
   zip -r9 lambda-layer.zip .
---
---

## 🔧 How to Deploy

### ✅ Prerequisites

- AWS Account
- Open Exchange Rates App ID
- Snowflake account & SQL Server instance (local or cloud)

### 🪜 Deployment Steps

1. **Create S3 Buckets**
   - One for raw data: `s3://<your-bucket-name>/raw/`
   - One for processed data: `s3://<your-bucket-name>/processed/`

2. **Deploy Lambda Functions**
   - One ingestion Lambda per data source (Yahoo, CoinMarketCap, Open Exchange)
   - One consumer Lambda per SQS queue
   - Attach required Lambda Layers (for pandas, yfinance, etc.)

3. **Set up EventBridge Triggers**
   - Create 1-minute rules to invoke each ingestion Lambda

4. **Create SNS Topic**
   - Name it e.g., `data-pipeline-topic`

5. **Create SQS FIFO Queues**
   - `yahoo-finance-queue.fifo`
   - `coinmarketcap-queue.fifo`
   - `openexchangerates-queue.fifo`

6. **Configure S3 Event Notifications**
   - For raw uploads, trigger the SNS topic

7. **Subscribe Queues to SNS**
   - Add filter policies based on message attributes (e.g., `"source": "coinmarketcap"`)

8. **Attach Lambda Consumers to Queues**
   - Each consumer Lambda listens to its relevant queue

9. **Store Secrets**
   - Use AWS Secrets Manager or Lambda environment variables for:
     - Open Exchange Rates App ID
     - Snowflake credentials
     - SQL Server connection string

---

## 🎯 Key Learnings

- ✅ Designed and deployed a real-time streaming pipeline using serverless AWS tools
- ✅ Implemented a scalable ETL system with decoupled architecture
- ✅ Learned to configure and manage **FIFO queues** for message ordering
- ✅ Automated data routing using **SNS → SQS** with filtering
- ✅ Built and packaged Lambda Layers for native Python dependencies
- ✅ Enabled Lambda to interact with a local SQL Server using **Ngrok**
- ✅ Managed event orchestration using **EventBridge**, running every minute





