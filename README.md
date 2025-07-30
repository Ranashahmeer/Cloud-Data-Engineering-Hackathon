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
