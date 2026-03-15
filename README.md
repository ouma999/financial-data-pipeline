# 📊 Real-Time Financial Data Pipeline

> **AWS Data Engineering Demo Project**
> An end-to-end real-time data pipeline that ingests live stock market data, processes it with ETL transformations, stores it in a data lake, and serves it for SQL analytics — built entirely on AWS managed services.

I designed and built a real-time financial data pipeline on AWS using Kinesis, S3 Data Lake, Glue PySpark ETL, and Athena — deployed via CloudFormation IaC. The pipeline ingests live stock data, engineers 15+ technical indicators including actuarial VaR metrics, and serves processed Parquet data for SQL analytics
---

## 🏗️ Architecture

```
Yahoo Finance API
      │
      ▼
Kinesis Data Streams ──► Kinesis Firehose ──► S3 Raw Zone
(real-time ingestion)    (buffering/delivery)  (data lake)
                                                    │
                                                    ▼
                                             AWS Glue ETL
                                          (clean, transform,
                                           add indicators)
                                                    │
                                                    ▼
                                           S3 Processed Zone
                                           (Parquet + partitioned)
                                                    │
                                                    ▼
                                        Athena (SQL analytics)
                                                    │
                                                    ▼
                                        QuickSight Dashboard
```

---

## ✅ AWS Services Demonstrated

| Service | Purpose | DE Exam Domain |
|---------|---------|----------------|
| **Kinesis Data Streams** | Real-time stock data ingestion | Ingest |
| **Kinesis Firehose** | Buffered delivery to S3 | Ingest |
| **S3 Data Lake** | Raw + Processed zones with partitioning | Store |
| **AWS Glue** | ETL — clean, transform, add indicators | Transform |
| **Glue Data Catalog** | Schema registry for all tables | Transform |
| **Athena** | Serverless SQL on S3 | Serve |
| **EventBridge** | Scheduled Glue job triggers | Orchestrate |
| **CloudWatch** | Pipeline monitoring + alarms | Monitor |
| **IAM** | Least-privilege roles per service | Security |
| **CloudFormation** | Full IaC deployment | All |

---

## 📁 Project Structure

```
financial-data-pipeline/
│
├── producer/
│   └── producer.py          # Streams Yahoo Finance data to Kinesis
│
├── glue_jobs/
│   └── etl_transform.py     # PySpark ETL — cleans + adds indicators
│
├── athena/
│   └── queries.sql          # Analytics queries for processed data
│
├── cloudformation/
│   └── template.yaml        # Full IaC — deploys entire pipeline
│
├── tests/
│   └── test_producer.py     # Test suite (pytest)
│
├── requirements.txt
└── README.md
```

---

## 🔄 Data Flow Detail

### Layer 1 — Ingestion
The producer pulls live data from **Yahoo Finance** every 60 seconds for 12 tickers across 4 sectors (Tech, Banking, Healthcare, Energy) and streams records to **Kinesis Data Streams**.

Each record contains:
- OHLCV prices (Open, High, Low, Close, Volume)
- Market metadata (market cap, P/E ratio, sector)
- 52-week range data
- Moving averages

### Layer 2 — Storage (Raw Zone)
**Kinesis Firehose** buffers records and delivers to S3 every 5 minutes or 128MB — whichever comes first. Files are partitioned by:
```
s3://raw-bucket/raw/stock_prices/year=2026/month=03/day=14/hour=14/
```

### Layer 3 — Transform (Glue ETL)
**AWS Glue** runs hourly via EventBridge and:
- Drops nulls and duplicates
- Casts all data types correctly
- Engineers 15+ technical indicators
- Calculates actuarial risk metrics
- Writes compressed **Parquet** (10x cheaper to query than CSV)
- Partitions by `year/month/ticker`

### Layer 4 — Serve (Athena)
**Athena** queries processed Parquet directly on S3 — no database server needed. Queries run in seconds and cost ~$5 per TB scanned.

---

## 📈 Technical Indicators Calculated

| Indicator | Description |
|-----------|-------------|
| `price_change_pct` | Daily % price change |
| `ma_7` / `ma_14` / `ma_30` | Moving averages |
| `volatility_7d` | 7-day rolling standard deviation |
| `volume_spike` | Flag when volume > 2x average |
| `above_ma_7` / `above_ma_30` | Trend signals |
| `value_at_risk_1d` | 1-day VaR at 95% confidence |
| `risk_tier` | LOW / MODERATE / ELEVATED / HIGH |
| `intraday_range_pct` | High-Low spread as % of open |

---

## 🚀 Deployment Guide

### Prerequisites
- AWS CLI configured
- Python 3.11+
- AWS account with permissions

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Deploy infrastructure
```bash
aws cloudformation create-stack \
  --stack-name financial-pipeline-dev \
  --template-body file://cloudformation/template.yaml \
  --parameters \
    ParameterKey=ProjectName,ParameterValue=financial-pipeline \
    ParameterKey=Environment,ParameterValue=dev \
  --capabilities CAPABILITY_NAMED_IAM \
  --profile root \
  --region us-east-1
```

### 3. Upload Glue script to S3
```bash
aws s3 cp glue_jobs/etl_transform.py \
  s3://financial-pipeline-glue-scripts-ACCOUNT_ID/scripts/etl_transform.py \
  --profile root
```

### 4. Start streaming data
```bash
python producer/producer.py \
  --stream financial-pipeline-stream-dev \
  --interval 60 \
  --profile root
```

### 5. Run Glue ETL manually
```bash
aws glue start-job-run \
  --job-name financial-pipeline-etl-dev \
  --profile root \
  --region us-east-1
```

### 6. Query with Athena
Open AWS Athena console, select workgroup `financial-pipeline-workgroup-dev` and run queries from `athena/queries.sql`

---

## 🧪 Running Tests

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=producer
```

---

## 💰 Cost Estimate

| Service | Free Tier | Est. Cost (dev) |
|---------|-----------|-----------------|
| Kinesis | 1M PUT/month free | ~$0.015/hr |
| S3 | 5GB free | ~$0.02/GB |
| Glue | 1M objects free | ~$0.44/DPU-hour |
| Athena | 1TB/month free | $5/TB scanned |
| EventBridge | 1M events free | ~$0.00 |
| **Total** | | **~$5-10/month** |

> 💡 **Cost tip:** Always partition Athena queries by `year` and `month` to minimize data scanned.

---

## 🔐 Security Design

| Decision | Rationale |
|----------|-----------|
| Separate IAM roles per service | Firehose, Glue, EventBridge each have minimal permissions |
| S3 public access blocked | All buckets private — no public data exposure |
| Kinesis KMS encryption | Data encrypted at rest in stream |
| S3 SSE-AES256 | All stored data encrypted |
| Athena query limits | 1GB per query limit prevents runaway costs |

---

## 📊 What This Demonstrates for Data Engineering

- **Real-time streaming** with Kinesis end-to-end
- **Data lake architecture** with raw/processed zones
- **Columnar storage** optimization (Parquet vs CSV)
- **Partition pruning** for cost-efficient queries
- **PySpark ETL** with window functions and technical indicators
- **Schema management** with Glue Data Catalog
- **Pipeline orchestration** with EventBridge
- **Serverless analytics** with Athena
- **IaC deployment** with CloudFormation

---

## 🔗 Related Project

This pipeline feeds processed data to the [Serverless Risk Scoring API](https://github.com/ouma999/serverless-risk-api) — together they form a complete financial data product.

---

## 👤 Author

**ouma999**
AWS Certified Solutions Architect | AWS Certified Data Engineer
Data Scientist & Actuary

📧 polexouma@gmail.com
💼 [GitHub](https://github.com/ouma999)

---

*Built to demonstrate production-grade AWS Data Engineering using real financial market data.*
