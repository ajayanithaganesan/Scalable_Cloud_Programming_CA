# Real-Time Crypto Contagion & Liquidity Monitor using Lambda Architecture

## MSc Cloud Computing

**Module:** Scalable Cloud Programming

### Team Members

- Muthukumar - Batch Layer
- Ajay Anitha Ganesan - Speed Layer

---

## Project Overview

This project builds a simple Lambda Architecture application for cryptocurrency trade analytics using AWS services, Apache Spark, and a web dashboard.

The system combines:

- a real-time speed layer for live market metrics
- a batch layer for historical analytics
- a serving layer based on Athena
- a front-end dashboard for visual reporting

The project focuses on crypto trade data for:

- BTCUSDT
- ETHUSDT
- SOLUSDT

---

## Architecture

```text
Binance WebSocket
    |
    v
Python Producer
    |
    v
Amazon Kinesis Data Stream
    |
    +--------------------+
    |                    |
    v                    v
Firehose               Lambda
    |                    |
    v                    v
S3 Raw JSON           DynamoDB
    |
    v
Spark on EMR
    |
    v
Processed Batch Views in S3
    |
    v
Athena
    |
    v
HTML Dashboard
```

---

## Dashboard Views

The dashboard will combine batch-layer and speed-layer metrics so the historical and live views stay aligned.

1. Liquidity Health from DynamoDB
2. Crypto Contagion from DynamoDB
3. Live Market Metrics from DynamoDB
4. Historical Trend from Athena over batch outputs

---

## Current Project Status

### Completed

- Repository setup
- GitHub configuration
- Folder structure
- Kinesis Data Stream
- `.env` configuration
- Ingestion `config.py`
- Binance WebSocket producer
- Automatic reconnection
- JSON publishing to Kinesis
- Firehose configuration
- Firehose to S3 verification
- Raw S3 storage
- Historical Binance trade dataset collected and validated
- Local Apache Spark development environment configured
- Batch layer architecture redesigned for cost-efficient EMR execution

### In Progress

- Spark batch layer redesign for local-first development
- Cost-aware EMR execution planning

### Upcoming

- EMR execution for batch jobs
- Athena setup over processed data
- Lambda and DynamoDB speed layer
- Dashboard implementation
- Benchmarking
- Final report

---

## Batch Layer Re-Plan

AWS Academy credit limits changed the batch-layer approach, so the batch work is now designed to be cost efficient.

The batch layer will still be implemented, but it will be:

- developed locally first
- tested on a small sample dataset
- moved to EMR only for short, controlled runs

### Batch Objectives

- Process historical cryptocurrency trade datasets
- Use Apache Spark DataFrame operations
- Store processed batch views in Amazon S3
- Query historical analytics through Athena
- Keep EMR usage to the minimum required for demonstration

### Dataset Strategy

Historical data will come from Binance historical trades.

Preferred layout in S3:

```text
historical/
    BTCUSDT/
    ETHUSDT/
    SOLUSDT/
```

Why this dataset:

- same schema as the live Binance WebSocket stream
- contains executed trades needed for analytics
- suitable for liquidity and contagion analysis
- large enough for Spark batch processing

### Development Workflow

The batch layer is planned in two stages:

1. Local development
   - build the Spark pipeline locally
   - read CSV or JSON trade data
   - clean and parse timestamps
   - compute analytics
   - validate the output

2. Cloud execution
   - upload finalized historical data to S3
   - launch EMR only when needed
   - run the Spark job
   - save processed results back to S3
   - terminate EMR immediately after completion

### EMR Usage Strategy

EMR will not stay running continuously.

```text
Launch EMR
    |
    v
Execute Spark Job
    |
    v
Write Results to S3
    |
    v
Terminate EMR
```

This keeps AWS Academy usage low while still demonstrating distributed batch processing.

---

## Batch Metrics

Spark will compute historical analytics including:

- Total Trading Volume
- Trade Count
- Average Price
- VWAP
- OHLCV
- Liquidity Metrics
- Historical Contagion Metrics
- Cross-Asset Correlation for BTC, ETH, and SOL

Preferred output format:

- Parquet

Alternative output format:

- CSV

Planned S3 destination structure:

```text
s3://crypto-batch-views/
    ohlcv/
    vwap/
    liquidity/
    contagion/
    correlation/
```

---

## Serving Layer

The dashboard will not query Spark directly.

```text
Spark
    |
    v
Processed Results in S3
    |
    v
Athena
    |
    v
Dashboard
```

Benefits:

- no running Spark cluster for queries
- fast dashboard response
- low cost
- serverless querying

---

## Speed Layer

The speed layer remains independent and real-time.

```text
Binance WebSocket
    |
    v
Kinesis Data Stream
    |
    v
Lambda
    |
    v
DynamoDB
```

Speed layer responsibilities:

- Rolling VWAP
- Rolling Volume
- Liquidity Ratio
- Contagion Score
- Live Market Metrics

---

## Cost Optimization Strategy

To minimize AWS Academy credit usage:

- develop the Spark job locally first
- test using a small sample dataset
- upload only finalized historical data to S3
- launch EMR only for execution
- terminate EMR immediately after processing
- store processed outputs permanently in S3
- query batch views using Athena
- keep the speed layer active only when needed for demonstrations

---

## Benchmarking

The project will compare:

- serial processing
- Spark processing

Metrics to measure:

- processing time
- throughput
- speedup

---

## Technologies

- Python
- AWS Lambda
- Amazon Kinesis Data Streams
- Amazon Kinesis Firehose
- Amazon S3
- Amazon EMR
- DynamoDB
- Athena
- Apache Spark
- HTML
- CSS
- JavaScript

---

