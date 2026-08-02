# Cryptometry: Real-Time Crypto Contagion & Liquidity Monitor (Lambda Architecture)

### Scalable Cloud Programming (MSc in Cloud Computing)
**Institution**: National College of Ireland (NCI)  
**Authors**: Ajay Anitha Ganesan (Speed & Serving Layer) & Muthukumar (Batch Layer)  
**Production Live URL**: [http://cryptometry.us-east-1.elasticbeanstalk.com/](http://cryptometry.us-east-1.elasticbeanstalk.com/)

---

## Executive Summary

**Cryptometry** is an end-to-end cloud-native platform designed to monitor cryptocurrency market liquidity crises and cross-asset contagion in real-time using the **Lambda Architecture** paradigm. 

The application ingests live streaming trades (`BTCUSDT`, `ETHUSDT`, `SOLUSDT`) via Binance WebSockets, processes stream metrics through serverless AWS Lambda into DynamoDB, and compares live trades against historical monthly baselines computed over **282.5 million trade records** using Apache Spark on AWS EMR.

---

## 1. System Architecture

```text
                               Binance Historical Trades (June 2026 CSVs)
                                                  │
                                                  ▼
                                       BATCH LAYER (Apache Spark on EMR)
                                                  │
                                       Historical Baselines (Parquet in S3)
                                                  │
                                                  ▼
                                        SERVING & COMPARISON ENGINE
                                         (Python / boto3 / S3 & DynamoDB)
                                                  ▲
                                                  │
Real-Time Binance WebSocket Stream                │
              │                                   │
              ▼                                   │
    INGESTION LAYER (Kinesis)                     │
              │                                   │
              ▼                                   │
     SPEED LAYER (AWS Lambda) ────────────────────┘
              │
              ▼
   STORAGE (DynamoDB Speed Metrics)
              │
              ▼
   PRESENTATION LAYER (Elastic Beanstalk Web Dashboard - Cryptometry)
```

---

## 2. Component Pipeline

### A. Ingestion Layer (`ingestion/`)
- **WebSocket Producer** (`ingestion/producer.py`): Ingests live executed trade streams for `BTCUSDT`, `ETHUSDT`, and `SOLUSDT` from Binance.
- **Kinesis Data Streams**: Partitioned streaming buffer (`crypto-stream`) delivering low-latency payloads.

### B. Speed Layer (`speed/`)
- **AWS Lambda** (`speed/lambda_function.py`): Event-driven stream processor triggered by Kinesis.
- **Amazon DynamoDB** (`CryptoSpeedMetrics`): Storage table with 24-hour TTL expiration.
- **Sliding Window**: Enforces a 5-minute rolling window over live trades to smooth single-trade noise.

### C. Batch Layer (`batch layer/`)
- **PySpark on AWS EMR** (`batch layer/EMR_spark_Job.py`): Processes **282,532,133 historical trade records**.
- Computes **OHLCV**, **VWAP**, **Price Volatility ($\text{StdDev}$)**, **Liquidity Metrics**, and **Top 10 Trade Spikes**.
- Writes partitioned Parquet files directly to Amazon S3 (`s3://scp-crypto-project/output/`).

### D. Serving Layer & Dashboard (`dashboard/` & `serving/`)
- **Serving Backend** (`dashboard/app.py` & `serving/batch_reader.py`): Queries DynamoDB and S3 to execute the Lambda comparison engine.
- **Web UI** (`dashboard/index.html`): Deployed on **AWS Elastic Beanstalk** (`CryptometryApp`). Features a 4-panel monitoring grid and interactive Dark/Light theme switcher.

---

## 3. Empirical Performance & Benchmarking Results

### A. AWS EMR Batch Speedup Benchmark (282.5 Million Records)

| EMR Cluster Configuration | Total Records Processed | Execution Time | Processing Throughput | Speedup Ratio |
| :--- | :--- | :--- | :--- | :--- |
| **PySpark (1 Core Serial)** | **282,532,133** | **405.74 seconds** (6.76 min) | **696,340.81 rec/sec** | **1.00x (Baseline)** |
| **PySpark (2 Core Nodes)** | **282,532,133** | **298.53 seconds** (4.98 min) | **946,421.50 rec/sec** | **1.36x Speedup** |
| **PySpark (3 Core Nodes)** | **282,532,133** | **230.40 seconds** (3.84 min) | **1,226,252.13 rec/sec** | **1.76x Speedup** 🚀 |

### B. Real-Time Streaming & Serving Metrics
- **Serving API Response Latency**: **326.96 ms** to **637.66 ms** (Measured via `benchmarks/live_stream_benchmark.py`).
- **Benchmark Plots**: Saved in `benchmarks/results/` (`speedup_vs_worker_count.png`, `latency_vs_ingestion_rate.png`, `throughput_over_time.png`).

---

## 4. Repository Directory Structure

```text
Scalable_Cloud_Programming_CA/
├── .env                       # Cloud configuration (Region, Table, Bucket)
├── Procfile                   # AWS Elastic Beanstalk web entrypoint
├── requirements.txt           # Deployment Python dependencies
├── ingestion/
│   ├── config.py              # Kinesis configuration
│   └── producer.py            # Binance WebSocket stream producer
├── speed/
│   ├── lambda_function.py     # Stream processor AWS Lambda function
│   └── test_lambda_local.py   # Local mock runner for Lambda
├── batch layer/
│   └── EMR_spark_Job.py       # Distributed PySpark batch processing script
├── serving/
│   └── batch_reader.py        # S3 Parquet batch benchmark reader
├── dashboard/
│   ├── app.py                 # Serving backend & comparison engine
│   └── index.html             # Cryptometry web dashboard UI
└── benchmarks/
    ├── live_stream_benchmark.py       # Real-time HTTP serving latency tester
    ├── generate_performance_graphs.py # Report benchmark figure renderer
    └── results/                       # High-resolution PNG plots & metrics
```

---

## 5. How to Run Locally

1. **Clone & Install Dependencies**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Start Live Ingestion Producer**:
   ```bash
   python ingestion/producer.py
   ```

3. **Start Dashboard Serving App**:
   ```bash
   python dashboard/app.py
   ```
   Open **`http://127.0.0.1:5000`** in your browser.

4. **Run Live Stream Benchmark**:
   ```bash
   python benchmarks/live_stream_benchmark.py
   ```
