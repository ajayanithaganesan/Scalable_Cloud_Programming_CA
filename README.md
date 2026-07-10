# Real-Time Crypto Contagion & Liquidity Monitor using Lambda Architecture

## MSc Cloud Computing

**Module:** Scalable Cloud Programming

### Team Members

- Muthukumar – Batch Layer
- Ajay Anitha Ganesan – Speed Layer

---

## Project Architecture

Binance WebSocket

↓

Amazon Kinesis

├── Firehose → S3 → EMR (Batch Layer)

└── Lambda → DynamoDB (Speed Layer)

↓

Athena

↓

S3 Hosted Dashboard

---

## Technologies

- Python
- AWS Lambda
- Amazon Kinesis
- Amazon Kinesis Firehose
- Amazon S3
- Amazon EMR
- DynamoDB
- Athena
- HTML
- CSS
- JavaScript
