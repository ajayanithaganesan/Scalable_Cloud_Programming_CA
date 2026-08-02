import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ------------------------------------
# 1. Create & Configure Spark Session
# ------------------------------------
start_job_time = time.time()

spark = SparkSession.builder \
    .appName("Crypto Batch Processing - Distinction Comprehensive Pipeline") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

print("=" * 70)
print("SPARK SESSION INITIALIZED FOR BATCH PROCESSING")
print("=" * 70)

# ------------------------------------
# 2. Define Explicit Schema
# ------------------------------------
schema = StructType([
    StructField("trade_id", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("quote_quantity", DoubleType(), True),
    StructField("trade_time", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("is_best_match", BooleanType(), True)
])

# S3 Data Paths
files = [
    "BTCUSDT-trades-2026-06.csv",
    "ETHUSDT-trades-2026-06.csv",
    "SOLUSDT-trades-2026-06.csv"
]

input_path = "s3://scp-crypto-project/input/"
output_path = "s3://scp-crypto-project/output/"

# ------------------------------------
# 3. Data Ingestion & Data Parallelism
# ------------------------------------
combined_df = None
print("\nIngesting Binance June historical trade files...")

for file in files:
    symbol = file.split("-")[0]
    print(f" -> Loading dataset: {file}")

    temp_df = (
        spark.read
        .option("header", "false")
        .schema(schema)
        .csv(input_path + file)
        .withColumn("symbol", lit(symbol))
    )

    combined_df = temp_df if combined_df is None else combined_df.unionByName(temp_df)

df = combined_df

# Cache dataset for multi-action pipeline performance optimization
df.cache()
total_records = df.count()
num_partitions = df.rdd.getNumPartitions()

print(f"\nTotal Historical Records Loaded : {total_records:,}")
print(f"Data Parallel Partitions Count  : {num_partitions}")

# ------------------------------------
# 4. Transformations & Feature Engineering
# ------------------------------------
df = (
    df.withColumn("timestamp", from_unixtime(col("trade_time") / 1000000).cast("timestamp"))
      .withColumn("trade_date", to_date("timestamp"))
      .withColumn("trade_month", date_format("timestamp", "yyyy-MM"))
      .withColumn("hour", date_trunc("hour", col("timestamp")))
      .withColumn("trade_value", col("price") * col("quantity"))
)

# ------------------------------------
# 5. Core Metric Calculations
# ------------------------------------

# A. Dataset Summary & Basic Aggregates
summary_df = df.groupBy("symbol").agg(
    count("*").alias("total_trades"),
    avg("price").alias("average_price"),
    min("price").alias("minimum_price"),
    max("price").alias("maximum_price"),
    sum("quantity").alias("total_quantity"),
    sum("trade_value").alias("total_trade_value")
)

# B. Monthly VWAP (Volume Weighted Average Price)
vwap_df = (
    df.groupBy("symbol", "trade_month")
      .agg((sum("trade_value") / sum("quantity")).alias("VWAP"))
)

# C. Historical Volatility & Risk Benchmark (Distinction Feature)
volatility_df = (
    df.groupBy("symbol", "trade_month")
      .agg(
          stddev("price").alias("price_stddev"),
          variance("price").alias("price_variance"),
          avg("trade_value").alias("avg_trade_value"),
          stddev("trade_value").alias("trade_value_stddev")
      )
)

# D. Liquidity Health Metrics
liquidity_df = (
    df.groupBy("symbol", "trade_month")
      .agg(
          count("*").alias("number_of_trades"),
          sum("trade_value").alias("total_trade_value"),
          avg("trade_value").alias("average_trade_value"),
          max("trade_value").alias("largest_trade_value")
      )
)

# E. Monthly OHLCV (Open, High, Low, Close, Volume)
open_window = Window.partitionBy("symbol", "trade_month").orderBy("timestamp")
close_window = Window.partitionBy("symbol", "trade_month").orderBy(col("timestamp").desc())

open_df = (
    df.withColumn("rn", row_number().over(open_window))
      .filter(col("rn") == 1)
      .select("symbol", "trade_month", col("price").alias("Open"))
)

close_df = (
    df.withColumn("rn", row_number().over(close_window))
      .filter(col("rn") == 1)
      .select("symbol", "trade_month", col("price").alias("Close"))
)

ohlcv_df = (
    df.groupBy("symbol", "trade_month")
      .agg(
          max("price").alias("High"),
          min("price").alias("Low"),
          sum("quantity").alias("Volume")
      )
      .join(open_df, ["symbol", "trade_month"])
      .join(close_df, ["symbol", "trade_month"])
      .select("symbol", "trade_month", "Open", "High", "Low", "Close", "Volume")
)

# F. Top-10 Largest Trades per Symbol (Top-N Aggregation Requirement)
top_n_window = Window.partitionBy("symbol").orderBy(col("trade_value").desc())
top_trades = (
    df.withColumn("rank", row_number().over(top_n_window))
      .filter(col("rank") <= 10)
      .select("symbol", "rank", "trade_id", "price", "quantity", "trade_value", "timestamp")
)

# G. Top Volume Trading Hours (Hourly Summarisation)
top_hourly_volume = (
    df.groupBy("symbol", "hour")
      .agg(sum("trade_value").alias("hourly_volume_usd"), count("*").alias("hourly_trade_count"))
      .orderBy(col("hourly_volume_usd").desc())
      .limit(10)
)

# ------------------------------------
# 6. Save Partitioned Parquet Views & Benchmark Graphs to S3
# ------------------------------------
print("\nWriting Parquet batch views to Amazon S3...")
summary_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "summary")
vwap_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "vwap")
volatility_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "volatility")
liquidity_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "liquidity_metrics")
ohlcv_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "monthly_ohlcv")
top_trades.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "top_trades")
top_hourly_volume.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "top_hourly_volume")

# Generate visual chart graphs for IEEE report
try:
    import matplotlib.pyplot as plt
    import pandas as pd
    
    # Collect small aggregate dataframes for plotting
    vwap_pdf = vwap_df.toPandas()
    vol_pdf = volatility_df.toPandas()
    
    # Plot Graph 1: Historical VWAP per Symbol
    plt.figure(figsize=(8, 5))
    plt.bar(vwap_pdf["symbol"], vwap_pdf["VWAP"], color=['#38bdf8', '#a855f7', '#34d399'])
    plt.title("Spark Batch Historical VWAP Benchmark")
    plt.xlabel("Symbol")
    plt.ylabel("VWAP (USD)")
    plt.tight_layout()
    # Save graphs locally and upload directly to S3 bucket
    import boto3
    s3_bucket = "scp-crypto-project"
    s3_client = boto3.client("s3")
    
    plt.savefig("/tmp/spark_vwap_benchmark.png")
    plt.close()
    
    # Plot Graph 2: Price Volatility
    plt.figure(figsize=(8, 5))
    plt.bar(vol_pdf["symbol"], vol_pdf["price_stddev"], color=['#ef4444', '#f59e0b', '#10b981'])
    plt.title("Spark Batch Historical Volatility (StdDev)")
    plt.xlabel("Symbol")
    plt.ylabel("Standard Deviation ($)")
    plt.tight_layout()
    plt.savefig("/tmp/spark_volatility_benchmark.png")
    plt.close()

    # Direct S3 upload
    s3_client.upload_file("/tmp/spark_vwap_benchmark.png", s3_bucket, "output/graphs/spark_vwap_benchmark.png")
    s3_client.upload_file("/tmp/spark_volatility_benchmark.png", s3_bucket, "output/graphs/spark_volatility_benchmark.png")

    print("Spark benchmark graphs automatically generated and uploaded to S3: s3://scp-crypto-project/output/graphs/")
except Exception as e:
    print(f"Note: Graph generation skipped ({e}). Continuing...")

# ------------------------------------
# 7. Print Benchmark & Execution Telemetry
# ------------------------------------
total_duration_sec = time.time() - start_job_time
throughput_rec_per_sec = total_records / total_duration_sec if total_duration_sec > 0 else 0

print("\n" + "=" * 70)
print("EMR SPARK BATCH PROCESSING BENCHMARK METRICS")
print("=" * 70)
print(f"Total Dataset Records Processed : {total_records:,}")
print(f"Spark RDD Partitions Count      : {num_partitions}")
print(f"Total Job Execution Time        : {total_duration_sec:.2f} seconds ({total_duration_sec / 60:.2f} minutes)")
print(f"Batch Processing Throughput     : {throughput_rec_per_sec:,.2f} records/second")
print("Parquet Output Target Directory : " + output_path)
print("=" * 70)

print("\nTop 10 Largest Historical Trades Summary:")
top_trades.show(10, truncate=False)

print("\nHistorical Volatility Baseline Summary:")
volatility_df.show(truncate=False)

print("\nResults written successfully to S3.")
spark.stop()
print("Spark Session Closed.")
