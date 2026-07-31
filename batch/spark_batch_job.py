
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ------------------------------------
# Create Spark Session
# ------------------------------------
spark = SparkSession.builder \
    .appName("Crypto Batch Processing - Monthly Analytics") \
    .getOrCreate()

print("="*50)
print("Spark Session Created Successfully")
print("="*50)

# ------------------------------------
# Define Schema
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

# ------------------------------------
# Read CSV
# ------------------------------------
df = spark.read \
    .option("header", "false") \
    .schema(schema) \
    .csv("s3://scp-crypto-project/input/SOLUSDT-trades-2026-06.csv")

print("\nSchema")
df.printSchema()

print("\nSample Data")
df.show(5, truncate=False)

print(f"\nTotal Records: {df.count():,}")

# ------------------------------------
# Transform Data
# ------------------------------------
df = df.withColumn(
    "timestamp",
    from_unixtime(col("trade_time") / 1000000).cast("timestamp")
)

df = df.withColumn("trade_date", to_date("timestamp"))

df = df.withColumn(
    "trade_month",
    date_format(col("timestamp"), "yyyy-MM")
)

df = df.withColumn(
    "trade_value",
    col("price") * col("quantity")
)

print("\nTransformed Data")
df.select(
    "trade_id",
    "price",
    "quantity",
    "trade_value",
    "timestamp",
    "trade_date",
    "trade_month"
).show(10, truncate=False)

# ------------------------------------
# Dataset Summary
# ------------------------------------
print("\nDataset Summary")

df.select(
    count("*").alias("Total Trades"),
    avg("price").alias("Average Price"),
    min("price").alias("Minimum Price"),
    max("price").alias("Maximum Price"),
    sum("quantity").alias("Total Quantity"),
    sum("trade_value").alias("Total Trade Value")
).show(truncate=False)

# ------------------------------------
# Monthly Trade Count
# ------------------------------------
monthly_trades = df.groupBy("trade_month") \
    .count() \
    .orderBy("trade_month")

print("\nMonthly Trade Count")
monthly_trades.show(truncate=False)

# ------------------------------------
# Monthly Volume
# ------------------------------------
monthly_volume = df.groupBy("trade_month") \
    .agg(sum("quantity").alias("Total Volume")) \
    .orderBy("trade_month")

print("\nMonthly Trading Volume")
monthly_volume.show(truncate=False)

# ------------------------------------
# Monthly Average Price
# ------------------------------------
monthly_price = df.groupBy("trade_month") \
    .agg(avg("price").alias("Average Price")) \
    .orderBy("trade_month")

print("\nMonthly Average Price")
monthly_price.show(truncate=False)

# ------------------------------------
# Top 10 Largest Trades
# ------------------------------------
print("\nTop 10 Largest Trades")

df.select(
    "trade_id",
    "price",
    "quantity",
    "trade_value"
).orderBy(
    col("trade_value").desc()
).show(10, truncate=False)

# ------------------------------------
# SQL Analytics
# ------------------------------------
df.createOrReplaceTempView("crypto_trades")

print("\nSQL - Monthly Trade Count")
spark.sql("""
SELECT trade_month,
COUNT(*) AS total_trades
FROM crypto_trades
GROUP BY trade_month
ORDER BY trade_month
""").show()

print("\nSQL - Monthly Average Price")
spark.sql("""
SELECT trade_month,
ROUND(AVG(price),2) AS average_price
FROM crypto_trades
GROUP BY trade_month
ORDER BY trade_month
""").show()

print("\nSQL - Monthly Volume")
spark.sql("""
SELECT trade_month,
ROUND(SUM(quantity),2) AS total_volume
FROM crypto_trades
GROUP BY trade_month
ORDER BY trade_month
""").show()

# ------------------------------------
# VWAP
# ------------------------------------
print("\nVWAP")
vwap_df = df.agg(
    (sum("trade_value") / sum("quantity")).alias("VWAP")
)
vwap_df.show(truncate=False)

# ------------------------------------
# Liquidity Metrics
# ------------------------------------
print("\nLiquidity Metrics")

liquidity_df = df.groupBy("trade_month").agg(
    count("*").alias("Number of Trades"),
    sum("trade_value").alias("Total Trade Value"),
    avg("trade_value").alias("Average Trade Value"),
    max("trade_value").alias("Largest Trade Value")
)

liquidity_df.show(truncate=False)

# ------------------------------------
# Monthly OHLCV
# ------------------------------------
open_window = Window.partitionBy("trade_month").orderBy("timestamp")
close_window = Window.partitionBy("trade_month").orderBy(col("timestamp").desc())

open_df = df.withColumn(
    "rn",
    row_number().over(open_window)
).filter(
    col("rn") == 1
).select(
    "trade_month",
    col("price").alias("Open")
)

close_df = df.withColumn(
    "rn",
    row_number().over(close_window)
).filter(
    col("rn") == 1
).select(
    "trade_month",
    col("price").alias("Close")
)

ohlcv_df = df.groupBy("trade_month").agg(
    max("price").alias("High"),
    min("price").alias("Low"),
    sum("quantity").alias("Volume")
)

ohlcv_df = ohlcv_df \
    .join(open_df, "trade_month") \
    .join(close_df, "trade_month") \
    .select(
        "trade_month",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume"
    )

print("\nMonthly OHLCV")
ohlcv_df.show(truncate=False)

# ------------------------------------
# Save Results
# ------------------------------------
monthly_trades.write.mode("overwrite").parquet("s3://scp-crypto-project/output/monthly_trades")
monthly_volume.write.mode("overwrite").parquet("s3://scp-crypto-project/output/monthly_volume")
monthly_price.write.mode("overwrite").parquet("s3://scp-crypto-project/output/monthly_price")
vwap_df.write.mode("overwrite").parquet("s3://scp-crypto-project/output/vwap")
liquidity_df.write.mode("overwrite").parquet("s3://scp-crypto-project/output/liquidity_metrics")
ohlcv_df.write.mode("overwrite").parquet("s3://scp-crypto-project/output/monthly_ohlcv")

print("\nResults saved successfully.")

spark.stop()
print("\nSpark Session Closed")
