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

print("=" * 60)
print("Spark Session Created Successfully")
print("=" * 60)

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
# Files to Process
# ------------------------------------
files = [
    "BTCUSDT-trades-2026-06.csv",
    "ETHUSDT-trades-2026-06.csv",
    "SOLUSDT-trades-2026-06.csv"
]

input_path = "s3://scp-crypto-project/input/"
output_path = "s3://scp-crypto-project/output/"

combined_df = None

print("\nReading CSV files...")

for file in files:
    symbol = file.split("-")[0]
    print(f"Loading {file}")

    temp_df = (
        spark.read
        .option("header", "false")
        .schema(schema)
        .csv(input_path + file)
        .withColumn("symbol", lit(symbol))
    )

    combined_df = temp_df if combined_df is None else combined_df.unionByName(temp_df)

df = combined_df

print("\nSchema")
df.printSchema()

print("\nSample Data")
df.show(5, truncate=False)

print(f"\nTotal Records: {df.count():,}")

# ------------------------------------
# Transformations
# ------------------------------------
df = (
    df.withColumn("timestamp", from_unixtime(col("trade_time") / 1000000).cast("timestamp"))
      .withColumn("trade_date", to_date("timestamp"))
      .withColumn("trade_month", date_format("timestamp", "yyyy-MM"))
      .withColumn("trade_value", col("price") * col("quantity"))
)

# ------------------------------------
# Dataset Summary
# ------------------------------------
summary_df = df.groupBy("symbol").agg(
    count("*").alias("total_trades"),
    avg("price").alias("average_price"),
    min("price").alias("minimum_price"),
    max("price").alias("maximum_price"),
    sum("quantity").alias("total_quantity"),
    sum("trade_value").alias("total_trade_value")
)

summary_df.show(truncate=False)

# ------------------------------------
# Monthly Analytics
# ------------------------------------
monthly_trades = (
    df.groupBy("symbol", "trade_month")
      .count()
      .orderBy("symbol", "trade_month")
)

monthly_volume = (
    df.groupBy("symbol", "trade_month")
      .agg(sum("quantity").alias("total_volume"))
      .orderBy("symbol", "trade_month")
)

monthly_price = (
    df.groupBy("symbol", "trade_month")
      .agg(avg("price").alias("average_price"))
      .orderBy("symbol", "trade_month")
)

# ------------------------------------
# Top Trades
# ------------------------------------
top_trades = (
    df.select("symbol", "trade_id", "price", "quantity", "trade_value")
      .orderBy(col("trade_value").desc())
      .limit(10)
)

top_trades.show(truncate=False)

# ------------------------------------
# SQL Analytics
# ------------------------------------
df.createOrReplaceTempView("crypto_trades")

spark.sql("""
SELECT
    symbol,
    trade_month,
    COUNT(*) AS total_trades,
    ROUND(AVG(price),2) AS average_price,
    ROUND(SUM(quantity),2) AS total_volume
FROM crypto_trades
GROUP BY symbol, trade_month
ORDER BY symbol, trade_month
""").show(truncate=False)

# ------------------------------------
# VWAP
# ------------------------------------
vwap_df = (
    df.groupBy("symbol")
      .agg((sum("trade_value") / sum("quantity")).alias("VWAP"))
)

# ------------------------------------
# Liquidity
# ------------------------------------
liquidity_df = (
    df.groupBy("symbol", "trade_month")
      .agg(
          count("*").alias("number_of_trades"),
          sum("trade_value").alias("total_trade_value"),
          avg("trade_value").alias("average_trade_value"),
          max("trade_value").alias("largest_trade_value")
      )
)

# ------------------------------------
# OHLCV
# ------------------------------------
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

# ------------------------------------
# Save Results
# ------------------------------------
summary_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "summary")
monthly_trades.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "monthly_trades")
monthly_volume.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "monthly_volume")
monthly_price.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "monthly_price")
vwap_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "vwap")
liquidity_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "liquidity_metrics")
ohlcv_df.write.mode("overwrite").partitionBy("symbol").parquet(output_path + "monthly_ohlcv")

print("\nResults saved successfully.")
spark.stop()
print("Spark Session Closed.")
