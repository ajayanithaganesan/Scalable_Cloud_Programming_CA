import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType


def create_spark_session(local_mode=True):
    """
    Initializes Apache Spark Session.
    Uses local mode for development/testing and cluster mode for EMR execution.
    """
    builder = SparkSession.builder.appName("CryptoBatchAnalytics")
    
    if local_mode:
        builder = builder.master("local[*]")
        
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_crypto_batch(spark, input_path, output_base_path):
    """
    Main PySpark Batch Layer computation pipeline.
    Reads historical trade data, performs timestamp parsing, cleans data,
    computes OHLCV, VWAP, and Liquidity metrics, and writes Parquet outputs.
    """
    print(f"Reading input dataset from: {input_path}")

    # Read trade dataset (handles CSV with header or standard Binance trade schema)
    # Schema: symbol, price, quantity, trade_time (epoch ms)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    # Clean & normalize column names and data types
    cleaned_df = df.withColumn("price", F.col("price").cast(DoubleType())) \
                   .withColumn("quantity", F.col("quantity").cast(DoubleType())) \
                   .withColumn("trade_time_ms", F.col("trade_time").cast(LongType())) \
                   .withColumn("timestamp", (F.col("trade_time_ms") / 1000).cast(TimestampType())) \
                   .withColumn("volume_usd", F.col("price") * F.col("quantity"))

    # Add windowing hour column for hourly aggregation
    windowed_df = cleaned_df.withColumn("hour", F.date_trunc("hour", F.col("timestamp")))

    # -------------------------------------------------------------
    # 1. Compute Hourly OHLCV (Open, High, Low, Close, Volume)
    # -------------------------------------------------------------
    print("Computing Hourly OHLCV Aggregations...")
    ohlcv_df = windowed_df.groupBy("symbol", "hour").agg(
        F.first("price").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.last("price").alias("close"),
        F.sum("volume_usd").alias("total_volume_usd"),
        F.count("*").alias("trade_count")
    ).orderBy("symbol", "hour")

    # -------------------------------------------------------------
    # 2. Compute VWAP (Volume Weighted Average Price)
    #    Formula: Sum(Price * Quantity) / Sum(Quantity)
    # -------------------------------------------------------------
    print("Computing Hourly VWAP...")
    vwap_df = windowed_df.groupBy("symbol", "hour").agg(
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        F.sum("volume_usd").alias("total_volume_usd")
    ).orderBy("symbol", "hour")

    # -------------------------------------------------------------
    # 3. Compute Historical Liquidity Metrics
    #    Average trade volume and trade size volatility
    # -------------------------------------------------------------
    print("Computing Liquidity Health Metrics...")
    liquidity_df = windowed_df.groupBy("symbol", "hour").agg(
        F.avg("volume_usd").alias("avg_trade_size_usd"),
        F.stddev("volume_usd").alias("volume_stddev"),
        F.count("*").alias("trade_count")
    ).orderBy("symbol", "hour")

    # -------------------------------------------------------------
    # Write Batch Views to Parquet Output Format
    # -------------------------------------------------------------
    ohlcv_output = os.path.join(output_base_path, "ohlcv")
    vwap_output = os.path.join(output_base_path, "vwap")
    liquidity_output = os.path.join(output_base_path, "liquidity")

    print(f"Writing OHLCV view to Parquet: {ohlcv_output}")
    ohlcv_df.write.mode("overwrite").parquet(ohlcv_output)

    print(f"Writing VWAP view to Parquet: {vwap_output}")
    vwap_df.write.mode("overwrite").parquet(vwap_output)

    print(f"Writing Liquidity view to Parquet: {liquidity_output}")
    liquidity_df.write.mode("overwrite").parquet(liquidity_output)

    print("Batch processing pipeline completed successfully.")
    return ohlcv_df, vwap_df, liquidity_df


if __name__ == "__main__":
    # Command line argument parser or default local paths
    input_file = sys.argv[1] if len(sys.argv) > 1 else "batch/data/sample_trades.csv"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "batch/output/"

    spark_sess = create_spark_session(local_mode=True)
    process_crypto_batch(spark_sess, input_file, output_dir)
    spark_sess.stop()
