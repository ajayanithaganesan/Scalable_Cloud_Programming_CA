from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ------------------------------------
# Create Spark Session
# ------------------------------------
spark = SparkSession.builder \
    .appName("Check Dataset Date Range") \
    .getOrCreate()

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
    .csv("batch/data/SOLUSDT-trades-2026-06.csv")

# ------------------------------------
# Convert Timestamp
# ------------------------------------
# Use /1000000 if your timestamps are in microseconds.
# If they are milliseconds, change it to /1000.

df = df.withColumn(
    "timestamp",
    from_unixtime(col("trade_time") / 1000000).cast("timestamp")
)

# ------------------------------------
# Extract Month
# ------------------------------------
df = df.withColumn(
    "trade_month",
    date_format(col("timestamp"), "yyyy-MM")
)

# ------------------------------------
# Dataset Date Range
# ------------------------------------
print("\n===================================")
print("Dataset Date Range")
print("===================================")

df.select(
    min("timestamp").alias("Start Date"),
    max("timestamp").alias("End Date")
).show(truncate=False)

# ------------------------------------
# Records Per Month
# ------------------------------------
print("\n===================================")
print("Records Per Month")
print("===================================")

df.groupBy("trade_month") \
    .count() \
    .orderBy("trade_month") \
    .show(truncate=False)

# ------------------------------------
# Show July Records
# ------------------------------------
print("\n===================================")
print("First 10 July Records")
print("===================================")

df.filter(col("trade_month") == "2026-07") \
    .select(
        "trade_id",
        "timestamp",
        "price",
        "quantity"
    ) \
    .show(10, truncate=False)

# ------------------------------------
# Stop Spark
# ------------------------------------
spark.stop()