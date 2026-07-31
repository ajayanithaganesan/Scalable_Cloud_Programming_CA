from pyspark.sql.functions import (
    col,
    from_unixtime,
    to_timestamp,
    lit
)

# =====================================================
# Data Cleaning & Transformation
# =====================================================

print("\n===================================")
print("📌 Transforming Dataset")
print("===================================\n")

# Convert microseconds to seconds
df = df.withColumn(
    "trade_timestamp",
    to_timestamp(from_unixtime(col("time") / lit(1000000)))
)

# Total USD value of each trade
df = df.withColumn(
    "volume_usd",
    col("price") * col("qty")
)

# Extract trade date
df = df.withColumn(
    "trade_date",
    col("trade_timestamp").cast("date")
)

print("✅ Transformations Completed")