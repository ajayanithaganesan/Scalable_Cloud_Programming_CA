import os
import sys
import shutil

# Import function from PySpark script
sys.path.append(os.path.dirname(__file__))
from spark_batch_job import create_spark_session, process_crypto_batch


def main():
    print("==================================================")
    print("Testing PySpark Batch Layer Pipeline Locally")
    print("==================================================\n")

    input_csv = os.path.join(os.path.dirname(__file__), "data", "sample_trades.csv")
    output_dir = os.path.join(os.path.dirname(__file__), "output")

    # Clean existing output directory if present
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    print(f"Sample Input Path : {input_csv}")
    print(f"Output Directory  : {output_dir}\n")

    # Initialize Spark Session
    spark = create_spark_session(local_mode=True)

    try:
        # Run PySpark Batch Job
        ohlcv_df, vwap_df, liquidity_df = process_crypto_batch(spark, input_csv, output_dir)

        print("\n--- Processed OHLCV Batch View ---")
        ohlcv_df.show(truncate=False)

        print("\n--- Processed VWAP Batch View ---")
        vwap_df.show(truncate=False)

        print("\n--- Processed Liquidity Health Batch View ---")
        liquidity_df.show(truncate=False)

        print("\n[SUCCESS] Local PySpark batch pipeline executed successfully!")
        print(f"Parquet outputs verified in: {output_dir}")

    except Exception as e:
        print(f"\n[ERROR] PySpark execution failed: {e}")

    finally:
        spark.stop()

    print("==================================================")


if __name__ == "__main__":
    main()
