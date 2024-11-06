# src/data_transformation.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, window, avg
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# S3 configurations
s3_raw_bucket = os.getenv("RAW_DATA_BUCKET_NAME")
s3_processed_bucket = os.getenv("PROCESSED_DATA_BUCKET_NAME")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataTransformation") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Define the transformation function
def transform_data():
    # Load data from raw S3 bucket (simulate batch processing)
    raw_data_path = f"s3a://{s3_raw_bucket}/ingestion/kafka-events/"
    df = spark.read.format("avro").load(raw_data_path)

    # Example transformations:
    # 1. Filter events with valid session ID
    df_filtered = df.filter(df.session_id.isNotNull())

    # 2. Aggregate data by event type with counts
    df_aggregated = df_filtered.groupBy("event_type").count()

    # 3. Sessionize data by user within 1-hour time windows
    df_sessionized = df_filtered \
        .groupBy(window(col("timestamp"), "1 hour"), "user_id") \
        .agg(count("session_id").alias("session_count"))

    # 4. Calculate average event frequency per user
    df_avg_event_freq = df_filtered \
        .groupBy("user_id") \
        .agg(avg("timestamp").alias("avg_event_time"))

    # Store transformed data back to S3
    processed_data_path = f"s3a://{s3_processed_bucket}/transformed/"
    df_sessionized.write.format("parquet").mode("overwrite").save(f"{processed_data_path}/sessionized/")
    df_avg_event_freq.write.format("parquet").mode("overwrite").save(f"{processed_data_path}/avg_event_freq/")

    logger.info("Data transformation complete and stored in S3.")

if __name__ == "__main__":
    transform_data()
    spark.stop()
