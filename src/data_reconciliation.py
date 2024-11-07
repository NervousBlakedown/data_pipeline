# src/data_reconciliation.py
import os
import boto3
import psycopg2
import logging
import pandas as pd
from dotenv import load_dotenv
from hashlib import md5

# Load environment variables
load_dotenv()

# AWS and Redshift configurations
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
s3_processed_bucket = os.getenv("PROCESSED_DATA_BUCKET_NAME")
redshift_host = os.getenv("REDSHIFT_HOST")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_port = os.getenv("REDSHIFT_PORT", 5439)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to Redshift
def connect_redshift():
    return psycopg2.connect(
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port
    )

# Calculate checksum for a DataFrame
def calculate_checksum(df):
    return md5(pd.util.hash_pandas_object(df).values).hexdigest()

# Compare S3 and Redshift row counts
def compare_row_counts():
    # Fetch Redshift row count
    with connect_redshift() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM user_events")
        redshift_count = cursor.fetchone()[0]
    
    # Fetch S3 row count
    s3_obj = s3_client.get_object(Bucket=s3_processed_bucket, Key="transformed/sessionized/part-00000.parquet")
    s3_df = pd.read_parquet(s3_obj['Body'])
    s3_count = len(s3_df)

    if redshift_count == s3_count:
        logger.info("Row count matches between S3 and Redshift.")
    else:
        logger.warning(f"Row count mismatch: S3 has {s3_count} rows, Redshift has {redshift_count} rows.")

# Compare S3 and Redshift checksums
def compare_checksums():
    # Fetch Redshift data
    with connect_redshift() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM user_events")
        redshift_data = cursor.fetchall()
        redshift_df = pd.DataFrame(redshift_data)

    # Fetch S3 data
    s3_obj = s3_client.get_object(Bucket=s3_processed_bucket, Key="transformed/sessionized/part-00000.parquet")
    s3_df = pd.read_parquet(s3_obj['Body'])

    # Calculate checksums
    redshift_checksum = calculate_checksum(redshift_df)
    s3_checksum = calculate_checksum(s3_df)

    if redshift_checksum == s3_checksum:
        logger.info("Data checksum matches between S3 and Redshift.")
    else:
        logger.warning("Data checksum mismatch between S3 and Redshift.")

def reconcile_data():
    logger.info("Starting data reconciliation between S3 and Redshift.")
    compare_row_counts()
    compare_checksums()
    logger.info("Data reconciliation completed.")

if __name__ == "__main__":
    reconcile_data()
