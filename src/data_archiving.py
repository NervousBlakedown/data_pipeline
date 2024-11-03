# src/data_archiving.py
import os
import psycopg2
import boto3
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
import pyarrow.parquet as pq
import pyarrow as pa

# Load environment variables
load_dotenv()

# AWS and Redshift configurations
s3_bucket = os.getenv("S3_BUCKET_NAME")
redshift_host = os.getenv("REDSHIFT_HOST")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_port = os.getenv("REDSHIFT_PORT", 5439)
iam_role = os.getenv("REDSHIFT_IAM_ROLE")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# Define date cutoff for archiving (e.g., older than 1 year)
archive_cutoff_date = datetime.now() - timedelta(days=365)

# SQL Query to select data older than the cutoff date
select_query = f"""
SELECT *
FROM user_events
WHERE timestamp < '{archive_cutoff_date.strftime('%Y-%m-%d')}';
"""

# SQL Query to delete archived data from Redshift
delete_query = f"""
DELETE FROM user_events
WHERE timestamp < '{archive_cutoff_date.strftime('%Y-%m-%d')}';
"""

def export_to_s3(df, file_name):
    # Convert DataFrame to Parquet and upload to S3
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_name)
    s3_client.upload_file(file_name, s3_bucket, f"archive/{file_name}")
    logger.info(f"Archived data saved to S3 as {file_name}")
    os.remove(file_name)  # Clean up local file

def archive_old_data():
    try:
        # Connect to Redshift
        conn = psycopg2.connect(
            dbname=redshift_db,
            user=redshift_user,
            password=redshift_password,
            host=redshift_host,
            port=redshift_port
        )
        cursor = conn.cursor()

        # Fetch old data
        logger.info("Fetching data older than cutoff date from Redshift.")
        cursor.execute(select_query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

        # Export to S3 if data exists
        if not df.empty:
            file_name = f"user_events_archive_{archive_cutoff_date.strftime('%Y%m%d')}.parquet"
            export_to_s3(df, file_name)
            
            # Delete archived data from Redshift
            cursor.execute(delete_query)
            conn.commit()
            logger.info("Archived data successfully deleted from Redshift.")

        else:
            logger.info("No data found for archiving.")

    except Exception as e:
        logger.error(f"Error during data archiving process: {e}")

    finally:
        cursor.close()
        conn.close()
        logger.info("Redshift connection closed.")

if __name__ == "__main__":
    archive_old_data()
