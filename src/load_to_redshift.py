# src/load_to_redshift.py
import os
import psycopg2
from dotenv import load_dotenv
import logging
import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint

# Load environment variables
load_dotenv()

# AWS and Redshift configurations
s3_bucket = "bucket-name"
s3_data_prefix = "data/"
redshift_host = os.getenv("REDSHIFT_HOST")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_port = os.getenv("REDSHIFT_PORT", 5439)
iam_role = os.getenv("REDSHIFT_IAM_ROLE")

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the validation function
def validate_data(file_path):
    # Load the Great Expectations suite
    context = ge.data_context.DataContext()
    checkpoint = SimpleCheckpoint(
        name="cold_data_checkpoint",
        data_context=context,
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_s3_datasource",
                    "data_connector_name": "default_inferred_data_connector_name",
                    "data_asset_name": file_path,
                },
                "expectation_suite_name": "cold_data_suite",
            }
        ],
    )
    result = checkpoint.run()
    if result["success"]:
        logger.info("Data validation passed.")
    else:
        logger.error("Data validation failed.")
        raise ValueError("Validation failed for data in {}".format(file_path))

# SQL command to create the target table in Redshift
create_table_query = """
CREATE TABLE IF NOT EXISTS user_events (
    user_id INT,
    session_id VARCHAR(50),
    event_type VARCHAR(50),
    user_agent VARCHAR(100),
    geo_location VARCHAR(10),
    timestamp TIMESTAMP
);
"""

# SQL COPY command to load data from S3 into Redshift
copy_query = f"""
COPY user_events
FROM 's3://{s3_bucket}/{s3_data_prefix}'
IAM_ROLE '{iam_role}'
FORMAT AS AVRO 'auto';
"""

# Example usage of validation in data loading
def load_data_to_redshift():
    try:
        # Connect to Redshift
        logger.info("Connecting to Redshift")
        conn = psycopg2.connect(
            dbname=redshift_db,
            user=redshift_user,
            password=redshift_password,
            host=redshift_host,
            port=redshift_port
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Validate data
        validate_data(f"s3://{s3_bucket}/{s3_data_prefix}data_chunk.avro")

        # Create table and load data if validation passes
        cursor.execute(create_table_query)
        cursor.execute(copy_query)
        logger.info("Data loaded into Redshift from S3 successfully.")

    except psycopg2.Error as e:
        logger.error(f"Database error occurred: {e}")
    except Exception as e:
        logger.error(f"Error loading data to Redshift: {e}")

    finally:
        cursor.close()
        conn.close()
        logger.info("Connection to Redshift closed.")

if __name__ == "__main__":
    load_data_to_redshift()
