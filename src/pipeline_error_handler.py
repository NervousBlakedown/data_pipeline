# src/pipeline_error_handler.py
import logging
import boto3
import os
from dotenv import load_dotenv
import traceback

# Load environment variables
load_dotenv()

# AWS SNS setup for alerts
sns_client = boto3.client(
    'sns',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)
sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Error categories for classification
ERROR_CATEGORIES = {
    "DATA_INGESTION": "Error in data ingestion (Kafka/S3)",
    "DATA_TRANSFORMATION": "Error in data transformation (Spark)",
    "DATA_LOADING": "Error in data loading (Redshift)"
}

# Function to send an alert via AWS SNS
def send_alert(subject, message):
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=message
    )
    logger.info(f"Alert sent: {subject}")

# Error handler function to classify and log errors
def handle_error(error, error_category):
    error_message = ERROR_CATEGORIES.get(error_category, "Unknown error") + f": {str(error)}"
    logger.error(error_message)
    logger.error(traceback.format_exc())
    
    # Send alert for critical errors
    send_alert(f"Pipeline Alert - {error_category}", error_message)

# Example usage in other scripts
def example_pipeline_function():
    try:
        # Simulate code block where error might occur
        # e.g., ingestion, transformation, or loading
        raise ValueError("Simulated error in data transformation.")
    except Exception as e:
        handle_error(e, "DATA_TRANSFORMATION")

if __name__ == "__main__":
    # Test error handler by running an example function
    example_pipeline_function()
