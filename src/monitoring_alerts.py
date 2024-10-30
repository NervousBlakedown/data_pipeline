# src/monitoring_alerts.py
import os
import boto3
import logging
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient
from prometheus_client import start_http_server, Gauge
import psycopg2
import time

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS SNS setup for alerts
sns_client = boto3.client(
    'sns',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)
sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

# Kafka and Redshift configurations
kafka_bootstrap_servers = 'localhost:9092'
admin_client = AdminClient({'bootstrap.servers': kafka_bootstrap_servers})

redshift_host = os.getenv("REDSHIFT_HOST")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_port = os.getenv("REDSHIFT_PORT", 5439)

# Prometheus gauges for monitoring metrics
kafka_lag_gauge = Gauge('kafka_consumer_lag', 'Kafka consumer lag')
redshift_query_time_gauge = Gauge('redshift_query_time', 'Redshift query execution time')

# Helper function to send an SNS alert
def send_alert(message):
    response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        Subject="Pipeline Alert"
    )
    logger.info(f"Alert sent: {message}")
    return response

# Check Kafka lag for a specific consumer group
def check_kafka_lag():
    try:
        # Fetch the Kafka topic and partition information
        consumer_group = 'event-consumer-group'
        consumer_offsets = admin_client.list_consumer_group_offsets(consumer_group)
        total_lag = 0

        for topic_partition, offset_data in consumer_offsets.items():
            low, high = admin_client.get_watermark_offsets(topic_partition)
            partition_lag = high - offset_data.offset
            total_lag += max(partition_lag, 0)
        
        kafka_lag_gauge.set(total_lag)
        logger.info(f"Kafka consumer lag: {total_lag}")
        
        # Alert if Kafka lag exceeds threshold
        if total_lag > 1000:  # Adjust threshold as needed
            send_alert(f"High Kafka lag detected: {total_lag}")

    except Exception as e:
        logger.error(f"Error checking Kafka lag: {e}")

# Check Redshift query execution time
def check_redshift_query_time():
    try:
        conn = psycopg2.connect(
            dbname=redshift_db,
            user=redshift_user,
            password=redshift_password,
            host=redshift_host,
            port=redshift_port
        )
        cursor = conn.cursor()

        # Run a lightweight query and measure execution time
        start_time = time.time()
        cursor.execute("SELECT COUNT(*) FROM user_events;")
        cursor.fetchone()
        query_time = time.time() - start_time

        redshift_query_time_gauge.set(query_time)
        logger.info(f"Redshift query execution time: {query_time:.2f} seconds")

        # Alert if query time exceeds threshold
        if query_time > 2.0:  # Adjust threshold as needed
            send_alert(f"High Redshift query time detected: {query_time:.2f} seconds")

    except Exception as e:
        logger.error(f"Error checking Redshift query time: {e}")
    finally:
        cursor.close()
        conn.close()

def monitor_pipeline():
    """Main function to continuously monitor Kafka and Redshift metrics."""
    # Start Prometheus HTTP server to expose metrics
    start_http_server(8000)
    logger.info("Starting monitoring and alerting system on port 8000")

    while True:
        check_kafka_lag()
        check_redshift_query_time()
        time.sleep(60)  # Adjust frequency of checks as needed

if __name__ == "__main__":
    monitor_pipeline()
