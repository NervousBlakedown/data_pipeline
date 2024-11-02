# src/kafka_consumer.py
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import psycopg2
import os
from dotenv import load_dotenv
import great_expectations as ge
import logging

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redshift connection details
redshift_host = os.getenv("REDSHIFT_HOST")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_port = os.getenv("REDSHIFT_PORT", 5439)

# Kafka and Schema Registry setup
schema_registry_url = "http://localhost:8081"
schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
topic = "user-events"
schema_subject = f"{topic}-value"

# Load schema from the schema registry
schema = schema_registry_client.get_latest_version(schema_subject).schema
avro_deserializer = AvroDeserializer(schema_registry_client, schema)

# Consumer setup
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'event-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe([topic])

# Redshift connection function
def connect_redshift():
    return psycopg2.connect(
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port
    )

# Define SQL Insert query for Redshift
insert_query = """
    INSERT INTO user_events (user_id, session_id, event_type, user_agent, geo_location, timestamp)
    VALUES (%s, %s, %s, %s, %s, to_timestamp(%s / 1000.0));
"""

# Data validation function
def validate_hot_data(event):
    context = ge.data_context.DataContext()
    validator = context.get_validator(
        batch_request={
            "datasource_name": "my_kafka_datasource",
            "data_connector_name": "default_inferred_data_connector_name",
            "data_asset_name": "hot_data_stream",
        },
        expectation_suite_name="hot_data_suite",
    )
    # Pass data as a dictionary to validate
    validation_result = validator.expect_column_values_to_not_be_null("user_id")
    if not validation_result.success:
        logger.warning("Real-time data validation failed for event: %s", event)
    return validation_result.success

def consume_and_load_to_redshift():
    conn = connect_redshift()
    cursor = conn.cursor()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            # Deserialize the Kafka message
            event = avro_deserializer.decode(msg.value())
            logger.info("Consumed event: %s", event)

            # Validate event before loading into Redshift
            if validate_hot_data(event):
                cursor.execute(insert_query, (
                    event['user_id'],
                    event['session_id'],
                    event['event_type'],
                    event['user_agent'],
                    event['geo_location'],
                    event['timestamp']
                ))
                conn.commit()
                logger.info("Event successfully loaded into Redshift")
            else:
                logger.warning("Event failed validation and was not loaded.")

    except Exception as e:
        logger.error("Error during real-time data validation or loading: %s", str(e))

    finally:
        consumer.close()
        cursor.close()
        conn.close()
        logger.info("Consumer and Redshift connection closed.")

if __name__ == "__main__":
    consume_and_load_to_redshift()
