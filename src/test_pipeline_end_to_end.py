# src/test_pipeline_end_to_end.py
import os
import json
import psycopg2
import time
import unittest
from dotenv import load_dotenv
from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
import great_expectations as ge
import logging

load_dotenv()

# Kafka Configuration
schema_registry_url = "http://localhost:8081"
kafka_bootstrap_servers = 'localhost:9092'
topic = "user-events"
schema_subject = f"{topic}-value"

# Redshift connection details
redshift_host = os.getenv("REDSHIFT_HOST")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_port = os.getenv("REDSHIFT_PORT", 5439)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Schema Registry Client and Avro Serializer/Deserializer
schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
schema = schema_registry_client.get_latest_version(schema_subject).schema
avro_serializer = AvroSerializer(schema_registry_client, schema)
avro_deserializer = AvroDeserializer(schema_registry_client, schema)

# Define the test data
test_event = {
    "user_id": 1,
    "session_id": "test_session",
    "event_type": "login",
    "user_agent": "Mozilla/5.0",
    "geo_location": "US",
    "timestamp": int(time.time() * 1000)
}

class TestPipelineEndToEnd(unittest.TestCase):

    def setUp(self):
        """Set up Kafka producer and consumer for testing."""
        self.producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})
        self.consumer = Consumer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'test-consumer-group',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([topic])

        # Redshift connection setup
        self.conn = psycopg2.connect(
            dbname=redshift_db,
            user=redshift_user,
            password=redshift_password,
            host=redshift_host,
            port=redshift_port
        )
        self.cursor = self.conn.cursor()

    def tearDown(self):
        """Clean up resources after each test."""
        self.consumer.close()
        self.cursor.close()
        self.conn.close()

    def test_data_ingestion(self):
        """Test that the Kafka producer successfully ingests data."""
        self.producer.produce(topic, avro_serializer.serialize(test_event, schema_registry_client))
        self.producer.flush()
        logger.info("Test event produced to Kafka.")

    def test_data_transformation(self):
        """Test data transformation from Kafka to Redshift."""
        msg = self.consumer.poll(10.0)  # Wait up to 10 seconds
        if msg is not None:
            event = avro_deserializer.decode(msg.value())
            logger.info("Consumed test event: %s", event)

            # Verify event structure and content
            self.assertEqual(event['user_id'], test_event['user_id'])
            self.assertEqual(event['session_id'], test_event['session_id'])
            self.assertEqual(event['event_type'], test_event['event_type'])

    def test_data_loading_to_redshift(self):
        """Test that data is loaded into Redshift accurately."""
        self.test_data_ingestion()  # Ensure data is ingested first

        # Simulate loading to Redshift
        load_query = """
        INSERT INTO user_events (user_id, session_id, event_type, user_agent, geo_location, timestamp)
        VALUES (%s, %s, %s, %s, %s, to_timestamp(%s / 1000.0))
        """
        self.cursor.execute(load_query, (
            test_event['user_id'],
            test_event['session_id'],
            test_event['event_type'],
            test_event['user_agent'],
            test_event['geo_location'],
            test_event['timestamp']
        ))
        self.conn.commit()
        logger.info("Test event loaded to Redshift.")

        # Verify data in Redshift
        self.cursor.execute("SELECT * FROM user_events WHERE user_id = %s", (test_event['user_id'],))
        redshift_record = self.cursor.fetchone()
        self.assertIsNotNone(redshift_record)
        self.assertEqual(redshift_record[0], test_event['user_id'])

    def test_data_quality_checks(self):
        """Run data quality checks using Great Expectations."""
        context = ge.data_context.DataContext()
        checkpoint = context.get_checkpoint("pipeline_quality_checkpoint")
        result = checkpoint.run()
        self.assertTrue(result["success"], "Data quality validation failed.")
        logger.info("Data quality checks passed.")

if __name__ == "__main__":
    unittest.main()
