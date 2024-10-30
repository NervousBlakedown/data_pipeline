from confluent_kafka import Producer
import json
import time
import sqlite3
import fastavro
from kafka import KafkaProducer
import requests
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import random

# Schema Registry Setup
schema_registry_url = "http://localhost:8081"
schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

# Define the Kafka topic and Schema Registry subject
topic = "user-events"
schema_subject = f"{topic}-value"

# Load Avro schema
with open('config/event_schema.avsc', 'r') as file:
    schema_str = file.read()
schema = schema_registry_client.register_schema(schema_subject, schema_str)

producer = Producer({'bootstrap.servers': 'localhost:9092'})

events = [
    {'event_type': 'login', 'user_id': 123, 'location': 'US', 'timestamp': time.time()},
    {'event_type': 'page_view', 'user_id': 123, 'page': 'home', 'timestamp': time.time()},
    {'event_type': 'feature_use', 'user_id': 123, 'feature': 'chat', 'timestamp': time.time()},
    {'event_type': 'error', 'user_id': 123, 'error_code': 500, 'message': 'Server Error', 'timestamp': time.time()}
]

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

for i in range(10):
    data = json.dumps({'sensor_id': i, 'value': i * 10})
    producer.produce('test-topic', value=data, callback=delivery_report)
    producer.poll(0)

producer.flush()
