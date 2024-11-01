from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import time
import random

# Schema Registry setup
schema_registry_url = "http://localhost:8081"
schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

# Kafka topic and schema
topic = "user-events"
schema_subject = f"{topic}-value"

# Load Avro schema from file
with open('config/event_schema.avsc', 'r') as file:
    schema_str = file.read()
schema = schema_registry_client.register_schema(schema_subject, schema_str)
avro_serializer = AvroSerializer(schema_registry_client, schema, schema_str)

# Kafka Producer setup
producer = Producer({'bootstrap.servers': 'localhost:9092'})

def generate_event():
    """Simulate a user event."""
    event_types = ['user_login', 'product_view', 'add_to_cart', 'purchase_complete']
    user_agents = ['Chrome iOS', 'Firefox Android', 'Safari MacOS']
    geo_locations = ['US', 'CA', 'EU', 'IN']
    
    event = {
        "user_id": random.randint(1, 1000),
        "session_id": f"session_{random.randint(1, 100000)}",
        "event_type": random.choice(event_types),
        "user_agent": random.choice(user_agents),
        "geo_location": random.choice(geo_locations),
        "timestamp": int(time.time() * 1000)
    }
    return event

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_events():
    """Send events to Kafka."""
    for _ in range(100):  # Number of events
        event = generate_event()
        try:
            avro_event = avro_serializer.encode(event)
            producer.produce(topic, value=avro_event, callback=delivery_report)
            print(f"Produced event: {event}")
            producer.poll(0)
            time.sleep(random.uniform(0.5, 2.5))  # Simulate user traffic
        except Exception as e:
            print(f"Error producing event: {e}")

    producer.flush()

if __name__ == "__main__":
    produce_events()
