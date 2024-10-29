# src/kafka_consumer.py
from kafka import KafkaConsumer
import json

# Set up the Kafka consumer
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Start reading from the beginning of the topic
    enable_auto_commit=True,
    group_id='test-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def consume_messages():
    print("Starting consumer...")
    for message in consumer:
        data = message.value
        print(f"Received: {data}")

if __name__ == "__main__":
    try:
        consume_messages()
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
