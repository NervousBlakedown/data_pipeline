# src/kafka_producer.py
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_messages():
    while True:
        # Simulate data to send to Kafka
        data = {
            'sensor_id': random.randint(1, 10),
            'temperature': round(random.uniform(20.0, 30.0), 2),
            'humidity': round(random.uniform(30.0, 70.0), 2),
            'timestamp': time.time()
        }
        producer.send('test-topic', value=data)
        print(f"Sent: {data}")
        
        # Wait before sending the next message
        time.sleep(1)

if __name__ == "__main__":
    try:
        produce_messages()
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()
