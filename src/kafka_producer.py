from confluent_kafka import Producer
import json
import time

producer = Producer({'bootstrap.servers': 'localhost:9092'})

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
