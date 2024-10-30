# src/data_generation.py
import boto3
import fastavro
import os
from datetime import datetime, timedelta
import random

# Define Avro schema
schema = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "user_id", "type": "int"},
        {"name": "event_type", "type": "string"},
        {"name": "feature_used", "type": "string"},
        {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
    ]
}

# AWS S3 setup
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
bucket_name = "s3-bucket-name"

def generate_avro_data_to_s3(num_rows, chunk_size=10_000_000):
    event_types = ['login', 'page_view', 'feature_use', 'error']
    features = ['search', 'recommendation', 'playback', 'chat']

    for i in range(0, num_rows, chunk_size):
        # Generate chunk of data
        records = [
            {
                "user_id": random.randint(1, 1_000_000),
                "event_type": random.choice(event_types),
                "feature_used": random.choice(features),
                "timestamp": int((datetime.now() - timedelta(seconds=random.randint(0, 3600))).timestamp() * 1000)
            }
            for _ in range(chunk_size)
        ]

        # Write data to Avro format
        avro_filename = f"synthetic_events_chunk_{i // chunk_size}.avro"
        with open(avro_filename, 'wb') as out_file:
            fastavro.writer(out_file, schema, records)
        
        # Upload to S3
        s3_client.upload_file(avro_filename, bucket_name, f"data/{avro_filename}")
        print(f"Uploaded {chunk_size} rows to S3 as {avro_filename}")

        # Clean up local file
        os.remove(avro_filename)

# Generate 1 billion rows in chunks and upload to S3
generate_avro_data_to_s3(num_rows=1_000_000_000)
