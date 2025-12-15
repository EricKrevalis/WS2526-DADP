#!/usr/bin/env python3
"""
S3 to Kafka Bridge
Polls S3 for new files and pushes them to Kafka topics in real-time.
"""

import boto3
import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuration
S3_BUCKET = "youngleebigdatabucket"
S3_PREFIX = "ingest-data"
S3_REGION = "eu-north-1"
KAFKA_BROKER = "localhost:9092"
POLL_INTERVAL = 30  # seconds between S3 checks

# Map S3 folders to Kafka topics
FOLDER_TO_TOPIC = {
    "TomTom": "raw-traffic",
    "OWM": "raw-weather",
    "DB": "raw-db"
}

# Track processed files
PROCESSED_FILES_PATH = "processed_files.json"


def load_processed_files():
    """Load set of already processed files."""
    if os.path.exists(PROCESSED_FILES_PATH):
        with open(PROCESSED_FILES_PATH, 'r') as f:
            return set(json.load(f))
    return set()


def save_processed_files(processed: set):
    """Save processed files to disk."""
    with open(PROCESSED_FILES_PATH, 'w') as f:
        json.dump(list(processed), f)


def get_kafka_producer():
    """Create Kafka producer with retry logic."""
    for attempt in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"[{datetime.now()}] Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except KafkaError as e:
            print(f"[{datetime.now()}] Kafka connection failed (attempt {attempt + 1}): {e}")
            time.sleep(5)
    raise Exception("Could not connect to Kafka after 5 attempts")


def process_s3_file(s3_client, bucket: str, key: str, producer: KafkaProducer, topic: str):
    """Download S3 file and send each line to Kafka."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        line_count = 0
        for line in content.strip().split('\n'):
            if line:
                try:
                    record = json.loads(line)
                    # Add metadata
                    record['_source_file'] = key
                    record['_ingested_at'] = datetime.now().isoformat()
                    
                    producer.send(topic, value=record)
                    line_count += 1
                except json.JSONDecodeError:
                    # Send raw line if not valid JSON
                    producer.send(topic, value={'raw': line, '_source_file': key})
                    line_count += 1
        
        producer.flush()
        print(f"[{datetime.now()}] Sent {line_count} records from {key} to topic {topic}")
        return True
        
    except Exception as e:
        print(f"[{datetime.now()}] Error processing {key}: {e}")
        return False


def poll_s3(s3_client, producer: KafkaProducer, processed_files: set):
    """Poll S3 for new files and process them."""
    new_files_found = 0
    
    for folder, topic in FOLDER_TO_TOPIC.items():
        prefix = f"{S3_PREFIX}/{folder}/"
        
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    
                    # Skip if already processed or if it's a folder
                    if key in processed_files or key.endswith('/'):
                        continue
                    
                    # Only process .jsonl files
                    if not key.endswith('.jsonl'):
                        continue
                    
                    print(f"[{datetime.now()}] New file found: {key}")
                    
                    if process_s3_file(s3_client, S3_BUCKET, key, producer, topic):
                        processed_files.add(key)
                        new_files_found += 1
                        
        except Exception as e:
            print(f"[{datetime.now()}] Error listing {folder}: {e}")
    
    return new_files_found


def main():
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║              S3 to Kafka Bridge - Starting                   ║
╠══════════════════════════════════════════════════════════════╣
║  Bucket: {S3_BUCKET:<50} ║
║  Region: {S3_REGION:<50} ║
║  Kafka:  {KAFKA_BROKER:<50} ║
║  Poll:   Every {POLL_INTERVAL} seconds{' ' * 39}║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Initialize clients
    s3_client = boto3.client('s3', region_name=S3_REGION)
    producer = get_kafka_producer()
    processed_files = load_processed_files()
    
    print(f"[{datetime.now()}] Loaded {len(processed_files)} previously processed files")
    print(f"[{datetime.now()}] Watching folders: {list(FOLDER_TO_TOPIC.keys())}")
    print(f"[{datetime.now()}] Mapping to topics: {list(FOLDER_TO_TOPIC.values())}")
    print("-" * 60)
    
    try:
        while True:
            new_files = poll_s3(s3_client, producer, processed_files)
            
            if new_files > 0:
                save_processed_files(processed_files)
                print(f"[{datetime.now()}] Processed {new_files} new file(s)")
            
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        print(f"\n[{datetime.now()}] Shutting down...")
        save_processed_files(processed_files)
        producer.close()
        print(f"[{datetime.now()}] Saved state. Goodbye!")


if __name__ == "__main__":
    main()

