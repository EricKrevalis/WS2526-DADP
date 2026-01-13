"""
Silver Layer - Pure Python (No Spark)
Uses kafka-python and pyarrow to avoid Windows native library issues.
"""
import json
import os
from datetime import datetime
from kafka import KafkaConsumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = f"{os.getenv('KAFKA_BROKER_IP', 'localhost')}:9092"
OUTPUT_BASE = "./datalake/silver"


def consume_topic(topic, max_messages=10000, timeout_ms=30000):
    """Consume all messages from a Kafka topic."""
    print(f"  Reading from {topic}...")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    messages = []
    for msg in consumer:
        messages.append(msg.value)
        if len(messages) >= max_messages:
            break
    
    consumer.close()
    print(f"  Got {len(messages)} messages from {topic}")
    return messages


def process_tomtom(messages):
    """Process TomTom messages - explode samples."""
    rows = []
    for msg in messages:
        meta_city = msg.get('meta_city')
        meta_scraped_at = msg.get('meta_scraped_at')
        samples = msg.get('samples', [])
        
        for sample in samples:
            rows.append({
                'meta_city': meta_city,
                'meta_scraped_at': meta_scraped_at,
                'grid_id': sample.get('grid_id'),
                'lat': sample.get('lat'),
                'lon': sample.get('lon'),
                'current_speed': sample.get('current_speed'),
                'free_flow_speed': sample.get('free_flow_speed'),
                'congestion_ratio': sample.get('congestion_ratio'),
                'confidence': sample.get('confidence'),
                'ingestion_time': datetime.now().isoformat()
            })
    
    return pd.DataFrame(rows)


def process_weather(messages):
    """Process Weather messages - explode samples."""
    rows = []
    for msg in messages:
        meta_city = msg.get('meta_city')
        meta_scraped_at = msg.get('meta_scraped_at')
        samples = msg.get('samples', [])
        
        for sample in samples:
            rows.append({
                'meta_city': meta_city,
                'meta_scraped_at': meta_scraped_at,
                'grid_id': sample.get('grid_id'),
                'lat': sample.get('lat'),
                'lon': sample.get('lon'),
                'temp': sample.get('temp'),
                'humidity': sample.get('humidity'),
                'wind_speed': sample.get('wind_speed'),
                'rain_1h_mm': sample.get('rain_1h_mm'),
                'weather_main': sample.get('weather_main'),
                'description': sample.get('description'),
                'ingestion_time': datetime.now().isoformat()
            })
    
    return pd.DataFrame(rows)


def process_db(messages):
    """Process DB messages - flat structure."""
    rows = []
    for msg in messages:
        rows.append({
            'meta_city': msg.get('meta_city'),
            'meta_scraped_at': msg.get('meta_scraped_at'),
            'line': msg.get('line'),
            'delay': msg.get('delay'),
            'direction': msg.get('direction'),
            'planned_time': msg.get('planned_time'),
            'ingestion_time': datetime.now().isoformat()
        })
    
    return pd.DataFrame(rows)


def write_parquet(df, output_path, partition_col='meta_city'):
    """Write DataFrame to parquet, partitioned by city."""
    if df.empty:
        print(f"  No data to write to {output_path}")
        return 0
    
    os.makedirs(output_path, exist_ok=True)
    
    # Write partitioned parquet
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=output_path,
        partition_cols=[partition_col]
    )
    
    return len(df)


def main():
    print("=" * 60)
    print("  Silver Layer - Pure Python (No Spark)")
    print("=" * 60)
    print(f"Kafka: {KAFKA_BROKER}")
    print(f"Output: {OUTPUT_BASE}")
    print()
    
    # TomTom
    print("[TomTom]")
    try:
        messages = consume_topic('raw-traffic')
        df = process_tomtom(messages)
        count = write_parquet(df, f"{OUTPUT_BASE}/tomtom")
        print(f"  ✓ Wrote {count} records to {OUTPUT_BASE}/tomtom")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # Weather
    print("\n[Weather]")
    try:
        messages = consume_topic('raw-weather')
        df = process_weather(messages)
        count = write_parquet(df, f"{OUTPUT_BASE}/weather")
        print(f"  ✓ Wrote {count} records to {OUTPUT_BASE}/weather")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    # DB
    print("\n[DB]")
    try:
        messages = consume_topic('raw-db')
        df = process_db(messages)
        count = write_parquet(df, f"{OUTPUT_BASE}/db")
        print(f"  ✓ Wrote {count} records to {OUTPUT_BASE}/db")
    except Exception as e:
        print(f"  ✗ Error: {e}")
    
    print("\n" + "=" * 60)
    print("  Silver Layer Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()

