from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os
from dotenv import load_dotenv

load_dotenv()
broker = f"{os.getenv('KAFKA_BROKER_IP', 'localhost')}:9092"
print(f"Testing Kafka at: {broker}")

try:
    consumer = KafkaConsumer(
        bootstrap_servers=broker,
        consumer_timeout_ms=10000,
        auto_offset_reset='earliest'
    )
    topics = consumer.topics()
    print(f"Available topics: {topics}")
    
    # Check message counts
    for topic in ['raw-traffic', 'raw-weather', 'raw-db']:
        if topic in topics:
            consumer.subscribe([topic])
            count = 0
            for msg in consumer:
                count += 1
                if count >= 5:  # Just check first 5
                    break
            print(f"  {topic}: found messages (at least {count})")
            consumer.unsubscribe()
        else:
            print(f"  {topic}: NOT FOUND")
    
    consumer.close()
except Exception as e:
    print(f"Error: {e}")

