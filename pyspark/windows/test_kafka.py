"""
Test Kafka connection and check for messages in topics.
Windows-compatible version.
"""
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Windows-specific setup
os.environ["HADOOP_HOME"] = "C:\\tmp"

KAFKA_BROKER = f"{os.getenv('KAFKA_BROKER_IP', 'localhost')}:9092"
TOPICS = ["raw-traffic", "raw-weather", "raw-db"]

print("=" * 60)
print("  Kafka Connection Test (Windows)")
print("=" * 60)
print(f"  Broker: {KAFKA_BROKER}")
print("=" * 60)

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaTest_Windows") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.io.nativeio.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\nChecking Kafka topics...\n")

for topic in TOPICS:
    try:
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        count = df.count()
        
        if count > 0:
            print(f"✅ {topic}: {count} messages")
            # Show sample
            sample = df.selectExpr("CAST(value AS STRING) as message").first()
            if sample:
                msg = sample["message"][:100] + "..." if len(sample["message"]) > 100 else sample["message"]
                print(f"   Sample: {msg}")
        else:
            print(f"⚠️  {topic}: 0 messages (empty)")
            
    except Exception as e:
        print(f"❌ {topic}: Error - {str(e)[:80]}")

print("\n" + "=" * 60)
print("  Summary")
print("=" * 60)
print("""
If all topics show 0 messages:
  → s3_to_kafka.py is NOT running on EC2
  → SSH to EC2 and start it: python3 s3_to_kafka.py

If topics have messages but Silver Layer found nothing:
  → Check if messages have the expected JSON format
  → Run silver_layer.py again
""")

spark.stop()

