"""
Quick test to verify local Spark can connect to Kafka on EC2.
"""
from pyspark.sql import SparkSession

# Your EC2 public IP
KAFKA_BROKER = "13.60.200.41:9092"

# Create Spark session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaConnectionTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

print("=" * 60)
print("Testing Kafka Connection...")
print(f"Broker: {KAFKA_BROKER}")
print("=" * 60)

try:
    # Read from raw-traffic topic
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "raw-traffic") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    count = df.count()
    print(f"\n✅ SUCCESS! Connected to Kafka")
    print(f"   Messages in raw-traffic: {count}")
    
    # Show sample message
    if count > 0:
        print("\n📄 Sample message:")
        df.selectExpr("CAST(value AS STRING)").show(1, truncate=False)
        
except Exception as e:
    print(f"\n❌ Connection failed: {e}")
    print("\nTroubleshooting:")
    print("1. Is EC2 Security Group port 9092 open?")
    print("2. Is Kafka running? (docker ps on EC2)")
    print("3. Is your firewall blocking outbound 9092?")

finally:
    spark.stop()

