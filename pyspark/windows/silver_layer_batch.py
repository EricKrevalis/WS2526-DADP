"""
Silver Layer - Batch Mode (Windows Compatible)
Reads all data from Kafka and writes to Silver layer without streaming checkpoints.
This avoids the NativeIO Windows error with Structured Streaming.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, explode
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, ArrayType
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Windows-specific setup
os.environ["HADOOP_HOME"] = "C:\\tmp"

# --- SCHEMAS ---
tomtom_schema = StructType() \
    .add("meta_city", StringType()) \
    .add("meta_scraped_at", StringType()) \
    .add("samples", ArrayType(StructType() \
        .add("grid_id", StringType()) \
        .add("lat", DoubleType()) \
        .add("lon", DoubleType()) \
        .add("current_speed", IntegerType()) \
        .add("free_flow_speed", IntegerType()) \
        .add("congestion_ratio", DoubleType()) \
        .add("confidence", DoubleType())
    ))

weather_schema = StructType() \
    .add("meta_city", StringType()) \
    .add("meta_scraped_at", StringType()) \
    .add("samples", ArrayType(StructType() \
        .add("grid_id", StringType()) \
        .add("lat", DoubleType()) \
        .add("lon", DoubleType()) \
        .add("temp", DoubleType()) \
        .add("humidity", IntegerType()) \
        .add("wind_speed", DoubleType()) \
        .add("rain_1h_mm", DoubleType()) \
        .add("weather_main", StringType()) \
        .add("description", StringType())
    ))

db_schema = StructType() \
    .add("meta_city", StringType()) \
    .add("meta_scraped_at", StringType()) \
    .add("line", StringType()) \
    .add("delay", IntegerType()) \
    .add("direction", StringType()) \
    .add("planned_time", StringType())


def process_nested(df, schema, output_path):
    """Process nested data (TomTom/Weather)"""
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    exploded_df = parsed_df.select(
        col("meta_city"),
        col("meta_scraped_at"),
        explode(col("samples")).alias("sample")
    ).select(
        col("meta_city"),
        col("meta_scraped_at"),
        col("sample.*")
    ).withColumn("ingestion_time", current_timestamp())
    
    count = exploded_df.count()
    if count > 0:
        exploded_df.write \
            .mode("overwrite") \
            .partitionBy("meta_city") \
            .parquet(output_path)
    return count


def process_flat(df, schema, output_path):
    """Process flat data (DB)"""
    clean_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") \
     .withColumn("ingestion_time", current_timestamp())
    
    count = clean_df.count()
    if count > 0:
        clean_df.write \
            .mode("overwrite") \
            .partitionBy("meta_city") \
            .parquet(output_path)
    return count


def main():
    print("=" * 60)
    print("  Silver Layer - BATCH MODE (Windows Compatible)")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("HybridLakehouse_Silver_Batch") \
        .config("spark.sql.shuffle.partitions", "5") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    kafka_ip = os.getenv("KAFKA_BROKER_IP")
    if not kafka_ip:
        raise ValueError("KAFKA_BROKER_IP not set in .env")
    
    KAFKA_BROKER = f"{kafka_ip}:9092"
    print(f"[{datetime.now()}] Connecting to Kafka: {KAFKA_BROKER}")
    
    # --- TomTom ---
    print(f"\n[{datetime.now()}] Reading TomTom (raw-traffic)...")
    try:
        df_tt = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", "raw-traffic") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        count = process_nested(df_tt, tomtom_schema, "./datalake/silver/tomtom")
        print(f"[{datetime.now()}] ✓ TomTom: {count} records written")
    except Exception as e:
        print(f"[{datetime.now()}] ✗ TomTom error: {e}")
    
    # --- Weather ---
    print(f"\n[{datetime.now()}] Reading Weather (raw-weather)...")
    try:
        df_weather = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", "raw-weather") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        count = process_nested(df_weather, weather_schema, "./datalake/silver/weather")
        print(f"[{datetime.now()}] ✓ Weather: {count} records written")
    except Exception as e:
        print(f"[{datetime.now()}] ✗ Weather error: {e}")
    
    # --- DB ---
    print(f"\n[{datetime.now()}] Reading DB (raw-db)...")
    try:
        df_db = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", "raw-db") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        count = process_flat(df_db, db_schema, "./datalake/silver/db")
        print(f"[{datetime.now()}] ✓ DB: {count} records written")
    except Exception as e:
        print(f"[{datetime.now()}] ✗ DB error: {e}")
    
    print(f"\n[{datetime.now()}] Silver Layer complete!")
    spark.stop()


if __name__ == "__main__":
    main()

