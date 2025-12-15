from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- 1. DEFINE SCHEMAS ---

tomtom_schema = StructType() \
    .add("meta_city", StringType()) \
    .add("meta_scraped_at", StringType()) \
    .add("grid_id", StringType()) \
    .add("lat", DoubleType()) \
    .add("lon", DoubleType()) \
    .add("current_speed", IntegerType()) \
    .add("free_flow_speed", IntegerType()) \
    .add("congestion_ratio", DoubleType()) \
    .add("confidence", DoubleType())

weather_schema = StructType() \
    .add("meta_city", StringType()) \
    .add("meta_scraped_at", StringType()) \
    .add("grid_id", StringType()) \
    .add("lat", DoubleType()) \
    .add("lon", DoubleType()) \
    .add("temp", DoubleType()) \
    .add("humidity", IntegerType()) \
    .add("wind_speed", DoubleType()) \
    .add("rain_1h_mm", DoubleType()) \
    .add("weather_main", StringType()) \
    .add("description", StringType())

db_schema = StructType() \
    .add("meta_city", StringType()) \
    .add("meta_scraped_at", StringType()) \
    .add("line", StringType()) \
    .add("delay", IntegerType()) \
    .add("direction", StringType()) \
    .add("planned_time", StringType())

# --- 2. PROCESS FUNCTION ---
def process_stream(df, batch_id, topic_name, schema, output_path):
    """Generic function to parse JSON and save to Parquet"""
    if df.count() == 0:
        return

    print(f"Writing Batch {batch_id} for {topic_name}...")
    
    # Parse JSON column 'value'
    clean_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") \
     .withColumn("ingestion_time", current_timestamp()) # Audit time

    # Write to Disk
    clean_df.write \
        .mode("append") \
        .partitionBy("meta_city") \
        .parquet(output_path)

# --- 3. MAIN PIPELINE ---
def main():
    spark = SparkSession.builder \
        .appName("HybridLakehouse_Silver") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    # Get IP securely from .env
    kafka_ip = os.getenv("KAFKA_BROKER_IP")
    if not kafka_ip:
        raise ValueError("KAFKA_BROKER_IP not set in .env file. Please create one.")
    
    # AWS Kafka Broker Connection String
    KAFKA_BROKER = f"{kafka_ip}:9092"

    # --- STREAM 1: TomTom ---
    df_tt = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "raw-traffic") \
        .option("startingOffsets", "earliest") \
        .load()
    
    query_tt = df_tt.writeStream \
        .foreachBatch(lambda df, id: process_stream(df, id, "TomTom", tomtom_schema, "./datalake/silver/tomtom")) \
        .option("checkpointLocation", "./datalake/checkpoints/tt") \
        .start()

    # --- STREAM 2: Weather ---
    df_owm = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "raw-weather") \
        .option("startingOffsets", "earliest") \
        .load()

    query_owm = df_owm.writeStream \
        .foreachBatch(lambda df, id: process_stream(df, id, "Weather", weather_schema, "./datalake/silver/weather")) \
        .option("checkpointLocation", "./datalake/checkpoints/owm") \
        .start()

    # --- STREAM 3: Deutsche Bahn ---
    df_db = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "raw-db") \
        .option("startingOffsets", "earliest") \
        .load()

    query_db = df_db.writeStream \
        .foreachBatch(lambda df, id: process_stream(df, id, "DB", db_schema, "./datalake/silver/db")) \
        .option("checkpointLocation", "./datalake/checkpoints/db") \
        .start()

    # Wait for all streams
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()