from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- 1. DEFINE SCHEMAS ---
# Updated to capture full Bronze data + Lat/Lon for OWM

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

# Fixed syntax: Comments removed from inside the chain
db_schema = StructType() \
    .add("meta_city", StringType()) \
    .add("meta_scraped_at", StringType()) \
    .add("line", StringType()) \
    .add("delay", IntegerType(), True) \
    .add("direction", StringType()) \
    .add("planned_time", StringType())

# --- 2. PROCESS FUNCTION ---
def process_stream(df, batch_id, topic_name, schema, output_path):
    """Generic function to parse JSON and save to Parquet"""
    count = df.count()
    print(f"\n[DEBUG] Writing Batch {batch_id} for {topic_name} - Count: {count}")
    
    if count == 0:
        return
    
    # Parse JSON column 'value'
    clean_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") \
     .withColumn("ingestion_time", current_timestamp()) # Audit time

    # DEBUG: Show what is actually being parsed
    print(f"--- SAMPLE DATA FOR {topic_name} (First 5 Rows) ---")
    clean_df.show(5, truncate=False) # <--- THIS PRINTS THE TABLE IN YOUR TERMINAL

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

    # SILENCE THE NOISE: Only show Warnings/Errors so we can see the data tables
    spark.sparkContext.setLogLevel("WARN")

    # Get IP securely from .env
    kafka_ip = os.getenv("KAFKA_BROKER_IP")
    if not kafka_ip:
        raise ValueError("KAFKA_BROKER_IP not set in .env file. Please create one.")
    
    # AWS Kafka Broker Connection String
    KAFKA_BROKER = f"{kafka_ip}:9092"
    print(f"--- DEBUG: Attempting to connect to Kafka at {KAFKA_BROKER} ---")

    # --- STREAM 1: TomTom ---
    df_tt = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "raw-traffic") \
        .option("startingOffsets", "earliest") \
        .load()
    
    query_tt = df_tt.writeStream \
        .foreachBatch(lambda df, id: process_stream(df, id, "TomTom", tomtom_schema, "./datalake/silver/tomtom")) \
        .option("checkpointLocation", "./datalake/checkpoints/tt") \
        .trigger(availableNow=True) \
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
        .trigger(availableNow=True) \
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
        .trigger(availableNow=True) \
        .start()

    # Wait for all streams to finish processing available data
    # IMPORTANT: We must wait for EACH query specifically, otherwise the script
    # exits as soon as the first one (usually Weather) finishes.
    query_tt.awaitTermination()
    query_owm.awaitTermination()
    query_db.awaitTermination()

if __name__ == "__main__":
    main()