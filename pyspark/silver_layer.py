from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp, explode
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, ArrayType
import os
from dotenv import load_dotenv

load_dotenv()

# --- SCHEMAS (NESTED - Matching Local Scraper Output) ---

# TomTom: Root object -> 'samples' array -> grid objects
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

# Weather: Root object -> 'samples' array -> grid objects
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

# DB is Flat (no samples array)
db_schema = StructType() \
    .add("meta_city", StringType()) \
    .add("meta_scraped_at", StringType()) \
    .add("line", StringType()) \
    .add("delay", IntegerType()) \
    .add("direction", StringType()) \
    .add("planned_time", StringType())

def process_nested_stream(df, batch_id, topic_name, schema, output_path):
    """Handles TomTom/Weather (One Row per City -> Explode to Grid Points)"""
    count = df.count()
    print(f"\n[STATUS] Batch {batch_id} for {topic_name}: Found {count} records.")
    
    if count == 0:
        return
    
    # 1. Parse the Raw JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # 2. Explode the 'samples' array (Flattens the list into rows)
    exploded_df = parsed_df.select(
        col("meta_city"),
        col("meta_scraped_at"),
        explode(col("samples")).alias("sample")
    ).select(
        col("meta_city"),
        col("meta_scraped_at"),
        col("sample.*") # Unpack the struct columns (lat, lon, etc.)
    ).withColumn("ingestion_time", current_timestamp())

    print(f"--- SAMPLE DATA FOR {topic_name} (Exploded) ---")
    exploded_df.show(5, truncate=False)

    exploded_df.write \
        .mode("append") \
        .partitionBy("meta_city") \
        .parquet(output_path)

def process_flat_stream(df, batch_id, topic_name, schema, output_path):
    """Handles DB (Already Flat)"""
    count = df.count()
    print(f"\n[STATUS] Batch {batch_id} for {topic_name}: Found {count} records.")
    
    if count == 0:
        return
    
    clean_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*") \
     .withColumn("ingestion_time", current_timestamp())

    print(f"--- SAMPLE DATA FOR {topic_name} ---")
    clean_df.show(5, truncate=False)

    clean_df.write \
        .mode("append") \
        .partitionBy("meta_city") \
        .parquet(output_path)

def main():
    spark = SparkSession.builder \
        .appName("HybridLakehouse_Silver") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    kafka_ip = os.getenv("KAFKA_BROKER_IP")
    if not kafka_ip:
        raise ValueError("KAFKA_BROKER_IP not set")
    
    KAFKA_BROKER = f"{kafka_ip}:9092"
    print(f"--- CONNECTING TO: {KAFKA_BROKER} ---")

    # --- 1. TOMTOM (Nested) ---
    print(">>> Starting TomTom Stream...")
    df_tt = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "raw-traffic") \
        .option("startingOffsets", "earliest") \
        .load()
    
    q_tt = df_tt.writeStream \
        .foreachBatch(lambda df, id: process_nested_stream(df, id, "TomTom", tomtom_schema, "./datalake/silver/tomtom")) \
        .option("checkpointLocation", "./datalake/checkpoints/tt") \
        .trigger(availableNow=True) \
        .start()
    
    q_tt.awaitTermination()
    print("<<< Finished TomTom.")

    # --- 2. WEATHER (Nested) ---
    print(">>> Starting Weather Stream...")
    df_owm = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "raw-weather") \
        .option("startingOffsets", "earliest") \
        .load()

    q_owm = df_owm.writeStream \
        .foreachBatch(lambda df, id: process_nested_stream(df, id, "Weather", weather_schema, "./datalake/silver/weather")) \
        .option("checkpointLocation", "./datalake/checkpoints/owm") \
        .trigger(availableNow=True) \
        .start()
        
    q_owm.awaitTermination()
    print("<<< Finished Weather.")

    # --- 3. DB (Flat) ---
    print(">>> Starting DB Stream...")
    df_db = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "raw-db") \
        .option("startingOffsets", "earliest") \
        .load()

    q_db = df_db.writeStream \
        .foreachBatch(lambda df, id: process_flat_stream(df, id, "DB", db_schema, "./datalake/silver/db")) \
        .option("checkpointLocation", "./datalake/checkpoints/db") \
        .trigger(availableNow=True) \
        .start()
        
    q_db.awaitTermination()
    print("<<< Finished DB.")

if __name__ == "__main__":
    main()