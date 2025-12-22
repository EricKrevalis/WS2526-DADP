from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, to_timestamp, floor, 
    unix_timestamp, from_unixtime, hour, dayofweek, max as spark_max, first, coalesce, lit
)
import sys

def main():
    print("--- STARTING GOLD LAYER SCRIPT ---", flush=True)
    
    spark = SparkSession.builder \
        .appName("HybridLakehouse_Gold") \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. READ SILVER DATA
    print("Reading Silver Layer files...", flush=True)
    try:
        # We read specific paths. If a path is empty/missing, Spark might throw an error,
        # so ensure Silver Layer has run at least once.
        df_traffic = spark.read.parquet("./datalake/silver/tomtom")
        df_weather = spark.read.parquet("./datalake/silver/weather")
        df_db = spark.read.parquet("./datalake/silver/db")
    except Exception as e:
        print(f"❌ Error reading Silver Layer: {e}")
        return

    # 2. HELPER: CLEAN & BUCKET TIMESTAMPS
    def clean_and_bucket(df, time_col="meta_scraped_at", source_name="Unknown"):
        # Explicitly cast string to timestamp to handle ISO format from JSON
        df_clean = df.withColumn("ts", col(time_col).cast("timestamp"))
        
        # Round to 10 minutes (600 seconds)
        df_bucketed = df_clean.withColumn("time_bucket", 
            from_unixtime(floor(unix_timestamp(col("ts")) / 600) * 600).cast("timestamp")
        )
        
        # DEBUG: Count unique buckets for this source to ensure parsing worked
        bucket_count = df_bucketed.select("time_bucket").distinct().count()
        print(f"[{source_name}] Valid Time Buckets: {bucket_count}", flush=True)
        
        return df_bucketed

    print("\n--- STEP 1: PREPARING SOURCES ---", flush=True)
    t_bucket = clean_and_bucket(df_traffic, source_name="Traffic")
    w_bucket = clean_and_bucket(df_weather, source_name="Weather")
    db_bucket = clean_and_bucket(df_db, source_name="DB")

    # 3. PIVOT & AGGREGATE (Prepare the pieces)
    print("\n--- STEP 2: AGGREGATING & PIVOTING ---", flush=True)
    
    # Traffic (Wide) - One row per City/Time, columns for each grid point
    t_pivoted = t_bucket.groupBy("meta_city", "time_bucket") \
        .pivot("grid_id") \
        .agg(
            first("congestion_ratio").alias("congestion"),
            first("current_speed").alias("speed")
        )
        
    # Weather (Wide) - One row per City/Time, columns for each grid point
    w_pivoted = w_bucket.groupBy("meta_city", "time_bucket") \
        .pivot("grid_id") \
        .agg(
            first("rain_1h_mm").alias("rain"),
            first("temp").alias("temp")
        )

    # Rail (Aggregated) - One row per City/Time (City-wide metrics)
    db_stats = db_bucket.groupBy("meta_city", "time_bucket") \
        .agg(
            avg("delay").alias("rail_avg_delay"),
            (sum((col("delay") > 300).cast("int")) / count("*")).alias("rail_stress_index")
        )

    # 4. CREATE THE MASTER KEY (The Fix for Missing Data)
    # We combine ALL timestamps and cities from ALL sources so we don't lose rows
    # if one source (like TomTom) has a gap.
    print("\n--- STEP 3: CREATING MASTER TIME INDEX ---", flush=True)
    keys_t = t_pivoted.select("meta_city", "time_bucket")
    keys_w = w_pivoted.select("meta_city", "time_bucket")
    keys_db = db_stats.select("meta_city", "time_bucket")
    
    # Union all keys and get unique list
    master_keys = keys_t.union(keys_w).union(keys_db).distinct()
    
    print(f"Master Index Size: {master_keys.count()} unique (City, Time) pairs.", flush=True)

    # 5. SUPER JOIN (Left Join to Master Key)
    print("\n--- STEP 4: PERFORMING SUPER JOIN ---", flush=True)
    
    gold_df = master_keys.alias("base") \
        .join(t_pivoted.alias("t"), ["meta_city", "time_bucket"], "left") \
        .join(w_pivoted.alias("w"), ["meta_city", "time_bucket"], "left") \
        .join(db_stats.alias("db"), ["meta_city", "time_bucket"], "left") \
        .withColumn("hour_of_day", hour(col("time_bucket"))) \
        .withColumn("day_of_week", dayofweek(col("time_bucket")))

    # 6. SAVE
    print("\n--- STEP 5: SAVING TO DISK ---", flush=True)
    
    # Show a sample to verify columns are populated
    # gold_df.orderBy(col("time_bucket").desc()).show(5)
    
    gold_df.write \
        .mode("overwrite") \
        .parquet("./datalake/gold/ml_features_wide")
        
    print("✅ Done. Gold Layer Written.", flush=True)

if __name__ == "__main__":
    main()