from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, to_timestamp, floor, 
    unix_timestamp, from_unixtime, hour, dayofweek, max as spark_max, first, lit, when
)
import os

def main():
    print("--- STARTING GOLD LAYER SCRIPT (PENDLER PILOT) ---", flush=True)
    
    # Windows-specific: Disable native I/O to avoid NativeIO$Windows errors
    import os
    os.environ["HADOOP_HOME"] = "C:\\tmp"
    
    spark = SparkSession.builder \
        .appName("PendlerPilot_Gold") \
        .config("spark.sql.shuffle.partitions", "5") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.io.nativeio.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    GOLD_PATH = "./datalake/gold/pendler_pilot"
    
    # 1. READ SILVER DATA
    print("Reading Silver Layer files...", flush=True)
    try:
        df_traffic = spark.read.parquet("./datalake/silver/tomtom")
        df_weather = spark.read.parquet("./datalake/silver/weather")
        df_db = spark.read.parquet("./datalake/silver/db")
    except Exception as e:
        print(f"❌ Error reading Silver Layer: {e}")
        return

    # 2. HELPER: CLEAN & BUCKET
    def clean_and_bucket(df, time_col="meta_scraped_at"):
        df_clean = df.withColumn("ts", col(time_col).cast("timestamp"))
        
        # 30-min Flooring Logic
        return df_clean.withColumn("time_bucket", 
            from_unixtime(floor(unix_timestamp(col("ts")) / 1800) * 1800).cast("timestamp")
        )

    print("\n--- STEP 1: PREPARING SOURCES ---", flush=True)
    t_bucket = clean_and_bucket(df_traffic)
    w_bucket = clean_and_bucket(df_weather)
    db_bucket = clean_and_bucket(df_db)

    # ---------------------------------------------------------
    # STEP 2A: INCREMENTAL FILTER (The Floor)
    # ---------------------------------------------------------
    cutoff_timestamp = None
    try:
        if os.path.exists(GOLD_PATH):
            existing_gold = spark.read.parquet(GOLD_PATH)
            max_row = existing_gold.agg(spark_max("time_bucket")).collect()[0][0]
            if max_row:
                cutoff_timestamp = max_row
                print(f"🌊 High Watermark found: {cutoff_timestamp}", flush=True)
    except Exception:
        print(f"⚠️  Could not read existing Gold Layer. Assuming Full Load.", flush=True)

    if cutoff_timestamp:
        t_bucket = t_bucket.filter(col("time_bucket") > cutoff_timestamp)
        w_bucket = w_bucket.filter(col("time_bucket") > cutoff_timestamp)
        db_bucket = db_bucket.filter(col("time_bucket") > cutoff_timestamp)

    if t_bucket.isEmpty() and w_bucket.isEmpty() and db_bucket.isEmpty():
        print("✅ No new data found > High Watermark. Exiting.")
        return

    # ---------------------------------------------------------
    # STEP 2B: SAFETY CEILING (The Ceiling)
    # ---------------------------------------------------------
    max_t = t_bucket.agg(spark_max("time_bucket")).collect()[0][0]
    max_w = w_bucket.agg(spark_max("time_bucket")).collect()[0][0]
    max_db = db_bucket.agg(spark_max("time_bucket")).collect()[0][0]
    
    candidates = [ts for ts in [max_t, max_w, max_db] if ts is not None]
    
    if not candidates:
        print("✅ No valid timestamps found in new data.")
        return

    latest_seen_bucket = max(candidates)
    
    print(f"🛑 Leading Edge (Latest Bucket Seen): {latest_seen_bucket}")
    print("   -> Filtering out this bucket to ensure we only write COMPLETE history.")
    
    t_bucket = t_bucket.filter(col("time_bucket") < latest_seen_bucket)
    w_bucket = w_bucket.filter(col("time_bucket") < latest_seen_bucket)
    db_bucket = db_bucket.filter(col("time_bucket") < latest_seen_bucket)

    if t_bucket.isEmpty() and w_bucket.isEmpty() and db_bucket.isEmpty():
        print("⏳ New data exists, but it matches the Leading Edge. Waiting.")
        return

    # 3. PIVOT & AGGREGATE
    print("\n--- STEP 2: AGGREGATING & PIVOTING ---", flush=True)
    
    t_pivoted = t_bucket.groupBy("meta_city", "time_bucket") \
        .pivot("grid_id") \
        .agg(avg("congestion_ratio").alias("congestion"), avg("current_speed").alias("speed"))
        
    w_pivoted = w_bucket.groupBy("meta_city", "time_bucket") \
        .pivot("grid_id") \
        .agg(avg("rain_1h_mm").alias("rain"), avg("temp").alias("temp"))

    # --- SPLIT DB LOGIC ---
    # Condition: If line contains 'Bus' or 'STR' (Tram), it's Road. Else Rail.
    is_road_transit = col("line").contains("Bus") | col("line").contains("STR")
    is_rail_transit = ~is_road_transit

    db_stats = db_bucket.groupBy("meta_city", "time_bucket") \
        .agg(
            # METRIC 1: RAIL (Trains/Subways) - Independent of traffic
            avg(when(is_rail_transit, col("delay"))).alias("rail_avg_delay"),
            (sum(when(is_rail_transit & (col("delay") > 300), 1).otherwise(0)) / count(when(is_rail_transit, 1))).alias("rail_stress_index"),
            
            # METRIC 2: ROAD (Buses/Trams) - Correlated with traffic
            avg(when(is_road_transit, col("delay"))).alias("bus_avg_delay"),
            (sum(when(is_road_transit & (col("delay") > 300), 1).otherwise(0)) / count(when(is_road_transit, 1))).alias("bus_stress_index")
        )

    # 4. MASTER KEY & JOIN
    print("\n--- STEP 3: JOINING ---", flush=True)
    keys_t = t_pivoted.select("meta_city", "time_bucket")
    keys_w = w_pivoted.select("meta_city", "time_bucket")
    keys_db = db_stats.select("meta_city", "time_bucket")
    
    master_keys = keys_t.union(keys_w).union(keys_db).distinct()

    gold_df = master_keys.alias("base") \
        .join(t_pivoted.alias("t"), ["meta_city", "time_bucket"], "left") \
        .join(w_pivoted.alias("w"), ["meta_city", "time_bucket"], "left") \
        .join(db_stats.alias("db"), ["meta_city", "time_bucket"], "left") \
        .withColumn("hour_of_day", hour(col("time_bucket"))) \
        .withColumn("day_of_week", dayofweek(col("time_bucket"))) \
        .drop(col("w.meta_city")).drop(col("w.time_bucket")) \
        .drop(col("db.meta_city")).drop(col("db.time_bucket"))

    # 5. APPEND
    print("\n--- STEP 4: APPENDING TO DISK ---", flush=True)
    
    gold_df.write \
        .mode("append") \
        .parquet(GOLD_PATH)
        
    print(f"✅ Done. Appended new safe data to {GOLD_PATH}.", flush=True)

if __name__ == "__main__":
    main()

