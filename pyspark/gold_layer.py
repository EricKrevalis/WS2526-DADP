from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, to_timestamp, floor, 
    unix_timestamp, from_unixtime, hour, dayofweek, max as spark_max, first
)

def main():
    spark = SparkSession.builder \
        .appName("HybridLakehouse_Gold") \
        .config("spark.sql.shuffle.partitions", "5") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. READ SILVER DATA
    try:
        df_traffic = spark.read.parquet("./datalake/silver/tomtom")
        df_weather = spark.read.parquet("./datalake/silver/weather")
        df_db = spark.read.parquet("./datalake/silver/db")
    except Exception as e:
        print(f"Error reading Silver Layer: {e}")
        return

    # 2. CREATE TIME BUCKETS & PIVOT HELPER
    def clean_and_bucket(df, time_col="meta_scraped_at"):
        # FIX: Explicitly cast the ISO string to Timestamp first
        # This handles the "T" and microseconds correctly
        df_clean = df.withColumn("ts", col(time_col).cast("timestamp"))
        
        return df_clean.withColumn("time_bucket", 
            from_unixtime(floor(unix_timestamp(col("ts")) / 600) * 600).cast("timestamp")
        ).withColumn("hour_of_day", hour(col("time_bucket"))) \
         .withColumn("day_of_week", dayofweek(col("time_bucket")))

    # 3. PREPARE TRAFFIC (PIVOT)
    # Goal: One row per City/Time, columns like 'grid_0_0_congestion'
    t_bucket = clean_and_bucket(df_traffic)
    
    t_pivoted = t_bucket.groupBy("meta_city", "time_bucket", "hour_of_day", "day_of_week") \
        .pivot("grid_id") \
        .agg(
            first("congestion_ratio").alias("congestion"),
            first("current_speed").alias("speed")
        )
        
    # 4. PREPARE WEATHER (PIVOT)
    # Goal: One row per City/Time, columns like 'grid_0_0_rain'
    w_bucket = clean_and_bucket(df_weather)
    
    w_pivoted = w_bucket.groupBy("meta_city", "time_bucket") \
        .pivot("grid_id") \
        .agg(
            first("rain_1h_mm").alias("rain"),
            first("temp").alias("temp")
        )

    # 5. PREPARE RAIL (AGGREGATE)
    # Goal: One row per City/Time (City-wide stress metrics)
    db_bucket = clean_and_bucket(df_db)
    
    db_stats = db_bucket.groupBy("meta_city", "time_bucket") \
        .agg(
            avg("delay").alias("rail_avg_delay"),
            (sum((col("delay") > 300).cast("int")) / count("*")).alias("rail_stress_index")
        )

    # 6. JOIN EVERYTHING (Wide + Wide + Wide)
    # Traffic is the "Left" base because it dictates the structure
    gold_df = t_pivoted.alias("t").join(
        w_pivoted.alias("w"),
        (col("t.meta_city") == col("w.meta_city")) & 
        (col("t.time_bucket") == col("w.time_bucket")),
        "left"
    ).join(
        db_stats.alias("db"),
        (col("t.meta_city") == col("db.meta_city")) & 
        (col("t.time_bucket") == col("db.time_bucket")),
        "left"
    ).drop(col("w.meta_city")).drop(col("w.time_bucket")) \
     .drop(col("db.meta_city")).drop(col("db.time_bucket"))

    # 7. SAVE
    print("--- Wide ML Table Sample ---")
    gold_df.show(5)
    
    print("Writing Gold Layer...")
    gold_df.write \
        .mode("overwrite") \
        .parquet("./datalake/gold/ml_features_wide")
    print("Done.")

if __name__ == "__main__":
    main()