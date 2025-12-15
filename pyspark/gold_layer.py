from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, to_timestamp, floor, unix_timestamp, from_unixtime

def main():
    spark = SparkSession.builder.appName("HybridLakehouse_Gold").getOrCreate()

    # 1. READ SILVER DATA
    df_traffic = spark.read.parquet("./datalake/silver/tomtom")
    df_weather = spark.read.parquet("./datalake/silver/weather")
    df_db = spark.read.parquet("./datalake/silver/db")

    # 2. CREATE TIME BUCKETS (10-minute rounding)
    # We round timestamps to the nearest 10 mins (600 seconds) to enable joining
    def with_time_bucket(df, time_col="meta_scraped_at"):
        return df.withColumn("time_bucket", 
            from_unixtime(floor(unix_timestamp(col(time_col)) / 600) * 600).cast("timestamp")
        )

    t_bucket = with_time_bucket(df_traffic)
    w_bucket = with_time_bucket(df_weather)
    db_bucket = with_time_bucket(df_db)

    # 3. PRE-AGGREGATE DB DATA (Since it's not Grid-based, but City-based)
    # We want "City-Wide Rail Stress" per 10 mins
    db_stats = db_bucket.groupBy("meta_city", "time_bucket") \
        .agg(
            avg("delay").alias("avg_rail_delay_sec"),
            sum(col("delay") > 300).cast("int").alias("delayed_trains_count")
        )

    # 4. JOIN EVERYTHING (The Super Join)
    # Start with Traffic (Grid level) -> Join Weather (Grid level) -> Join DB (City level)
    
    gold_df = t_bucket.alias("t").join(
        w_bucket.alias("w"), 
        (col("t.meta_city") == col("w.meta_city")) & 
        (col("t.grid_id") == col("w.grid_id")) & 
        (col("t.time_bucket") == col("w.time_bucket")),
        "left"
    ).join(
        db_stats.alias("db"),
        (col("t.meta_city") == col("db.meta_city")) & 
        (col("t.time_bucket") == col("db.time_bucket")),
        "left"
    ).select(
        col("t.time_bucket"),
        col("t.meta_city"),
        col("t.grid_id"),
        
        # Traffic Features
        col("t.congestion_ratio"),
        col("t.current_speed"),
        col("t.free_flow_speed"),
        
        # Weather Features
        col("w.rain_1h_mm"),
        col("w.temp"),
        col("w.wind_speed"),
        col("w.humidity"),
        
        # Rail Features (Context)
        col("db.avg_rail_delay_sec"),
        col("db.delayed_trains_count")
    )

    # 5. SAVE FOR ML
    print("--- ML Feature Table Sample ---")
    gold_df.show(5)
    
    gold_df.write \
        .mode("overwrite") \
        .parquet("./datalake/gold/ml_features")

if __name__ == "__main__":
    main()