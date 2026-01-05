from pyspark.sql import SparkSession
from pyspark.sql.functions import col, minute, count, to_timestamp, floor, unix_timestamp, from_unixtime, to_date, collect_set, sort_array

def main():
    spark = SparkSession.builder \
        .appName("TimingInspector") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    print("\n🔎 RUNNING TIMING & HEARTBEAT DIAGNOSTICS...")

    # Paths to check
    sources = {
        "Traffic (TomTom)": "./datalake/silver/tomtom",
        "Weather (OWM)":    "./datalake/silver/weather",
        "Rail (DB)":        "./datalake/silver/db"
    }

    # 1. ANALYZE SCRAPE TIMING (The "Heartbeat")
    print("\n[1] SCRAPER HEARTBEAT ANALYSIS (Overall)")
    print(f"{'SOURCE':<20} | {'MINUTE':<10} | {'COUNT':<10} | {'% OF TOTAL'}")
    print("-" * 60)

    for name, path in sources.items():
        try:
            df = spark.read.parquet(path)
            total = df.count()
            
            # Extract minute from string timestamp
            df_time = df.withColumn("ts", to_timestamp(col("meta_scraped_at"))) \
                        .withColumn("raw_minute", minute(col("ts")))
            
            # Group by minute to see the schedule
            minute_dist = df_time.groupBy("raw_minute").count().orderBy("raw_minute").collect()
            
            for row in minute_dist:
                m = row['raw_minute']
                c = row['count']
                pct = (c / total) * 100
                if pct > 1.0:
                    print(f"{name:<20} | {m:<10} | {c:<10} | {pct:.1f}%")
            
            print("-" * 60)

        except Exception as e:
            print(f"⚠️  Could not read {name}: {e}")

    # 2. ROBUSTNESS VERIFICATION (Flooring Strategy)
    print("\n[2] ROBUSTNESS CHECK (Flooring to 30-min Buckets)")
    print("   -> Testing if :15 and :45 map safely to the center of buckets.")
    print("-" * 80)
    print(f"{'SOURCE':<18} | {'RAW MIN':<7} | {'SAFE BUCKET':<20} | {'COUNT'}")
    print("-" * 80)
    
    for name, path in sources.items():
        try:
            df = spark.read.parquet(path)
            
            # 1800s = 30 mins
            # Logic: floor(ts / 1800) * 1800
            # This puts :00-:29 into bucket :00
            # This puts :30-:59 into bucket :30
            df_calc = df.withColumn("ts", to_timestamp(col("meta_scraped_at"))) \
                        .withColumn("safe_bucket", from_unixtime(floor(unix_timestamp(col("ts")) / 1800) * 1800).cast("timestamp")) \
                        .withColumn("raw_minute", minute(col("ts"))) \
                        .withColumn("bucket_min", minute(col("safe_bucket")))
            
            rows = df_calc.groupBy("raw_minute", "bucket_min") \
                          .count() \
                          .orderBy("raw_minute") \
                          .collect()
            
            for r in rows:
                if r['count'] > 100: # Filter noise
                    print(f"{name:<18} | {r['raw_minute']:<7} | :{str(r['bucket_min']):<19} | {r['count']}")

        except Exception as e:
            pass

    # 3. TEMPORAL SHIFT ANALYSIS
    print("\n[3] TEMPORAL SHIFT ANALYSIS (Consistency Check)")
    print("   -> Verifying the new logic works for BOTH old (10-min) and new (30-min) schedules")
    print("-" * 100)
    
    for name, path in sources.items():
        try:
            df = spark.read.parquet(path)
            
            # Extract date and minute
            df_analysis = df.withColumn("ts", to_timestamp(col("meta_scraped_at"))) \
                            .withColumn("date", to_date(col("ts"))) \
                            .withColumn("raw_minute", minute(col("ts"))) \
                            .withColumn("bucket_min", minute(from_unixtime(floor(unix_timestamp(col("ts")) / 1800) * 1800).cast("timestamp")))

            print(f"\n   SOURCE: {name}")
            print(f"   {'DATE':<12} | {'SCRAPE MINUTES SEEN':<60}")
            
            # Collect distinct minutes per day
            daily_patterns = df_analysis.groupBy("date") \
                                        .agg(sort_array(collect_set("raw_minute")).alias("minutes")) \
                                        .orderBy("date") \
                                        .collect()
            
            for r in daily_patterns:
                mins = r['minutes']
                mins_str = ", ".join([str(m) for m in mins])
                if len(mins_str) > 60: mins_str = mins_str[:57] + "..."
                print(f"   {str(r['date']):<12} | {mins_str}")

            # ROBUSTNESS CHECK:
            # Does the logic produce any weird buckets?
            bad_mappings = df_analysis.filter(~col("bucket_min").isin([0, 30]))
            bad_count = bad_mappings.count()
            
            if bad_count == 0:
                print("   ✅ STABLE: All data maps to :00 or :30 buckets.")
            else:
                print(f"   ❌ WARNING: {bad_count} records failed to map.")

        except Exception as e:
            pass

if __name__ == "__main__":
    main()