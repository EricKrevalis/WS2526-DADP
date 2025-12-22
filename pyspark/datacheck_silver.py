from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, floor, unix_timestamp, from_unixtime, min, max, count, count_distinct

def main():
    spark = SparkSession.builder \
        .appName("DeepDiagnostic") \
        .config("spark.sql.shuffle.partitions", "5") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    print("\n🔎 RUNNING CROSS-SOURCE DIAGNOSTICS (RAW VS PARSED)...")

    # Paths to check
    sources = {
        "Traffic (TomTom)": "./datalake/silver/tomtom",
        "Weather (OWM)":    "./datalake/silver/weather",
        "Rail (DB)":        "./datalake/silver/db"
    }

    results = []
    
    # Global Time Range trackers
    global_min = None
    global_max = None

    for name, path in sources.items():
        print(f"\n[Analysing] {name}...")
        try:
            df = spark.read.parquet(path)
            
            # 1. Determine Time Column
            time_col = "meta_scraped_at" # Silver layer standard
            
            # 2. Normalize Timestamp & Calculate 10-min Buckets
            # This replicates the Gold Layer logic exactly
            df_calc = df.withColumn("ts", to_timestamp(col(time_col))) \
                        .withColumn("bucket", from_unixtime(floor(unix_timestamp(col("ts")) / 600) * 600).cast("timestamp"))
            
            # 3. Calculate Stats (RAW vs PARSED)
            print(f"   ... Computing statistics ...")
            
            # We calculate Raw Max String AND Parsed Max Timestamp to catch logic errors
            stats = df_calc.agg(
                count("*").alias("total_rows"),
                max(time_col).alias("raw_max_str"),
                min(time_col).alias("raw_min_str"),
                count_distinct("bucket").alias("buckets"),
                min("bucket").alias("start"),
                max("bucket").alias("end"),
                count(col("bucket")).alias("valid_rows")
            ).collect()[0]
            
            # Collect unique buckets for overlap analysis (Small data volume allows collect)
            unique_buckets = [r['bucket'] for r in df_calc.select("bucket").distinct().collect() if r['bucket'] is not None]
            bucket_set = set(unique_buckets)

            bucket_count = stats["buckets"]
            start = stats["start"]
            end = stats["end"]
            
            # Logic Check vars
            raw_max_str = stats["raw_max_str"]
            raw_min_str = stats["raw_min_str"]
            parse_failures = stats["total_rows"] - stats["valid_rows"]
            
            print(f"   ... Found {bucket_count} buckets.")
            if parse_failures > 0:
                print(f"   ⚠️  WARNING: {parse_failures} rows failed to parse timestamp!")
                print(f"       Raw range: {raw_min_str} -> {raw_max_str}")
            
            # Update globals for comparison
            if start and (global_min is None or start < global_min): global_min = start
            if end and (global_max is None or end > global_max): global_max = end

            results.append({
                "source": name,
                "buckets": bucket_count,
                "start": start,
                "end": end,
                "raw_end": raw_max_str,
                "parse_errors": parse_failures,
                "bucket_set": bucket_set
            })

        except Exception as e:
            print(f"⚠️  Could not read {name}: {e}")
            results.append({
                "source": name, 
                "buckets": 0, 
                "start": "N/A", 
                "end": "N/A", 
                "raw_end": "N/A", 
                "parse_errors": 0,
                "bucket_set": set()
            })

    # --- REPORTING ---
    print("\n" + "="*100)
    print(f"{'SOURCE NAME':<20} | {'BUCKETS':<8} | {'MAX BUCKET (Logic)':<20} | {'MAX RAW STRING (Data)':<25} | {'PARSE ERRORS'}")
    print("-" * 100)

    for r in results:
        print(f"{r['source']:<20} | {r['buckets']:<8} | {str(r['end']):<20} | {str(r['raw_end']):<25} | {r['parse_errors']}")

    print("="*100)

    # --- CROSS-SOURCE OVERLAP ANALYSIS ---
    print("\n" + "="*100)
    print("⚔️  CROSS-SOURCE OVERLAP ANALYSIS")
    print("(Checking if timestamps align between sources)")
    print("="*100)

    if len(results) > 1:
        for i in range(len(results)):
            for j in range(i + 1, len(results)):
                src1 = results[i]
                src2 = results[j]
                
                set1 = src1['bucket_set']
                set2 = src2['bucket_set']
                
                common = len(set1.intersection(set2))
                only_in_1 = len(set1 - set2)
                only_in_2 = len(set2 - set1)
                
                print(f"\nComparing {src1['source']} vs {src2['source']}:")
                print(f"   🔗 Matching Buckets: {common}")
                print(f"   ⬅️  Only in {src1['source']}: {only_in_1}")
                print(f"   ➡️  Only in {src2['source']}: {only_in_2}")
                
                if common == 0 and (len(set1) > 0 and len(set2) > 0):
                    print("   ⚠️  CRITICAL: No temporal overlap! Timezones or Formats might be completely mismatched.")

    # --- THEORETICAL MAX CHECK ---
    if global_min and global_max:
        total_seconds = (global_max - global_min).total_seconds()
        theoretical_buckets = int(total_seconds / 600) + 1
        
        print(f"\n\n⏱  Global Timeline: {global_min} to {global_max}")
        print(f"⏱  Theoretical Max Buckets (10-min slots): {theoretical_buckets}")
        
        print("\n--- GAPS DETECTED (Relative to Global Timeline) ---")
        for r in results:
            if r['buckets'] > 0:
                missing = theoretical_buckets - r['buckets']
                pct = (missing / theoretical_buckets) * 100
                status = "✅" if pct < 5 else "❌"
                print(f"{status} {r['source']}: Missing {missing} buckets ({pct:.1f}%)")

if __name__ == "__main__":
    main()