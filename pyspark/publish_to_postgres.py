from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pyspark.sql.functions import col

# Explicitly load .env from project root
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
dotenv_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path)

def main():
    # 1. Fetch Credentials
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_db = os.getenv("POSTGRES_DB", "traffic_lake")
    
    if not pg_user or not pg_password:
        print("❌ Error: Credentials missing in .env")
        return

    # 2. Init Spark
    spark = SparkSession.builder \
        .appName("PublishToPostgres") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    jdbc_url = f"jdbc:postgresql://localhost:5432/{pg_db}"
    db_properties = {
        "user": pg_user, 
        "password": pg_password, 
        "driver": "org.postgresql.Driver"
    }
    
    # NEW NAME: PendlerPilot
    gold_path = "./datalake/gold/pendler_pilot"
    table_name = "pendler_pilot"

    print(f"Reading Gold Layer from {gold_path}...")
    try:
        df_features = spark.read.parquet(gold_path)
    except Exception as e:
        print(f"Error reading Parquet: {e}")
        return

    # Basic check
    row_count = df_features.count()
    print(f"Loaded {row_count} rows from Gold Layer.")

    # 3. DETERMINE HIGH WATERMARK (QUERY POSTGRES)
    print(f"Checking Postgres table '{table_name}'...")
    max_pg_time = None
    try:
        # Check what we already have in the DB so we don't duplicate rows
        max_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"(SELECT MAX(time_bucket) as max_ts FROM {table_name}) as tmp",
            properties=db_properties
        )
        max_pg_time = max_df.collect()[0]["max_ts"]
    except Exception as e:
        print(f"⚠️  Table '{table_name}' likely doesn't exist yet (First Run).")
        print("   -> Proceeding with FULL LOAD.")

    # 4. FILTER NEW DATA
    df_to_write = df_features
    if max_pg_time:
        print(f"🌊 High Watermark in DB: {max_pg_time}")
        print("   -> Filtering for newer rows...")
        # Only take rows that are STRICTLY NEWER than what is in Postgres
        df_to_write = df_features.filter(col("time_bucket") > max_pg_time)
        
        count = df_to_write.count()
        if count == 0:
            print("✅ Database is already up to date. Nothing to write.")
            return
        print(f"   -> Found {count} new rows to append.")
    else:
        print("   -> Writing ALL rows.")

    # 5. APPEND
    print(f"Appending to PostgreSQL table '{table_name}'...")
    try:
        df_to_write.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="append",
            properties=db_properties
        )
        print("✅ Success! Data published.")
        
    except Exception as e:
        print(f"❌ Error writing to PostgreSQL: {e}")

if __name__ == "__main__":
    main()