from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Load environment variables (if any needed for DB credentials)
load_dotenv()

def main():
    # Initialize Spark with Postgres Driver
    # NOTE: You must run this with --packages org.postgresql:postgresql:42.6.0
    spark = SparkSession.builder \
        .appName("PublishToPostgres") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    # Database Config - Update these if your local setup differs!
    jdbc_url = "jdbc:postgresql://localhost:5432/traffic_lake"
    db_properties = {
        "user": "traffic_user",
        "password": "traffic_password",
        "driver": "org.postgresql.Driver"
    }

    # Path to the WIDE Gold table
    gold_path = "./datalake/gold/ml_features_wide"

    print(f"Reading Gold Layer from {gold_path}...")
    try:
        df_features = spark.read.parquet(gold_path)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return

    # Basic check
    row_count = df_features.count()
    print(f"Loaded {row_count} rows. Schema:")
    df_features.printSchema()

    print("Writing to PostgreSQL table 'ml_features_wide'...")
    
    try:
        # mode("overwrite") will DROP the table if it exists and create a new one
        # based on the DataFrame schema. This is perfect for our dynamic pivoted columns.
        df_features.write.jdbc(
            url=jdbc_url,
            table="ml_features_wide",
            mode="overwrite",
            properties=db_properties
        )
        print("Success! Data published to PostgreSQL.")
        print("You can now query it in Grafana using: SELECT * FROM ml_features_wide LIMIT 10")
        
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")
        print("----------------------------------------------------------------")
        print("DATABASE CONNECTION FAILED")
        print("Did you remember to start the database?")
        print("Run this command: sudo systemctl start postgresql")
        print("----------------------------------------------------------------")

if __name__ == "__main__":
    main()