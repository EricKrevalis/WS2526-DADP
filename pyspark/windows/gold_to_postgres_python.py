"""
Gold Layer + PostgreSQL - Pure Python (No Spark)
Reads silver parquet files, aggregates, and writes directly to PostgreSQL.
"""
import os
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
import numpy as np

load_dotenv()

SILVER_PATH = "./datalake/silver"

# PostgreSQL connection
pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")
pg_db = os.getenv("POSTGRES_DB", "traffic_lake")
pg_host = os.getenv("POSTGRES_HOST", "localhost")


def read_silver_parquet(folder):
    """Read all parquet files from a silver folder."""
    path = f"{SILVER_PATH}/{folder}"
    if not os.path.exists(path):
        print(f"  Warning: {path} not found")
        return pd.DataFrame()
    
    try:
        df = pq.read_table(path).to_pandas()
        print(f"  Read {len(df)} records from {folder}")
        return df
    except Exception as e:
        print(f"  Error reading {folder}: {e}")
        return pd.DataFrame()


def bucket_timestamp(df, time_col="meta_scraped_at"):
    """Bucket timestamps into 30-minute intervals."""
    df = df.copy()
    df['ts'] = pd.to_datetime(df[time_col], errors='coerce')
    # Floor to 30-minute buckets
    df['time_bucket'] = df['ts'].dt.floor('30min')
    return df


def aggregate_traffic(df):
    """Aggregate traffic data by city and time bucket, pivoting by grid_id."""
    if df.empty:
        return pd.DataFrame()
    
    df = bucket_timestamp(df)
    
    # Group by city, time_bucket, grid_id and get averages
    agg = df.groupby(['meta_city', 'time_bucket', 'grid_id']).agg({
        'congestion_ratio': 'mean',
        'current_speed': 'mean'
    }).reset_index()
    
    # Pivot to get columns like: grid_0_congestion, grid_0_speed, etc.
    pivoted_congestion = agg.pivot_table(
        index=['meta_city', 'time_bucket'],
        columns='grid_id',
        values='congestion_ratio'
    ).add_suffix('_congestion')
    
    pivoted_speed = agg.pivot_table(
        index=['meta_city', 'time_bucket'],
        columns='grid_id',
        values='current_speed'
    ).add_suffix('_speed')
    
    result = pivoted_congestion.join(pivoted_speed).reset_index()
    return result


def aggregate_weather(df):
    """Aggregate weather data by city and time bucket, pivoting by grid_id."""
    if df.empty:
        return pd.DataFrame()
    
    df = bucket_timestamp(df)
    
    # Group by city, time_bucket, grid_id and get averages
    agg = df.groupby(['meta_city', 'time_bucket', 'grid_id']).agg({
        'rain_1h_mm': 'mean',
        'temp': 'mean'
    }).reset_index()
    
    # Pivot
    pivoted_rain = agg.pivot_table(
        index=['meta_city', 'time_bucket'],
        columns='grid_id',
        values='rain_1h_mm'
    ).add_suffix('_rain')
    
    pivoted_temp = agg.pivot_table(
        index=['meta_city', 'time_bucket'],
        columns='grid_id',
        values='temp'
    ).add_suffix('_temp')
    
    result = pivoted_rain.join(pivoted_temp).reset_index()
    return result


def aggregate_db(df):
    """Aggregate transit data - rail vs road metrics."""
    if df.empty:
        return pd.DataFrame()
    
    df = bucket_timestamp(df)
    
    # Classify: Bus/STR = road, rest = rail
    df['is_road'] = df['line'].str.contains('Bus|STR', case=False, na=False)
    df['is_rail'] = ~df['is_road']
    
    # Aggregate
    result = df.groupby(['meta_city', 'time_bucket']).apply(
        lambda x: pd.Series({
            'rail_avg_delay': x.loc[x['is_rail'], 'delay'].mean(),
            'rail_stress_index': (x.loc[x['is_rail'] & (x['delay'] > 300)].shape[0] / 
                                  max(x['is_rail'].sum(), 1)),
            'bus_avg_delay': x.loc[x['is_road'], 'delay'].mean(),
            'bus_stress_index': (x.loc[x['is_road'] & (x['delay'] > 300)].shape[0] / 
                                 max(x['is_road'].sum(), 1))
        })
    ).reset_index()
    
    return result


def main():
    print("=" * 60)
    print("  Gold Layer + PostgreSQL - Pure Python")
    print("=" * 60)
    print(f"Silver Path: {SILVER_PATH}")
    print(f"PostgreSQL: {pg_user}@{pg_host}/{pg_db}")
    print()
    
    # Check credentials
    if not pg_user or not pg_password:
        print("Error: PostgreSQL credentials missing in .env")
        return
    
    # Read silver data
    print("[1/4] Reading Silver Layer...")
    df_traffic = read_silver_parquet("tomtom")
    df_weather = read_silver_parquet("weather")
    df_db = read_silver_parquet("db")
    
    if df_traffic.empty and df_weather.empty and df_db.empty:
        print("Error: No data found in Silver Layer")
        return
    
    # Aggregate
    print("\n[2/4] Aggregating...")
    print("  Processing traffic...")
    traffic_agg = aggregate_traffic(df_traffic)
    print(f"  -> {len(traffic_agg)} traffic rows")
    
    print("  Processing weather...")
    weather_agg = aggregate_weather(df_weather)
    print(f"  -> {len(weather_agg)} weather rows")
    
    print("  Processing transit...")
    db_agg = aggregate_db(df_db)
    print(f"  -> {len(db_agg)} transit rows")
    
    # Join all
    print("\n[3/4] Joining datasets...")
    
    # Get all unique (city, time_bucket) combinations
    all_keys = set()
    for df in [traffic_agg, weather_agg, db_agg]:
        if not df.empty and 'meta_city' in df.columns and 'time_bucket' in df.columns:
            all_keys.update(df[['meta_city', 'time_bucket']].itertuples(index=False, name=None))
    
    if not all_keys:
        print("Error: No data to aggregate")
        return
    
    # Create master key dataframe
    master_keys = pd.DataFrame(list(all_keys), columns=['meta_city', 'time_bucket'])
    
    # Join
    gold_df = master_keys
    if not traffic_agg.empty:
        gold_df = gold_df.merge(traffic_agg, on=['meta_city', 'time_bucket'], how='left')
    if not weather_agg.empty:
        gold_df = gold_df.merge(weather_agg, on=['meta_city', 'time_bucket'], how='left')
    if not db_agg.empty:
        gold_df = gold_df.merge(db_agg, on=['meta_city', 'time_bucket'], how='left')
    
    # Add time features
    gold_df['hour_of_day'] = gold_df['time_bucket'].dt.hour
    gold_df['day_of_week'] = gold_df['time_bucket'].dt.dayofweek + 1  # 1-7
    
    print(f"  -> Gold DataFrame: {len(gold_df)} rows, {len(gold_df.columns)} columns")
    
    # Write to PostgreSQL
    print("\n[4/4] Writing to PostgreSQL...")
    try:
        engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:5432/{pg_db}")
        
        # Write to table
        gold_df.to_sql(
            'pendler_pilot',
            engine,
            if_exists='replace',  # or 'append' for incremental
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"  ✓ Wrote {len(gold_df)} rows to pendler_pilot table")
        
        # Verify
        result = pd.read_sql("SELECT COUNT(*) as count FROM pendler_pilot", engine)
        print(f"  ✓ Verified: {result['count'].iloc[0]} rows in database")
        
    except Exception as e:
        print(f"  ✗ Error writing to PostgreSQL: {e}")
        return
    
    print("\n" + "=" * 60)
    print("  Gold Layer + PostgreSQL Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()

