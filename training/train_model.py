"""
Traffic Congestion Prediction Model Training

This script trains a machine learning model to predict traffic congestion
based on weather conditions, train delays, and temporal features.

The trained model can then be used to recommend: Car 🚗 or Train 🚆

Usage:
    spark-submit training/train_model.py

Input:  ./datalake/gold/ml_features/ (Parquet files from gold_layer.py)
Output: ./training/models/congestion_model (Saved model)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, hour, dayofweek, month, 
    isnan, isnull, count, avg
)
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import os

# Configuration
GOLD_DATA_PATH = "./datalake/gold/ml_features"
MODEL_OUTPUT_PATH = "./training/models/congestion_model"
MODEL_TYPE = "random_forest"  # Options: "random_forest" or "gbt"


def create_spark_session():
    """Create and configure Spark session."""
    return SparkSession.builder \
        .appName("TrafficCongestionTraining") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def load_and_explore_data(spark):
    """Load Gold layer data and show basic statistics."""
    print("\n" + "=" * 60)
    print("STEP 1: Loading Gold Layer Data")
    print("=" * 60)
    
    df = spark.read.parquet(GOLD_DATA_PATH)
    
    print(f"\nTotal records: {df.count():,}")
    print(f"Columns: {df.columns}")
    
    print("\n--- Data Sample ---")
    df.show(5, truncate=False)
    
    print("\n--- Data Statistics ---")
    df.describe().show()
    
    return df


def engineer_features(df):
    """
    Create additional features from existing data.
    
    New features:
    - hour_of_day: Extract hour from timestamp (rush hour patterns)
    - day_of_week: 1=Sunday to 7=Saturday (weekend vs weekday)
    - is_rush_hour: Boolean flag for 7-9am and 5-7pm
    - is_weekend: Boolean flag for Saturday/Sunday
    - rain_intensity: Categorize rain (none, light, moderate, heavy)
    """
    print("\n" + "=" * 60)
    print("STEP 2: Feature Engineering")
    print("=" * 60)
    
    df_features = df \
        .withColumn("hour_of_day", hour(col("time_bucket"))) \
        .withColumn("day_of_week", dayofweek(col("time_bucket"))) \
        .withColumn("month", month(col("time_bucket"))) \
        .withColumn("is_rush_hour", 
            when((hour(col("time_bucket")).between(7, 9)) | 
                 (hour(col("time_bucket")).between(17, 19)), 1).otherwise(0)) \
        .withColumn("is_weekend",
            when(dayofweek(col("time_bucket")).isin([1, 7]), 1).otherwise(0)) \
        .withColumn("rain_intensity",
            when(col("rain_1h_mm").isNull() | (col("rain_1h_mm") == 0), 0)
            .when(col("rain_1h_mm") < 2.5, 1)  # Light rain
            .when(col("rain_1h_mm") < 7.5, 2)  # Moderate rain
            .otherwise(3))  # Heavy rain
    
    # Fill nulls with reasonable defaults
    df_clean = df_features \
        .fillna(0, subset=["rain_1h_mm", "avg_rail_delay_sec", "delayed_trains_count"]) \
        .fillna(20, subset=["temp"])  # Default temp
    
    # Drop rows where target is null
    df_clean = df_clean.filter(col("congestion_ratio").isNotNull())
    
    print(f"Records after cleaning: {df_clean.count():,}")
    print("\n--- Engineered Features Sample ---")
    df_clean.select("time_bucket", "hour_of_day", "day_of_week", 
                    "is_rush_hour", "is_weekend", "rain_intensity").show(5)
    
    return df_clean


def prepare_ml_data(df):
    """
    Prepare data for ML: assemble features into vector.
    
    Features used:
    - temp: Temperature (affects driving conditions)
    - humidity: Humidity level
    - wind_speed: Wind speed
    - rain_1h_mm: Rainfall in last hour
    - rain_intensity: Categorized rain level
    - avg_rail_delay_sec: Average train delay (people switch to cars)
    - delayed_trains_count: Number of delayed trains
    - hour_of_day: Time patterns
    - day_of_week: Day patterns
    - is_rush_hour: Rush hour flag
    - is_weekend: Weekend flag
    """
    print("\n" + "=" * 60)
    print("STEP 3: Preparing ML Features")
    print("=" * 60)
    
    feature_columns = [
        "temp",
        "humidity", 
        "wind_speed",
        "rain_1h_mm",
        "rain_intensity",
        "avg_rail_delay_sec",
        "delayed_trains_count",
        "hour_of_day",
        "day_of_week",
        "is_rush_hour",
        "is_weekend"
    ]
    
    # Check which features actually exist in the data
    available_features = [f for f in feature_columns if f in df.columns]
    missing_features = [f for f in feature_columns if f not in df.columns]
    
    if missing_features:
        print(f"⚠️  Missing features (will be skipped): {missing_features}")
    
    print(f"Using features: {available_features}")
    
    # Assemble features into a single vector
    assembler = VectorAssembler(
        inputCols=available_features,
        outputCol="features",
        handleInvalid="skip"  # Skip rows with nulls
    )
    
    df_assembled = assembler.transform(df)
    
    # Select only what we need
    df_ml = df_assembled.select("features", col("congestion_ratio").alias("label"))
    
    print(f"\nML-ready records: {df_ml.count():,}")
    
    return df_ml, available_features


def train_model(df_ml, model_type="random_forest"):
    """
    Train the prediction model.
    
    Options:
    - random_forest: Good default, interpretable, handles mixed features
    - gbt: Often more accurate, but slower and harder to interpret
    """
    print("\n" + "=" * 60)
    print(f"STEP 4: Training Model ({model_type.upper()})")
    print("=" * 60)
    
    # Split data: 80% training, 20% testing
    train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)
    
    print(f"Training samples: {train_data.count():,}")
    print(f"Testing samples: {test_data.count():,}")
    
    # Choose model
    if model_type == "gbt":
        model = GBTRegressor(
            featuresCol="features",
            labelCol="label",
            maxIter=50,
            maxDepth=5
        )
        print("\nUsing Gradient Boosted Trees (GBT)")
    else:
        model = RandomForestRegressor(
            featuresCol="features",
            labelCol="label",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        print("\nUsing Random Forest (100 trees, max depth 10)")
    
    # Train
    print("\nTraining... (this may take a few minutes)")
    trained_model = model.fit(train_data)
    
    # Evaluate on test data
    predictions = trained_model.transform(test_data)
    
    evaluator_rmse = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="r2"
    )
    
    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    
    print("\n--- Model Performance ---")
    print(f"RMSE: {rmse:.4f} (lower is better)")
    print(f"R² Score: {r2:.4f} (closer to 1 is better)")
    
    # Show feature importance (Random Forest only)
    if model_type == "random_forest" and hasattr(trained_model, "featureImportances"):
        print("\n--- Feature Importance ---")
        importances = trained_model.featureImportances.toArray()
        # Note: We'd need to pass feature names to show them here
        for i, imp in enumerate(importances):
            print(f"  Feature {i}: {imp:.4f}")
    
    # Show sample predictions
    print("\n--- Sample Predictions ---")
    predictions.select("label", "prediction").show(10)
    
    return trained_model, rmse, r2


def save_model(model, path):
    """Save the trained model to disk."""
    print("\n" + "=" * 60)
    print("STEP 5: Saving Model")
    print("=" * 60)
    
    model.write().overwrite().save(path)
    print(f"✅ Model saved to: {path}")


def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║     Traffic Congestion Prediction - Model Training           ║
╠══════════════════════════════════════════════════════════════╣
║  Input:  Gold layer data (traffic + weather + train delays)  ║
║  Output: Trained model to predict congestion_ratio           ║
║  Model:  {model_type}                                        ║
╚══════════════════════════════════════════════════════════════╝
    """.format(model_type=MODEL_TYPE.upper()))
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Step 1: Load data
        df = load_and_explore_data(spark)
        
        # Step 2: Engineer features
        df_features = engineer_features(df)
        
        # Step 3: Prepare ML data
        df_ml, feature_names = prepare_ml_data(df_features)
        
        # Step 4: Train model
        model, rmse, r2 = train_model(df_ml, MODEL_TYPE)
        
        # Step 5: Save model
        save_model(model, MODEL_OUTPUT_PATH)
        
        print("\n" + "=" * 60)
        print("✅ TRAINING COMPLETE!")
        print("=" * 60)
        print(f"Model saved to: {MODEL_OUTPUT_PATH}")
        print(f"Performance: RMSE={rmse:.4f}, R²={r2:.4f}")
        print("\nNext step: Run 'spark-submit training/recommend.py' to make predictions")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure you have Gold layer data in ./datalake/gold/ml_features/")
        print("Run 'spark-submit pyspark/gold_layer.py' first.")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

