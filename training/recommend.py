"""
Car vs Train Recommendation System

Uses the trained congestion prediction model to recommend whether
to take a car or public transport based on current conditions.

Usage:
    spark-submit training/recommend.py

Or for interactive mode:
    spark-submit training/recommend.py --interactive

Input:  Trained model from ./training/models/congestion_model
Output: Recommendation (Car 🚗 or Train 🚆)
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler
import sys

# Configuration
MODEL_PATH = "./training/models/congestion_model"

# Thresholds for recommendation
CONGESTION_THRESHOLD_HIGH = 0.7   # Above this = heavy traffic, take train
CONGESTION_THRESHOLD_LOW = 0.3    # Below this = light traffic, take car
TRAIN_DELAY_THRESHOLD = 300       # 5 minutes delay = trains unreliable


def create_spark_session():
    """Create Spark session."""
    return SparkSession.builder \
        .appName("TransportRecommendation") \
        .getOrCreate()


def load_model(spark):
    """Load the trained model."""
    print("\n📦 Loading trained model...")
    try:
        model = RandomForestRegressionModel.load(MODEL_PATH)
        print("✅ Model loaded successfully!")
        return model
    except Exception as e:
        print(f"❌ Error loading model: {e}")
        print(f"\nMake sure you've trained the model first:")
        print("  spark-submit training/train_model.py")
        sys.exit(1)


def get_sample_conditions():
    """
    Return sample current conditions for demonstration.
    In production, this would fetch real-time data from APIs.
    """
    return {
        # Weather conditions
        "temp": 15.0,              # Temperature in Celsius
        "humidity": 70,            # Humidity percentage
        "wind_speed": 5.0,         # Wind speed m/s
        "rain_1h_mm": 2.0,         # Rainfall in last hour (mm)
        "rain_intensity": 1,       # 0=none, 1=light, 2=moderate, 3=heavy
        
        # Train conditions
        "avg_rail_delay_sec": 120, # Average delay in seconds
        "delayed_trains_count": 3, # Number of delayed trains
        
        # Time conditions
        "hour_of_day": 8,          # Current hour (24h format)
        "day_of_week": 2,          # 1=Sunday, 2=Monday, ...
        "is_rush_hour": 1,         # 1 if rush hour
        "is_weekend": 0            # 1 if weekend
    }


def get_user_conditions():
    """Interactive mode: Get conditions from user input."""
    print("\n" + "=" * 50)
    print("Enter current conditions (press Enter for default):")
    print("=" * 50)
    
    def get_input(prompt, default, cast_type=float):
        user_input = input(f"{prompt} [{default}]: ").strip()
        if user_input == "":
            return default
        return cast_type(user_input)
    
    conditions = {
        "temp": get_input("Temperature (°C)", 15.0),
        "humidity": get_input("Humidity (%)", 70, int),
        "wind_speed": get_input("Wind speed (m/s)", 5.0),
        "rain_1h_mm": get_input("Rain in last hour (mm)", 0.0),
        "rain_intensity": get_input("Rain intensity (0-3)", 0, int),
        "avg_rail_delay_sec": get_input("Avg train delay (seconds)", 0, int),
        "delayed_trains_count": get_input("# of delayed trains", 0, int),
        "hour_of_day": get_input("Hour of day (0-23)", 12, int),
        "day_of_week": get_input("Day of week (1=Sun, 7=Sat)", 2, int),
        "is_rush_hour": get_input("Rush hour? (0/1)", 0, int),
        "is_weekend": get_input("Weekend? (0/1)", 0, int),
    }
    
    return conditions


def predict_congestion(spark, model, conditions):
    """Use the model to predict congestion ratio."""
    
    # Create DataFrame from conditions
    schema = StructType([
        StructField("temp", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("rain_1h_mm", DoubleType(), True),
        StructField("rain_intensity", IntegerType(), True),
        StructField("avg_rail_delay_sec", IntegerType(), True),
        StructField("delayed_trains_count", IntegerType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("is_rush_hour", IntegerType(), True),
        StructField("is_weekend", IntegerType(), True),
    ])
    
    data = [(
        float(conditions["temp"]),
        int(conditions["humidity"]),
        float(conditions["wind_speed"]),
        float(conditions["rain_1h_mm"]),
        int(conditions["rain_intensity"]),
        int(conditions["avg_rail_delay_sec"]),
        int(conditions["delayed_trains_count"]),
        int(conditions["hour_of_day"]),
        int(conditions["day_of_week"]),
        int(conditions["is_rush_hour"]),
        int(conditions["is_weekend"]),
    )]
    
    df = spark.createDataFrame(data, schema)
    
    # Assemble features
    feature_columns = [
        "temp", "humidity", "wind_speed", "rain_1h_mm", "rain_intensity",
        "avg_rail_delay_sec", "delayed_trains_count",
        "hour_of_day", "day_of_week", "is_rush_hour", "is_weekend"
    ]
    
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features",
        handleInvalid="skip"
    )
    
    df_features = assembler.transform(df)
    
    # Predict
    predictions = model.transform(df_features)
    predicted_congestion = predictions.select("prediction").collect()[0][0]
    
    return predicted_congestion


def make_recommendation(predicted_congestion, conditions):
    """
    Make a recommendation based on predicted congestion and train reliability.
    
    Logic:
    1. If trains are very delayed AND congestion low → Car
    2. If trains are reliable AND congestion high → Train
    3. If both are bad → depends on severity
    """
    train_delay = conditions["avg_rail_delay_sec"]
    delayed_count = conditions["delayed_trains_count"]
    is_raining = conditions["rain_1h_mm"] > 0
    
    # Calculate scores
    car_score = 0
    train_score = 0
    reasons = []
    
    # Congestion factor
    if predicted_congestion > CONGESTION_THRESHOLD_HIGH:
        train_score += 3
        reasons.append(f"High predicted congestion ({predicted_congestion:.1%})")
    elif predicted_congestion < CONGESTION_THRESHOLD_LOW:
        car_score += 3
        reasons.append(f"Low predicted congestion ({predicted_congestion:.1%})")
    else:
        reasons.append(f"Moderate congestion ({predicted_congestion:.1%})")
    
    # Train reliability factor
    if train_delay > TRAIN_DELAY_THRESHOLD:
        car_score += 2
        reasons.append(f"Trains delayed (avg {train_delay//60} min)")
    elif train_delay < 60:
        train_score += 2
        reasons.append("Trains running on time")
    
    if delayed_count > 5:
        car_score += 1
        reasons.append(f"{delayed_count} trains delayed")
    
    # Weather factor
    if is_raining:
        train_score += 1  # Rain makes driving harder
        reasons.append("Rainy conditions favor train")
    
    # Rush hour factor
    if conditions["is_rush_hour"]:
        train_score += 1
        reasons.append("Rush hour - parking difficult")
    
    # Make decision
    if train_score > car_score:
        recommendation = "TRAIN"
        emoji = "🚆"
        confidence = train_score / (train_score + car_score) if (train_score + car_score) > 0 else 0.5
    else:
        recommendation = "CAR"
        emoji = "🚗"
        confidence = car_score / (train_score + car_score) if (train_score + car_score) > 0 else 0.5
    
    return recommendation, emoji, confidence, reasons


def display_recommendation(conditions, predicted_congestion, recommendation, emoji, confidence, reasons):
    """Display the recommendation in a nice format."""
    
    print("\n" + "=" * 60)
    print("               TRANSPORT RECOMMENDATION")
    print("=" * 60)
    
    print("\n📊 Current Conditions:")
    print(f"   🌡️  Temperature: {conditions['temp']}°C")
    print(f"   🌧️  Rain: {conditions['rain_1h_mm']} mm/h")
    print(f"   🕐 Time: {'Rush hour' if conditions['is_rush_hour'] else 'Off-peak'}")
    print(f"   🚂 Train delays: {conditions['avg_rail_delay_sec']//60} min avg")
    
    print(f"\n🎯 Predicted Congestion: {predicted_congestion:.1%}")
    
    print("\n" + "-" * 60)
    print(f"\n   RECOMMENDATION: Take the {recommendation} {emoji}")
    print(f"   Confidence: {confidence:.0%}")
    
    print("\n📝 Reasoning:")
    for reason in reasons:
        print(f"   • {reason}")
    
    print("\n" + "=" * 60)


def main():
    interactive = "--interactive" in sys.argv or "-i" in sys.argv
    
    print("""
╔══════════════════════════════════════════════════════════════╗
║         🚗 Car vs Train Recommendation System 🚆             ║
╠══════════════════════════════════════════════════════════════╣
║  Uses ML to predict traffic and recommend transport mode     ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Load model
        model = load_model(spark)
        
        # Get conditions
        if interactive:
            conditions = get_user_conditions()
        else:
            print("\n📋 Using sample conditions (use --interactive for custom input)")
            conditions = get_sample_conditions()
        
        # Predict congestion
        print("\n🔮 Predicting traffic congestion...")
        predicted_congestion = predict_congestion(spark, model, conditions)
        
        # Make recommendation
        recommendation, emoji, confidence, reasons = make_recommendation(
            predicted_congestion, conditions
        )
        
        # Display results
        display_recommendation(
            conditions, predicted_congestion, 
            recommendation, emoji, confidence, reasons
        )
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

