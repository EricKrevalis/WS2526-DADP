# Traffic Prediction & Transport Recommendation

Machine learning system to predict traffic congestion and recommend whether to take a **car 🚗** or **public transport 🚆**.

## Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Lambda → S3 → Kafka → Silver Layer → Gold Layer → ML Training    │
│                                                                     │
│   [APIs]   [Storage] [Queue]  [Clean]    [Join]    [Predict]       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `train_model.py` | Trains the congestion prediction model |
| `recommend.py` | Makes car/train recommendations |
| `models/` | Saved trained models (created after training) |

## Quick Start

### 1. Prerequisites

Make sure you have data in the Gold layer:

```powershell
# First, run silver layer to process Kafka data
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 pyspark/silver_layer.py

# Then, run gold layer to join all data
spark-submit pyspark/gold_layer.py
```

### 2. Train the Model

```powershell
spark-submit training/train_model.py
```

This will:
- Load data from `./datalake/gold/ml_features/`
- Engineer additional features (rush hour, weekend, rain intensity)
- Train a Random Forest model
- Evaluate model performance (RMSE, R²)
- Save the model to `./training/models/congestion_model`

### 3. Make Recommendations

```powershell
# Using sample conditions
spark-submit training/recommend.py

# Interactive mode (enter your own conditions)
spark-submit training/recommend.py --interactive
```

## How It Works

### Features Used

| Feature | Source | Description |
|---------|--------|-------------|
| `temp` | Weather API | Temperature (°C) |
| `humidity` | Weather API | Humidity (%) |
| `wind_speed` | Weather API | Wind speed (m/s) |
| `rain_1h_mm` | Weather API | Rainfall in last hour |
| `rain_intensity` | Derived | 0=none, 1=light, 2=mod, 3=heavy |
| `avg_rail_delay_sec` | Train API | Average train delay |
| `delayed_trains_count` | Train API | Number of delayed trains |
| `hour_of_day` | Time | Hour (0-23) |
| `day_of_week` | Time | Day (1=Sun, 7=Sat) |
| `is_rush_hour` | Derived | 1 if 7-9am or 5-7pm |
| `is_weekend` | Derived | 1 if Saturday/Sunday |

### Prediction Target

**`congestion_ratio`** - A value between 0 and 1:
- `0.0` = No traffic (free flow)
- `0.5` = Moderate traffic
- `1.0` = Complete standstill

### Recommendation Logic

```
IF predicted_congestion > 70% AND trains on time:
    → TAKE TRAIN 🚆

IF predicted_congestion < 30% AND trains delayed:
    → TAKE CAR 🚗

ADDITIONAL FACTORS:
    - Rain → favors train (driving harder)
    - Rush hour → favors train (parking difficult)
    - Weekend → favors car (less traffic)
```

## Model Options

### Random Forest (Default)

```python
MODEL_TYPE = "random_forest"
```

- ✅ Good default choice
- ✅ Handles mixed feature types
- ✅ Shows feature importance
- ✅ Works well without tuning

### Gradient Boosted Trees (Alternative)

```python
MODEL_TYPE = "gbt"
```

- ✅ Often more accurate
- ⚠️ Slower to train
- ⚠️ May overfit on small data

To switch models, edit `train_model.py` and change the `MODEL_TYPE` variable.

## Performance Metrics

After training, you'll see:

- **RMSE** (Root Mean Square Error): Lower is better. Typical good value: < 0.15
- **R² Score**: Closer to 1 is better. Typical good value: > 0.7

## Example Output

```
╔══════════════════════════════════════════════════════════════╗
║         🚗 Car vs Train Recommendation System 🚆             ║
╚══════════════════════════════════════════════════════════════╝

📊 Current Conditions:
   🌡️  Temperature: 15°C
   🌧️  Rain: 2.0 mm/h
   🕐 Time: Rush hour
   🚂 Train delays: 2 min avg

🎯 Predicted Congestion: 65%

------------------------------------------------------------

   RECOMMENDATION: Take the TRAIN 🚆
   Confidence: 75%

📝 Reasoning:
   • Moderate congestion (65%)
   • Trains running on time
   • Rainy conditions favor train
   • Rush hour - parking difficult
```

## Troubleshooting

### "Model not found"

Make sure you've trained the model first:
```powershell
spark-submit training/train_model.py
```

### "No data in Gold layer"

Run the data pipeline first:
```powershell
# 1. Make sure EC2 Kafka is running
# 2. Run silver layer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 pyspark/silver_layer.py
# 3. Run gold layer
spark-submit pyspark/gold_layer.py
```

### "Low R² score" (< 0.5)

- Need more training data (let data collect for a few days)
- Try Gradient Boosted Trees: change `MODEL_TYPE = "gbt"`
- Check if Gold data has enough variation in conditions

## Future Improvements

- [ ] Real-time predictions using streaming data
- [ ] Web UI for recommendations
- [ ] Mobile app integration
- [ ] Multi-city support
- [ ] Time-of-arrival predictions

