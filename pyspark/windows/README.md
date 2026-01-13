# Windows-Specific Spark Scripts

This folder contains Windows-optimized versions of the Spark pipeline scripts that work around Windows-specific issues (winutils.exe requirement).

## What's Different

These scripts include:
- **Windows file system configuration**: `spark.hadoop.fs.file.impl` set to bypass winutils.exe requirement
- **Same functionality**: Identical logic to the main scripts, just with Windows compatibility fixes

## Files

- `silver_layer.py` - Processes Kafka data into Silver layer (Windows version)
- `gold_layer.py` - Aggregates Silver data into Gold layer (Windows version)
- `publish_to_postgres.py` - Publishes Gold layer to PostgreSQL (Windows version)
- `run-pipeline.ps1` - PowerShell script to run all three steps in sequence

## Usage

### Option 1: Run Individual Scripts

```powershell
# Make sure you're in the project root and venv is activated
.venv\Scripts\Activate.ps1

# Set environment variables
$env:SPARK_HOME = ".venv\lib\site-packages\pyspark"
$env:PYSPARK_PYTHON = ".venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = ".venv\Scripts\python.exe"
$env:HADOOP_HOME = "C:\tmp"
New-Item -ItemType Directory -Force -Path "C:\tmp\bin" | Out-Null

# Run scripts
.venv\lib\site-packages\pyspark\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark/windows/silver_layer.py

.venv\lib\site-packages\pyspark\bin\spark-submit pyspark/windows/gold_layer.py

.venv\lib\site-packages\pyspark\bin\spark-submit --packages org.postgresql:postgresql:42.6.0 pyspark/windows/publish_to_postgres.py
```

### Option 2: Use the Pipeline Script (Recommended)

```powershell
# Run all three steps automatically
.\pyspark\windows\run-pipeline.ps1
```

This script:
- Sets up all required environment variables
- Runs all three pipeline steps in order
- Stops if any step fails
- Shows clear progress messages

## Prerequisites

1. **Virtual environment activated**
2. **PySpark installed**: `pip install pyspark==3.5.3`
3. **Java 11+ installed**
4. **PostgreSQL running** (for step 3)
5. **Kafka running** (for step 1)
6. **`.env` file configured** with:
   - `KAFKA_BROKER_IP`
   - `POSTGRES_USER`
   - `POSTGRES_PASSWORD`
   - `POSTGRES_DB`

## Notes

- These scripts are identical to the main scripts except for the Windows compatibility fix
- The `spark.hadoop.fs.file.impl` config bypasses the winutils.exe requirement
- You still need to set `HADOOP_HOME` environment variable (can be a dummy directory)
- All output goes to the same `./datalake/` directory structure

