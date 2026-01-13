# Windows Pipeline Runner for Pendler Pilot
# Sets up environment and runs all three pipeline steps

# Get the project root directory (go up two levels from pyspark/windows)
$projectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$venvPath = Join-Path $projectRoot ".venv"

# Set Spark environment variables
$env:SPARK_HOME = Join-Path $venvPath "lib\site-packages\pyspark"
$env:PYSPARK_PYTHON = Join-Path $venvPath "Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = Join-Path $venvPath "Scripts\python.exe"

# Set HADOOP_HOME to avoid winutils error (dummy directory)
$env:HADOOP_HOME = "C:\tmp"
New-Item -ItemType Directory -Force -Path "C:\tmp\bin" | Out-Null

# Get spark-submit path
$sparkSubmit = Join-Path $env:SPARK_HOME "bin\spark-submit"

# Change to project root
Set-Location $projectRoot

Write-Host "=== Pendler Pilot Pipeline (Windows) ===" -ForegroundColor Cyan
Write-Host "Project Root: $projectRoot" -ForegroundColor Gray
Write-Host "Spark Home: $env:SPARK_HOME" -ForegroundColor Gray
Write-Host ""

# Step 1: Silver Layer
Write-Host "Step 1: Running Silver Layer..." -ForegroundColor Yellow
& $sparkSubmit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark/windows/silver_layer.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Silver Layer failed. Stopping pipeline." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Step 2: Running Gold Layer..." -ForegroundColor Yellow
& $sparkSubmit pyspark/windows/gold_layer.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Gold Layer failed. Stopping pipeline." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Step 3: Publishing to PostgreSQL..." -ForegroundColor Yellow
& $sparkSubmit --packages org.postgresql:postgresql:42.6.0 pyspark/windows/publish_to_postgres.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Publish to Postgres failed." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✅ Pipeline completed successfully!" -ForegroundColor Green

