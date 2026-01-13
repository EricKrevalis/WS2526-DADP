# Quick Setup Check Script
Write-Host "=== Checking Setup ===" -ForegroundColor Cyan

# 1. Check if venv is activated
Write-Host "`n1. Virtual Environment:" -ForegroundColor Yellow
if ($env:VIRTUAL_ENV -or (Test-Path ".venv")) {
    Write-Host "   ✓ Virtual environment found" -ForegroundColor Green
} else {
    Write-Host "   ✗ Virtual environment not found" -ForegroundColor Red
    Write-Host "   Run: .venv\Scripts\Activate.ps1" -ForegroundColor Yellow
}

# 2. Check PySpark
Write-Host "`n2. PySpark Installation:" -ForegroundColor Yellow
try {
    $pysparkVersion = .venv\Scripts\python.exe -c "import pyspark; print(pyspark.__version__)" 2>&1
    Write-Host "   ✓ PySpark $pysparkVersion installed" -ForegroundColor Green
} catch {
    Write-Host "   ✗ PySpark not installed" -ForegroundColor Red
    Write-Host "   Run: pip install pyspark>=3.5.0" -ForegroundColor Yellow
}

# 3. Check Java
Write-Host "`n3. Java:" -ForegroundColor Yellow
try {
    $javaVersion = java -version 2>&1 | Select-Object -First 1
    Write-Host "   ✓ Java found: $javaVersion" -ForegroundColor Green
} catch {
    Write-Host "   ✗ Java not found" -ForegroundColor Red
    Write-Host "   Install Java 11 from: https://adoptium.net/" -ForegroundColor Yellow
}

# 4. Check .env file
Write-Host "`n4. Environment File:" -ForegroundColor Yellow
if (Test-Path ".env") {
    Write-Host "   ✓ .env file exists" -ForegroundColor Green
    $envContent = Get-Content ".env" | Where-Object { $_ -match "KAFKA_BROKER_IP" }
    if ($envContent) {
        Write-Host "   ✓ KAFKA_BROKER_IP found" -ForegroundColor Green
    } else {
        Write-Host "   ⚠ KAFKA_BROKER_IP not found in .env" -ForegroundColor Yellow
    }
} else {
    Write-Host "   ✗ .env file not found" -ForegroundColor Red
    Write-Host "   Create .env file with KAFKA_BROKER_IP" -ForegroundColor Yellow
}

# 5. Check datalake directory
Write-Host "`n5. Data Directory:" -ForegroundColor Yellow
if (Test-Path "datalake") {
    Write-Host "   ✓ datalake directory exists" -ForegroundColor Green
    $silverDirs = Get-ChildItem -Path "datalake\silver" -Directory -ErrorAction SilentlyContinue
    if ($silverDirs) {
        Write-Host "   ✓ Silver layer data found:" -ForegroundColor Green
        $silverDirs | ForEach-Object { Write-Host "     - $($_.Name)" -ForegroundColor Gray }
    } else {
        Write-Host "   ⚠ No silver layer data yet" -ForegroundColor Yellow
    }
} else {
    Write-Host "   ⚠ datalake directory does not exist (will be created on first run)" -ForegroundColor Yellow
}

# 6. Check Kafka connection (if .env exists)
Write-Host "`n6. Kafka Connection Test:" -ForegroundColor Yellow
if (Test-Path ".env") {
    $env:PYTHONPATH = "."
    try {
        $kafkaIP = (.venv\Scripts\python.exe -c "from dotenv import load_dotenv; import os; load_dotenv(); print(os.getenv('KAFKA_BROKER_IP', 'not_set'))" 2>&1)
        if ($kafkaIP -and $kafkaIP -ne "not_set") {
            Write-Host "   ✓ KAFKA_BROKER_IP = $kafkaIP" -ForegroundColor Green
            Write-Host "   (Note: This doesn't test if Kafka is actually running)" -ForegroundColor Gray
        } else {
            Write-Host "   ✗ KAFKA_BROKER_IP not set" -ForegroundColor Red
        }
    } catch {
        Write-Host "   ⚠ Could not read .env file" -ForegroundColor Yellow
    }
}

Write-Host "`n=== Check Complete ===" -ForegroundColor Cyan

