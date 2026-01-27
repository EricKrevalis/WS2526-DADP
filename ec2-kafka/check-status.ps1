# =============================================================================
# check-status.ps1 - Check Kafka Broker (and optionally Kafka Connect)
# =============================================================================
# Usage: .\check-status.ps1 -EC2_IP "54.123.45.67"
# =============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$EC2_IP
)

$KAFKA_BROKER = "${EC2_IP}:9092"
$CONNECT_URL = "http://${EC2_IP}:8083"
$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"

Write-Host "=========================================="
Write-Host "  EC2 Kafka Status Check"
Write-Host "=========================================="
Write-Host ""

# 1. Check Kafka broker (port 9092) - used by PySpark, s3_to_kafka
Write-Host "Kafka broker ($KAFKA_BROKER):" -ForegroundColor Cyan
$kafkaCheck = @"
from kafka import KafkaConsumer
import sys
try:
    c = KafkaConsumer(bootstrap_servers='$KAFKA_BROKER', consumer_timeout_ms=5000)
    topics = [t for t in c.topics() if not t.startswith('_')]
    c.close()
    print('OK|' + ','.join(sorted(topics)))
except Exception as e:
    print('FAIL|' + str(e))
    sys.exit(1)
"@
try {
    $result = & $pythonExe -c $kafkaCheck 2>&1
    if ($LASTEXITCODE -eq 0 -and $result -match '^OK\|') {
        $topics = ($result -replace '^OK\|', '').Split(',')
        Write-Host "  OK Reachable" -ForegroundColor Green
        if ($topics.Count -gt 0 -and $topics[0] -ne '') {
            Write-Host "  Topics: $($topics -join ', ')" -ForegroundColor Gray
        } else {
            Write-Host "  Topics: (none yet)" -ForegroundColor Gray
        }
    } else {
        $err = if ($result -match '^FAIL\|') { $result -replace '^FAIL\|', '' } else { $result }
        Write-Host "  X Not reachable" -ForegroundColor Red
        Write-Host "  $err" -ForegroundColor Red
    }
} catch {
    Write-Host "  X Not reachable" -ForegroundColor Red
    Write-Host "  $($_.Exception.Message)" -ForegroundColor Red
}

# 2. Kafka Connect (port 8083) - optional; we use s3_to_kafka instead
Write-Host ""
Write-Host "Kafka Connect (optional, port 8083):" -ForegroundColor Cyan
try {
    $response = Invoke-RestMethod -Uri "$CONNECT_URL/" -Method Get -TimeoutSec 3
    Write-Host "  OK Running (version: $($response.version))" -ForegroundColor Green
} catch {
    Write-Host "  Not used / not reachable (we use s3_to_kafka)" -ForegroundColor Gray
}

Write-Host ""
Write-Host "=========================================="
Write-Host "  Useful Commands (run on EC2 via SSH)"
Write-Host "=========================================="
Write-Host ""
Write-Host "List Kafka topics:"
Write-Host "  docker exec broker kafka-topics --list --bootstrap-server localhost:29092" -ForegroundColor Gray
Write-Host ""
Write-Host "View messages in raw-traffic topic:"
Write-Host "  docker exec broker kafka-console-consumer --bootstrap-server localhost:29092 --topic raw-traffic --from-beginning --max-messages 5" -ForegroundColor Gray
Write-Host ""
Write-Host "Check broker logs:"
Write-Host '  docker logs broker -f' -ForegroundColor Gray
Write-Host ""
