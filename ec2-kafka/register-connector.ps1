# =============================================================================
# register-connector.ps1 - Register S3 Source Connector from your local machine
# =============================================================================
# Usage: .\register-connector.ps1 -EC2_IP "54.123.45.67"
# =============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$EC2_IP
)

$CONNECT_URL = "http://${EC2_IP}:8083"

Write-Host "=========================================="
Write-Host "  Registering S3 Source Connector"
Write-Host "=========================================="
Write-Host ""

# Check if Connect is ready
Write-Host "Checking if Kafka Connect is ready..."
try {
    $response = Invoke-RestMethod -Uri "$CONNECT_URL/" -Method Get -TimeoutSec 5
    Write-Host "  ✓ Kafka Connect is running (version: $($response.version))" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Cannot reach Kafka Connect at $CONNECT_URL" -ForegroundColor Red
    Write-Host "  Make sure:" -ForegroundColor Yellow
    Write-Host "    1. EC2 instance is running"
    Write-Host "    2. Docker containers are up (docker ps)"
    Write-Host "    3. Security group allows port 8083 from your IP"
    exit 1
}

# Get the connector config file path
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ConnectorConfig = Join-Path $ScriptDir "s3-source-connector.json"

if (-not (Test-Path $ConnectorConfig)) {
    Write-Host "  ✗ Connector config not found: $ConnectorConfig" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Registering connector from: $ConnectorConfig"

# Check if connector already exists
$connectorName = "s3-traffic-source"
try {
    $existing = Invoke-RestMethod -Uri "$CONNECT_URL/connectors/$connectorName" -Method Get -ErrorAction SilentlyContinue
    Write-Host "  Connector '$connectorName' already exists. Deleting..." -ForegroundColor Yellow
    Invoke-RestMethod -Uri "$CONNECT_URL/connectors/$connectorName" -Method Delete | Out-Null
    Start-Sleep -Seconds 2
} catch {
    # Connector doesn't exist, that's fine
}

# Register the connector
try {
    $response = Invoke-RestMethod -Uri "$CONNECT_URL/connectors" -Method Post -ContentType "application/json" -InFile $ConnectorConfig
    Write-Host "  ✓ Connector registered successfully!" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Failed to register connector" -ForegroundColor Red
    Write-Host "  Error: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Waiting for connector to start..."
Start-Sleep -Seconds 5

# Check connector status
try {
    $status = Invoke-RestMethod -Uri "$CONNECT_URL/connectors/$connectorName/status" -Method Get
    Write-Host ""
    Write-Host "Connector Status:" -ForegroundColor Cyan
    Write-Host "  Name:   $($status.name)"
    Write-Host "  State:  $($status.connector.state)"
    
    if ($status.tasks.Count -gt 0) {
        Write-Host "  Tasks:"
        foreach ($task in $status.tasks) {
            $color = if ($task.state -eq "RUNNING") { "Green" } else { "Yellow" }
            Write-Host "    - Task $($task.id): $($task.state)" -ForegroundColor $color
        }
    }
} catch {
    Write-Host "  Could not get connector status" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=========================================="
Write-Host "  Done! Data should start flowing soon."
Write-Host "=========================================="
Write-Host ""
Write-Host "To check topics, SSH into EC2 and run:"
Write-Host "  docker exec broker kafka-topics --list --bootstrap-server localhost:29092"
Write-Host ""

