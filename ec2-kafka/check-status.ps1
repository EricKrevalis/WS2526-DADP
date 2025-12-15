# =============================================================================
# check-status.ps1 - Check Kafka Connect and Connector Status
# =============================================================================
# Usage: .\check-status.ps1 -EC2_IP "54.123.45.67"
# =============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$EC2_IP
)

$CONNECT_URL = "http://${EC2_IP}:8083"

Write-Host "=========================================="
Write-Host "  Kafka Connect Status Check"
Write-Host "=========================================="
Write-Host ""

# Check Connect health
Write-Host "Kafka Connect:" -ForegroundColor Cyan
try {
    $response = Invoke-RestMethod -Uri "$CONNECT_URL/" -Method Get -TimeoutSec 5
    Write-Host "  ✓ Running (version: $($response.version))" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Not reachable at $CONNECT_URL" -ForegroundColor Red
    exit 1
}

# List connectors
Write-Host ""
Write-Host "Registered Connectors:" -ForegroundColor Cyan
try {
    $connectors = Invoke-RestMethod -Uri "$CONNECT_URL/connectors" -Method Get
    if ($connectors.Count -eq 0) {
        Write-Host "  (none)" -ForegroundColor Yellow
    } else {
        foreach ($connector in $connectors) {
            $status = Invoke-RestMethod -Uri "$CONNECT_URL/connectors/$connector/status" -Method Get
            $state = $status.connector.state
            $color = switch ($state) {
                "RUNNING" { "Green" }
                "PAUSED" { "Yellow" }
                default { "Red" }
            }
            Write-Host "  - $connector : $state" -ForegroundColor $color
            
            # Show task status
            foreach ($task in $status.tasks) {
                $taskColor = if ($task.state -eq "RUNNING") { "Green" } else { "Red" }
                Write-Host "      Task $($task.id): $($task.state)" -ForegroundColor $taskColor
                if ($task.trace) {
                    Write-Host "      Error: $($task.trace.Substring(0, [Math]::Min(100, $task.trace.Length)))..." -ForegroundColor Red
                }
            }
        }
    }
} catch {
    Write-Host "  Error getting connectors: $_" -ForegroundColor Red
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
Write-Host "Check container logs:"
Write-Host "  docker logs connect -f" -ForegroundColor Gray
Write-Host ""

