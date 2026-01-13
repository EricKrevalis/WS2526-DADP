# Grafana Setup Script for Windows

Write-Host "=== Grafana Setup ===" -ForegroundColor Cyan

# Check if Docker is running
Write-Host "`n1. Checking Docker..." -ForegroundColor Yellow
try {
    docker ps | Out-Null
    Write-Host "   ✓ Docker is running" -ForegroundColor Green
} catch {
    Write-Host "   ✗ Docker is not running" -ForegroundColor Red
    Write-Host "   Please start Docker Desktop" -ForegroundColor Yellow
    exit 1
}

# Check if PostgreSQL is running
Write-Host "`n2. Checking PostgreSQL..." -ForegroundColor Yellow
$pgService = Get-Service -Name "postgresql*" -ErrorAction SilentlyContinue
if ($pgService -and $pgService.Status -eq "Running") {
    Write-Host "   ✓ PostgreSQL is running" -ForegroundColor Green
} else {
    Write-Host "   ⚠ PostgreSQL service not found or not running" -ForegroundColor Yellow
    Write-Host "   Make sure PostgreSQL is installed and running" -ForegroundColor Yellow
}

# Check if .env file exists
Write-Host "`n3. Checking environment file..." -ForegroundColor Yellow
$envFile = Join-Path (Split-Path -Parent $PSScriptRoot) ".env"
if (Test-Path $envFile) {
    Write-Host "   ✓ .env file found" -ForegroundColor Green
    $envContent = Get-Content $envFile
    if ($envContent -match "POSTGRES_PASSWORD") {
        Write-Host "   ✓ POSTGRES_PASSWORD found in .env" -ForegroundColor Green
    } else {
        Write-Host "   ⚠ POSTGRES_PASSWORD not found in .env" -ForegroundColor Yellow
    }
} else {
    Write-Host "   ⚠ .env file not found" -ForegroundColor Yellow
    Write-Host "   Create .env file in project root with POSTGRES_PASSWORD" -ForegroundColor Yellow
}

# Navigate to grafana directory
$grafanaDir = Join-Path $PSScriptRoot "grafana"
if (-not (Test-Path $grafanaDir)) {
    Write-Host "`n4. Creating grafana directory..." -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $grafanaDir | Out-Null
}

Write-Host "`n5. Starting Grafana..." -ForegroundColor Yellow
Set-Location $grafanaDir

# Load .env if it exists
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]*)\s*=\s*(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($key, $value, "Process")
        }
    }
}

docker-compose up -d

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ Grafana started successfully!" -ForegroundColor Green
    Write-Host "`nAccess Grafana at: http://localhost:3000" -ForegroundColor Cyan
    Write-Host "Default credentials:" -ForegroundColor Yellow
    Write-Host "  Username: admin" -ForegroundColor Gray
    Write-Host "  Password: admin" -ForegroundColor Gray
    Write-Host "`n(You'll be prompted to change the password on first login)" -ForegroundColor Yellow
} else {
    Write-Host "`n❌ Failed to start Grafana" -ForegroundColor Red
    Write-Host "Check Docker logs: docker-compose logs" -ForegroundColor Yellow
}


