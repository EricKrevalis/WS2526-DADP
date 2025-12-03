# Download S3 data from Lambda ingestion to local device
# Usage: .\download_s3_data.ps1 -BucketName "bucket-name" [-OutputDir "./downloaded_data"] [-S3Prefix "ingest-data"]

param(
    [Parameter(Mandatory=$true)]
    [string]$BucketName,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputDir = "./downloaded_data",
    
    [Parameter(Mandatory=$false)]
    [string]$S3Prefix = "ingest-data"
)

Write-Host "=== Downloading S3 Data ===" -ForegroundColor Cyan
Write-Host "Bucket: $BucketName"
Write-Host "Prefix: $S3Prefix"
Write-Host "Output Directory: $OutputDir"
Write-Host ""

# Create output directory
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir | Out-Null
}

# Download all data
Write-Host "Downloading traffic data..." -ForegroundColor Yellow
$trafficPath = Join-Path $OutputDir "traffic_data"
if (-not (Test-Path $trafficPath)) {
    New-Item -ItemType Directory -Path $trafficPath | Out-Null
}
aws s3 sync "s3://${BucketName}/${S3Prefix}/traffic_data/" $trafficPath --quiet

Write-Host "Downloading flow data..." -ForegroundColor Yellow
$flowPath = Join-Path $OutputDir "flow_data"
if (-not (Test-Path $flowPath)) {
    New-Item -ItemType Directory -Path $flowPath | Out-Null
}
aws s3 sync "s3://${BucketName}/${S3Prefix}/flow_data/" $flowPath --quiet

Write-Host "Downloading weather data..." -ForegroundColor Yellow
$weatherPath = Join-Path $OutputDir "weather_data"
if (-not (Test-Path $weatherPath)) {
    New-Item -ItemType Directory -Path $weatherPath | Out-Null
}
aws s3 sync "s3://${BucketName}/${S3Prefix}/weather_data/" $weatherPath --quiet

Write-Host ""
Write-Host "=== Download Complete ===" -ForegroundColor Green
Write-Host "Data saved to: $OutputDir"
Write-Host ""
Write-Host "File counts:"
$trafficFiles = (Get-ChildItem -Path $trafficPath -File -Recurse -ErrorAction SilentlyContinue).Count
$flowFiles = (Get-ChildItem -Path $flowPath -File -Recurse -ErrorAction SilentlyContinue).Count
$weatherFiles = (Get-ChildItem -Path $weatherPath -File -Recurse -ErrorAction SilentlyContinue).Count
Write-Host "  Traffic files: $trafficFiles"
Write-Host "  Flow files: $flowFiles"
Write-Host "  Weather files: $weatherFiles"

