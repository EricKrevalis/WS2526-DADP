# Quick script to check deployment status
Write-Host "=== Deployment Status Check ===" -ForegroundColor Cyan
Write-Host ""

# Check zip files
Write-Host "Zip Files:" -ForegroundColor Yellow
$zips = @("db_scraper.zip", "tomtom_scraper.zip", "owm_scraper.zip")
foreach ($zip in $zips) {
    if (Test-Path $zip) {
        $size = (Get-Item $zip).Length / 1MB
        $sizeMB = [math]::Round($size, 2)
        Write-Host "  [OK] $zip ($sizeMB MB)" -ForegroundColor Green
    } else {
        Write-Host "  [MISSING] $zip" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "Checking Lambda functions..." -ForegroundColor Yellow
$REGION = $env:REGION
if (-not $REGION) { $REGION = "eu-north-1" }

$functions = @("data-ingest-db-scraper", "data-ingest-tomtom-scraper", "data-ingest-owm-scraper")
foreach ($func in $functions) {
    $check = aws lambda get-function --function-name $func --region $REGION 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] $func (exists)" -ForegroundColor Green
    } else {
        Write-Host "  [NOT FOUND] $func" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "To continue deployment, run: .\deploy_lambda.ps1" -ForegroundColor Yellow

