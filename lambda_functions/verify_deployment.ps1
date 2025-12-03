# Verification script for Lambda deployment
# Checks if Lambda functions exist, are scheduled, and are working

$REGION = $env:REGION
if (-not $REGION) { $REGION = "eu-north-1" }

$S3_BUCKET = $env:S3_BUCKET
if (-not $S3_BUCKET) { $S3_BUCKET = "youngleebigdatabucket" }

Write-Host "=== Lambda Deployment Verification ===" -ForegroundColor Cyan
Write-Host "Region: $REGION"
Write-Host "S3 Bucket: $S3_BUCKET"
Write-Host ""

# Check Lambda Functions
Write-Host "1. Checking Lambda Functions..." -ForegroundColor Yellow
$functions = @("data-ingest-db-scraper", "data-ingest-tomtom-scraper", "data-ingest-owm-scraper")
$allFunctionsExist = $true

foreach ($func in $functions) {
    $check = aws lambda get-function --function-name $func --region $REGION 2>&1
    if ($LASTEXITCODE -eq 0) {
        $state = (aws lambda get-function-configuration --function-name $func --region $REGION --query 'State' --output text 2>&1)
        Write-Host "  [OK] $func (State: $state)" -ForegroundColor Green
    } else {
        Write-Host "  [MISSING] $func" -ForegroundColor Red
        $allFunctionsExist = $false
    }
}

Write-Host ""

# Check EventBridge Schedules
Write-Host "2. Checking EventBridge Schedules..." -ForegroundColor Yellow
$schedules = @("lambda-db-scraper-schedule", "lambda-tomtom-scraper-schedule", "lambda-owm-scraper-schedule")
$allSchedulesExist = $true

foreach ($schedule in $schedules) {
    $check = aws events describe-rule --name $schedule --region $REGION 2>&1
    if ($LASTEXITCODE -eq 0) {
        $state = (aws events describe-rule --name $schedule --region $REGION --query 'State' --output text 2>&1)
        Write-Host "  [OK] $schedule (State: $state)" -ForegroundColor Green
    } else {
        Write-Host "  [MISSING] $schedule" -ForegroundColor Red
        $allSchedulesExist = $false
    }
}

Write-Host ""

# Check S3 Bucket for Data
Write-Host "3. Checking S3 Bucket for Data..." -ForegroundColor Yellow
$s3Check = aws s3 ls "s3://$S3_BUCKET/ingest-data/" --recursive 2>&1
if ($LASTEXITCODE -eq 0) {
    $fileCount = ($s3Check | Measure-Object -Line).Lines
    if ($fileCount -gt 0) {
        Write-Host "  [OK] Found $fileCount files in S3 bucket" -ForegroundColor Green
        
        # Show latest files
        Write-Host "  Latest files:" -ForegroundColor Cyan
        $latest = aws s3 ls "s3://$S3_BUCKET/ingest-data/" --recursive | Sort-Object -Descending | Select-Object -First 5
        foreach ($file in $latest) {
            Write-Host "    $file" -ForegroundColor Gray
        }
    } else {
        Write-Host "  [NO DATA] No files found yet (functions may not have run yet)" -ForegroundColor Yellow
    }
} else {
    Write-Host "  [ERROR] Cannot access S3 bucket: $S3_BUCKET" -ForegroundColor Red
    Write-Host "    Error: $s3Check" -ForegroundColor Gray
}

Write-Host ""

# Check Recent Invocations
Write-Host "4. Checking Recent Function Invocations..." -ForegroundColor Yellow
foreach ($func in $functions) {
    $invocations = aws lambda get-function --function-name $func --region $REGION 2>&1
    if ($LASTEXITCODE -eq 0) {
        # Get recent logs
        $logGroup = "/aws/lambda/$func"
        $logs = aws logs tail $logGroup --since 1h --region $REGION 2>&1 | Select-Object -First 3
        if ($logs -and $logs.Count -gt 0) {
            Write-Host "  [OK] $func has recent activity" -ForegroundColor Green
        } else {
            Write-Host "  [NO ACTIVITY] $func - no recent invocations" -ForegroundColor Yellow
        }
    }
}

Write-Host ""
Write-Host "=== Verification Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "To manually test a function:" -ForegroundColor Yellow
Write-Host "  aws lambda invoke --function-name data-ingest-db-scraper --region $REGION response.json" -ForegroundColor Gray
Write-Host ""
Write-Host "To view logs:" -ForegroundColor Yellow
Write-Host "  aws logs tail /aws/lambda/data-ingest-db-scraper --follow --region $REGION" -ForegroundColor Gray
Write-Host ""
Write-Host "To check S3 data:" -ForegroundColor Yellow
Write-Host "  aws s3 ls s3://$S3_BUCKET/ingest-data/ --recursive" -ForegroundColor Gray

