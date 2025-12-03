# AWS Lambda Deployment Script for PowerShell
# This script creates Lambda functions for continuous data ingestion

# Configuration - Update these values
$FUNCTION_NAME_PREFIX = "data-ingest"
$S3_BUCKET = $env:S3_BUCKET
if (-not $S3_BUCKET) { $S3_BUCKET = "your-data-bucket-name" }
$S3_OUTPUT_PATH = $env:S3_OUTPUT_PATH
if (-not $S3_OUTPUT_PATH) { $S3_OUTPUT_PATH = "ingest-data" }
$REGION = $env:REGION
if (-not $REGION) { $REGION = "eu-north-1" }
$LAMBDA_ROLE_NAME = $env:LAMBDA_ROLE_NAME
if (-not $LAMBDA_ROLE_NAME) { $LAMBDA_ROLE_NAME = "younglambda" }

# API Keys - Set these as environment variables
$TOMTOM_API_KEY = $env:TOMTOM_API_KEY
$WEATHER_API_KEY = $env:WEATHER_API_KEY

# Lambda configuration
$MEMORY_SIZE = 256
$TIMEOUT = 300  # 5 minutes
$RUNTIME = "python3.10"

Write-Host "=== AWS Lambda Data Ingestion Deployment ===" -ForegroundColor Cyan
Write-Host ""

# Get AWS account ID
$ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)
Write-Host "AWS Account ID: $ACCOUNT_ID"
Write-Host "Region: $REGION"
Write-Host ""

# Get IAM role ARN (role should already exist - created via console)
Write-Host "Checking IAM role..." -ForegroundColor Yellow
$roleCheck = aws iam get-role --role-name $LAMBDA_ROLE_NAME --query 'Role.Arn' --output text 2>&1
if ($LASTEXITCODE -eq 0 -and $roleCheck) {
    $ROLE_ARN = $roleCheck
    Write-Host "Role found: $ROLE_ARN" -ForegroundColor Green
} else {
    Write-Host "ERROR: IAM role '$LAMBDA_ROLE_NAME' not found!" -ForegroundColor Red
    Write-Host "Please create the role via AWS Console first:" -ForegroundColor Yellow
    Write-Host "  1. Go to IAM -> Roles -> Create role" -ForegroundColor Yellow
    Write-Host "  2. Select 'Lambda' as service" -ForegroundColor Yellow
    Write-Host "  3. Attach 'AWSLambdaBasicExecutionRole' policy" -ForegroundColor Yellow
    Write-Host "  4. Add S3 access policy for bucket: $S3_BUCKET" -ForegroundColor Yellow
    Write-Host "  5. Name it: $LAMBDA_ROLE_NAME" -ForegroundColor Yellow
    exit 1
}

# Install dependencies
Write-Host ""
Write-Host "Installing dependencies..." -ForegroundColor Yellow
if (Test-Path "package") {
    Remove-Item -Recurse -Force "package"
}
New-Item -ItemType Directory -Path "package" | Out-Null

# Install dependencies using python -m pip (more reliable)
python -m pip install -r requirements.txt -t ./package --quiet 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Warning: pip install had issues, continuing anyway..." -ForegroundColor Yellow
}

# Create deployment packages
Write-Host "Creating deployment packages..." -ForegroundColor Yellow

# DB Scraper
Write-Host "Packaging DB scraper..."
Compress-Archive -Path "package\*", "lambda_db_scraper.py" -DestinationPath "db_scraper.zip" -Force

# Flow Ingest
Write-Host "Packaging Flow ingest..."
Compress-Archive -Path "package\*", "lambda_flow_ingest.py" -DestinationPath "flow_ingest.zip" -Force

# Weather Ingest
Write-Host "Packaging Weather ingest..."
Compress-Archive -Path "package\*", "lambda_weather_ingest.py" -DestinationPath "weather_ingest.zip" -Force

# Create or update Lambda functions
Write-Host ""
Write-Host "Creating/Updating Lambda functions..." -ForegroundColor Yellow

# DB Scraper Function
$FUNCTION_NAME = "${FUNCTION_NAME_PREFIX}-db-scraper"
$envVars = "S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH"

$functionCheck = aws lambda get-function --function-name $FUNCTION_NAME --region $REGION 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "Updating function: $FUNCTION_NAME"
    aws lambda update-function-code `
        --function-name $FUNCTION_NAME `
        --zip-file "fileb://db_scraper.zip" `
        --region $REGION | Out-Null
    
    aws lambda update-function-configuration `
        --function-name $FUNCTION_NAME `
        --environment "Variables={$envVars}" `
        --timeout $TIMEOUT `
        --memory-size $MEMORY_SIZE `
        --region $REGION | Out-Null
} else {
    Write-Host "Creating function: $FUNCTION_NAME"
    aws lambda create-function `
        --function-name $FUNCTION_NAME `
        --runtime $RUNTIME `
        --role $ROLE_ARN `
        --handler lambda_db_scraper.lambda_handler `
        --zip-file "fileb://db_scraper.zip" `
        --timeout $TIMEOUT `
        --memory-size $MEMORY_SIZE `
        --environment "Variables={$envVars}" `
        --region $REGION | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to create function $FUNCTION_NAME" -ForegroundColor Red
    }
}

# Flow Ingest Function
$FUNCTION_NAME = "${FUNCTION_NAME_PREFIX}-flow-ingest"
$envVars = "S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH,TOMTOM_API_KEY=$TOMTOM_API_KEY"

$functionCheck = aws lambda get-function --function-name $FUNCTION_NAME --region $REGION 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "Updating function: $FUNCTION_NAME"
    aws lambda update-function-code `
        --function-name $FUNCTION_NAME `
        --zip-file "fileb://flow_ingest.zip" `
        --region $REGION | Out-Null
    
    aws lambda update-function-configuration `
        --function-name $FUNCTION_NAME `
        --environment "Variables={$envVars}" `
        --timeout $TIMEOUT `
        --memory-size $MEMORY_SIZE `
        --region $REGION | Out-Null
} else {
    Write-Host "Creating function: $FUNCTION_NAME"
    aws lambda create-function `
        --function-name $FUNCTION_NAME `
        --runtime $RUNTIME `
        --role $ROLE_ARN `
        --handler lambda_flow_ingest.lambda_handler `
        --zip-file "fileb://flow_ingest.zip" `
        --timeout $TIMEOUT `
        --memory-size $MEMORY_SIZE `
        --environment "Variables={$envVars}" `
        --region $REGION | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to create function $FUNCTION_NAME" -ForegroundColor Red
    }
}

# Weather Ingest Function
$FUNCTION_NAME = "${FUNCTION_NAME_PREFIX}-weather-ingest"
$envVars = "S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH,WEATHER_API_KEY=$WEATHER_API_KEY"

$functionCheck = aws lambda get-function --function-name $FUNCTION_NAME --region $REGION 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "Updating function: $FUNCTION_NAME"
    aws lambda update-function-code `
        --function-name $FUNCTION_NAME `
        --zip-file "fileb://weather_ingest.zip" `
        --region $REGION | Out-Null
    
    aws lambda update-function-configuration `
        --function-name $FUNCTION_NAME `
        --environment "Variables={$envVars}" `
        --timeout $TIMEOUT `
        --memory-size $MEMORY_SIZE `
        --region $REGION | Out-Null
} else {
    Write-Host "Creating function: $FUNCTION_NAME"
    aws lambda create-function `
        --function-name $FUNCTION_NAME `
        --runtime $RUNTIME `
        --role $ROLE_ARN `
        --handler lambda_weather_ingest.lambda_handler `
        --zip-file "fileb://weather_ingest.zip" `
        --timeout $TIMEOUT `
        --memory-size $MEMORY_SIZE `
        --environment "Variables={$envVars}" `
        --region $REGION | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Failed to create function $FUNCTION_NAME" -ForegroundColor Red
    }
}

# Create EventBridge rules for scheduling
Write-Host ""
Write-Host "Creating EventBridge schedules..." -ForegroundColor Yellow

# DB Scraper - Every 10 minutes
$RULE_NAME = "lambda-db-scraper-schedule"
aws events put-rule `
    --name $RULE_NAME `
    --schedule-expression "rate(10 minutes)" `
    --state ENABLED `
    --region $REGION | Out-Null

aws events put-targets `
    --rule $RULE_NAME `
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME_PREFIX}-db-scraper" `
    --region $REGION | Out-Null

aws lambda add-permission `
    --function-name "${FUNCTION_NAME_PREFIX}-db-scraper" `
    --statement-id allow-eventbridge `
    --action lambda:InvokeFunction `
    --principal events.amazonaws.com `
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" `
    --region $REGION 2>$null

# Flow Ingest - Every 10 minutes
$RULE_NAME = "lambda-flow-ingest-schedule"
aws events put-rule `
    --name $RULE_NAME `
    --schedule-expression "rate(10 minutes)" `
    --state ENABLED `
    --region $REGION | Out-Null

aws events put-targets `
    --rule $RULE_NAME `
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME_PREFIX}-flow-ingest" `
    --region $REGION | Out-Null

aws lambda add-permission `
    --function-name "${FUNCTION_NAME_PREFIX}-flow-ingest" `
    --statement-id allow-eventbridge `
    --action lambda:InvokeFunction `
    --principal events.amazonaws.com `
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" `
    --region $REGION 2>$null

# Weather Ingest - Every 10 minutes
$RULE_NAME = "lambda-weather-ingest-schedule"
aws events put-rule `
    --name $RULE_NAME `
    --schedule-expression "rate(10 minutes)" `
    --state ENABLED `
    --region $REGION | Out-Null

aws events put-targets `
    --rule $RULE_NAME `
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME_PREFIX}-weather-ingest" `
    --region $REGION | Out-Null

aws lambda add-permission `
    --function-name "${FUNCTION_NAME_PREFIX}-weather-ingest" `
    --statement-id allow-eventbridge `
    --action lambda:InvokeFunction `
    --principal events.amazonaws.com `
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" `
    --region $REGION 2>$null

# Cleanup
Write-Host ""
Write-Host "Cleaning up temporary files..." -ForegroundColor Yellow
Remove-Item -Recurse -Force "package" -ErrorAction SilentlyContinue
Remove-Item -Force "db_scraper.zip", "flow_ingest.zip", "weather_ingest.zip" -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "=== Deployment Complete! ===" -ForegroundColor Green
Write-Host ""
Write-Host "Lambda Functions:"
Write-Host "  - ${FUNCTION_NAME_PREFIX}-db-scraper"
Write-Host "  - ${FUNCTION_NAME_PREFIX}-flow-ingest"
Write-Host "  - ${FUNCTION_NAME_PREFIX}-weather-ingest"
Write-Host ""
Write-Host "Schedules:"
Write-Host "  - All functions run every 10 minutes"
Write-Host ""
Write-Host "Monitor functions:"
Write-Host "  aws lambda list-functions --region $REGION"
Write-Host "  aws logs tail /aws/lambda/${FUNCTION_NAME_PREFIX}-db-scraper --follow --region $REGION"

