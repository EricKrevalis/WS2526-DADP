# AWS Lambda Deployment Script for PowerShell
# This script creates Lambda functions for continuous data ingestion

# Configuration - Update these values
$FUNCTION_NAME_PREFIX = "data-ingest"
$S3_BUCKET = $env:S3_BUCKET
if (-not $S3_BUCKET) { $S3_BUCKET = "your-data-bucket-name" }
$S3_OUTPUT_PATH = $env:S3_OUTPUT_PATH
if (-not $S3_OUTPUT_PATH) { $S3_OUTPUT_PATH = "ingest-data" }
$REGION = $env:REGION
if (-not $REGION) { $REGION = "eu-central-1" }
$LAMBDA_ROLE_NAME = $env:LAMBDA_ROLE_NAME
if (-not $LAMBDA_ROLE_NAME) { $LAMBDA_ROLE_NAME = "lambda-data-ingest-role" }

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

# Create IAM role for Lambda if it doesn't exist
Write-Host "Creating/Checking IAM role..." -ForegroundColor Yellow
try {
    $ROLE_ARN = (aws iam get-role --role-name $LAMBDA_ROLE_NAME --query 'Role.Arn' --output text 2>$null)
} catch {
    $ROLE_ARN = $null
}

if (-not $ROLE_ARN) {
    Write-Host "Creating IAM role: $LAMBDA_ROLE_NAME" -ForegroundColor Green
    
    # Create trust policy
    $trustPolicy = @{
        Version = "2012-10-17"
        Statement = @(
            @{
                Effect = "Allow"
                Principal = @{
                    Service = "lambda.amazonaws.com"
                }
                Action = "sts:AssumeRole"
            }
        )
    } | ConvertTo-Json -Depth 10
    
    $trustPolicy | Out-File -FilePath "$env:TEMP\trust-policy.json" -Encoding utf8
    
    # Create role
    aws iam create-role `
        --role-name $LAMBDA_ROLE_NAME `
        --assume-role-policy-document "file://$env:TEMP\trust-policy.json" `
        --description "Role for data ingestion Lambda functions" | Out-Null
    
    # Attach policies
    aws iam attach-role-policy `
        --role-name $LAMBDA_ROLE_NAME `
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" | Out-Null
    
    # Create and attach S3 policy
    $s3Policy = @{
        Version = "2012-10-17"
        Statement = @(
            @{
                Effect = "Allow"
                Action = @("s3:PutObject", "s3:GetObject")
                Resource = "arn:aws:s3:::$S3_BUCKET/*"
            }
        )
    } | ConvertTo-Json -Depth 10
    
    $s3Policy | Out-File -FilePath "$env:TEMP\s3-policy.json" -Encoding utf8
    
    $POLICY_ARN = (aws iam create-policy `
        --policy-name "${LAMBDA_ROLE_NAME}-s3-policy" `
        --policy-document "file://$env:TEMP\s3-policy.json" `
        --query 'Policy.Arn' --output text)
    
    aws iam attach-role-policy `
        --role-name $LAMBDA_ROLE_NAME `
        --policy-arn $POLICY_ARN | Out-Null
    
    $ROLE_ARN = (aws iam get-role --role-name $LAMBDA_ROLE_NAME --query 'Role.Arn' --output text)
    Write-Host "Role created: $ROLE_ARN" -ForegroundColor Green
} else {
    Write-Host "Role already exists: $ROLE_ARN" -ForegroundColor Green
}

# Install dependencies
Write-Host ""
Write-Host "Installing dependencies..." -ForegroundColor Yellow
if (Test-Path "package") {
    Remove-Item -Recurse -Force "package"
}
New-Item -ItemType Directory -Path "package" | Out-Null

# Try pip, then pip3
try {
    pip install -r requirements.txt -t ./package --quiet 2>$null
} catch {
    pip3 install -r requirements.txt -t ./package --quiet 2>$null
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

try {
    aws lambda get-function --function-name $FUNCTION_NAME --region $REGION | Out-Null
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
} catch {
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
}

# Flow Ingest Function
$FUNCTION_NAME = "${FUNCTION_NAME_PREFIX}-flow-ingest"
$envVars = "S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH,TOMTOM_API_KEY=$TOMTOM_API_KEY"

try {
    aws lambda get-function --function-name $FUNCTION_NAME --region $REGION | Out-Null
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
} catch {
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
}

# Weather Ingest Function
$FUNCTION_NAME = "${FUNCTION_NAME_PREFIX}-weather-ingest"
$envVars = "S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH,WEATHER_API_KEY=$WEATHER_API_KEY"

try {
    aws lambda get-function --function-name $FUNCTION_NAME --region $REGION | Out-Null
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
} catch {
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

