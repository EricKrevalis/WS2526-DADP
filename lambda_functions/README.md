# AWS Lambda Data Ingestion Functions

This directory contains AWS Lambda functions for continuous data collection from various APIs. Lambda is **92% cheaper** than AWS Glue for this use case (~$1/month vs ~$12.50/month).

## Overview

The Lambda functions collect data from:
- **DB Transport API**: Public transport departures for major German cities
- **TomTom Traffic API**: Traffic flow data for major German cities
- **OpenWeatherMap API**: Weather data for major German cities

All data is written to S3 in JSONL format, organized by date and time.

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **AWS CLI** configured with credentials
3. **Python 3.10+** installed locally (for packaging)
4. **S3 Bucket** for storing data
5. **IAM Permissions** to create Lambda functions, IAM roles, and EventBridge rules

## Quick Start

### 1. Set Environment Variables

```bash
# Linux/Mac
export S3_BUCKET="your-data-bucket-name"
export S3_OUTPUT_PATH="ingest-data"
export TOMTOM_API_KEY="your-tomtom-api-key"
export WEATHER_API_KEY="your-weather-api-key"
export REGION="eu-central-1"

# Windows PowerShell
$env:S3_BUCKET = "your-data-bucket-name"
$env:S3_OUTPUT_PATH = "ingest-data"
$env:TOMTOM_API_KEY = "your-tomtom-api-key"
$env:WEATHER_API_KEY = "your-weather-api-key"
$env:REGION = "eu-central-1"
```

### 2. Deploy Lambda Functions

```bash
# Linux/Mac
cd lambda_functions
chmod +x deploy_lambda.sh
./deploy_lambda.sh

# Windows PowerShell
cd lambda_functions
.\deploy_lambda.ps1
```

The deployment script will:
- Create IAM role with necessary permissions
- Install Python dependencies
- Package Lambda functions
- Create/update Lambda functions
- Set up EventBridge schedules (every 10 minutes)

## Manual Deployment

### 1. Create IAM Role

```bash
# Create trust policy
cat > trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

# Create role
aws iam create-role \
    --role-name lambda-data-ingest-role \
    --assume-role-policy-document file://trust-policy.json

# Attach policies
aws iam attach-role-policy \
    --role-name lambda-data-ingest-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

### 2. Package Lambda Functions

```bash
# Install dependencies
pip install -r requirements.txt -t ./package

# Package DB Scraper
cd package
zip -r ../db_scraper.zip .
cd ..
zip db_scraper.zip lambda_db_scraper.py

# Package Flow Ingest
cd package
zip -r ../flow_ingest.zip .
cd ..
zip flow_ingest.zip lambda_flow_ingest.py

# Package Weather Ingest
cd package
zip -r ../weather_ingest.zip .
cd ..
zip weather_ingest.zip lambda_weather_ingest.py
```

### 3. Create Lambda Functions

```bash
# DB Scraper
aws lambda create-function \
    --function-name data-ingest-db-scraper \
    --runtime python3.10 \
    --role arn:aws:iam::ACCOUNT_ID:role/lambda-data-ingest-role \
    --handler lambda_db_scraper.lambda_handler \
    --zip-file fileb://db_scraper.zip \
    --timeout 300 \
    --memory-size 256 \
    --environment "Variables={S3_BUCKET=your-bucket,S3_OUTPUT_PATH=ingest-data}"

# Flow Ingest
aws lambda create-function \
    --function-name data-ingest-flow-ingest \
    --runtime python3.10 \
    --role arn:aws:iam::ACCOUNT_ID:role/lambda-data-ingest-role \
    --handler lambda_flow_ingest.lambda_handler \
    --zip-file fileb://flow_ingest.zip \
    --timeout 300 \
    --memory-size 256 \
    --environment "Variables={S3_BUCKET=your-bucket,S3_OUTPUT_PATH=ingest-data,TOMTOM_API_KEY=your-key}"

# Weather Ingest
aws lambda create-function \
    --function-name data-ingest-weather-ingest \
    --runtime python3.10 \
    --role arn:aws:iam::ACCOUNT_ID:role/lambda-data-ingest-role \
    --handler lambda_weather_ingest.lambda_handler \
    --zip-file fileb://weather_ingest.zip \
    --timeout 300 \
    --memory-size 256 \
    --environment "Variables={S3_BUCKET=your-bucket,S3_OUTPUT_PATH=ingest-data,WEATHER_API_KEY=your-key}"
```

### 4. Create EventBridge Schedules

```bash
# DB Scraper - Every 10 minutes
aws events put-rule \
    --name lambda-db-scraper-schedule \
    --schedule-expression "rate(10 minutes)" \
    --state ENABLED

aws events put-targets \
    --rule lambda-db-scraper-schedule \
    --targets "Id=1,Arn=arn:aws:lambda:REGION:ACCOUNT:function:data-ingest-db-scraper"

aws lambda add-permission \
    --function-name data-ingest-db-scraper \
    --statement-id allow-eventbridge \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:REGION:ACCOUNT:rule/lambda-db-scraper-schedule"

# Repeat for flow-ingest and weather-ingest
```

## Configuration

### Environment Variables

Each Lambda function uses these environment variables:

**Common:**
- `S3_BUCKET`: S3 bucket name (required)
- `S3_OUTPUT_PATH`: S3 prefix path (default: `ingest-data`)

**Flow Ingest:**
- `TOMTOM_API_KEY`: TomTom API key (required)

**Weather Ingest:**
- `WEATHER_API_KEY`: OpenWeatherMap API key (required)

### Lambda Settings

- **Runtime**: Python 3.10
- **Memory**: 256 MB (adjust if needed)
- **Timeout**: 300 seconds (5 minutes)
- **Handler**: `[function_name].lambda_handler`

## Monitoring

### CloudWatch Logs

View logs for each function:

```bash
# Tail logs
aws logs tail /aws/lambda/data-ingest-db-scraper --follow

# View recent logs
aws logs get-log-events \
    --log-group-name /aws/lambda/data-ingest-db-scraper \
    --log-stream-name [stream-name]
```

### CloudWatch Metrics

Monitor function metrics:
- Invocations
- Duration
- Errors
- Throttles

```bash
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Invocations \
    --dimensions Name=FunctionName,Value=data-ingest-db-scraper \
    --start-time 2024-01-01T00:00:00Z \
    --end-time 2024-01-02T00:00:00Z \
    --period 3600 \
    --statistics Sum
```

### Test Functions Manually

```bash
# Invoke function
aws lambda invoke \
    --function-name data-ingest-db-scraper \
    --payload '{}' \
    response.json

# View response
cat response.json
```

## Cost Analysis

**Running every 10 minutes (4,320 runs/month per function):**

| Component | Monthly Cost |
|-----------|--------------|
| Lambda Invocations (3 functions) | ~$0.04 |
| Lambda Compute Time | ~$0.08 |
| S3 Storage & Requests | ~$0.12 |
| CloudWatch Logs | ~$0.50 |
| **Total** | **~$1.00/month** |

**Comparison:**
- AWS Lambda: **~$1.00/month** ⭐
- AWS Glue: ~$12.50/month
- **Savings: 92%**

## Troubleshooting

### Function Times Out

- Increase timeout in function configuration
- Optimize API calls (reduce sleep times if rate limits allow)
- Check CloudWatch logs for slow operations

### Permission Errors

- Verify IAM role has S3 write permissions
- Check Lambda execution role policies
- Ensure EventBridge has permission to invoke Lambda

### Module Not Found Errors

- Ensure dependencies are included in deployment package
- Check that `requirements.txt` includes all needed packages
- Verify package structure includes all files

### API Rate Limits

- Adjust sleep intervals in code
- Reduce execution frequency
- Implement exponential backoff retry logic

## Updating Functions

After making code changes:

```bash
# Re-package
pip install -r requirements.txt -t ./package
cd package && zip -r ../function.zip . && cd ..
zip function.zip lambda_[function_name].py

# Update function
aws lambda update-function-code \
    --function-name data-ingest-[function-name] \
    --zip-file fileb://function.zip
```

Or use the deployment script which handles updates automatically.

## S3 Data Structure

Data will be organized in S3 as:

```
s3://your-bucket/ingest-data/
├── traffic_data/
│   └── YYYY/MM/DD/HHMMSS_traffic_data.jsonl
├── flow_data/
│   └── YYYY/MM/DD/HHMMSS_flow_data.jsonl
└── weather_data/
    └── YYYY/MM/DD/HHMMSS_weather_data.jsonl
```

## Best Practices

1. **Monitor Costs**: Set up CloudWatch billing alarms
2. **Error Handling**: Functions include error handling and logging
3. **Idempotency**: Functions can be safely retried
4. **Security**: API keys stored as environment variables (use AWS Secrets Manager for production)
5. **Scaling**: Lambda automatically scales to handle concurrent invocations

## Migration from Glue

If migrating from AWS Glue:
1. Functions use same data format and S3 structure
2. No changes needed to downstream consumers
3. Simply deploy Lambda functions and disable Glue jobs
4. Cost savings: ~$11.50/month

