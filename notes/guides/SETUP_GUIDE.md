# AWS Services Setup Guide

Complete step-by-step guide to set up AWS services for data ingestion Lambda functions.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [AWS Account Setup](#aws-account-setup)
3. [AWS CLI Configuration](#aws-cli-configuration)
4. [S3 Bucket Creation](#s3-bucket-creation)
5. [IAM Role Creation](#iam-role-creation)
6. [Deploy Lambda Functions](#deploy-lambda-functions)
7. [Configure EventBridge Schedules](#configure-eventbridge-schedules)
8. [Testing](#testing)
9. [Monitoring](#monitoring)
10. [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before starting, ensure you have:

- ✅ AWS Account (create at https://aws.amazon.com/) 
- ✅ AWS CLI installed (download from https://aws.amazon.com/cli/)
- ✅ Python 3.10+ installed
- ✅ API Keys:
  - TomTom API Key (get from https://developer.tomtom.com/)
  - OpenWeatherMap API Key (get from https://openweathermap.org/api)
- ✅ Basic understanding of AWS services (S3, Lambda, IAM)

---

## AWS Account Setup

### 1. Create AWS Account

If you don't have an AWS account:

1. Go to https://aws.amazon.com/
2. Click "Create an AWS Account"
3. Follow the registration process
4. Complete payment information (free tier available)
5. Verify your email address

### 2. Access AWS Console

1. Go to https://console.aws.amazon.com/
2. Sign in with your AWS account credentials
3. Select your preferred region (e.g., `eu-central-1` for Frankfurt)

**Important**: Note your AWS Account ID (found in top-right corner of console)

---

## AWS CLI Configuration

### 1. Install AWS CLI

**Windows:**
```powershell
# Download MSI installer from AWS website
# Or use Chocolatey:
choco install awscli
```

### 2. Configure AWS CLI

```bash
aws configure
```

You'll be prompted for:

```
AWS Access Key ID: [Your Access Key]
AWS Secret Access Key: [Your Secret Key]
Default region name: eu-central-1
Default output format: json
``

**Security Best Practice**: Create a dedicated IAM user for CLI access (not root account)

### 4. Verify Configuration

```bash
# Test AWS CLI connection
aws sts get-caller-identity

# Should return:
# {
#     "UserId": "...",
#     "Account": "123456789012",
#     "Arn": "arn:aws:iam::123456789012:user/your-username"
# }
```

---

## S3 Bucket Creation

### Option 1: Using AWS CLI

```bash
# Set your bucket name (must be globally unique)
youngleebigdatabucket="your-unique-bucket-name-2024"

# Create bucket in your region
aws s3 mb s3://youngleebigdatabucket --region eu-central-1

# Create folder structure
aws s3api put-object --bucket youngleebigdatabucket --key ingest-data/
aws s3api put-object --bucket youngleebigdatabucket --key ingest-data/traffic_data/
aws s3api put-object --bucket youngleebigdatabucket --key ingest-data/flow_data/
aws s3api put-object --bucket youngleebigdatabucket --key ingest-data/weather_data/

# Verify bucket creation
aws s3 ls s3://youngleebigdatabucket
```

### Option 2: Using AWS Console

1. Go to AWS Console → S3
2. Click "Create bucket"
3. Enter bucket name (must be globally unique)
4. Select region: `eu-central-1` (or your preferred region)
5. **Uncheck** "Block all public access" (or configure as needed)
6. Click "Create bucket"
7. Navigate into the bucket
8. Click "Create folder" → Name: `ingest-data`
9. Inside `ingest-data`, create folders:
   - `traffic_data`
   - `flow_data`
   - `weather_data`

### 3. Note Your Bucket Name

Save this for later - you'll need it for Lambda deployment:

```bash
export S3_BUCKET="youngleebigdatabucket"
```

---

## IAM Role Creation

Lambda functions need an IAM role with permissions to:
- Write to S3
- Write CloudWatch Logs

### Option 1: Using AWS CLI (Automated)

The deployment script will create the role automatically. Skip to [Deploy Lambda Functions](#deploy-lambda-functions).

### Option 2: Manual Creation via Console

1. Go to AWS Console → IAM → Roles
2. Click "Create role"
3. Select "AWS service" → "Lambda"
4. Click "Next"
5. Attach policies:
   - `AWSLambdaBasicExecutionRole` (for CloudWatch Logs)
   - Create custom policy for S3 access (see below)
6. Name the role: `lambda-data-ingest-role`
7. Click "Create role"

### Custom S3 Policy

Create a policy with this JSON:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::YOUR-BUCKET-NAME/*"
        }
    ]
}
```

Replace `YOUR-BUCKET-NAME` with your actual bucket name.

---

## Deploy Lambda Functions

### 1. Prepare Environment Variables

Set these environment variables before deployment:

**Windows PowerShell:**
```powershell
$env:S3_BUCKET = "youngleebigdatabucket"
$env:S3_OUTPUT_PATH = "ingest-data"
$env:TOMTOM_API_KEY = "C9A2Op6bcWPrr2AGEJv6mFpdz3aOftO1"
$env:WEATHER_API_KEY = "76d3c2c705805e009533b5cee35fafef"
$env:REGION = "eu-north-1"
```

### 2. Navigate to Lambda Functions Directory

```bash
cd lambda_functions
```

### 3. Run Deployment Script

**Windows PowerShell:**
```powershell
.\deploy_lambda.ps1
```

**Linux/Mac:**
```bash
chmod +x deploy_lambda.sh
./deploy_lambda.sh
```

### 4. What the Script Does

The deployment script will:
1. ✅ Create IAM role (if it doesn't exist)
2. ✅ Install Python dependencies (`requests`, `boto3`)
3. ✅ Package Lambda functions with dependencies
4. ✅ Create/update 3 Lambda functions:
   - `data-ingest-db-scraper`
   - `data-ingest-flow-ingest`
   - `data-ingest-weather-ingest`
5. ✅ Configure environment variables
6. ✅ Set memory (256 MB) and timeout (5 minutes)
7. ✅ Create EventBridge schedules (every 10 minutes)

### 5. Verify Deployment

```bash
# List Lambda functions
aws lambda list-functions --region eu-central-1 --query 'Functions[?starts_with(FunctionName, `data-ingest`)].FunctionName'

# Should show:
# - data-ingest-db-scraper
# - data-ingest-flow-ingest
# - data-ingest-weather-ingest
```

---

## Configure EventBridge Schedules

The deployment script creates schedules automatically, but you can verify/manage them:

### View Schedules

```bash
# List EventBridge rules
aws events list-rules --region eu-central-1 --name-prefix lambda-

# Should show:
# - lambda-db-scraper-schedule
# - lambda-flow-ingest-schedule
# - lambda-weather-ingest-schedule
```

### Manual Schedule Creation (if needed)

```bash
# DB Scraper - Every 10 minutes
aws events put-rule \
    --name lambda-db-scraper-schedule \
    --schedule-expression "rate(10 minutes)" \
    --state ENABLED \
    --region eu-central-1

# Add Lambda as target
aws events put-targets \
    --rule lambda-db-scraper-schedule \
    --targets "Id=1,Arn=arn:aws:lambda:eu-central-1:ACCOUNT_ID:function:data-ingest-db-scraper" \
    --region eu-central-1

# Grant EventBridge permission to invoke Lambda
aws lambda add-permission \
    --function-name data-ingest-db-scraper \
    --statement-id allow-eventbridge \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:eu-central-1:ACCOUNT_ID:rule/lambda-db-scraper-schedule" \
    --region eu-central-1
```

Replace `ACCOUNT_ID` with your AWS Account ID.

---

## Testing

### 1. Test Lambda Functions Manually

```bash
# Test DB Scraper
aws lambda invoke \
    --function-name data-ingest-db-scraper \
    --region eu-central-1 \
    --payload '{}' \
    response.json

# View response
cat response.json  # Linux/Mac
Get-Content response.json  # Windows PowerShell

# Test Flow Ingest
aws lambda invoke \
    --function-name data-ingest-flow-ingest \
    --region eu-central-1 \
    --payload '{}' \
    response.json

# Test Weather Ingest
aws lambda invoke \
    --function-name data-ingest-weather-ingest \
    --region eu-central-1 \
    --payload '{}' \
    response.json
```

### 2. Check S3 for Data

```bash
# List files in S3
aws s3 ls s3://your-bucket-name/ingest-data/ --recursive

# Should show files like:
# ingest-data/traffic_data/2024/01/15/120000_traffic_data.jsonl
# ingest-data/flow_data/2024/01/15/120000_flow_data.jsonl
# ingest-data/weather_data/2024/01/15/120000_weather_data.jsonl
```

### 3. View CloudWatch Logs

```bash
# View logs for DB Scraper
aws logs tail /aws/lambda/data-ingest-db-scraper --follow --region eu-central-1

# View recent logs
aws logs get-log-events \
    --log-group-name /aws/lambda/data-ingest-db-scraper \
    --log-stream-name [stream-name] \
    --region eu-central-1
```

### 4. Verify EventBridge Schedule

```bash
# Check rule status
aws events describe-rule --name lambda-db-scraper-schedule --region eu-central-1

# Should show: "State": "ENABLED"
```

---

## Monitoring

### 1. CloudWatch Metrics

View Lambda metrics in AWS Console:
1. Go to AWS Console → Lambda → Functions
2. Click on a function name
3. Go to "Monitoring" tab
4. View:
   - Invocations (should increase every 10 minutes)
   - Duration
   - Errors
   - Throttles

### 2. CloudWatch Logs

View logs in AWS Console:
1. Go to AWS Console → CloudWatch → Log groups
2. Find `/aws/lambda/data-ingest-*`
3. Click on a log group
4. View recent log streams

### 3. S3 Data Verification

```bash
# Count files
aws s3 ls s3://your-bucket-name/ingest-data/ --recursive | wc -l

# Download latest file
aws s3 cp s3://your-bucket-name/ingest-data/traffic_data/2024/01/15/120000_traffic_data.jsonl ./
```

### 4. Cost Monitoring

Set up billing alerts:
1. Go to AWS Console → CloudWatch → Billing
2. Create alarm for estimated charges
3. Set threshold (e.g., $5/month)

---

## Troubleshooting

### Problem: Lambda function fails with "Access Denied"

**Solution:**
- Check IAM role has S3 write permissions
- Verify bucket name is correct
- Check bucket policy allows Lambda role access

```bash
# Check Lambda function role
aws lambda get-function-configuration \
    --function-name data-ingest-db-scraper \
    --query 'Role' \
    --region eu-central-1
```

### Problem: "ModuleNotFoundError" in Lambda

**Solution:**
- Dependencies not packaged correctly
- Re-run deployment script
- Check `requirements.txt` includes all dependencies

### Problem: EventBridge not triggering Lambda

**Solution:**
- Check rule is enabled: `aws events describe-rule --name lambda-db-scraper-schedule`
- Verify Lambda permission: Check Lambda function → Configuration → Permissions
- Check CloudWatch Logs for errors

### Problem: API rate limits

**Solution:**
- Increase sleep times in Lambda code
- Reduce execution frequency (change schedule)
- Check API provider limits

### Problem: Lambda timeout

**Solution:**
- Increase timeout in Lambda configuration
- Optimize API calls (reduce sleep times if rate limits allow)
- Check CloudWatch Logs for slow operations

### Problem: No data in S3

**Solution:**
- Check Lambda logs for errors
- Verify environment variables are set correctly
- Test Lambda function manually
- Check S3 bucket permissions

---

## Quick Reference Commands

```bash
# Set environment variables
export S3_BUCKET="your-bucket-name"
export TOMTOM_API_KEY="your-key"
export WEATHER_API_KEY="your-key"

# Deploy
cd lambda_functions
./deploy_lambda.sh

# Test
aws lambda invoke --function-name data-ingest-db-scraper --payload '{}' response.json

# View logs
aws logs tail /aws/lambda/data-ingest-db-scraper --follow

# Check S3
aws s3 ls s3://your-bucket-name/ingest-data/ --recursive

# List functions
aws lambda list-functions --query 'Functions[?starts_with(FunctionName, `data-ingest`)].FunctionName'
```

---

## Next Steps

After setup is complete:

1. ✅ Verify data is being collected (check S3)
2. ✅ Set up monitoring alerts
3. ✅ Configure data download scripts (see `scripts/README.md`)
4. ✅ Set up data processing pipeline (if needed)
5. ✅ Review costs after first week

---

## Support

If you encounter issues:

1. Check CloudWatch Logs for error messages
2. Verify all environment variables are set
3. Test Lambda functions manually
4. Review IAM permissions
5. Check AWS Service Health Dashboard

For detailed documentation, see:
- `lambda_functions/README.md` - Lambda function documentation
- `scripts/README.md` - Data download scripts

