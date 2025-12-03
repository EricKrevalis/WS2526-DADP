#!/bin/bash

# AWS Lambda Deployment Script
# This script creates Lambda functions for continuous data ingestion

set -e

# Configuration - Update these values
FUNCTION_NAME_PREFIX="data-ingest"
S3_BUCKET="${S3_BUCKET:-your-data-bucket-name}"
S3_OUTPUT_PATH="${S3_OUTPUT_PATH:-ingest-data}"
REGION="${REGION:-eu-central-1}"
LAMBDA_ROLE_NAME="${LAMBDA_ROLE_NAME:-lambda-data-ingest-role}"

# API Keys - Set these as environment variables
TOMTOM_API_KEY="${TOMTOM_API_KEY:-your-tomtom-api-key}"
WEATHER_API_KEY="${WEATHER_API_KEY:-your-weather-api-key}"

# Lambda configuration
MEMORY_SIZE=256
TIMEOUT=300  # 5 minutes
RUNTIME="python3.10"

echo "=== AWS Lambda Data Ingestion Deployment ==="
echo ""

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "AWS Account ID: $ACCOUNT_ID"
echo "Region: $REGION"
echo ""

# Create IAM role for Lambda if it doesn't exist
echo "Creating/Checking IAM role..."
ROLE_ARN=$(aws iam get-role --role-name $LAMBDA_ROLE_NAME --query 'Role.Arn' --output text 2>/dev/null || echo "")

if [ -z "$ROLE_ARN" ]; then
    echo "Creating IAM role: $LAMBDA_ROLE_NAME"
    
    # Create trust policy
    cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

    # Create role
    aws iam create-role \
        --role-name $LAMBDA_ROLE_NAME \
        --assume-role-policy-document file:///tmp/trust-policy.json \
        --description "Role for data ingestion Lambda functions"
    
    # Attach policies
    aws iam attach-role-policy \
        --role-name $LAMBDA_ROLE_NAME \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
    
    # Create and attach S3 policy
    cat > /tmp/s3-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::$S3_BUCKET/*"
    }
  ]
}
EOF

    POLICY_ARN=$(aws iam create-policy \
        --policy-name ${LAMBDA_ROLE_NAME}-s3-policy \
        --policy-document file:///tmp/s3-policy.json \
        --query 'Policy.Arn' --output text)
    
    aws iam attach-role-policy \
        --role-name $LAMBDA_ROLE_NAME \
        --policy-arn $POLICY_ARN
    
    ROLE_ARN=$(aws iam get-role --role-name $LAMBDA_ROLE_NAME --query 'Role.Arn' --output text)
    echo "Role created: $ROLE_ARN"
else
    echo "Role already exists: $ROLE_ARN"
fi

# Install dependencies
echo ""
echo "Installing dependencies..."
mkdir -p package
pip install -r requirements.txt -t ./package --quiet 2>/dev/null || pip3 install -r requirements.txt -t ./package --quiet

# Create deployment packages
echo "Creating deployment packages..."

# DB Scraper
echo "Packaging DB scraper..."
cd package
zip -r ../db_scraper.zip . -q
cd ..
zip -q db_scraper.zip lambda_db_scraper.py

# Flow Ingest
echo "Packaging Flow ingest..."
cd package
zip -r ../flow_ingest.zip . -q
cd ..
zip -q flow_ingest.zip lambda_flow_ingest.py

# Weather Ingest
echo "Packaging Weather ingest..."
cd package
zip -r ../weather_ingest.zip . -q
cd ..
zip -q weather_ingest.zip lambda_weather_ingest.py

# Create or update Lambda functions
echo ""
echo "Creating/Updating Lambda functions..."

# DB Scraper Function
FUNCTION_NAME="${FUNCTION_NAME_PREFIX}-db-scraper"
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION &>/dev/null; then
    echo "Updating function: $FUNCTION_NAME"
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://db_scraper.zip \
        --region $REGION > /dev/null
    
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --environment "Variables={S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH}" \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --region $REGION > /dev/null
else
    echo "Creating function: $FUNCTION_NAME"
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime $RUNTIME \
        --role $ROLE_ARN \
        --handler lambda_db_scraper.lambda_handler \
        --zip-file fileb://db_scraper.zip \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --environment "Variables={S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH}" \
        --region $REGION > /dev/null
fi

# Flow Ingest Function
FUNCTION_NAME="${FUNCTION_NAME_PREFIX}-flow-ingest"
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION &>/dev/null; then
    echo "Updating function: $FUNCTION_NAME"
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://flow_ingest.zip \
        --region $REGION > /dev/null
    
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --environment "Variables={S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH,TOMTOM_API_KEY=$TOMTOM_API_KEY}" \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --region $REGION > /dev/null
else
    echo "Creating function: $FUNCTION_NAME"
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime $RUNTIME \
        --role $ROLE_ARN \
        --handler lambda_flow_ingest.lambda_handler \
        --zip-file fileb://flow_ingest.zip \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --environment "Variables={S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH,TOMTOM_API_KEY=$TOMTOM_API_KEY}" \
        --region $REGION > /dev/null
fi

# Weather Ingest Function
FUNCTION_NAME="${FUNCTION_NAME_PREFIX}-weather-ingest"
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION &>/dev/null; then
    echo "Updating function: $FUNCTION_NAME"
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://weather_ingest.zip \
        --region $REGION > /dev/null
    
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --environment "Variables={S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH,WEATHER_API_KEY=$WEATHER_API_KEY}" \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --region $REGION > /dev/null
else
    echo "Creating function: $FUNCTION_NAME"
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime $RUNTIME \
        --role $ROLE_ARN \
        --handler lambda_weather_ingest.lambda_handler \
        --zip-file fileb://weather_ingest.zip \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --environment "Variables={S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH,WEATHER_API_KEY=$WEATHER_API_KEY}" \
        --region $REGION > /dev/null
fi

# Create EventBridge rules for scheduling
echo ""
echo "Creating EventBridge schedules..."

# DB Scraper - Every 10 minutes
RULE_NAME="lambda-db-scraper-schedule"
aws events put-rule \
    --name $RULE_NAME \
    --schedule-expression "rate(10 minutes)" \
    --state ENABLED \
    --region $REGION > /dev/null

aws events put-targets \
    --rule $RULE_NAME \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME_PREFIX}-db-scraper" \
    --region $REGION > /dev/null

# Add Lambda permission for EventBridge
aws lambda add-permission \
    --function-name ${FUNCTION_NAME_PREFIX}-db-scraper \
    --statement-id allow-eventbridge \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
    --region $REGION > /dev/null 2>&1 || true

# Flow Ingest - Every 10 minutes
RULE_NAME="lambda-flow-ingest-schedule"
aws events put-rule \
    --name $RULE_NAME \
    --schedule-expression "rate(10 minutes)" \
    --state ENABLED \
    --region $REGION > /dev/null

aws events put-targets \
    --rule $RULE_NAME \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME_PREFIX}-flow-ingest" \
    --region $REGION > /dev/null

aws lambda add-permission \
    --function-name ${FUNCTION_NAME_PREFIX}-flow-ingest \
    --statement-id allow-eventbridge \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
    --region $REGION > /dev/null 2>&1 || true

# Weather Ingest - Every 10 minutes (or 30 minutes to save costs)
RULE_NAME="lambda-weather-ingest-schedule"
aws events put-rule \
    --name $RULE_NAME \
    --schedule-expression "rate(10 minutes)" \
    --state ENABLED \
    --region $REGION > /dev/null

aws events put-targets \
    --rule $RULE_NAME \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME_PREFIX}-weather-ingest" \
    --region $REGION > /dev/null

aws lambda add-permission \
    --function-name ${FUNCTION_NAME_PREFIX}-weather-ingest \
    --statement-id allow-eventbridge \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
    --region $REGION > /dev/null 2>&1 || true

# Cleanup
echo ""
echo "Cleaning up temporary files..."
rm -rf package
rm -f db_scraper.zip flow_ingest.zip weather_ingest.zip
rm -f /tmp/trust-policy.json /tmp/s3-policy.json

echo ""
echo "=== Deployment Complete! ==="
echo ""
echo "Lambda Functions:"
echo "  - ${FUNCTION_NAME_PREFIX}-db-scraper"
echo "  - ${FUNCTION_NAME_PREFIX}-flow-ingest"
echo "  - ${FUNCTION_NAME_PREFIX}-weather-ingest"
echo ""
echo "Schedules:"
echo "  - All functions run every 10 minutes"
echo ""
echo "Monitor functions:"
echo "  aws lambda list-functions --region $REGION"
echo "  aws logs tail /aws/lambda/${FUNCTION_NAME_PREFIX}-db-scraper --follow --region $REGION"

