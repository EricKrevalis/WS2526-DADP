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
zip -q db_scraper.zip lambda_DB_scraper.py

# TomTom Scraper
echo "Packaging TomTom scraper..."
cd package
zip -r ../tomtom_scraper.zip . -q
cd ..
zip -q tomtom_scraper.zip lambda_TomTom_scraper.py

# OWM Scraper
echo "Packaging OWM scraper..."
cd package
zip -r ../owm_scraper.zip . -q
cd ..
zip -q owm_scraper.zip lambda_OWM_scraper.py

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
        --handler lambda_DB_scraper.lambda_handler \
        --zip-file fileb://db_scraper.zip \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --environment "Variables={S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH}" \
        --region $REGION > /dev/null
fi

# TomTom Scraper Function
FUNCTION_NAME="${FUNCTION_NAME_PREFIX}-tomtom-scraper"
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION &>/dev/null; then
    echo "Updating function: $FUNCTION_NAME"
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://tomtom_scraper.zip \
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
        --handler lambda_TomTom_scraper.lambda_handler \
        --zip-file fileb://tomtom_scraper.zip \
        --timeout $TIMEOUT \
        --memory-size $MEMORY_SIZE \
        --environment "Variables={S3_BUCKET=$S3_BUCKET,S3_OUTPUT_PATH=$S3_OUTPUT_PATH,TOMTOM_API_KEY=$TOMTOM_API_KEY}" \
        --region $REGION > /dev/null
fi

# OWM Scraper Function
FUNCTION_NAME="${FUNCTION_NAME_PREFIX}-owm-scraper"
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION &>/dev/null; then
    echo "Updating function: $FUNCTION_NAME"
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://owm_scraper.zip \
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
        --handler lambda_OWM_scraper.lambda_handler \
        --zip-file fileb://owm_scraper.zip \
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

# TomTom Scraper - Every 10 minutes
RULE_NAME="lambda-tomtom-scraper-schedule"
aws events put-rule \
    --name $RULE_NAME \
    --schedule-expression "rate(10 minutes)" \
    --state ENABLED \
    --region $REGION > /dev/null

aws events put-targets \
    --rule $RULE_NAME \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME_PREFIX}-tomtom-scraper" \
    --region $REGION > /dev/null

aws lambda add-permission \
    --function-name ${FUNCTION_NAME_PREFIX}-tomtom-scraper \
    --statement-id allow-eventbridge \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
    --region $REGION > /dev/null 2>&1 || true

# OWM Scraper - Every 10 minutes
RULE_NAME="lambda-owm-scraper-schedule"
aws events put-rule \
    --name $RULE_NAME \
    --schedule-expression "rate(10 minutes)" \
    --state ENABLED \
    --region $REGION > /dev/null

aws events put-targets \
    --rule $RULE_NAME \
    --targets "Id=1,Arn=arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME_PREFIX}-owm-scraper" \
    --region $REGION > /dev/null

aws lambda add-permission \
    --function-name ${FUNCTION_NAME_PREFIX}-owm-scraper \
    --statement-id allow-eventbridge \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
    --region $REGION > /dev/null 2>&1 || true

# Cleanup
echo ""
echo "Cleaning up temporary files..."
rm -rf package
rm -f db_scraper.zip tomtom_scraper.zip owm_scraper.zip
rm -f /tmp/trust-policy.json /tmp/s3-policy.json

echo ""
echo "=== Deployment Complete! ==="
echo ""
echo "Lambda Functions:"
echo "  - ${FUNCTION_NAME_PREFIX}-db-scraper"
echo "  - ${FUNCTION_NAME_PREFIX}-tomtom-scraper"
echo "  - ${FUNCTION_NAME_PREFIX}-owm-scraper"
echo ""
echo "Schedules:"
echo "  - All functions run every 10 minutes"
echo ""
echo "Monitor functions:"
echo "  aws lambda list-functions --region $REGION"
echo "  aws logs tail /aws/lambda/${FUNCTION_NAME_PREFIX}-db-scraper --follow --region $REGION"

