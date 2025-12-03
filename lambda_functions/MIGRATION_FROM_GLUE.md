# Migration from AWS Glue to AWS Lambda

This document explains how to migrate from AWS Glue jobs to AWS Lambda functions.

## Why Migrate?

- **92% cost savings**: ~$1/month vs ~$12.50/month
- **Faster startup**: ~100ms vs ~1-2 minutes
- **Better for simple API calls**: Lambda is optimized for this use case
- **No minimum billing**: Pay only for actual execution time

## Migration Steps

### 1. Deploy Lambda Functions

```bash
cd lambda_functions
export S3_BUCKET="your-bucket-name"
export TOMTOM_API_KEY="your-key"
export WEATHER_API_KEY="your-key"
./deploy_lambda.sh  # or deploy_lambda.ps1 on Windows
```

### 2. Verify Lambda Functions Work

Test each function manually:

```bash
# Test DB scraper
aws lambda invoke \
    --function-name data-ingest-db-scraper \
    --payload '{}' \
    response.json

# Check response
cat response.json

# Verify data in S3
aws s3 ls s3://your-bucket/ingest-data/traffic_data/ --recursive
```

### 3. Monitor Lambda Functions

Watch logs for a few cycles:

```bash
# Tail logs
aws logs tail /aws/lambda/data-ingest-db-scraper --follow
```

### 4. Disable Glue Jobs

Once Lambda functions are working correctly:

```bash
# Disable EventBridge rules for Glue jobs
aws events disable-rule --name glue-db-scraper-schedule
aws events disable-rule --name glue-flow-ingest-schedule
aws events disable-rule --name glue-weather-ingest-schedule

# Optionally delete Glue jobs (after confirming Lambda works)
aws glue delete-job --job-name db-traffic-scraper
aws glue delete-job --job-name traffic-flow-ingest
aws glue delete-job --job-name weather-ingest
```

### 5. Clean Up Glue Resources (Optional)

After confirming Lambda works for a few days:

```bash
# Delete Glue jobs
aws glue delete-job --job-name db-traffic-scraper
aws glue delete-job --job-name traffic-flow-ingest
aws glue delete-job --job-name weather-ingest

# Delete EventBridge rules
aws events delete-rule --name glue-db-scraper-schedule
aws events delete-rule --name glue-flow-ingest-schedule
aws events delete-rule --name glue-weather-ingest-schedule
```

## Data Compatibility

✅ **No changes needed** - Lambda functions:
- Use the same S3 structure
- Write data in the same JSONL format
- Use the same timestamp format
- Compatible with existing downstream consumers

## Cost Comparison

| Service | Monthly Cost (every 10 min) |
|---------|----------------------------|
| AWS Glue | ~$12.50 |
| AWS Lambda | ~$1.00 |
| **Savings** | **$11.50/month (92%)** |

## Rollback Plan

If you need to rollback to Glue:

1. Re-enable Glue EventBridge rules:
```bash
aws events enable-rule --name glue-db-scraper-schedule
aws events enable-rule --name glue-flow-ingest-schedule
aws events enable-rule --name glue-weather-ingest-schedule
```

2. Disable Lambda EventBridge rules:
```bash
aws events disable-rule --name lambda-db-scraper-schedule
aws events disable-rule --name lambda-flow-ingest-schedule
aws events disable-rule --name lambda-weather-ingest-schedule
```

## Notes

- Lambda functions are already deployed and scheduled
- Both systems can run simultaneously during migration
- No data format changes required
- Same S3 bucket and paths used

