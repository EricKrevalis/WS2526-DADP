# Download S3 Data Scripts

These scripts help you download data from S3 (written by Lambda functions) to your local device.

## Quick Download (All Data)

### Using AWS CLI (Simplest)

```bash
# Download all data
aws s3 sync s3://your-bucket-name/ingest-data/ ./downloaded_data/

# Download specific data type
aws s3 sync s3://your-bucket-name/ingest-data/traffic_data/ ./traffic_data/
aws s3 sync s3://your-bucket-name/ingest-data/flow_data/ ./flow_data/
aws s3 sync s3://your-bucket-name/ingest-data/weather_data/ ./weather_data/
```

### Using Download Scripts

**Linux/Mac:**
```bash
chmod +x download_s3_data.sh
./download_s3_data.sh your-bucket-name ./downloaded_data
```

**Windows PowerShell:**
```powershell
.\download_s3_data.ps1 -BucketName "your-bucket-name" -OutputDir "./downloaded_data"
```

## Download Latest Files Only

Use the Python script to download only the most recent files:

```bash
# Download 10 most recent files from each data type
python download_latest.py your-bucket-name --latest --output ./latest_data
```

## Download Specific Date Range

```bash
# Download files from a specific date range
python download_latest.py your-bucket-name \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --output ./january_data
```

## Download Specific Files

```bash
# Download a specific file
aws s3 cp s3://your-bucket/ingest-data/traffic_data/2024/01/15/120000_traffic_data.jsonl ./traffic_data.jsonl

# List files to see what's available
aws s3 ls s3://your-bucket/ingest-data/traffic_data/ --recursive
```

## Examples

### Example 1: Download All Data
```bash
aws s3 sync s3://my-data-bucket/ingest-data/ ./all_data/
```

### Example 2: Download Only Today's Data
```bash
TODAY=$(date +%Y/%m/%d)
aws s3 sync s3://my-data-bucket/ingest-data/traffic_data/$TODAY/ ./today_traffic/
aws s3 sync s3://my-data-bucket/ingest-data/flow_data/$TODAY/ ./today_flow/
aws s3 sync s3://my-data-bucket/ingest-data/weather_data/$TODAY/ ./today_weather/
```

### Example 3: Download Last 24 Hours
```bash
# List recent files
aws s3 ls s3://my-data-bucket/ingest-data/traffic_data/ --recursive | tail -20

# Download specific files
aws s3 cp s3://my-data-bucket/ingest-data/traffic_data/2024/01/15/120000_traffic_data.jsonl ./
```

## Data Structure

Data is organized in S3 as:
```
s3://bucket/ingest-data/
├── traffic_data/
│   └── YYYY/MM/DD/HHMMSS_traffic_data.jsonl
├── flow_data/
│   └── YYYY/MM/DD/HHMMSS_flow_data.jsonl
└── weather_data/
    └── YYYY/MM/DD/HHMMSS_weather_data.jsonl
```

## Prerequisites

1. **AWS CLI** installed and configured
   ```bash
   aws configure
   ```

2. **Python 3** (for Python scripts)
   ```bash
   pip install boto3
   ```

3. **S3 Access Permissions** - Your AWS credentials need read access to the S3 bucket

## Viewing Data

After downloading, you can view the JSONL files:

```bash
# View first few lines
head traffic_data/2024/01/15/120000_traffic_data.jsonl

# Count lines (records)
wc -l traffic_data/2024/01/15/120000_traffic_data.jsonl

# Pretty print JSON (requires jq)
cat traffic_data/2024/01/15/120000_traffic_data.jsonl | jq '.'
```

## Cost Considerations

- **Downloading data**: Free (no egress charges within same region)
- **Storage**: You pay for S3 storage, not downloads
- **Data transfer**: Free if downloading to EC2 in same region, otherwise standard data transfer rates apply

## Troubleshooting

### Permission Denied
```bash
# Check AWS credentials
aws sts get-caller-identity

# Verify bucket access
aws s3 ls s3://your-bucket-name/
```

### No Files Found
- Check that Lambda functions are running and writing to S3
- Verify the bucket name and prefix path are correct
- Check S3 bucket in AWS Console

### Large Downloads
For large datasets, consider:
- Using `--exclude` and `--include` filters
- Downloading specific date ranges
- Using AWS DataSync for very large datasets

