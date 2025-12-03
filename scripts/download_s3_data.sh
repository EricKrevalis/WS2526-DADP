#!/bin/bash

# Download S3 data from Lambda ingestion to local device
# Usage: ./download_s3_data.sh [bucket-name] [output-dir]

set -e

S3_BUCKET="${1:-your-data-bucket-name}"
OUTPUT_DIR="${2:-./downloaded_data}"
S3_PREFIX="${3:-ingest-data}"

if [ "$S3_BUCKET" == "your-data-bucket-name" ]; then
    echo "Error: Please provide S3 bucket name"
    echo "Usage: $0 <bucket-name> [output-dir] [s3-prefix]"
    exit 1
fi

echo "=== Downloading S3 Data ==="
echo "Bucket: $S3_BUCKET"
echo "Prefix: $S3_PREFIX"
echo "Output Directory: $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Download all data
echo "Downloading traffic data..."
aws s3 sync "s3://${S3_BUCKET}/${S3_PREFIX}/traffic_data/" "${OUTPUT_DIR}/traffic_data/" --quiet

echo "Downloading flow data..."
aws s3 sync "s3://${S3_BUCKET}/${S3_PREFIX}/flow_data/" "${OUTPUT_DIR}/flow_data/" --quiet

echo "Downloading weather data..."
aws s3 sync "s3://${S3_BUCKET}/${S3_PREFIX}/weather_data/" "${OUTPUT_DIR}/weather_data/" --quiet

echo ""
echo "=== Download Complete ==="
echo "Data saved to: $OUTPUT_DIR"
echo ""
echo "File counts:"
echo "  Traffic files: $(find ${OUTPUT_DIR}/traffic_data -type f 2>/dev/null | wc -l)"
echo "  Flow files: $(find ${OUTPUT_DIR}/flow_data -type f 2>/dev/null | wc -l)"
echo "  Weather files: $(find ${OUTPUT_DIR}/weather_data -type f 2>/dev/null | wc -l)"

