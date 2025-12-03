#!/usr/bin/env python3
"""
Download the latest data files from S3 to local device.
This script downloads only the most recent files from each data type.
"""

import boto3
import os
from datetime import datetime
import argparse

def download_latest_files(bucket_name, s3_prefix='ingest-data', output_dir='./latest_data'):
    """
    Download the latest files from each data type in S3.
    
    Args:
        bucket_name: S3 bucket name
        s3_prefix: S3 prefix path (default: 'ingest-data')
        output_dir: Local directory to save files
    """
    s3_client = boto3.client('s3')
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    data_types = ['traffic_data', 'flow_data', 'weather_data']
    
    for data_type in data_types:
        print(f"\nProcessing {data_type}...")
        s3_path = f"{s3_prefix}/{data_type}/"
        
        # List all objects in this path
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=s3_path
            )
            
            if 'Contents' not in response:
                print(f"  No files found in {s3_path}")
                continue
            
            # Sort by last modified (newest first)
            objects = sorted(response['Contents'], 
                           key=lambda x: x['LastModified'], 
                           reverse=True)
            
            # Download the 10 most recent files
            count = 0
            for obj in objects[:10]:
                key = obj['Key']
                filename = os.path.basename(key)
                
                # Create subdirectory structure matching S3
                local_path = os.path.join(output_dir, data_type, filename)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                # Download file
                s3_client.download_file(bucket_name, key, local_path)
                print(f"  Downloaded: {filename}")
                count += 1
            
            print(f"  Downloaded {count} files")
            
        except Exception as e:
            print(f"  Error processing {data_type}: {e}")
    
    print(f"\n=== Download Complete ===")
    print(f"Files saved to: {output_dir}")

def download_date_range(bucket_name, start_date, end_date, s3_prefix='ingest-data', output_dir='./date_range_data'):
    """
    Download all files from a specific date range.
    
    Args:
        bucket_name: S3 bucket name
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        s3_prefix: S3 prefix path
        output_dir: Local directory to save files
    """
    s3_client = boto3.client('s3')
    
    os.makedirs(output_dir, exist_ok=True)
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    data_types = ['traffic_data', 'flow_data', 'weather_data']
    
    for data_type in data_types:
        print(f"\nProcessing {data_type}...")
        s3_path = f"{s3_prefix}/{data_type}/"
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=s3_path
            )
            
            if 'Contents' not in response:
                continue
            
            count = 0
            for obj in response['Contents']:
                # Extract date from S3 key (format: YYYY/MM/DD/HHMMSS_filename.jsonl)
                key = obj['Key']
                parts = key.split('/')
                
                if len(parts) >= 4:
                    try:
                        file_date_str = f"{parts[-4]}-{parts[-3]}-{parts[-2]}"
                        file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                        
                        if start <= file_date <= end:
                            filename = os.path.basename(key)
                            local_path = os.path.join(output_dir, data_type, filename)
                            os.makedirs(os.path.dirname(local_path), exist_ok=True)
                            
                            s3_client.download_file(bucket_name, key, local_path)
                            count += 1
                    except ValueError:
                        continue
            
            print(f"  Downloaded {count} files from {start_date} to {end_date}")
            
        except Exception as e:
            print(f"  Error processing {data_type}: {e}")
    
    print(f"\n=== Download Complete ===")
    print(f"Files saved to: {output_dir}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download data from S3')
    parser.add_argument('bucket', help='S3 bucket name')
    parser.add_argument('--prefix', default='ingest-data', help='S3 prefix path')
    parser.add_argument('--output', default='./downloaded_data', help='Output directory')
    parser.add_argument('--latest', action='store_true', help='Download only latest files')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD) for date range download')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD) for date range download')
    
    args = parser.parse_args()
    
    if args.start_date and args.end_date:
        download_date_range(args.bucket, args.start_date, args.end_date, args.prefix, args.output)
    elif args.latest:
        download_latest_files(args.bucket, args.prefix, args.output)
    else:
        # Download all files
        print("Downloading all files...")
        import subprocess
        import sys
        
        if sys.platform == 'win32':
            subprocess.run(['aws', 's3', 'sync', 
                          f's3://{args.bucket}/{args.prefix}/', 
                          args.output])
        else:
            subprocess.run(['aws', 's3', 'sync', 
                          f's3://{args.bucket}/{args.prefix}/', 
                          args.output])

