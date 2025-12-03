"""
Lambda Function: TomTom Traffic Flow Data Ingestion

This Lambda function collects real-time traffic flow data from the TomTom Traffic API
for major German cities. It uses a grid-based sampling approach to get representative
traffic conditions across each city.

Data Source: TomTom Traffic Flow Segment Data API
API Documentation: https://developer.tomtom.com/traffic-api/documentation/product/traffic-flow/traffic-flow-segment-data

What it does:
1. Divides each city into a 3x3 grid (9 sample points)
2. Queries TomTom API for traffic flow at each grid point
3. Calculates congestion metrics (current speed vs free-flow speed)
4. Aggregates data per city with average congestion ratios
5. Writes data to S3 in JSONL format

S3 Output Structure:
    s3://bucket/ingest-data/TomTom/YYYY/MM/DD/HHMMSS_TomTom_data.jsonl

Environment Variables Required:
    - S3_BUCKET: Name of the S3 bucket to write data to
    - S3_OUTPUT_PATH: Prefix path in S3 (default: 'ingest-data')
    - TOMTOM_API_KEY: TomTom API key for authentication
"""

import json
import time
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import boto3
import os

# Initialize S3 client for writing data
s3_client = boto3.client('s3')

# Standardized "Big 5" Bounding Boxes (minLon, minLat, maxLon, maxLat)
# Updated to match ingest script exactly
CITY_BBOXES = {
    "Hamburg":   "9.725,53.395,10.325,53.695",
    "Berlin":    "13.088,52.338,13.761,52.675",
    "Frankfurt": "8.480,50.000,8.800,50.230",
    "Munich":    "11.360,48.060,11.722,48.248",
    "Cologne":   "6.772,50.830,7.162,51.085"
}

def dict_to_jsonl(data):
    """Convert a list of dictionaries (or a single dictionary) to a JSONL string."""
    if isinstance(data, dict):
        data = [data]
    return '\n'.join(json.dumps(item) for item in data)

def get_grid_points(bbox_str, grid_size=3):
    """
    Divide a bounding box into a grid and return the center points of each cell.
    
    Args:
        bbox_str (str): Bounding box string in format "minLon,minLat,maxLon,maxLat"
        grid_size (int): Number of grid cells per side (default: 3, creates 3x3=9 cells)
        
    Returns:
        list: List of dictionaries with 'id', 'lat', 'lon' keys
    """
    try:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox_str.split(','))
    except ValueError:
        print(f"Invalid BBOX format: {bbox_str}")
        return []

    width = max_lon - min_lon
    height = max_lat - min_lat
    
    step_x = width / grid_size
    step_y = height / grid_size
    
    points = []
    for i in range(grid_size):
        for j in range(grid_size):
            center_lon = min_lon + (i * step_x) + (step_x / 2)
            center_lat = min_lat + (j * step_y) + (step_y / 2)
            points.append({
                'id': f"grid_{i}_{j}",
                'lat': center_lat,
                'lon': center_lon
            })
    return points

def fetch_flow_segment(lat, lon, api_key, session):
    """
    Fetch traffic flow data for a specific point.
    
    Args:
        lat (float): Latitude coordinate
        lon (float): Longitude coordinate
        api_key (str): TomTom API key
        session (requests.Session): HTTP session with retry logic
        
    Returns:
        dict: JSON response from API, or None on error
    """
    base_url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    url = f"{base_url}?point={lat},{lon}&unit=KMPH&key={api_key}"

    try:
        # Note: unit=KMPH
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception:
        # We fail silently here to not spam logs for every missing road segment
        return None

def process_city(city_name, bbox, api_key, session):
    """
    Process traffic flow data for a single city using grid-based sampling.
    
    Args:
        city_name (str): Name of the city
        bbox (str): Bounding box string
        api_key (str): TomTom API key
        session (requests.Session): HTTP session with retry logic
        
    Returns:
        dict: City traffic data with structure matching ingest script
    """
    points = get_grid_points(bbox, grid_size=3)
    print(f"Sampling {len(points)} points...", end=" ")
    
    city_samples = []
    total_current = 0
    total_free_flow = 0
    valid_samples = 0
    
    for pt in points:
        data = fetch_flow_segment(pt['lat'], pt['lon'], api_key, session)
        
        if data and 'flowSegmentData' in data:
            flow = data['flowSegmentData']
            current = flow.get('currentSpeed', 0)
            free = flow.get('freeFlowSpeed', 0)
            confidence = flow.get('confidence', 0)
            
            # Ratio: 1.0 = Clear, 0.5 = Half Speed
            ratio = current / free if free > 0 else 1.0
            
            city_samples.append({
                "grid_id": pt['id'],
                "lat": pt['lat'],
                "lon": pt['lon'],
                "current_speed": current,
                "free_flow_speed": free,
                "congestion_ratio": round(ratio, 2),
                "confidence": confidence
            })

            # Only aggregate high-confidence data for the city-wide score
            if confidence > 0.5:
                total_current += current
                total_free_flow += free
                valid_samples += 1
        
        # Rate limit politeness per point
        time.sleep(0.1)

    # Calculate City-Wide Index
    avg_ratio = 1.0
    if total_free_flow > 0:
        avg_ratio = total_current / total_free_flow
        
    status = "flowing"
    if avg_ratio < 0.7: status = "congested"
    elif avg_ratio < 0.85: status = "heavy_traffic"

    # Final Document Structure matching ingest script
    return {
        "meta_city": city_name,
        "meta_scraped_at": datetime.now().isoformat(),
        "meta_type": "tomtom_grid_flow",
        "grid_size": "3x3",
        "average_congestion_ratio": round(avg_ratio, 2),
        "status_label": status,
        "samples": city_samples
    }

def lambda_handler(event, context):
    """
    AWS Lambda handler function - entry point for the Lambda execution.
    """
    try:
        s3_bucket = os.environ.get('S3_BUCKET')
        s3_output_path = os.environ.get('S3_OUTPUT_PATH', 'ingest-data')
        tomtom_api_key = os.environ.get('TOMTOM_API_KEY')
        
        if not s3_bucket:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'S3_BUCKET environment variable not set'})
            }
        
        if not tomtom_api_key:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'TOMTOM_API_KEY environment variable not set'})
            }
        
        print("Starting TomTom Grid Flow Cycle...")
        
        # Create session with retry logic
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        all_data = []
        
        for city, bbox in CITY_BBOXES.items():
            print(f"Scanning {city}...", end=" ")
            
            result = process_city(city, bbox, tomtom_api_key, session)
            all_data.append(result)
            
            print(f"Done. Status: {result['status_label']} ({result['average_congestion_ratio']})")
            time.sleep(1)
                
        if all_data:
            jsonl_string = dict_to_jsonl(all_data)
            
            timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
            s3_key = f"{s3_output_path}/TomTom/{timestamp}_TomTom_data.jsonl"
            
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=jsonl_string.encode('utf-8'),
                ContentType='application/json'
            )
            
            total_samples = sum(len(item.get('samples', [])) for item in all_data)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Flow data collection successful',
                    'cities_processed': len(all_data),
                    'total_samples': total_samples,
                    's3_location': f's3://{s3_bucket}/{s3_key}'
                })
            }
        else:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data collected',
                    'cities_processed': 0
                })
            }
            
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
