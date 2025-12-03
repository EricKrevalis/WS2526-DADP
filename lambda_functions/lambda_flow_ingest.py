import json
import time
from datetime import datetime
import requests
import boto3
import os

s3_client = boto3.client('s3')

# Constants
CITIES = [
    "Hamburg",
    "Berlin",
    "Frankfurt",
    "Muenchen",
    "Koeln"
]

CITY_BBOXES = {
    "Hamburg": "9.7,53.4,10.3,53.7",
    "Berlin": "13.1,52.3,13.7,52.7",
    "Frankfurt": "8.4,49.9,8.9,50.3",
    "Muenchen": "11.3,48.0,11.8,48.3",
    "Koeln": "6.7,50.8,7.2,51.1"
}

def dict_to_jsonl(data):
    """Convert a list of dictionaries (or a single dictionary) to a jsonl string."""
    if isinstance(data, dict):
        data = [data]
    return '\n'.join(json.dumps(item) for item in data)

def get_grid_points(bbox_str, grid_size=3):
    """Divides a bounding box into a grid and returns the center points of each cell."""
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

def fetch_flow_segment(lat, lon, api_key):
    """Fetches traffic flow data for a specific point."""
    base_url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    url = f"{base_url}?point={lat},{lon}&unit=KMPH&key={api_key}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching flow for {lat},{lon}: {e}")
        return None

def process_city_flow(city_name, api_key):
    bbox = CITY_BBOXES.get(city_name)
    if not bbox:
        return None
        
    points = get_grid_points(bbox, grid_size=3)
    
    city_samples = []
    total_current_speed = 0
    total_free_flow_speed = 0
    valid_samples = 0
    
    print(f"Sampling {len(points)} points for {city_name}...")
    
    for pt in points:
        data = fetch_flow_segment(pt['lat'], pt['lon'], api_key)
        
        if data and 'flowSegmentData' in data:
            flow = data['flowSegmentData']
            current_speed = flow.get('currentSpeed', 0)
            free_flow_speed = flow.get('freeFlowSpeed', 0)
            confidence = flow.get('confidence', 0)
            
            ratio = current_speed / free_flow_speed if free_flow_speed > 0 else 1.0
            
            sample_record = {
                "grid_id": pt['id'],
                "location": {"lat": pt['lat'], "lon": pt['lon']},
                "current_speed_kmph": current_speed,
                "free_flow_speed_kmph": free_flow_speed,
                "congestion_ratio": round(ratio, 2),
                "confidence": confidence
            }
            
            city_samples.append(sample_record)
            
            if confidence > 0.5:
                total_current_speed += current_speed
                total_free_flow_speed += free_flow_speed
                valid_samples += 1
        
        # Reduced sleep time for Lambda (faster execution)
        time.sleep(0.1)

    avg_ratio = 1.0
    if total_free_flow_speed > 0:
        avg_ratio = total_current_speed / total_free_flow_speed

    return {
        "city": city_name,
        "timestamp": datetime.utcnow().isoformat(),
        "data_source": "tomtom_flow_segment_grid",
        "grid_size": "3x3",
        "samples_collected": len(city_samples),
        "average_congestion_ratio": round(avg_ratio, 2),
        "city_status": "congested" if avg_ratio < 0.7 else "heavy_traffic" if avg_ratio < 0.85 else "flowing",
        "samples": city_samples
    }

def lambda_handler(event, context):
    """
    AWS Lambda handler for TomTom traffic flow ingestion.
    
    Environment variables required:
    - S3_BUCKET: S3 bucket name for output
    - S3_OUTPUT_PATH: S3 prefix path (default: 'ingest-data')
    - TOMTOM_API_KEY: TomTom API key
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
        
        print("Starting Gridded Flow Data Ingestion...")
        
        all_data = []
        
        for city in CITIES:
            result = process_city_flow(city, tomtom_api_key)
            if result:
                all_data.append(result)
                
        if all_data:
            jsonl_string = dict_to_jsonl(all_data)
            
            # Generate S3 key with timestamp
            timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
            s3_key = f"{s3_output_path}/flow_data/{timestamp}_flow_data.jsonl"
            
            # Write to S3
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=jsonl_string.encode('utf-8'),
                ContentType='application/json'
            )
            
            total_samples = sum(item.get('samples_collected', 0) for item in all_data)
            
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

