"""
Lambda Function: OpenWeatherMap Weather Data Ingestion

This Lambda function collects current weather data from the OpenWeatherMap API
for major German cities using a grid-based sampling approach (matching TomTom grid).

Data Source: OpenWeatherMap Current Weather Data API
API Documentation: https://openweathermap.org/current

What it does:
1. Divides each city into a 3x3 grid (9 sample points) - matches TomTom grid exactly
2. Queries OpenWeatherMap API for weather at each grid point
3. Extracts relevant metrics for traffic correlation
4. Aggregates data per city with samples array
5. Writes data to S3 in JSONL format

S3 Output Structure:
    s3://bucket/ingest-data/OWM/YYYY/MM/DD/HHMMSS_OWM_data.jsonl

Environment Variables Required:
    - S3_BUCKET: Name of the S3 bucket to write data to
    - S3_OUTPUT_PATH: Prefix path in S3 (default: 'ingest-data')
    - WEATHER_API_KEY: OpenWeatherMap API key for authentication
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

# Standardized "Big 5" Bounding Boxes (Matches TomTom exactly for correlation)
# Format: "minLon,minLat,maxLon,maxLat"
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
    Reused logic from TomTom scraper - ensures same grid points for correlation.
    
    Args:
        bbox_str (str): Bounding box string in format "minLon,minLat,maxLon,maxLat"
        grid_size (int): Number of grid cells per side (default: 3)
        
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

def fetch_weather_at_point(lat, lon, api_key, session):
    """
    Fetch weather data for a specific geographic point.
    
    Args:
        lat (float): Latitude coordinate
        lon (float): Longitude coordinate
        api_key (str): OpenWeatherMap API key
        session (requests.Session): HTTP session with retry logic
        
    Returns:
        dict: Weather data dictionary, or None on error
    """
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'units': 'metric',  # Get temperature in Celsius
    }
    
    try:
        res = session.get(base_url, params=params, timeout=10)
        res.raise_for_status()
        return res.json()
    except Exception:
        # Silent fail for individual grid points is acceptable
        return None

def process_city(city_name, bbox, api_key, session):
    """
    Process weather data for a single city using grid-based sampling.
    
    Args:
        city_name (str): Name of the city
        bbox (str): Bounding box string
        api_key (str): OpenWeatherMap API key
        session (requests.Session): HTTP session with retry logic
        
    Returns:
        dict: City weather data with samples array
    """
    points = get_grid_points(bbox, grid_size=3)
    print(f"Sampling {len(points)} weather points...", end=" ")
    
    city_samples = []
    
    for pt in points:
        data = fetch_weather_at_point(pt['lat'], pt['lon'], api_key, session)
        
        if data:
            # Extract highly relevant metrics for traffic correlation
            rain_data = data.get('rain', {})
            # API sometimes returns rain: {'1h': 0.5} or just rain: {}
            rain_1h = rain_data.get('1h', 0) if isinstance(rain_data, dict) else 0

            sample = {
                "grid_id": pt['id'],
                "lat": pt['lat'],
                "lon": pt['lon'],
                "temp": data.get('main', {}).get('temp'),
                "humidity": data.get('main', {}).get('humidity'),
                "weather_main": data.get('weather', [{}])[0].get('main'),
                "description": data.get('weather', [{}])[0].get('description'),
                "wind_speed": data.get('wind', {}).get('speed'),
                "rain_1h_mm": rain_1h,
                "visibility": data.get('visibility')
            }
            city_samples.append(sample)
        
        # Rate limit politeness
        time.sleep(0.1)

    return {
        "meta_city": city_name,
        "meta_scraped_at": datetime.now().isoformat(),
        "meta_type": "owm_grid_weather",
        "grid_size": "3x3",
        "samples": city_samples
    }

def lambda_handler(event, context):
    """
    AWS Lambda handler function - entry point for the Lambda execution.
    """
    try:
        s3_bucket = os.environ.get('S3_BUCKET')
        s3_output_path = os.environ.get('S3_OUTPUT_PATH', 'ingest-data')
        weather_api_key = os.environ.get('WEATHER_API_KEY')
        
        if not s3_bucket:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'S3_BUCKET environment variable not set'})
            }
        
        if not weather_api_key:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'WEATHER_API_KEY environment variable not set'})
            }
        
        print("Starting Weather Grid Cycle...")
        
        # Create session with retry logic
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        all_data = []
        
        for city, bbox in CITY_BBOXES.items():
            print(f"Scanning {city}...", end=" ")
            
            result = process_city(city, bbox, weather_api_key, session)
            all_data.append(result)
            
            print(f"Done. Captured {len(result['samples'])} points.")
            time.sleep(1)
        
        if all_data:
            jsonl_string = dict_to_jsonl(all_data)
            
            timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
            s3_key = f"{s3_output_path}/OWM/{timestamp}_OWM_data.jsonl"
            
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=jsonl_string.encode('utf-8'),
                ContentType='application/json'
            )
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Weather data collection successful',
                    'cities_processed': len(all_data),
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
