import json
from datetime import datetime
import requests
import boto3
import os

s3_client = boto3.client('s3')

# Constants
CITY_COORDINATES = {
    "Hamburg": {"lat": 53.5511, "lon": 9.9937},
    "Berlin": {"lat": 52.5200, "lon": 13.4050},
    "Frankfurt": {"lat": 50.1109, "lon": 8.6821},
    "Muenchen": {"lat": 48.1351, "lon": 11.5820},
    "Koeln": {"lat": 50.9375, "lon": 6.9603}
}

CITIES = list(CITY_COORDINATES.keys())

def dict_to_jsonl(data):
    """Convert a list of dictionaries (or a single dictionary) to a jsonl string."""
    if isinstance(data, dict):
        data = [data]
    return '\n'.join(json.dumps(item) for item in data)

def get_weather(lat, lon, city_name, api_key):
    """Fetch weather data for a specific location."""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            print(f"Open Weather Map API Error: {res.status_code} - {res.text[:50]}")
            return None
            
        data = res.json()
        # Add metadata
        data['meta_city'] = city_name
        data['meta_scraped_at'] = datetime.now().isoformat()
        return data
    except Exception as e:
        print(f"Error fetching weather data for {city_name}: {e}")
        return None

def lambda_handler(event, context):
    """
    AWS Lambda handler for OpenWeatherMap ingestion.
    
    Environment variables required:
    - S3_BUCKET: S3 bucket name for output
    - S3_OUTPUT_PATH: S3 prefix path (default: 'ingest-data')
    - WEATHER_API_KEY: OpenWeatherMap API key
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
        
        print("Starting Weather Data Ingestion...")
        
        all_data = []
        
        for city_name, coords in CITY_COORDINATES.items():
            print(f"Fetching weather for {city_name}...")
            weather_data = get_weather(coords['lat'], coords['lon'], city_name, weather_api_key)
            if weather_data:
                all_data.append(weather_data)
        
        if all_data:
            jsonl_string = dict_to_jsonl(all_data)
            
            # Generate S3 key with timestamp
            timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
            s3_key = f"{s3_output_path}/weather_data/{timestamp}_weather_data.jsonl"
            
            # Write to S3
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

