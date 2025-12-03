"""
Lambda Function: OpenWeatherMap Weather Data Ingestion

This Lambda function collects current weather data from the OpenWeatherMap API
for major German cities. It fetches real-time weather conditions including
temperature, humidity, pressure, wind, and weather descriptions.

Data Source: OpenWeatherMap Current Weather Data API
API Documentation: https://openweathermap.org/current

What it does:
1. Queries OpenWeatherMap API for each of the 5 major German cities
2. Enriches weather data with metadata (city name, scrape timestamp)
3. Aggregates all city weather data into a single dataset
4. Writes data to S3 in JSONL format

Weather Data Includes:
    - Temperature (current, feels like, min, max)
    - Atmospheric pressure
    - Humidity percentage
    - Wind speed and direction
    - Cloud coverage
    - Weather description (e.g., "clear sky", "light rain")
    - Visibility
    - Sunrise/sunset times

S3 Output Structure:
    s3://bucket/ingest-data/weather_data/YYYY/MM/DD/HHMMSS_weather_data.jsonl

Environment Variables Required:
    - S3_BUCKET: Name of the S3 bucket to write data to
    - S3_OUTPUT_PATH: Prefix path in S3 (default: 'ingest-data')
    - WEATHER_API_KEY: OpenWeatherMap API key for authentication
"""

import json
from datetime import datetime
import requests
import boto3
import os

# Initialize S3 client for writing data
s3_client = boto3.client('s3')

# Geographic coordinates (latitude, longitude) for each city's center
# These coordinates are used to query weather data for each city
# Format: Decimal degrees (WGS84 coordinate system)
CITY_COORDINATES = {
    "Hamburg": {"lat": 53.5511, "lon": 9.9937},      # Hamburg city center
    "Berlin": {"lat": 52.5200, "lon": 13.4050},      # Berlin city center
    "Frankfurt": {"lat": 50.1109, "lon": 8.6821},    # Frankfurt am Main city center
    "Muenchen": {"lat": 48.1351, "lon": 11.5820},    # Munich city center
    "Koeln": {"lat": 50.9375, "lon": 6.9603}          # Cologne city center
}

# Extract list of city names from coordinates dictionary
# This list is used to iterate through all cities during data collection
CITIES = list(CITY_COORDINATES.keys())

def dict_to_jsonl(data):
    """
    Convert a list of dictionaries (or a single dictionary) to a JSONL string.
    
    JSONL (JSON Lines) format: Each line is a valid JSON object, separated by newlines.
    This format is efficient for streaming and processing large datasets.
    
    Args:
        data: A list of dictionaries or a single dictionary to convert
        
    Returns:
        str: A string where each line is a JSON object (newline-separated)
    """
    if isinstance(data, dict):
        data = [data]
    return '\n'.join(json.dumps(item) for item in data)

def get_weather(lat, lon, city_name, api_key):
    """
    Fetch current weather data for a specific geographic location.
    
    This function queries the OpenWeatherMap Current Weather Data API to get
    real-time weather conditions at a given latitude/longitude coordinate.
    The API returns comprehensive weather information including temperature,
    humidity, pressure, wind, and weather conditions.
    
    Args:
        lat (float): Latitude coordinate of the location
        lon (float): Longitude coordinate of the location
        city_name (str): Name of the city (for metadata enrichment)
        api_key (str): OpenWeatherMap API key for authentication
        
    Returns:
        dict: Weather data dictionary with metadata, or None on error
        
    API Endpoint:
        GET /data/2.5/weather
        
    API Parameters:
        - lat: Latitude coordinate
        - lon: Longitude coordinate
        - appid: API key (authentication)
        
    Response Structure (enriched with metadata):
        {
            "coord": {"lon": float, "lat": float},
            "weather": [{"id": int, "main": str, "description": str, "icon": str}],
            "base": str,
            "main": {
                "temp": float,           # Temperature in Kelvin
                "feels_like": float,     # Feels-like temperature
                "temp_min": float,       # Minimum temperature
                "temp_max": float,       # Maximum temperature
                "pressure": int,         # Atmospheric pressure (hPa)
                "humidity": int          # Humidity percentage
            },
            "visibility": int,           # Visibility in meters
            "wind": {
                "speed": float,          # Wind speed (m/s)
                "deg": int              # Wind direction (degrees)
            },
            "clouds": {"all": int},      # Cloud coverage percentage
            "dt": int,                  # Data time (Unix timestamp)
            "sys": {
                "type": int,
                "id": int,
                "country": str,
                "sunrise": int,         # Sunrise time (Unix timestamp)
                "sunset": int           # Sunset time (Unix timestamp)
            },
            "timezone": int,            # Timezone offset (seconds)
            "id": int,                 # City ID
            "name": str,               # City name
            "cod": int,               # Internal parameter
            "meta_city": str,          # Added: City name for tracking
            "meta_scraped_at": str     # Added: ISO timestamp of data collection
        }
        
    Note: Temperature values are in Kelvin by default. Convert to Celsius: K - 273.15
    """
    # Construct API URL with query parameters
    # OpenWeatherMap Current Weather Data API endpoint
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
    
    try:
        # Make HTTP GET request with 10-second timeout
        # Timeout prevents Lambda from hanging if API is slow/unresponsive
        res = requests.get(url, timeout=10)
        
        # Check for HTTP errors
        if res.status_code != 200:
            # Log error details (first 50 chars of response)
            print(f"Open Weather Map API Error: {res.status_code} - {res.text[:50]}")
            return None
            
        # Parse JSON response
        data = res.json()
        
        # Enrich weather data with metadata
        # This helps track where and when data was collected
        data['meta_city'] = city_name                    # City name for filtering/grouping
        data['meta_scraped_at'] = datetime.now().isoformat()  # Collection timestamp (ISO 8601)
        
        return data
        
    except Exception as e:
        # Log error but don't fail - allows other cities to be processed
        print(f"Error fetching weather data for {city_name}: {e}")
        return None

def lambda_handler(event, context):
    """
    AWS Lambda handler function - entry point for the Lambda execution.
    
    This function orchestrates the weather data collection process for all
    configured cities. It processes each city sequentially, aggregates the results,
    and writes them to S3.
    
    Args:
        event (dict): Event data passed to Lambda (usually empty dict for scheduled invocations)
        context (LambdaContext): Runtime information about the Lambda execution
        
    Returns:
        dict: HTTP-like response with statusCode and body
            - statusCode 200: Success (with or without data)
            - statusCode 500: Error occurred
            
    Environment Variables:
        S3_BUCKET (required): Name of S3 bucket to write data to
        S3_OUTPUT_PATH (optional): Prefix path in S3 bucket (default: 'ingest-data')
        WEATHER_API_KEY (required): OpenWeatherMap API key for authentication
        
    S3 Output Format:
        s3://{S3_BUCKET}/{S3_OUTPUT_PATH}/weather_data/YYYY/MM/DD/HHMMSS_weather_data.jsonl
        
    Processing Flow:
        1. Read configuration from environment variables
        2. Validate required variables are set
        3. Iterate through all cities in CITY_COORDINATES
        4. Fetch weather data for each city
        5. Aggregate all city weather data
        6. Convert to JSONL format
        7. Upload to S3
        8. Return success response
        
    Example Response:
        {
            'statusCode': 200,
            'body': '{"message": "Weather data collection successful", "cities_processed": 5, ...}'
        }
    """
    try:
        # Read configuration from environment variables
        # These are set when the Lambda function is created/updated
        s3_bucket = os.environ.get('S3_BUCKET')
        s3_output_path = os.environ.get('S3_OUTPUT_PATH', 'ingest-data')
        weather_api_key = os.environ.get('WEATHER_API_KEY')
        
        # Validate required environment variables
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
        
        # Process each city and collect weather data
        all_data = []
        
        # Iterate through all configured cities
        # Sequential processing is fine here since we only have 5 cities
        for city_name, coords in CITY_COORDINATES.items():
            print(f"Fetching weather for {city_name}...")
            
            # Fetch current weather data for this city
            # Uses city center coordinates to get representative weather conditions
            weather_data = get_weather(coords['lat'], coords['lon'], city_name, weather_api_key)
            
            # Add to results if data was successfully fetched
            if weather_data:
                all_data.append(weather_data)
        
        # Write data to S3 if we collected any
        if all_data:
            # Convert list of city weather dictionaries to JSONL format
            # JSONL is efficient for large datasets and streaming processing
            jsonl_string = dict_to_jsonl(all_data)
            
            # Generate S3 object key with timestamp-based path
            # Format: YYYY/MM/DD/HHMMSS_weather_data.jsonl
            # This creates a hierarchical structure in S3 that's easy to query by date
            timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
            s3_key = f"{s3_output_path}/weather_data/{timestamp}_weather_data.jsonl"
            
            # Upload data to S3
            # ContentType is set to 'application/json' for proper handling by S3
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=jsonl_string.encode('utf-8'),  # Encode string to bytes for S3
                ContentType='application/json'
            )
            
            # Return success response with metadata
            # This information is useful for monitoring and debugging
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Weather data collection successful',
                    'cities_processed': len(all_data),  # Number of cities successfully processed
                    's3_location': f's3://{s3_bucket}/{s3_key}'  # S3 location for downloaded data
                })
            }
        else:
            # No data collected, but not necessarily an error
            # Could happen if API is down or rate-limited
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data collected',
                    'cities_processed': 0
                })
            }
            
    except Exception as e:
        # Catch any unexpected errors and return error response
        # Logging to CloudWatch Logs for debugging
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

