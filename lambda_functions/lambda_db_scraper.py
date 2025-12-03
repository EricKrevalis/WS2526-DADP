"""
Lambda Function: DB Transport Scraper

This Lambda function collects public transport departure data from the DB Transport REST API
for major German cities. It runs on a schedule (every 10 minutes) and writes the collected
data to S3 in JSONL format.

Data Source: https://v6.db.transport.rest
API Documentation: https://github.com/derhuerst/db-rest/blob/6/docs/readme.md

What it does:
1. Queries departure boards for 5 major German cities (Hamburg, Berlin, Frankfurt, Munich, Cologne)
2. Filters for local transport only (S-Bahn, U-Bahn, Tram, Bus, Ferry) - excludes long-distance trains
3. Enriches each departure record with metadata (city name, scrape timestamp)
4. Converts data to JSONL format (one JSON object per line)
5. Uploads to S3 with timestamped path structure

S3 Output Structure:
    s3://bucket/ingest-data/traffic_data/YYYY/MM/DD/HHMMSS_traffic_data.jsonl

Environment Variables Required:
    - S3_BUCKET: Name of the S3 bucket to write data to
    - S3_OUTPUT_PATH: Prefix path in S3 (default: 'ingest-data')
"""

import json
import time
from datetime import datetime
import requests
import boto3
import os

# Initialize S3 client for writing data
# This client is reused across all S3 operations in this Lambda function
s3_client = boto3.client('s3')

def dict_to_jsonl(data):
    """
    Convert a list of dictionaries (or a single dictionary) to a JSONL string.
    
    JSONL (JSON Lines) format: Each line is a valid JSON object, separated by newlines.
    This format is efficient for streaming and processing large datasets.
    
    Args:
        data: A list of dictionaries or a single dictionary to convert
        
    Returns:
        str: A string where each line is a JSON object (newline-separated)
        
    Example:
        Input: [{"city": "Berlin"}, {"city": "Hamburg"}]
        Output: '{"city": "Berlin"}\n{"city": "Hamburg"}'
    """
    # Handle single dictionary case by wrapping it in a list
    if isinstance(data, dict):
        data = [data]
    # Convert each dictionary to JSON string and join with newlines
    return '\n'.join(json.dumps(item) for item in data)

class ProductionTrafficScraper:
    """
    Scraper class for collecting public transport departure data from DB Transport API.
    
    This class handles API interactions, data enrichment, and coordinates the scraping
    process for multiple cities. It uses a requests Session for connection pooling and
    efficient HTTP requests.
    """
    
    def __init__(self):
        """
        Initialize the scraper with API endpoint and city configurations.
        
        Sets up:
        - Base URL for DB Transport REST API (v6)
        - HTTP session for connection reuse (improves performance)
        - City-to-station-ID mapping for the 5 major German cities
        
        Station IDs are DB-specific identifiers for each city's main station.
        These are used to query departure boards for that location.
        """
        # DB Transport REST API v6 endpoint (public, no authentication required)
        self.base_url = "https://v6.db.transport.rest"
        
        # Use Session for connection pooling - reuses TCP connections for better performance
        # This is important when making multiple API calls in sequence
        self.session = requests.Session()
        
        # Map of city names to their DB station IDs
        # These IDs correspond to the main train stations in each city
        # Station IDs can be found at: https://v6.db.transport.rest/stations
        self.cities = {
            "Hamburg": "8002549",      # Hamburg Hauptbahnhof
            "Berlin": "8011160",        # Berlin Hauptbahnhof
            "Frankfurt": "8000105",     # Frankfurt (Main) Hauptbahnhof
            "Munich": "8000261",        # München Hauptbahnhof
            "Cologne": "8000207"        # Köln Hauptbahnhof
        }

    def get_local_departures(self, city_name, station_id, duration=30):
        """
        Fetch local transport departures for a specific station.
        
        This method queries the DB Transport API for departure information at a given station.
        It filters for local transport only (excludes long-distance trains) and enriches
        each departure record with metadata.
        
        Args:
            city_name (str): Name of the city (for metadata)
            station_id (str): DB station ID for the station to query
            duration (int): Time window in minutes to look ahead (default: 30 minutes)
                           This determines how far into the future departures are fetched
        
        Returns:
            list: List of enriched departure dictionaries, or empty list on error
            
        API Endpoint: GET /stops/{station_id}/departures
        Documentation: https://github.com/derhuerst/db-rest/blob/6/docs/departures.md
        """
        try:
            # API request parameters
            # These parameters filter what types of transport to include
            params = {
                'duration': duration,           # Look ahead window: 30 minutes
                'results': 500,                 # Maximum number of results to return
                
                # Exclude long-distance trains (we only want local transport)
                'nationalExpress': 'false',    # ICE (InterCity Express) - excluded
                'national': 'false',          # IC/EC (InterCity/EuroCity) - excluded
                'regional': 'false',          # RE/RB (Regional trains) - excluded
                
                # Include local transport modes
                'suburban': 'true',           # S-Bahn (suburban rail)
                'subway': 'true',             # U-Bahn (underground/metro)
                'tram': 'true',               # Tram/Streetcar
                'bus': 'true',                # Bus
                'ferry': 'true',              # Ferry
                
                # Exclude remarks/notes to reduce payload size
                'remarks': 'false'
            }
            
            # Make API request to get departures
            # URL format: /stops/{station_id}/departures
            res = self.session.get(f"{self.base_url}/stops/{station_id}/departures", params=params)
            
            # Raise exception for HTTP error status codes (4xx, 5xx)
            # This helps catch API errors early
            res.raise_for_status()
            
            # Parse JSON response
            data = res.json()

            # Handle different API response formats
            # API may return either a dict with 'departures' key or a direct list
            departures = []
            if isinstance(data, dict):
                # Response wrapped in object: {"departures": [...]}
                departures = data.get('departures', [])
            elif isinstance(data, list):
                # Response is direct list: [...]
                departures = data
            
            # Enrich departure data with metadata
            # This helps track where and when data was collected
            enriched_data = []
            scrape_timestamp = datetime.now().isoformat()  # ISO 8601 format: 2024-01-15T12:00:00
            
            for dep in departures:
                # Add city name to each departure record
                # This allows filtering/grouping by city in downstream processing
                dep['meta_city'] = city_name
                
                # Add timestamp of when this data was scraped
                # Important for data lineage and understanding data freshness
                dep['meta_scraped_at'] = scrape_timestamp
                
                enriched_data.append(dep)
                
            return enriched_data

        except Exception as e:
            # Log error but don't fail the entire job
            # This allows other cities to be processed even if one fails
            print(f"Error fetching {city_name}: {e}")
            return []

    def run_cycle(self):
        """
        Execute a complete scraping cycle for all configured cities.
        
        This method iterates through all cities, fetches departure data for each,
        and aggregates the results. It includes rate limiting to avoid overwhelming
        the API server.
        
        Returns:
            list: Aggregated list of all departure records from all cities
            
        Process:
        1. Iterate through each city in the configuration
        2. Fetch departures for that city's main station
        3. Aggregate all results into a single list
        4. Sleep between requests to respect rate limits
        5. Return all collected data
        """
        print(f"--- Starting Scrape Cycle: {datetime.now()} ---")
        all_data = []      # Accumulator for all departure records
        total_count = 0    # Counter for logging/tracking
        
        # Process each city sequentially
        # Sequential processing ensures we don't overwhelm the API
        for city, station_id in self.cities.items():
            print(f"Scanning {city}...", end=" ")
            
            # Fetch departures for this city's main station
            # Duration of 30 minutes means we get all departures in the next 30 minutes
            data = self.get_local_departures(city, station_id, duration=30)
            
            # Add this city's data to the aggregated list
            all_data.extend(data)
            
            # Track statistics for logging
            count = len(data)
            total_count += count
            print(f"Captured {count} events.")
            
            # Rate limiting: Wait 1 second between requests
            # This prevents hitting API rate limits and being blocked
            # DB Transport API is public and free, so we're respectful with our usage
            time.sleep(1)

        print(f"--- Cycle Complete. Total Logged: {total_count} ---")
        return all_data

def lambda_handler(event, context):
    """
    AWS Lambda handler function - entry point for the Lambda execution.
    
    This function is called by AWS Lambda when the function is invoked (either by
    EventBridge schedule or manual invocation). It orchestrates the data collection
    process and handles errors gracefully.
    
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
        
    S3 Output Format:
        s3://{S3_BUCKET}/{S3_OUTPUT_PATH}/traffic_data/YYYY/MM/DD/HHMMSS_traffic_data.jsonl
        
    Example Response:
        {
            'statusCode': 200,
            'body': '{"message": "Data collection successful", "records_collected": 150, ...}'
        }
    """
    try:
        # Read configuration from environment variables
        # These are set when the Lambda function is created/updated
        s3_bucket = os.environ.get('S3_BUCKET')
        s3_output_path = os.environ.get('S3_OUTPUT_PATH', 'ingest-data')
        
        # Validate required environment variables
        if not s3_bucket:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'S3_BUCKET environment variable not set'})
            }
        
        # Initialize scraper and run data collection cycle
        # This will fetch data from all configured cities
        scraper = ProductionTrafficScraper()
        data = scraper.run_cycle()
        
        # Only write to S3 if we collected data
        # This prevents creating empty files
        if data:
            # Convert list of dictionaries to JSONL format
            # JSONL is efficient for large datasets and streaming processing
            jsonl_string = dict_to_jsonl(data)
            
            # Generate S3 object key with timestamp-based path
            # Format: YYYY/MM/DD/HHMMSS_traffic_data.jsonl
            # This creates a hierarchical structure in S3 that's easy to query by date
            timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
            s3_key = f"{s3_output_path}/traffic_data/{timestamp}_traffic_data.jsonl"
            
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
                    'message': 'Data collection successful',
                    'records_collected': len(data),
                    's3_location': f's3://{s3_bucket}/{s3_key}'
                })
            }
        else:
            # No data collected, but this is not necessarily an error
            # Could happen if API is down or no departures scheduled
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data collected in this cycle',
                    'records_collected': 0
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

