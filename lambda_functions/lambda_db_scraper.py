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
3. Enriches each departure record with flattened structure for easier analysis
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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import boto3
import os

# Initialize S3 client for writing data
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
    """
    if isinstance(data, dict):
        data = [data]
    return '\n'.join(json.dumps(item) for item in data)

class DBLocalTransportScraper:
    """
    Scraper class for collecting public transport departure data from DB Transport API.
    
    This class handles API interactions, data enrichment, and coordinates the scraping
    process for multiple cities. It uses a requests Session with retry logic for
    robust HTTP requests.
    """
    
    def __init__(self):
        """
        Initialize the scraper with API endpoint and city configurations.
        
        Sets up:
        - Base URL for DB Transport REST API (v6)
        - HTTP session with retry logic for connection reuse and resilience
        - User-Agent header for polite API usage (required by community API)
        - City-to-station-ID mapping for the 5 major German cities
        """
        # DB Transport REST API v6 endpoint (public, no authentication required)
        self.base_url = "https://v6.db.transport.rest"
        
        # Polite Scraping: Identification is required by the community API
        self.headers = {
            'User-Agent': 'TrafficScraperProject/1.0 (Eric.Krevalis@haw-hamburg.de)',
            'Content-Type': 'application/json'
        }
        
        # Robust Session with Retry Logic
        # Retry 3 times on server errors (5xx) or timeouts
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # Map of city names to their DB station IDs (EVA / IBNR Codes)
        # These IDs correspond to the main train stations in each city
        self.cities = {
            "Hamburg": "8002549",      # Hamburg Hauptbahnhof
            "Berlin": "8011160",        # Berlin Hauptbahnhof
            "Frankfurt": "8000105",     # Frankfurt (Main) Hauptbahnhof
            "Munich": "8000261",        # München Hauptbahnhof
            "Cologne": "8000207"        # Köln Hauptbahnhof
        }

    def get_local_departures(self, city_name, station_id, duration=10):
        """
        Fetch local transport departures for a specific station.
        
        This method queries the DB Transport API for departure information at a given station.
        It filters for local transport only (excludes long-distance trains) and creates
        a flattened data structure for easier analysis.
        
        Args:
            city_name (str): Name of the city (for metadata)
            station_id (str): DB station ID for the station to query
            duration (int): Time window in minutes to look ahead (default: 10 minutes)
                           Reduced from 30 to be kinder to the free API
        
        Returns:
            list: List of enriched departure dictionaries with flattened structure
            
        Data Structure:
            Each departure record contains:
            - meta_city: City name
            - meta_scraped_at: ISO timestamp of collection
            - line: Line name (e.g., "S1", "U3")
            - direction: Destination/direction
            - planned_time: Scheduled departure time
            - delay: Delay in seconds (0 if on time)
            - platform: Platform number
            - raw_id: Trip ID for reference
        """
        try:
            # API request parameters
            params = {
                'duration': duration,           # Look ahead window: 10 minutes (reduced from 30)
                'results': 300,                 # Maximum results (reduced from 500 for free API)
                
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
            
            # Make API request with timeout
            url = f"{self.base_url}/stops/{station_id}/departures"
            res = self.session.get(url, params=params, timeout=10)
            res.raise_for_status()
            data = res.json()

            # Extract departures from response
            departures = data.get('departures', []) if isinstance(data, dict) else data
            
            # Enrich and flatten departure data
            enriched_data = []
            scrape_timestamp = datetime.now().isoformat()
            
            for dep in departures:
                # Flatten structure for easier analysis later
                # Extracts only relevant fields instead of keeping full nested object
                entry = {
                    'meta_city': city_name,
                    'meta_scraped_at': scrape_timestamp,
                    'line': dep.get('line', {}).get('name'),      # Line name (e.g., "S1", "U3")
                    'direction': dep.get('direction'),            # Destination/direction
                    'planned_time': dep.get('plannedWhen'),       # Scheduled departure time
                    'delay': dep.get('delay', 0),                 # Delay in seconds (0 if on time)
                    'platform': dep.get('platform'),              # Platform number
                    'raw_id': dep.get('tripId')                   # Trip ID for reference
                }
                enriched_data.append(entry)
                
            return enriched_data

        except Exception as e:
            print(f"Error fetching {city_name}: {e}")
            return []

    def run_cycle(self):
        """
        Execute a complete scraping cycle for all configured cities.
        
        Returns:
            list: Aggregated list of all departure records from all cities
        """
        print(f"--- Starting DB Scrape Cycle: {datetime.now()} ---")
        all_data = []
        total_count = 0
        
        for city, station_id in self.cities.items():
            print(f"Scanning {city}...", end=" ")
            
            data = self.get_local_departures(city, station_id, duration=10)
            all_data.extend(data)
            
            count = len(data)
            total_count += count
            print(f"Captured {count} events.")
            
            # Respect rate limits (community API is strict)
            time.sleep(1.5)

        print(f"--- Cycle Complete. Total Logged: {total_count} ---")
        return all_data

def lambda_handler(event, context):
    """
    AWS Lambda handler function - entry point for the Lambda execution.
    """
    try:
        s3_bucket = os.environ.get('S3_BUCKET')
        s3_output_path = os.environ.get('S3_OUTPUT_PATH', 'ingest-data')
        
        if not s3_bucket:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'S3_BUCKET environment variable not set'})
            }
        
        scraper = DBLocalTransportScraper()
        data = scraper.run_cycle()
        
        if data:
            jsonl_string = dict_to_jsonl(data)
            
            timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
            s3_key = f"{s3_output_path}/traffic_data/{timestamp}_traffic_data.jsonl"
            
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=jsonl_string.encode('utf-8'),
                ContentType='application/json'
            )
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Data collection successful',
                    'records_collected': len(data),
                    's3_location': f's3://{s3_bucket}/{s3_key}'
                })
            }
        else:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data collected in this cycle',
                    'records_collected': 0
                })
            }
            
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
