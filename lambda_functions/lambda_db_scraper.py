import json
import time
from datetime import datetime
import requests
import boto3
import os

s3_client = boto3.client('s3')

def dict_to_jsonl(data):
    """Convert a list of dictionaries (or a single dictionary) to a jsonl string."""
    if isinstance(data, dict):
        data = [data]
    return '\n'.join(json.dumps(item) for item in data)

class ProductionTrafficScraper:
    def __init__(self):
        self.base_url = "https://v6.db.transport.rest"
        self.session = requests.Session()
        
        # The Big 5 Cities
        self.cities = {
            "Hamburg": "8002549",
            "Berlin": "8011160",
            "Frankfurt": "8000105",
            "Munich": "8000261",
            "Cologne": "8000207"
        }

    def get_local_departures(self, city_name, station_id, duration=30):
        try:
            params = {
                'duration': duration,
                'results': 500,
                'nationalExpress': 'false',
                'national': 'false',
                'regional': 'false',
                'suburban': 'true',
                'subway': 'true',
                'tram': 'true',
                'bus': 'true',
                'ferry': 'true',
                'remarks': 'false'
            }
            
            res = self.session.get(f"{self.base_url}/stops/{station_id}/departures", params=params)
            res.raise_for_status()
            data = res.json()

            departures = []
            if isinstance(data, dict):
                departures = data.get('departures', [])
            elif isinstance(data, list):
                departures = data
            
            enriched_data = []
            scrape_timestamp = datetime.now().isoformat()
            
            for dep in departures:
                dep['meta_city'] = city_name
                dep['meta_scraped_at'] = scrape_timestamp
                enriched_data.append(dep)
                
            return enriched_data

        except Exception as e:
            print(f"Error fetching {city_name}: {e}")
            return []

    def run_cycle(self):
        print(f"--- Starting Scrape Cycle: {datetime.now()} ---")
        all_data = []
        total_count = 0
        
        for city, station_id in self.cities.items():
            print(f"Scanning {city}...", end=" ")
            
            data = self.get_local_departures(city, station_id, duration=30)
            all_data.extend(data)
            
            count = len(data)
            total_count += count
            print(f"Captured {count} events.")
            
            time.sleep(1)  # Rate limit safety

        print(f"--- Cycle Complete. Total Logged: {total_count} ---")
        return all_data

def lambda_handler(event, context):
    """
    AWS Lambda handler for DB Transport scraper.
    
    Environment variables required:
    - S3_BUCKET: S3 bucket name for output
    - S3_OUTPUT_PATH: S3 prefix path (default: 'ingest-data')
    """
    try:
        s3_bucket = os.environ.get('S3_BUCKET')
        s3_output_path = os.environ.get('S3_OUTPUT_PATH', 'ingest-data')
        
        if not s3_bucket:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'S3_BUCKET environment variable not set'})
            }
        
        scraper = ProductionTrafficScraper()
        data = scraper.run_cycle()
        
        if data:
            # Convert to JSONL
            jsonl_string = dict_to_jsonl(data)
            
            # Generate S3 key with timestamp
            timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
            s3_key = f"{s3_output_path}/traffic_data/{timestamp}_traffic_data.jsonl"
            
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

