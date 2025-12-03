import requests
import json
import time
import os
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class DBLocalTransportScraper:
    def __init__(self):
        self.base_url = "https://v6.db.transport.rest"
        
        # Get directory of the current script to anchor output
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Create 'output_examples' folder in the same directory as the script
        self.output_dir = os.path.join(script_dir, "..\output_examples")
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.output_file = os.path.join(self.output_dir, "db_scraper_output.jsonl")
        
        # Polite Scraping: Identification is required by the community API.
        # IMPORTANT: Replace 'your_email@example.com' with your actual email!
        #             'User-Agent': 'TrafficScraperProject/1.0 (your_email@example.com)',
        self.headers = {
            'User-Agent': 'TrafficScraperProject/1.0 (Eric.Krevalis@haw-hamburg.de)',
            'Content-Type': 'application/json'
        }

        # Robust Session with Retry Logic
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Retry 3 times on server errors (5xx) or timeouts
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        # The Big 5 Cities (EVA / IBNR Codes)
        self.cities = {
            "Hamburg": "8002549",
            "Berlin": "8011160",
            "Frankfurt": "8000105",
            "Munich": "8000261",
            "Cologne": "8000207"
        }

    def get_local_departures(self, city_name, station_id, duration=10):
        try:
            params = {
                'duration': duration,
                'results': 300, # Reduced from 500 to be kinder to the free API
                
                # --- TRAFFIC FILTERS ---
                # We disable long-distance to focus on local city flow
                'nationalExpress': 'false', 
                'national': 'false',       
                'regional': 'false',       
                
                'suburban': 'true',         # S-Bahn
                'subway': 'true',           # U-Bahn
                'tram': 'true',            
                'bus': 'true',             
                'ferry': 'true',           
                
                'remarks': 'false'          # Keep payload smaller
            }
            
            # Using the /stops/{id}/departures endpoint
            url = f"{self.base_url}/stops/{station_id}/departures"
            res = self.session.get(url, params=params, timeout=10)
            res.raise_for_status()
            data = res.json()

            departures = data.get('departures', []) if isinstance(data, dict) else data
            
            # Enrich raw data
            enriched_data = []
            scrape_timestamp = datetime.now().isoformat()
            
            for dep in departures:
                # Flattens the structure slightly for easier analysis later
                entry = {
                    'meta_city': city_name,
                    'meta_scraped_at': scrape_timestamp,
                    'line': dep.get('line', {}).get('name'),
                    'direction': dep.get('direction'),
                    'planned_time': dep.get('plannedWhen'),
                    'delay': dep.get('delay', 0),
                    'platform': dep.get('platform'),
                    'raw_id': dep.get('tripId')
                }
                enriched_data.append(entry)
                
            return enriched_data

        except Exception as e:
            print(f"Error fetching {city_name}: {e}")
            return []

    def save_to_jsonl(self, data):
        """
        Self-contained JSONL writer to avoid external dependencies.
        """
        if not data:
            return

        with open(self.output_file, 'a', encoding='utf-8') as f:
            for entry in data:
                f.write(json.dumps(entry) + '\n')

    def run_cycle(self):
        print(f"--- Starting DB Scrape Cycle: {datetime.now()} ---")
        total_count = 0
        
        for city, station_id in self.cities.items():
            print(f"Scanning {city}...", end=" ")
            
            data = self.get_local_departures(city, station_id, duration=10)
            self.save_to_jsonl(data)
            
            count = len(data)
            total_count += count
            print(f"Captured {count} events.")
            
            # Respect rate limits (community API is strict)
            time.sleep(1.5) 

        print(f"--- Cycle Complete. Total Logged: {total_count} ---")
        # Helper to show where the file is
        print(f"File saved to: {os.path.abspath(self.output_file)}")

if __name__ == "__main__":
    scraper = DBLocalTransportScraper()
    scraper.run_cycle()