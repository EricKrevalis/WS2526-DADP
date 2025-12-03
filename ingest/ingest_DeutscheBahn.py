import requests
import json
import os
import time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class DeutscheBahnLocalTransportScraper:
    def __init__(self):
        self.base_url = "https://v6.db.transport.rest"
        
        # Local Output Setup
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = os.path.join(script_dir, "..\output_examples")
        os.makedirs(self.output_dir, exist_ok=True)
        self.output_file = os.path.join(self.output_dir, "db_scraper_output.jsonl")
        
        # Polite Scraping Headers
        self.headers = {
            'User-Agent': 'TrafficProject/1.0 (Eric.Krevalis@haw-hamburg.de)',
            'Content-Type': 'application/json'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # INCREASED BACKOFF: Sleep longer between retries (2s, 4s, 8s)
        retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

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
                'results': 300,
                'nationalExpress': 'false', 'national': 'false', 'regional': 'false',
                'suburban': 'true', 'subway': 'true', 'tram': 'true', 'bus': 'true', 'ferry': 'true',
                'remarks': 'false'
            }
            url = f"{self.base_url}/stops/{station_id}/departures"
            
            # INCREASED TIMEOUT: From 10 to 30 seconds
            res = self.session.get(url, params=params, timeout=30)
            res.raise_for_status()
            data = res.json()
            departures = data.get('departures', []) if isinstance(data, dict) else data
            
            enriched_data = []
            scrape_timestamp = datetime.now().isoformat()
            
            for dep in departures:
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
            print(f"Error {city_name}: {e}")
            return []

    def save_to_jsonl(self, data):
        if not data: return
        with open(self.output_file, 'a', encoding='utf-8') as f:
            for entry in data:
                f.write(json.dumps(entry) + '\n')

    def run_cycle(self):
        print(f"--- Starting DB Scrape Cycle: {datetime.now()} ---")
        for city, station_id in self.cities.items():
            print(f"Scanning {city}...", end=" ")
            data = self.get_local_departures(city, station_id, duration=10)
            self.save_to_jsonl(data)
            print(f"Captured {len(data)} events.")
            time.sleep(1.0) # Rate limit safety
        print(f"Saved to: {self.output_file}")

if __name__ == "__main__":
    scraper = DeutscheBahnLocalTransportScraper()
    scraper.run_cycle()