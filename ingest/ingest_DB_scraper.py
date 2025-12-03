import requests
import json
import time
from datetime import datetime
import os
from util import dict_to_jsonl

class ProductionTrafficScraper:
    def __init__(self):
        self.base_url = "https://v6.db.transport.rest"
        self.session = requests.Session()
        self.output_file = "traffic_raw_data.jsonl"
        
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
                'results': 500, # High limit for dense local traffic
                
                # --- TRAFFIC FILTERS ---
                'nationalExpress': 'false', # No ICE
                'national': 'false',        # No IC/EC
                'regional': 'false',        # No RE/RB
                
                'suburban': 'true',         # S-Bahn
                'subway': 'true',           # U-Bahn
                'tram': 'true',             # Tram
                'bus': 'true',              # Bus
                'ferry': 'true',            # Ferry
                
                'remarks': 'false'          # Keep payload smaller
            }
            
            res = self.session.get(f"{self.base_url}/stops/{station_id}/departures", params=params)
            res.raise_for_status()
            data = res.json()

            # Robust handling for API wrappers vs Lists
            departures = []
            if isinstance(data, dict):
                departures = data.get('departures', [])
            elif isinstance(data, list):
                departures = data
            
            # Enrich raw data with metadata for your Pipeline/LLM
            enriched_data = []
            scrape_timestamp = datetime.now().isoformat()
            
            for dep in departures:
                # We inject these fields into the raw object so your database knows 
                # where and when this data came from.
                dep['meta_city'] = city_name
                dep['meta_scraped_at'] = scrape_timestamp
                enriched_data.append(dep)
                
            return enriched_data

        except Exception as e:
            print(f"Error fetching {city_name}: {e}")
            return []

#    def save_to_jsonl(self, data):
#        """
#        Appends data to JSONL. 
#        Each line is 1 valid JSON object.
#        """
#       if not data:
#           return
#
#        with open(self.output_file, 'a', encoding='utf-8') as f:
#            for entry in data:
#                f.write(json.dumps(entry) + '\n')

    def run_cycle(self):
        print(f"--- Starting Scrape Cycle: {datetime.now()} ---")
        total_count = 0
        
        for city, station_id in self.cities.items():
            print(f"Scanning {city}...", end=" ")
            
            data = self.get_local_departures(city, station_id, duration=30)
            #self.save_to_jsonl(data)
            jsonl_string=dict_to_jsonl(data)
            
            with open(self.output_file, 'a') as f:
                f.write(jsonl_string)
            
            count = len(data)
            total_count += count
            print(f"Captured {count} events.")
            
            time.sleep(1) # Rate limit safety

        print(f"--- Cycle Complete. Total Logged: {total_count} ---")

if __name__ == "__main__":
    scraper = ProductionTrafficScraper()
    scraper.run_cycle()
    
    # Continuous Loop (Run every 5 minutes)
    #while True:
    #    scraper.run_cycle()
    #    print("Waiting 5 minutes...")
    #    time.sleep(300)