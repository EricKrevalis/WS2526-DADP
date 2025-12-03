import requests
import json
import os
import time
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ENV VAR or Hardcoded
API_KEY = os.environ.get('OWM_API_KEY', '76d3c2c705805e009533b5cee35fafef')

class OpenWeatherMapWeatherScraper:
    def __init__(self):
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        
        # Local Output Setup
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = os.path.join(script_dir, "..\output_examples")
        os.makedirs(self.output_dir, exist_ok=True)
        self.output_file = os.path.join(self.output_dir, "owm_scraper_output.jsonl")
        
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        self.city_bboxes = {
            "Hamburg":   "9.725,53.395,10.325,53.695",
            "Berlin":    "13.088,52.338,13.761,52.675",
            "Frankfurt": "8.480,50.000,8.800,50.230",
            "Munich":    "11.360,48.060,11.722,48.248",
            "Cologne":   "6.772,50.830,7.162,51.085"
        }

    def get_grid_points(self, bbox_str, grid_size=3):
        min_lon, min_lat, max_lon, max_lat = map(float, bbox_str.split(','))
        step_x = (max_lon - min_lon) / grid_size
        step_y = (max_lat - min_lat) / grid_size
        points = []
        for i in range(grid_size):
            for j in range(grid_size):
                points.append({
                    'id': f"{i}_{j}",
                    'lat': min_lat + (j * step_y) + (step_y / 2),
                    'lon': min_lon + (i * step_x) + (step_x / 2)
                })
        return points

    def fetch_weather(self, lat, lon):
        try:
            params = {'lat': lat, 'lon': lon, 'appid': API_KEY, 'units': 'metric'}
            res = self.session.get(self.base_url, params=params, timeout=5)
            return res.json() if res.status_code == 200 else None
        except:
            return None

    def save_to_jsonl(self, data):
        if not data: return
        with open(self.output_file, 'a', encoding='utf-8') as f:
            for entry in data:
                f.write(json.dumps(entry) + '\n')

    def run_cycle(self):
        print(f"--- Starting Weather Scrape: {datetime.now()} ---")
        timestamp = datetime.now().isoformat()
        
        for city, bbox in self.city_bboxes.items():
            print(f"Scanning {city}...", end=" ")
            points = self.get_grid_points(bbox)
            city_records = []
            
            for pt in points:
                data = self.fetch_weather(pt['lat'], pt['lon'])
                
                if data:
                    rain_data = data.get('rain', {})
                    rain_1h = rain_data.get('1h', 0) if isinstance(rain_data, dict) else 0

                    # FLATTENED RECORD
                    record = {
                        "meta_city": city,
                        "meta_scraped_at": timestamp,
                        "grid_id": pt['id'], # Key for Joining
                        "lat": pt['lat'],
                        "lon": pt['lon'],
                        "temp": data.get('main', {}).get('temp'),
                        "humidity": data.get('main', {}).get('humidity'),
                        "weather_main": data.get('weather', [{}])[0].get('main'),
                        "description": data.get('weather', [{}])[0].get('description'),
                        "wind_speed": data.get('wind', {}).get('speed'),
                        "rain_1h_mm": rain_1h
                    }
                    city_records.append(record)
                time.sleep(0.1) # Be polite
            
            self.save_to_jsonl(city_records)
            print(f"Captured {len(city_records)} weather points.")
            
        print(f"Saved to: {self.output_file}")

if __name__ == "__main__":
    scraper = OpenWeatherMapWeatherScraper()
    scraper.run_cycle()