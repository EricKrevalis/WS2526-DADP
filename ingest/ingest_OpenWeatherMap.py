import requests
import json
import time
import os
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class OWMWeatherGridScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        # Using standard current weather endpoint
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        
        # Path Anchoring: Output to 'output_examples' folder relative to this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = os.path.join(script_dir, "..\output_examples")
        os.makedirs(self.output_dir, exist_ok=True)
        self.output_file = os.path.join(self.output_dir, "owm_scraper_output.jsonl")
        
        # Robust Session with Retry Logic
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        # Standardized "Big 5" Bounding Boxes (Matches TomTom exactly for correlation)
        self.city_bboxes = {
            "Hamburg":   "9.725,53.395,10.325,53.695",
            "Berlin":    "13.088,52.338,13.761,52.675",
            "Frankfurt": "8.480,50.000,8.800,50.230",
            "Munich":    "11.360,48.060,11.722,48.248",
            "Cologne":   "6.772,50.830,7.162,51.085"
        }

    def get_grid_points(self, bbox_str, grid_size=3):
        """
        Reused logic: Divides a bounding box into a grid.
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

    def fetch_weather_at_point(self, lat, lon):
        try:
            params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key,
                'units': 'metric',
            }
            # Timeout slightly shorter for grid operations to keep things moving
            res = self.session.get(self.base_url, params=params, timeout=10)
            res.raise_for_status()
            return res.json()
        except Exception:
            # Silent fail for individual grid points is acceptable here
            return None

    def process_city(self, city_name, bbox):
        points = self.get_grid_points(bbox, grid_size=3)
        print(f"Sampling {len(points)} weather points...", end=" ")
        
        city_samples = []
        
        for pt in points:
            data = self.fetch_weather_at_point(pt['lat'], pt['lon'])
            
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

    def save_to_jsonl(self, data):
        if not data: return
        with open(self.output_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data) + '\n')

    def run_cycle(self):
        print(f"--- Starting Weather Grid Cycle: {datetime.now()} ---")
        
        for city, bbox in self.city_bboxes.items():
            print(f"Scanning {city}...", end=" ")
            
            result = self.process_city(city, bbox)
            self.save_to_jsonl(result)
            
            print(f"Done. Captured {len(result['samples'])} points.")
            time.sleep(1)

        print(f"--- Cycle Complete ---")
        print(f"File saved to: {os.path.abspath(self.output_file)}")

if __name__ == "__main__":
    # REPLACE WITH YOUR ACTUAL KEY
    API_KEY = "76d3c2c705805e009533b5cee35fafef"
    
    scraper = OWMWeatherGridScraper(API_KEY)
    scraper.run_cycle()