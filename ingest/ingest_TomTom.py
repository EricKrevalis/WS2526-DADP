import requests
import json
import time
import os
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class TomTomFlowScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        
        # Get directory of the current script to anchor output
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_dir = os.path.join(script_dir, "..\output_examples")
        os.makedirs(self.output_dir, exist_ok=True)
        self.output_file = os.path.join(self.output_dir, "tomtom_scraper_output.jsonl")
        
        # Robust Session
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        # Standardized "Big 5" Bounding Boxes (minLon, minLat, maxLon, maxLat)
        self.city_bboxes = {
            "Hamburg":   "9.725,53.395,10.325,53.695",
            "Berlin":    "13.088,52.338,13.761,52.675",
            "Frankfurt": "8.480,50.000,8.800,50.230",
            "Munich":    "11.360,48.060,11.722,48.248",
            "Cologne":   "6.772,50.830,7.162,51.085"
        }

    def get_grid_points(self, bbox_str, grid_size=3):
        """
        Divides a bounding box into a grid and returns the center points of each cell.
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

    def fetch_flow_segment(self, lat, lon):
        try:
            # Note: unit=KMPH
            url = f"{self.base_url}?point={lat},{lon}&unit=KMPH&key={self.api_key}"
            res = self.session.get(url, timeout=10)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            # We fail silently here to not spam logs for every missing road segment
            return None

    def process_city(self, city_name, bbox):
        points = self.get_grid_points(bbox, grid_size=3)
        print(f"Sampling {len(points)} points...", end=" ")
        
        city_samples = []
        total_current = 0
        total_free_flow = 0
        valid_samples = 0
        
        for pt in points:
            data = self.fetch_flow_segment(pt['lat'], pt['lon'])
            
            if data and 'flowSegmentData' in data:
                flow = data['flowSegmentData']
                current = flow.get('currentSpeed', 0)
                free = flow.get('freeFlowSpeed', 0)
                confidence = flow.get('confidence', 0)
                
                # Ratio: 1.0 = Clear, 0.5 = Half Speed
                ratio = current / free if free > 0 else 1.0
                
                city_samples.append({
                    "grid_id": pt['id'],
                    "lat": pt['lat'],
                    "lon": pt['lon'],
                    "current_speed": current,
                    "free_flow_speed": free,
                    "congestion_ratio": round(ratio, 2),
                    "confidence": confidence
                })

                # Only aggregate high-confidence data for the city-wide score
                if confidence > 0.5:
                    total_current += current
                    total_free_flow += free
                    valid_samples += 1
            
            # Rate limit politeness per point
            time.sleep(0.1)

        # Calculate City-Wide Index
        avg_ratio = 1.0
        if total_free_flow > 0:
            avg_ratio = total_current / total_free_flow
            
        status = "flowing"
        if avg_ratio < 0.7: status = "congested"
        elif avg_ratio < 0.85: status = "heavy_traffic"

        # Final Document Structure
        return {
            "meta_city": city_name,
            "meta_scraped_at": datetime.now().isoformat(),
            "meta_type": "tomtom_grid_flow",
            "grid_size": "3x3",
            "average_congestion_ratio": round(avg_ratio, 2),
            "status_label": status,
            "samples": city_samples
        }

    def save_to_jsonl(self, data):
        if not data: return
        with open(self.output_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data) + '\n')

    def run_cycle(self):
        print(f"--- Starting TomTom Grid Flow Cycle: {datetime.now()} ---")
        
        for city, bbox in self.city_bboxes.items():
            print(f"Scanning {city}...", end=" ")
            
            result = self.process_city(city, bbox)
            self.save_to_jsonl(result)
            
            print(f"Done. Status: {result['status_label']} ({result['average_congestion_ratio']})")
            time.sleep(1)

        print(f"--- Cycle Complete ---")
        print(f"File saved to: {os.path.abspath(self.output_file)}")

if __name__ == "__main__":
    # REPLACE WITH YOUR ACTUAL KEY
    API_KEY = "C9A2Op6bcWPrr2AGEJv6mFpdz3aOftO1"
    
    scraper = TomTomFlowScraper(API_KEY)
    scraper.run_cycle()