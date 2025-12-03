import requests
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from constants import CITIES, CITY_BBOXES

from util import dict_to_jsonl

# Load environment variables
load_dotenv('env')

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

def get_grid_points(bbox_str, grid_size=3):
    """
    Divides a bounding box into a grid and returns the center points of each cell.
    bbox_str format: "minLon,minLat,maxLon,maxLat"
    Returns: List of dicts {'lat': float, 'lon': float, 'id': str}
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
    
    for i in range(grid_size): # x-axis (longitude)
        for j in range(grid_size): # y-axis (latitude)
            # Calculate center of the grid cell
            center_lon = min_lon + (i * step_x) + (step_x / 2)
            center_lat = min_lat + (j * step_y) + (step_y / 2)
            
            points.append({
                'id': f"grid_{i}_{j}",
                'lat': center_lat,
                'lon': center_lon
            })
            
    return points

def fetch_flow_segment(lat, lon):
    """
    Fetches traffic flow data for a specific point (snaps to nearest road).
    """
    base_url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    
    # Correct parameter is 'point' not 'bbox' for this endpoint
    url = f"{base_url}?point={lat},{lon}&unit=KMPH&key={TOMTOM_API_KEY}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # print(f"Error fetching flow for {lat},{lon}: {e}")
        return None

def process_city_flow(city_name):
    bbox = CITY_BBOXES.get(city_name)
    if not bbox:
        return None
        
    # Generate 3x3 = 9 sample points covering the city
    points = get_grid_points(bbox, grid_size=3)
    
    city_samples = []
    total_current_speed = 0
    total_free_flow_speed = 0
    valid_samples = 0
    
    print(f"Sampling {len(points)} points for {city_name}...")
    
    for pt in points:
        data = fetch_flow_segment(pt['lat'], pt['lon'])
        
        if data and 'flowSegmentData' in data:
            flow = data['flowSegmentData']
            current_speed = flow.get('currentSpeed', 0)
            free_flow_speed = flow.get('freeFlowSpeed', 0)
            confidence = flow.get('confidence', 0)
            
            # Calculate congestion ratio (0.0 to 1.0, lower is worse)
            # Ratio = Current / FreeFlow. 
            # 1.0 = No Traffic. 0.5 = Half speed.
            ratio = current_speed / free_flow_speed if free_flow_speed > 0 else 1.0
            
            sample_record = {
                "grid_id": pt['id'],
                "location": {"lat": pt['lat'], "lon": pt['lon']},
                "current_speed_kmph": current_speed,
                "free_flow_speed_kmph": free_flow_speed,
                "congestion_ratio": round(ratio, 2),
                "confidence": confidence
            }
            
            city_samples.append(sample_record)
            
            if confidence > 0.5: # Only count confident readings for average
                total_current_speed += current_speed
                total_free_flow_speed += free_flow_speed
                valid_samples += 1
        
        # Respect rate limits slightly
        time.sleep(0.2)

    # Aggregate City Stats
    avg_ratio = 1.0
    if total_free_flow_speed > 0:
        avg_ratio = total_current_speed / total_free_flow_speed

    return {
        "city": city_name,
        "timestamp": datetime.utcnow().isoformat(),
        "data_source": "tomtom_flow_segment_grid",
        "grid_size": "3x3",
        "samples_collected": len(city_samples),
        "average_congestion_ratio": round(avg_ratio, 2),
        "city_status": "congested" if avg_ratio < 0.7 else "heavy_traffic" if avg_ratio < 0.85 else "flowing",
        "samples": city_samples
    }

def main():
    print("Starting Gridded Flow Data Ingestion...")
    
    if not TOMTOM_API_KEY:
        print("ERROR: TOMTOM_API_KEY not set.")
        return

    all_data = []
    
    for city in CITIES:
        result = process_city_flow(city)
        if result:
            all_data.append(result)
            
    print("\n--- Flow Data Output ---")
    jsonl_string = dict_to_jsonl(all_data)
    print(jsonl_string)
    
    # Save to file
    with open('flow_output.jsonl', 'w') as f:
        f.write(jsonl_string)
    print("Saved to flow_output.json")

if __name__ == "__main__":
    main()

