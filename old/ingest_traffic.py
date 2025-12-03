import requests
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv
from constants import CITIES, CITY_BBOXES

# Load environment variables
load_dotenv('env')

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

def fetch_traffic_incidents(city_name, bbox):
    """
    Fetches traffic incidents for a given bounding box.
    """
    base_url = "https://api.tomtom.com/traffic/services/5/incidentDetails"
    
    # Use parameters dict to let requests handle encoding correctly
    # Using the syntax provided by the user
    params = {
        "bbox": bbox,
        "fields": "{incidents{type,geometry{type,coordinates},properties{iconCategory,magnitudeOfDelay,length,delay}}}",
        "language": "en-GB",
        "timeValidityFilter": "present",
        "key": TOMTOM_API_KEY
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # Suppress full error trace, just print status code if available
        if hasattr(e, 'response') and e.response is not None:
             print(f"Error {city_name}: {e.response.status_code} - {e.response.text}")
        else:
             print(f"Error {city_name}: {e}")
        return None

def process_city_traffic(city_name):
    """
    Fetches and aggregates traffic data for a city.
    """
    bbox = CITY_BBOXES.get(city_name)
    if not bbox:
        print(f"No bounding box found for {city_name}")
        return None

    data = fetch_traffic_incidents(city_name, bbox)
    
    if not data or 'incidents' not in data:
        return None

    incidents = data['incidents']
    
    # Aggregation Logic
    total_delay_seconds = 0
    total_length_meters = 0
    incident_count = len(incidents)
    major_incidents = 0

    for incident in incidents:
        props = incident.get('properties', {})
        # Updated field names based on user input
        # Handle None values safely by converting to 0 if None
        delay = props.get('delay')
        length = props.get('length')
        
        delay = delay if delay is not None else 0
        length = length if length is not None else 0

        total_delay_seconds += delay
        total_length_meters += length
        
        # Count "Major" incidents (Delay > 5 minutes)
        if delay > 300:
            major_incidents += 1

    # Create the final JSON object
    traffic_summary = {
        "city": city_name,
        "timestamp": datetime.utcnow().isoformat(),
        "data_source": "tomtom_incident_api",
        "metrics": {
            "incident_count": incident_count,
            "major_incidents_count": major_incidents,
            "total_delay_seconds": total_delay_seconds,
            "total_jam_length_meters": total_length_meters,
        },
        "status": "high_congestion" if total_delay_seconds > 1200 else "moderate" if total_delay_seconds > 300 else "low_congestion"
    }
    
    return traffic_summary

def main():
    print("Starting Traffic Data Ingestion...")
    
    if not TOMTOM_API_KEY:
        print("ERROR: Please set a valid TOMTOM_API_KEY in your .env file.")
        return

    all_city_data = []

    for city in CITIES:
        print(f"Fetching data for {city}...")
        summary = process_city_traffic(city)
        if summary:
            all_city_data.append(summary)
    
    # Output for verification
    print("\n--- Collected Data (JSON) ---")
    print(json.dumps(all_city_data, indent=2))

if __name__ == "__main__":
    main()
