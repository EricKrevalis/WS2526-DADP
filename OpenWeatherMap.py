import requests
import json
import time
import csv
import os
from datetime import datetime

API_KEY = "76d3c2c705805e009533b5cee35fafef"

cities = {
    "test": (1, 2),
    "hamburg": (53.550341, 10.000654)
}

def get_city_coords(city_name: str) -> tuple[float, float]:
    if city_name.lower() in cities.keys():
        return cities[city_name.lower()]
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},DE&limit=1&appid={API_KEY}"
    try:
        print("Getting city coordinates from API")
        res = requests.get(url, timeout=15)
        if res.status_code != 200:
            print(f"Open Weather Map API Error when fetching city: {res.status_code} - {res.text[:50]}")
            return (0,0)
            
        data = res.json()[0]
        return (data["lat"], data["lon"])
    except Exception as e:
        print(f"Error fetching city data: {e}")
        return (0,0)

# TODO Daten von einem bestimmten datum + uhrzeit?
def get_weather(lat: float=None, lon: float=None, city_name: str=None):
    if (lat is None or lon is None) and city_name is not None:
        lat, lon = get_city_coords(city_name)
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
    try:
        res = requests.get(url, timeout=15)
        if res.status_code != 200:
            print(f"Open Weather Map API Error when fetching weather: {res.status_code} - {res.text[:50]}")
            return []
            
        return res
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return []

if __name__ == "__main__":
    print(get_city_coords("Hamburg"))
    print(get_weather(city_name="Hamburg").json())