# List of 5 German cities for data collection
CITIES = [
    "Hamburg",
    "Berlin",
    "Frankfurt",
    "Muenchen",
    "Koeln"
]

# Mapped coordinates for the 5 cities (Center Points)
CITY_COORDINATES = {
    "Hamburg": {"lat": 53.5511, "lon": 9.9937},
    "Berlin": {"lat": 52.5200, "lon": 13.4050},
    "Frankfurt": {"lat": 50.1109, "lon": 8.6821},
    "Muenchen": {"lat": 48.1351, "lon": 11.5820},
    "Koeln": {"lat": 50.9375, "lon": 6.9603}
}

# Bounding Boxes for Traffic Incidents (approx +/- 0.15 degrees ~ 15-20km radius)
# Format: minLon, minLat, maxLon, maxLat
CITY_BBOXES = {
    "Hamburg": "9.7,53.4,10.3,53.7",
    "Berlin": "13.1,52.3,13.7,52.7",
    "Frankfurt": "8.4,49.9,8.9,50.3",
    "Muenchen": "11.3,48.0,11.8,48.3",
    "Koeln": "6.7,50.8,7.2,51.1"
}
