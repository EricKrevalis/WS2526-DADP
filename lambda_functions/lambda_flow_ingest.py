"""
Lambda Function: TomTom Traffic Flow Data Ingestion

This Lambda function collects real-time traffic flow data from the TomTom Traffic API
for major German cities. It uses a grid-based sampling approach to get representative
traffic conditions across each city.

Data Source: TomTom Traffic Flow Segment Data API
API Documentation: https://developer.tomtom.com/traffic-api/documentation/product/traffic-flow/traffic-flow-segment-data

What it does:
1. Divides each city into a 3x3 grid (9 sample points)
2. Queries TomTom API for traffic flow at each grid point
3. Calculates congestion metrics (current speed vs free-flow speed)
4. Aggregates data per city with average congestion ratios
5. Writes data to S3 in JSONL format

Grid Sampling Approach:
    - Each city is divided into a 3x3 grid (9 cells)
    - Sample point is at the center of each grid cell
    - API snaps to nearest road segment automatically
    - Provides representative traffic conditions across the city

S3 Output Structure:
    s3://bucket/ingest-data/flow_data/YYYY/MM/DD/HHMMSS_flow_data.jsonl

Environment Variables Required:
    - S3_BUCKET: Name of the S3 bucket to write data to
    - S3_OUTPUT_PATH: Prefix path in S3 (default: 'ingest-data')
    - TOMTOM_API_KEY: TomTom API key for authentication
"""

import json
import time
from datetime import datetime
import requests
import boto3
import os

# Initialize S3 client for writing data
s3_client = boto3.client('s3')

# List of cities to collect traffic data for
# These are the 5 major German cities we're monitoring
CITIES = [
    "Hamburg",
    "Berlin",
    "Frankfurt",
    "Muenchen",  # Munich
    "Koeln"      # Cologne
]

# Bounding boxes for each city (geographic boundaries)
# Format: "minLongitude,minLatitude,maxLongitude,maxLatitude"
# These define the rectangular area to sample within each city
# Values are in decimal degrees (WGS84 coordinate system)
CITY_BBOXES = {
    "Hamburg": "9.7,53.4,10.3,53.7",      # ~15-20km radius around city center
    "Berlin": "13.1,52.3,13.7,52.7",
    "Frankfurt": "8.4,49.9,8.9,50.3",
    "Muenchen": "11.3,48.0,11.8,48.3",
    "Koeln": "6.7,50.8,7.2,51.1"
}

def dict_to_jsonl(data):
    """
    Convert a list of dictionaries (or a single dictionary) to a JSONL string.
    
    JSONL (JSON Lines) format: Each line is a valid JSON object, separated by newlines.
    This format is efficient for streaming and processing large datasets.
    
    Args:
        data: A list of dictionaries or a single dictionary to convert
        
    Returns:
        str: A string where each line is a JSON object (newline-separated)
    """
    if isinstance(data, dict):
        data = [data]
    return '\n'.join(json.dumps(item) for item in data)

def get_grid_points(bbox_str, grid_size=3):
    """
    Divide a bounding box into a grid and return the center points of each cell.
    
    This function creates a grid of sample points across a city's bounding box.
    Each grid cell's center point is used as a sampling location for traffic data.
    The TomTom API will automatically snap these points to the nearest road segment.
    
    Args:
        bbox_str (str): Bounding box string in format "minLon,minLat,maxLon,maxLat"
        grid_size (int): Number of grid cells per side (default: 3, creates 3x3=9 cells)
        
    Returns:
        list: List of dictionaries with keys:
            - 'id': Grid cell identifier (e.g., "grid_0_0")
            - 'lat': Latitude of grid cell center
            - 'lon': Longitude of grid cell center
            
    Example:
        Input: "9.7,53.4,10.3,53.7", grid_size=3
        Output: 9 points evenly distributed across the bounding box
        
    Grid Layout (3x3 example):
        [0,0] [1,0] [2,0]
        [0,1] [1,1] [2,1]
        [0,2] [1,2] [2,2]
    """
    try:
        # Parse bounding box string into numeric coordinates
        # Format: "minLongitude,minLatitude,maxLongitude,maxLatitude"
        min_lon, min_lat, max_lon, max_lat = map(float, bbox_str.split(','))
    except ValueError:
        print(f"Invalid BBOX format: {bbox_str}")
        return []

    # Calculate dimensions of the bounding box
    width = max_lon - min_lon   # Longitude span (east-west)
    height = max_lat - min_lat  # Latitude span (north-south)
    
    # Calculate step size for grid cells
    # Each cell has equal width and height
    step_x = width / grid_size   # Longitude step per cell
    step_y = height / grid_size  # Latitude step per cell
    
    points = []
    
    # Generate grid points
    # i = column (longitude/x-axis), j = row (latitude/y-axis)
    for i in range(grid_size):
        for j in range(grid_size):
            # Calculate center point of this grid cell
            # Start at min + (cell_index * step) + (half step to center)
            center_lon = min_lon + (i * step_x) + (step_x / 2)
            center_lat = min_lat + (j * step_y) + (step_y / 2)
            
            # Store point with identifier and coordinates
            points.append({
                'id': f"grid_{i}_{j}",  # Unique identifier for this grid cell
                'lat': center_lat,       # Latitude coordinate
                'lon': center_lon        # Longitude coordinate
            })
            
    return points

def fetch_flow_segment(lat, lon, api_key):
    """
    Fetch traffic flow data for a specific geographic point.
    
    This function queries the TomTom Traffic Flow Segment Data API for real-time
    traffic conditions at a given location. The API automatically snaps the point
    to the nearest road segment and returns current speed, free-flow speed, and
    confidence metrics.
    
    Args:
        lat (float): Latitude coordinate of the point to query
        lon (float): Longitude coordinate of the point to query
        api_key (str): TomTom API key for authentication
        
    Returns:
        dict: JSON response from API containing flowSegmentData, or None on error
        
    API Endpoint:
        GET /traffic/services/4/flowSegmentData/absolute/10/json
        
    API Parameters:
        - point: Latitude,longitude of query point
        - unit: Speed unit (KMPH = kilometers per hour)
        - key: API authentication key
        
    Response Structure:
        {
            "flowSegmentData": {
                "frc": "Road class",
                "currentSpeed": 45,        # Current speed in km/h
                "freeFlowSpeed": 60,       # Free-flow speed in km/h
                "currentTravelTime": 120,   # Current travel time in seconds
                "freeFlowTravelTime": 90,   # Free-flow travel time in seconds
                "confidence": 0.95,        # Data confidence (0.0-1.0)
                "coordinates": {...}       # Road segment coordinates
            }
        }
    """
    # TomTom Traffic Flow Segment Data API endpoint
    # Version 4, absolute data (not relative), zoom level 10, JSON format
    base_url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    
    # Construct API URL with query parameters
    # Point format: latitude,longitude
    url = f"{base_url}?point={lat},{lon}&unit=KMPH&key={api_key}"

    try:
        # Make HTTP GET request with 10-second timeout
        # Timeout prevents Lambda from hanging if API is slow/unresponsive
        response = requests.get(url, timeout=10)
        
        # Raise exception for HTTP error status codes (4xx, 5xx)
        response.raise_for_status()
        
        # Parse and return JSON response
        return response.json()
        
    except requests.exceptions.RequestException as e:
        # Log error but don't fail - allows other grid points to be processed
        print(f"Error fetching flow for {lat},{lon}: {e}")
        return None

def process_city_flow(city_name, api_key):
    """
    Process traffic flow data for a single city using grid-based sampling.
    
    This function:
    1. Creates a grid of sample points across the city
    2. Queries TomTom API for traffic data at each point
    3. Calculates congestion metrics (current speed vs free-flow speed)
    4. Aggregates results into city-level statistics
    
    Args:
        city_name (str): Name of the city to process
        api_key (str): TomTom API key for authentication
        
    Returns:
        dict: City traffic data with structure:
            {
                "city": str,
                "timestamp": str (ISO format),
                "data_source": str,
                "grid_size": str,
                "samples_collected": int,
                "average_congestion_ratio": float (0.0-1.0),
                "city_status": str ("congested" | "heavy_traffic" | "flowing"),
                "samples": list of sample records
            }
            
    Congestion Ratio Calculation:
        ratio = current_speed / free_flow_speed
        - 1.0 = No traffic (flowing freely)
        - 0.85-0.99 = Light traffic
        - 0.70-0.84 = Heavy traffic
        - < 0.70 = Congested
        
    City Status Classification:
        - "congested": Average ratio < 0.7 (traffic moving at <70% of free-flow speed)
        - "heavy_traffic": Average ratio 0.7-0.85
        - "flowing": Average ratio > 0.85 (traffic flowing well)
    """
    # Get bounding box for this city
    bbox = CITY_BBOXES.get(city_name)
    if not bbox:
        return None
        
    # Generate grid of sample points (3x3 = 9 points)
    points = get_grid_points(bbox, grid_size=3)
    
    # Initialize accumulators for aggregation
    city_samples = []              # Store individual sample records
    total_current_speed = 0        # Sum of current speeds (for averaging)
    total_free_flow_speed = 0      # Sum of free-flow speeds (for averaging)
    valid_samples = 0              # Count of samples with high confidence
    
    print(f"Sampling {len(points)} points for {city_name}...")
    
    # Query traffic data for each grid point
    for pt in points:
        # Fetch traffic flow data for this grid point
        data = fetch_flow_segment(pt['lat'], pt['lon'], api_key)
        
        # Process response if valid
        if data and 'flowSegmentData' in data:
            flow = data['flowSegmentData']
            
            # Extract speed metrics from API response
            current_speed = flow.get('currentSpeed', 0)        # Current traffic speed (km/h)
            free_flow_speed = flow.get('freeFlowSpeed', 0)    # Free-flow speed (km/h)
            confidence = flow.get('confidence', 0)              # Data quality (0.0-1.0)
            
            # Calculate congestion ratio
            # Ratio = current / free-flow
            # Lower ratio = more congestion
            ratio = current_speed / free_flow_speed if free_flow_speed > 0 else 1.0
            
            # Create sample record with all relevant data
            sample_record = {
                "grid_id": pt['id'],                           # Grid cell identifier
                "location": {"lat": pt['lat'], "lon": pt['lon']},  # Geographic coordinates
                "current_speed_kmph": current_speed,          # Current speed
                "free_flow_speed_kmph": free_flow_speed,        # Free-flow speed
                "congestion_ratio": round(ratio, 2),           # Congestion metric
                "confidence": confidence                       # Data quality
            }
            
            city_samples.append(sample_record)
            
            # Only include high-confidence samples in average calculation
            # Low confidence samples may be inaccurate or outdated
            if confidence > 0.5:
                total_current_speed += current_speed
                total_free_flow_speed += free_flow_speed
                valid_samples += 1
        
        # Rate limiting: Small delay between API calls
        # Reduced from 0.2s to 0.1s for Lambda (faster execution, still respects limits)
        time.sleep(0.1)

    # Calculate average congestion ratio for the city
    # Only uses high-confidence samples for accuracy
    avg_ratio = 1.0  # Default to no congestion if no valid samples
    if total_free_flow_speed > 0:
        avg_ratio = total_current_speed / total_free_flow_speed

    # Return aggregated city data
    return {
        "city": city_name,                                    # City name
        "timestamp": datetime.utcnow().isoformat(),           # Collection timestamp (UTC)
        "data_source": "tomtom_flow_segment_grid",            # Data source identifier
        "grid_size": "3x3",                                   # Grid configuration
        "samples_collected": len(city_samples),               # Total samples (including low confidence)
        "average_congestion_ratio": round(avg_ratio, 2),     # City-wide congestion metric
        "city_status": (                                      # Human-readable status
            "congested" if avg_ratio < 0.7 
            else "heavy_traffic" if avg_ratio < 0.85 
            else "flowing"
        ),
        "samples": city_samples                               # Individual sample records
    }

def lambda_handler(event, context):
    """
    AWS Lambda handler function - entry point for the Lambda execution.
    
    This function orchestrates the traffic flow data collection process for all
    configured cities. It processes each city sequentially, aggregates the results,
    and writes them to S3.
    
    Args:
        event (dict): Event data passed to Lambda (usually empty dict for scheduled invocations)
        context (LambdaContext): Runtime information about the Lambda execution
        
    Returns:
        dict: HTTP-like response with statusCode and body
            - statusCode 200: Success (with or without data)
            - statusCode 500: Error occurred
            
    Environment Variables:
        S3_BUCKET (required): Name of S3 bucket to write data to
        S3_OUTPUT_PATH (optional): Prefix path in S3 bucket (default: 'ingest-data')
        TOMTOM_API_KEY (required): TomTom API key for authentication
        
    S3 Output Format:
        s3://{S3_BUCKET}/{S3_OUTPUT_PATH}/flow_data/YYYY/MM/DD/HHMMSS_flow_data.jsonl
        
    Processing Flow:
        1. Read configuration from environment variables
        2. Validate required variables are set
        3. Process each city in CITIES list
        4. Aggregate all city data
        5. Convert to JSONL format
        6. Upload to S3
        7. Return success response
        
    Example Response:
        {
            'statusCode': 200,
            'body': '{"message": "Flow data collection successful", "cities_processed": 5, ...}'
        }
    """
    try:
        # Read configuration from environment variables
        s3_bucket = os.environ.get('S3_BUCKET')
        s3_output_path = os.environ.get('S3_OUTPUT_PATH', 'ingest-data')
        tomtom_api_key = os.environ.get('TOMTOM_API_KEY')
        
        # Validate required environment variables
        if not s3_bucket:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'S3_BUCKET environment variable not set'})
            }
        
        if not tomtom_api_key:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'TOMTOM_API_KEY environment variable not set'})
            }
        
        print("Starting Gridded Flow Data Ingestion...")
        
        # Process each city and collect results
        all_data = []
        
        # Iterate through all configured cities
        # Sequential processing ensures we don't overwhelm the API
        for city in CITIES:
            # Process this city's traffic flow data
            # This will query 9 grid points per city
            result = process_city_flow(city, tomtom_api_key)
            
            # Add to results if data was successfully collected
            if result:
                all_data.append(result)
                
        # Write data to S3 if we collected any
        if all_data:
            # Convert list of city data dictionaries to JSONL format
            jsonl_string = dict_to_jsonl(all_data)
            
            # Generate S3 object key with timestamp-based path
            # Format: YYYY/MM/DD/HHMMSS_flow_data.jsonl
            timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
            s3_key = f"{s3_output_path}/flow_data/{timestamp}_flow_data.jsonl"
            
            # Upload data to S3
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=jsonl_string.encode('utf-8'),  # Encode string to bytes
                ContentType='application/json'
            )
            
            # Calculate total number of samples collected across all cities
            # Useful for monitoring and understanding data volume
            total_samples = sum(item.get('samples_collected', 0) for item in all_data)
            
            # Return success response with metadata
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Flow data collection successful',
                    'cities_processed': len(all_data),
                    'total_samples': total_samples,  # Total grid samples across all cities
                    's3_location': f's3://{s3_bucket}/{s3_key}'
                })
            }
        else:
            # No data collected, but not necessarily an error
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No data collected',
                    'cities_processed': 0
                })
            }
            
    except Exception as e:
        # Catch any unexpected errors and return error response
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

