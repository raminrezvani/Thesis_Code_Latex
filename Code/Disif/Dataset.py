#https://grok.com/chat/d3f44813-545a-4364-86c9-45fa50dc174b


import random
import datetime
import csv
import json
import os
from pathlib import Path

# Define constants for the simulation
LOCATIONS = [
    {"id": "INT1", "name": "Intersection_1", "lat": 35.6895, "lon": 51.3890},
    {"id": "INT2", "name": "Intersection_2", "lat": 35.7000, "lon": 51.4000},
    {"id": "HWY1", "name": "Highway_1", "lat": 35.7100, "lon": 51.4100},
    {"id": "HWY2", "name": "Highway_2", "lat": 35.7200, "lon": 51.4200}
]
SENSOR_IDS = [f"SENSOR_{i}" for i in range(1, 21)]  # 20 sensors
EVENT_TYPES = ["Normal", "Congestion", "Incident"]
NUM_VEHICLES = 1000  # Number of unique vehicles
SIMULATION_HOURS = 24
RECORDS_PER_HOUR = 10000  # High-volume data for realistic simulation

# Function to generate a random timestamp within the last 24 hours
def random_timestamp():
    now = datetime.datetime.now()
    start_time = now - datetime.timedelta(hours=SIMULATION_HOURS)
    time_diff = random.randint(0, SIMULATION_HOURS * 3600)
    return (start_time + datetime.timedelta(seconds=time_diff)).strftime("%Y-%m-%d %H:%M:%S")

# Function to simulate traffic patterns (e.g., rush hour, night)
def get_speed_and_event(hour):
    # Rush hours: 7-9 AM, 4-6 PM -> higher congestion probability, lower speeds
    if 7 <= hour <= 9 or 16 <= hour <= 18:
        speed = random.uniform(10, 50)  # Slower speeds during rush hours
        event_probs = [0.3, 0.6, 0.1]  # Higher chance of congestion
    else:
        speed = random.uniform(30, 100)  # Normal or higher speeds
        event_probs = [0.7, 0.2, 0.1]  # Normal traffic more likely
    return speed, random.choices(EVENT_TYPES, event_probs)[0]

# Function to generate synthetic traffic data
def generate_traffic_data():
    data = []
    for _ in range(SIMULATION_HOURS * RECORDS_PER_HOUR):
        vehicle_id = f"VEH_{random.randint(1, NUM_VEHICLES)}"
        timestamp = random_timestamp()
        hour = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").hour
        location = random.choice(LOCATIONS)
        speed, event_type = get_speed_and_event(hour)
        sensor_id = random.choice(SENSOR_IDS)
        
        record = {
            "vehicle_id": vehicle_id,
            "timestamp": timestamp,
            "location_id": location["id"],
            "latitude": location["lat"],
            "longitude": location["lon"],
            "speed": round(speed, 2),
            "event_type": event_type,
            "sensor_id": sensor_id
        }
        data.append(record)
    return data

# Function to save data to CSV
def save_to_csv(data, filename="traffic_data.csv"):
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    filepath = output_dir / filename
    with open(filepath, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Data saved to {filepath}")

# Function to save data to JSON
def save_to_json(data, filename="traffic_data.json"):
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    filepath = output_dir / filename
    with open(filepath, mode="w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filepath}")

# Main function to generate and save data
def main():
    print("Generating synthetic traffic data...")
    traffic_data = generate_traffic_data()
    
    # Save to CSV
    save_to_csv(traffic_data)
    
    # Save to JSON
    save_to_json(traffic_data)
    
    print(f"Generated {len(traffic_data)} records for {SIMULATION_HOURS} hours.")

if __name__ == "__main__":
    main()