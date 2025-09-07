#!/usr/bin/env python3
"""
Smart Traffic Digital Twin System
Integrates multiple data sources: vehicle sensors, weather, traffic infrastructure
Creates comprehensive RDF knowledge graph for intelligent traffic management
"""

import random
import datetime
import csv
import json
import os
from pathlib import Path
import math

# Enhanced constants for comprehensive traffic simulation
LOCATIONS = [
    {"id": "INT1", "name": "Tehran_Intersection_1", "lat": 35.6895, "lon": 51.3890, "type": "intersection", "traffic_lights": True},
    {"id": "INT2", "name": "Tehran_Intersection_2", "lat": 35.7000, "lon": 51.4000, "type": "intersection", "traffic_lights": True},
    {"id": "HWY1", "name": "Tehran_Highway_1", "lat": 35.7100, "lon": 51.4100, "type": "highway", "lanes": 4},
    {"id": "HWY2", "name": "Tehran_Highway_2", "lat": 35.7200, "lon": 51.4200, "type": "highway", "lanes": 3},
    {"id": "TUN1", "name": "Tehran_Tunnel_1", "lat": 35.7150, "lon": 51.4150, "type": "tunnel", "length": 500},
    {"id": "BRG1", "name": "Tehran_Bridge_1", "lat": 35.7250, "lon": 51.4250, "type": "bridge", "height": 20}
]

SENSOR_TYPES = {
    "traffic": [f"TRAFFIC_SENSOR_{i}" for i in range(1, 31)],
    "weather": [f"WEATHER_SENSOR_{i}" for i in range(1, 16)],
    "air_quality": [f"AIR_SENSOR_{i}" for i in range(1, 11)],
    "infrastructure": [f"INFRA_SENSOR_{i}" for i in range(1, 21)]
}

VEHICLE_TYPES = ["car", "truck", "bus", "motorcycle", "emergency"]
VEHICLE_FEATURES = {
    "car": {"avg_speed": 60, "size": "medium", "emission": "low"},
    "truck": {"avg_speed": 45, "size": "large", "emission": "high"},
    "bus": {"avg_speed": 35, "size": "large", "emission": "medium"},
    "motorcycle": {"avg_speed": 70, "size": "small", "emission": "low"},
    "emergency": {"avg_speed": 80, "size": "medium", "emission": "medium"}
}

WEATHER_CONDITIONS = ["sunny", "rainy", "cloudy", "foggy", "snowy"]
AIR_QUALITY_LEVELS = ["excellent", "good", "moderate", "poor", "very_poor"]

class SmartTrafficDigitalTwin:
    def __init__(self):
        self.traffic_data = []
        self.weather_data = []
        self.air_quality_data = []
        self.infrastructure_data = []
        self.vehicle_sensor_data = []
        
    def generate_weather_data(self, hours=24):
        """Generate realistic weather data affecting traffic"""
        weather_records = []
        base_temp = 20  # Base temperature in Celsius
        
        for hour in range(hours):
            timestamp = datetime.datetime.now() - datetime.timedelta(hours=hours-hour)
            
            # Simulate daily temperature cycle
            temp_variation = 10 * math.sin(2 * math.pi * hour / 24)
            temperature = base_temp + temp_variation + random.uniform(-2, 2)
            
            # Weather affects traffic patterns
            if temperature < 5:  # Cold weather
                condition = random.choices(WEATHER_CONDITIONS, weights=[0.1, 0.3, 0.2, 0.3, 0.1])[0]
                visibility = max(0.1, random.uniform(0.3, 1.0))
            elif temperature > 30:  # Hot weather
                condition = random.choices(WEATHER_CONDITIONS, weights=[0.6, 0.1, 0.2, 0.05, 0.05])[0]
                visibility = random.uniform(0.7, 1.0)
            else:  # Moderate weather
                condition = random.choices(WEATHER_CONDITIONS, weights=[0.4, 0.2, 0.3, 0.05, 0.05])[0]
                visibility = random.uniform(0.8, 1.0)
            
            humidity = random.uniform(30, 90)
            wind_speed = random.uniform(0, 25)
            precipitation = random.uniform(0, 10) if condition in ["rainy", "snowy"] else 0
            
            weather_records.append({
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "location_id": random.choice(LOCATIONS)["id"],
                "temperature": round(temperature, 1),
                "humidity": round(humidity, 1),
                "wind_speed": round(wind_speed, 1),
                "precipitation": round(precipitation, 1),
                "condition": condition,
                "visibility": round(visibility, 2),
                "sensor_id": random.choice(SENSOR_TYPES["weather"])
            })
        
        return weather_records
    
    def generate_air_quality_data(self, hours=24):
        """Generate air quality data affecting traffic decisions"""
        air_records = []
        
        for hour in range(hours):
            timestamp = datetime.datetime.now() - datetime.timedelta(hours=hours-hour)
            
            # Air quality varies with traffic and weather
            hour_of_day = timestamp.hour
            if 7 <= hour_of_day <= 9 or 16 <= hour_of_day <= 18:  # Rush hours
                base_pm25 = random.uniform(30, 80)  # Higher pollution during rush hours
                base_co = random.uniform(2, 8)
            else:
                base_pm25 = random.uniform(15, 40)
                base_co = random.uniform(1, 4)
            
            # Add some randomness
            pm25 = max(0, base_pm25 + random.uniform(-10, 10))
            co = max(0, base_co + random.uniform(-1, 1))
            no2 = random.uniform(20, 60)
            o3 = random.uniform(20, 80)
            
            # Determine air quality level
            if pm25 < 12 and co < 2:
                quality = "excellent"
            elif pm25 < 35 and co < 4:
                quality = "good"
            elif pm25 < 55 and co < 6:
                quality = "moderate"
            elif pm25 < 150 and co < 9:
                quality = "poor"
            else:
                quality = "very_poor"
            
            air_records.append({
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "location_id": random.choice(LOCATIONS)["id"],
                "pm25": round(pm25, 1),
                "co": round(co, 1),
                "no2": round(no2, 1),
                "o3": round(o3, 1),
                "quality_level": quality,
                "sensor_id": random.choice(SENSOR_TYPES["air_quality"])
            })
        
        return air_records
    
    def generate_infrastructure_data(self, hours=24):
        """Generate infrastructure health and status data"""
        infra_records = []
        
        for hour in range(hours):
            timestamp = datetime.datetime.now() - datetime.timedelta(hours=hours-hour)
            
            for location in LOCATIONS:
                # Infrastructure health varies over time
                if location["type"] == "bridge":
                    structural_health = max(0.5, random.uniform(0.7, 1.0))
                    vibration = random.uniform(0, 5)
                    load_capacity = random.uniform(80, 100)
                elif location["type"] == "tunnel":
                    structural_health = max(0.6, random.uniform(0.8, 1.0))
                    air_quality_tunnel = random.uniform(70, 95)
                    lighting_status = random.choice(["optimal", "good", "needs_maintenance"])
                else:
                    structural_health = max(0.8, random.uniform(0.9, 1.0))
                    vibration = random.uniform(0, 2)
                    load_capacity = random.uniform(90, 100)
                
                # Traffic light status for intersections
                traffic_light_status = "operational"
                if location.get("traffic_lights"):
                    if random.random() < 0.05:  # 5% chance of malfunction
                        traffic_light_status = random.choice(["malfunction", "maintenance_needed"])
                
                infra_records.append({
                    "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "location_id": location["id"],
                    "location_type": location["type"],
                    "structural_health": round(structural_health, 2),
                    "vibration": round(vibration, 2) if "vibration" in locals() else None,
                    "load_capacity": round(load_capacity, 1) if "load_capacity" in locals() else None,
                    "air_quality_tunnel": round(air_quality_tunnel, 1) if "air_quality_tunnel" in locals() else None,
                    "lighting_status": lighting_status if "lighting_status" in locals() else None,
                    "traffic_light_status": traffic_light_status,
                    "sensor_id": random.choice(SENSOR_TYPES["infrastructure"])
                })
        
        return infra_records
    
    def generate_enhanced_traffic_data(self, hours=24):
        """Generate enhanced traffic data with multiple factors"""
        traffic_records = []
        num_records = hours * 5000  # 5000 records per hour
        
        for _ in range(num_records):
            vehicle_type = random.choice(VEHICLE_TYPES)
            vehicle_features = VEHICLE_FEATURES[vehicle_type]
            
            timestamp = datetime.datetime.now() - datetime.timedelta(
                hours=random.uniform(0, hours)
            )
            hour = timestamp.hour
            
            # Base speed based on vehicle type and time
            base_speed = vehicle_features["avg_speed"]
            
            # Adjust speed based on time of day (rush hours)
            if 7 <= hour <= 9 or 16 <= hour <= 18:
                speed_multiplier = random.uniform(0.3, 0.7)  # Slower during rush hours
            else:
                speed_multiplier = random.uniform(0.8, 1.2)
            
            speed = max(5, min(120, base_speed * speed_multiplier))
            
            location = random.choice(LOCATIONS)
            
            # Event type based on speed and conditions
            if speed < 20:
                event_type = random.choices(["Normal", "Congestion", "Incident"], weights=[0.2, 0.7, 0.1])[0]
            elif speed > 80:
                event_type = random.choices(["Normal", "Congestion", "Incident"], weights=[0.8, 0.1, 0.1])[0]
            else:
                event_type = random.choices(["Normal", "Congestion", "Incident"], weights=[0.6, 0.3, 0.1])[0]
            
            # Additional vehicle data
            fuel_level = random.uniform(10, 100)
            engine_temp = random.uniform(80, 110)
            tire_pressure = random.uniform(28, 35)
            
            # Driver behavior indicators
            acceleration = random.uniform(-5, 8)
            braking_frequency = random.uniform(0, 10)
            
            traffic_records.append({
                "vehicle_id": f"VEH_{random.randint(1, 2000)}",
                "vehicle_type": vehicle_type,
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "location_id": location["id"],
                "latitude": location["lat"],
                "longitude": location["lon"],
                "speed": round(speed, 2),
                "event_type": event_type,
                "sensor_id": random.choice(SENSOR_TYPES["traffic"]),
                "fuel_level": round(fuel_level, 1),
                "engine_temp": round(engine_temp, 1),
                "tire_pressure": round(tire_pressure, 1),
                "acceleration": round(acceleration, 2),
                "braking_frequency": round(braking_frequency, 1),
                "emission_level": vehicle_features["emission"],
                "vehicle_size": vehicle_features["size"]
            })
        
        return traffic_records
    
    def generate_all_data(self, hours=24):
        """Generate all types of data for the digital twin"""
        print("Generating comprehensive traffic digital twin data...")
        
        self.traffic_data = self.generate_enhanced_traffic_data(hours)
        self.weather_data = self.generate_weather_data(hours)
        self.air_quality_data = self.generate_air_quality_data(hours)
        self.infrastructure_data = self.generate_infrastructure_data(hours)
        
        print(f"Generated {len(self.traffic_data)} traffic records")
        print(f"Generated {len(self.weather_data)} weather records")
        print(f"Generated {len(self.air_quality_data)} air quality records")
        print(f"Generated {len(self.infrastructure_data)} infrastructure records")
        
        return {
            "traffic": self.traffic_data,
            "weather": self.weather_data,
            "air_quality": self.air_quality_data,
            "infrastructure": self.infrastructure_data
        }
    
    def save_all_data(self):
        """Save all data types to CSV and JSON files"""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        # Save traffic data
        self._save_to_csv(self.traffic_data, "smart_traffic_data.csv")
        self._save_to_json(self.traffic_data, "smart_traffic_data.json")
        
        # Save weather data
        self._save_to_csv(self.weather_data, "weather_data.csv")
        self._save_to_json(self.weather_data, "weather_data.json")
        
        # Save air quality data
        self._save_to_csv(self.air_quality_data, "air_quality_data.csv")
        self._save_to_json(self.air_quality_data, "air_quality_data.json")
        
        # Save infrastructure data
        self._save_to_csv(self.infrastructure_data, "infrastructure_data.csv")
        self._save_to_json(self.infrastructure_data, "infrastructure_data.json")
        
        # Save combined data
        combined_data = {
            "metadata": {
                "generated_at": datetime.datetime.now().isoformat(),
                "total_records": {
                    "traffic": len(self.traffic_data),
                    "weather": len(self.weather_data),
                    "air_quality": len(self.air_quality_data),
                    "infrastructure": len(self.infrastructure_data)
                }
            },
            "traffic": self.traffic_data[:100],  # Sample for preview
            "weather": self.weather_data[:50],
            "air_quality": self.air_quality_data[:50],
            "infrastructure": self.infrastructure_data[:50]
        }
        
        self._save_to_json(combined_data, "digital_twin_combined_data.json")
        
        print("All data saved successfully!")
    
    def _save_to_csv(self, data, filename):
        """Helper method to save data to CSV"""
        if not data:
            return
        
        filepath = Path("output") / filename
        with open(filepath, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"Data saved to {filepath}")
    
    def _save_to_json(self, data, filename):
        """Helper method to save data to JSON"""
        filepath = Path("output") / filename
        with open(filepath, mode="w", encoding="utf-8") as file:
            json.dump(data, file, indent=2, default=str)
        print(f"Data saved to {filepath}")

def main():
    """Main function to generate smart traffic digital twin data"""
    print("🚗 Smart Traffic Digital Twin Data Generator")
    print("=" * 50)
    
    # Create digital twin instance
    digital_twin = SmartTrafficDigitalTwin()
    
    # Generate all data types
    all_data = digital_twin.generate_all_data(hours=24)
    
    # Save all data
    digital_twin.save_all_data()
    
    print("\n✅ Digital Twin Data Generation Complete!")
    print(f"📊 Total Records Generated: {sum(len(data) for data in all_data.values())}")
    print("🎯 Ready for RDF conversion and MAS processing!")

if __name__ == "__main__":
    main()

