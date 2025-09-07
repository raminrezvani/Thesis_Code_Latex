#!/usr/bin/env python3
"""
Advanced RDFizer for Smart Traffic Digital Twin System
Converts multiple data sources to comprehensive RDF knowledge graph
Integrates traffic, weather, air quality, and infrastructure data
"""

import csv
from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD, RDFS, OWL, GEO, TIME
from pathlib import Path
import datetime
import json

# Define comprehensive namespaces for digital twin ontology
DT = Namespace("http://example.org/digitaltwin#")
TRAFFIC = Namespace("http://example.org/traffic#")
WEATHER = Namespace("http://example.org/weather#")
AIR = Namespace("http://example.org/airquality#")
INFRA = Namespace("http://example.org/infrastructure#")
VEHICLE = Namespace("http://example.org/vehicle#")
SENSOR = Namespace("http://example.org/sensor#")
LOCATION = Namespace("http://example.org/location#")
EVENT = Namespace("http://example.org/event#")

class DigitalTwinRDFizer:
    def __init__(self):
        self.graph = Graph()
        self.init_ontology()
        self.bind_namespaces()
        
    def init_ontology(self):
        """Initialize comprehensive ontology for digital twin"""
        # Digital Twin Core Classes
        self.graph.add((DT.DigitalTwin, RDF.type, RDFS.Class))
        self.graph.add((DT.TrafficSystem, RDF.type, RDFS.Class))
        self.graph.add((DT.WeatherSystem, RDF.type, RDFS.Class))
        self.graph.add((DT.AirQualitySystem, RDF.type, RDFS.Class))
        self.graph.add((DT.InfrastructureSystem, RDF.type, RDFS.Class))
        
        # Traffic Domain Classes
        self.graph.add((TRAFFIC.Vehicle, RDF.type, RDFS.Class))
        self.graph.add((TRAFFIC.TrafficEvent, RDF.type, RDFS.Class))
        self.graph.add((TRAFFIC.TrafficPattern, RDF.type, RDFS.Class))
        self.graph.add((TRAFFIC.DriverBehavior, RDF.type, RDFS.Class))
        
        # Weather Domain Classes
        self.graph.add((WEATHER.WeatherCondition, RDF.type, RDFS.Class))
        self.graph.add((WEATHER.WeatherEvent, RDF.type, RDFS.Class))
        self.graph.add((WEATHER.AtmosphericData, RDF.type, RDFS.Class))
        
        # Air Quality Domain Classes
        self.graph.add((AIR.AirQualityMeasurement, RDF.type, RDFS.Class))
        self.graph.add((AIR.Pollutant, RDF.type, RDFS.Class))
        self.graph.add((AIR.AirQualityIndex, RDF.type, RDFS.Class))
        
        # Infrastructure Domain Classes
        self.graph.add((INFRA.InfrastructureElement, RDF.type, RDFS.Class))
        self.graph.add((INFRA.StructuralHealth, RDF.type, RDFS.Class))
        self.graph.add((INFRA.MaintenanceStatus, RDF.type, RDFS.Class))
        
        # Sensor and Location Classes
        self.graph.add((SENSOR.Sensor, RDF.type, RDFS.Class))
        self.graph.add((LOCATION.Location, RDF.type, RDFS.Class))
        self.graph.add((EVENT.Event, RDF.type, RDFS.Class))
        
        # Define Object Properties
        self._define_object_properties()
        
        # Define Data Properties
        self._define_data_properties()
        
        # Define Subclass Relationships
        self._define_subclass_relationships()
        
    def _define_object_properties(self):
        """Define object properties for relationships"""
        # Traffic relationships
        self.graph.add((TRAFFIC.hasLocation, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.detectedBy, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasDriverBehavior, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.createsEvent, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.affectsAirQuality, RDF.type, RDF.Property))
        
        # Weather relationships
        self.graph.add((WEATHER.measuredAt, RDF.type, RDF.Property))
        self.graph.add((WEATHER.affectsTraffic, RDF.type, RDF.Property))
        self.graph.add((WEATHER.affectsVisibility, RDF.type, RDF.Property))
        
        # Air quality relationships
        self.graph.add((AIR.measuredAt, RDF.type, RDF.Property))
        self.graph.add((AIR.affectsTraffic, RDF.type, RDF.Property))
        self.graph.add((AIR.measuredBy, RDF.type, RDF.Property))
        
        # Infrastructure relationships
        self.graph.add((INFRA.hasLocation, RDF.type, RDF.Property))
        self.graph.add((INFRA.hasHealthStatus, RDF.type, RDF.Property))
        self.graph.add((INFRA.requiresMaintenance, RDF.type, RDF.Property))
        
        # General relationships
        self.graph.add((DT.hasComponent, RDF.type, RDF.Property))
        self.graph.add((DT.monitors, RDF.type, RDF.Property))
        self.graph.add((DT.generatesData, RDF.type, RDF.Property))
        
    def _define_data_properties(self):
        """Define data properties for values"""
        # Traffic data properties
        self.graph.add((TRAFFIC.hasSpeed, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasTimestamp, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasVehicleType, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasFuelLevel, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasEngineTemp, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasTirePressure, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasAcceleration, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasBrakingFrequency, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasEmissionLevel, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasVehicleSize, RDF.type, RDF.Property))
        
        # Weather data properties
        self.graph.add((WEATHER.hasTemperature, RDF.type, RDF.Property))
        self.graph.add((WEATHER.hasHumidity, RDF.type, RDF.Property))
        self.graph.add((WEATHER.hasWindSpeed, RDF.type, RDF.Property))
        self.graph.add((WEATHER.hasPrecipitation, RDF.type, RDF.Property))
        self.graph.add((WEATHER.hasCondition, RDF.type, RDF.Property))
        self.graph.add((WEATHER.hasVisibility, RDF.type, RDF.Property))
        
        # Air quality data properties
        self.graph.add((AIR.hasPM25, RDF.type, RDF.Property))
        self.graph.add((AIR.hasCO, RDF.type, RDF.Property))
        self.graph.add((AIR.hasNO2, RDF.type, RDF.Property))
        self.graph.add((AIR.hasO3, RDF.type, RDF.Property))
        self.graph.add((AIR.hasQualityLevel, RDF.type, RDF.Property))
        
        # Infrastructure data properties
        self.graph.add((INFRA.hasStructuralHealth, RDF.type, RDF.Property))
        self.graph.add((INFRA.hasVibration, RDF.type, RDF.Property))
        self.graph.add((INFRA.hasLoadCapacity, RDF.type, RDF.Property))
        self.graph.add((INFRA.hasAirQualityTunnel, RDF.type, RDF.Property))
        self.graph.add((INFRA.hasLightingStatus, RDF.type, RDF.Property))
        self.graph.add((INFRA.hasTrafficLightStatus, RDF.type, RDF.Property))
        
        # Location data properties
        self.graph.add((LOCATION.hasLatitude, RDF.type, RDF.Property))
        self.graph.add((LOCATION.hasLongitude, RDF.type, RDF.Property))
        self.graph.add((LOCATION.hasType, RDF.type, RDF.Property))
        self.graph.add((LOCATION.hasName, RDF.type, RDF.Property))
        
    def _define_subclass_relationships(self):
        """Define subclass relationships"""
        # Vehicle types
        self.graph.add((VEHICLE.Car, RDFS.subClassOf, TRAFFIC.Vehicle))
        self.graph.add((VEHICLE.Truck, RDFS.subClassOf, TRAFFIC.Vehicle))
        self.graph.add((VEHICLE.Bus, RDFS.subClassOf, TRAFFIC.Vehicle))
        self.graph.add((VEHICLE.Motorcycle, RDFS.subClassOf, TRAFFIC.Vehicle))
        self.graph.add((VEHICLE.EmergencyVehicle, RDFS.subClassOf, TRAFFIC.Vehicle))
        
        # Location types
        self.graph.add((LOCATION.Intersection, RDFS.subClassOf, LOCATION.Location))
        self.graph.add((LOCATION.Highway, RDFS.subClassOf, LOCATION.Location))
        self.graph.add((LOCATION.Tunnel, RDFS.subClassOf, LOCATION.Location))
        self.graph.add((LOCATION.Bridge, RDFS.subClassOf, LOCATION.Location))
        
        # Sensor types
        self.graph.add((SENSOR.TrafficSensor, RDFS.subClassOf, SENSOR.Sensor))
        self.graph.add((SENSOR.WeatherSensor, RDFS.subClassOf, SENSOR.Sensor))
        self.graph.add((SENSOR.AirQualitySensor, RDFS.subClassOf, SENSOR.Sensor))
        self.graph.add((SENSOR.InfrastructureSensor, RDFS.subClassOf, SENSOR.Sensor))
        
    def bind_namespaces(self):
        """Bind all namespaces to the graph"""
        self.graph.bind("dt", DT)
        self.graph.bind("traffic", TRAFFIC)
        self.graph.bind("weather", WEATHER)
        self.graph.bind("air", AIR)
        self.graph.bind("infra", INFRA)
        self.graph.bind("vehicle", VEHICLE)
        self.graph.bind("sensor", SENSOR)
        self.graph.bind("location", LOCATION)
        self.graph.bind("event", EVENT)
        self.graph.bind("geo", GEO)
        self.graph.bind("time", TIME)
        
    def convert_traffic_data(self, csv_file):
        """Convert traffic data to RDF triples"""
        print(f"Converting traffic data from {csv_file}...")
        
        filepath = Path("output") / csv_file
        if not filepath.exists():
            print(f"File {filepath} not found!")
            return
            
        with open(filepath, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for i, row in enumerate(reader):
                if i >= 10000:  # Limit for performance
                    break
                    
                # Create URIs
                vehicle_uri = URIRef(f"{VEHICLE}{row['vehicle_id']}")
                location_uri = URIRef(f"{LOCATION}{row['location_id']}")
                sensor_uri = URIRef(f"{SENSOR}{row['sensor_id']}")
                event_uri = URIRef(f"{EVENT}traffic_{row['vehicle_id']}_{row['timestamp'].replace(' ', '_').replace(':', '-')}")
                
                # Vehicle triples
                self.graph.add((vehicle_uri, RDF.type, TRAFFIC.Vehicle))
                self.graph.add((vehicle_uri, TRAFFIC.hasSpeed, Literal(float(row['speed']), datatype=XSD.float)))
                self.graph.add((vehicle_uri, TRAFFIC.hasLocation, location_uri))
                self.graph.add((vehicle_uri, TRAFFIC.detectedBy, sensor_uri))
                self.graph.add((vehicle_uri, TRAFFIC.hasTimestamp, Literal(row['timestamp'], datatype=XSD.dateTime)))
                self.graph.add((vehicle_uri, TRAFFIC.hasVehicleType, Literal(row['vehicle_type'], datatype=XSD.string)))
                self.graph.add((vehicle_uri, TRAFFIC.hasFuelLevel, Literal(float(row['fuel_level']), datatype=XSD.float)))
                self.graph.add((vehicle_uri, TRAFFIC.hasEngineTemp, Literal(float(row['engine_temp']), datatype=XSD.float)))
                self.graph.add((vehicle_uri, TRAFFIC.hasTirePressure, Literal(float(row['tire_pressure']), datatype=XSD.float)))
                self.graph.add((vehicle_uri, TRAFFIC.hasAcceleration, Literal(float(row['acceleration']), datatype=XSD.float)))
                self.graph.add((vehicle_uri, TRAFFIC.hasBrakingFrequency, Literal(float(row['braking_frequency']), datatype=XSD.float)))
                self.graph.add((vehicle_uri, TRAFFIC.hasEmissionLevel, Literal(row['emission_level'], datatype=XSD.string)))
                self.graph.add((vehicle_uri, TRAFFIC.hasVehicleSize, Literal(row['vehicle_size'], datatype=XSD.string)))
                
                # Location triples
                self.graph.add((location_uri, RDF.type, LOCATION.Location))
                self.graph.add((location_uri, LOCATION.hasLatitude, Literal(float(row['latitude']), datatype=XSD.float)))
                self.graph.add((location_uri, LOCATION.hasLongitude, Literal(float(row['longitude']), datatype=XSD.float)))
                
                # Sensor triples
                self.graph.add((sensor_uri, RDF.type, SENSOR.TrafficSensor))
                
                # Event triples
                self.graph.add((event_uri, RDF.type, TRAFFIC.TrafficEvent))
                self.graph.add((event_uri, TRAFFIC.hasEventType, Literal(row['event_type'], datatype=XSD.string)))
                self.graph.add((event_uri, TRAFFIC.occursAt, vehicle_uri))
                self.graph.add((event_uri, TRAFFIC.occursAt, location_uri))
                
        print(f"Converted {min(10000, i+1)} traffic records to RDF")
        
    def convert_weather_data(self, csv_file):
        """Convert weather data to RDF triples"""
        print(f"Converting weather data from {csv_file}...")
        
        filepath = Path("output") / csv_file
        if not filepath.exists():
            print(f"File {filepath} not found!")
            return
            
        with open(filepath, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for i, row in enumerate(reader):
                if i >= 5000:  # Limit for performance
                    break
                    
                # Create URIs
                weather_uri = URIRef(f"{WEATHER}weather_{row['timestamp'].replace(' ', '_').replace(':', '-')}_{row['location_id']}")
                location_uri = URIRef(f"{LOCATION}{row['location_id']}")
                sensor_uri = URIRef(f"{SENSOR}{row['sensor_id']}")
                
                # Weather triples
                self.graph.add((weather_uri, RDF.type, WEATHER.WeatherCondition))
                self.graph.add((weather_uri, WEATHER.measuredAt, location_uri))
                self.graph.add((weather_uri, WEATHER.measuredBy, sensor_uri))
                self.graph.add((weather_uri, WEATHER.hasTimestamp, Literal(row['timestamp'], datatype=XSD.dateTime)))
                self.graph.add((weather_uri, WEATHER.hasTemperature, Literal(float(row['temperature']), datatype=XSD.float)))
                self.graph.add((weather_uri, WEATHER.hasHumidity, Literal(float(row['humidity']), datatype=XSD.float)))
                self.graph.add((weather_uri, WEATHER.hasWindSpeed, Literal(float(row['wind_speed']), datatype=XSD.float)))
                self.graph.add((weather_uri, WEATHER.hasPrecipitation, Literal(float(row['precipitation']), datatype=XSD.float)))
                self.graph.add((weather_uri, WEATHER.hasCondition, Literal(row['condition'], datatype=XSD.string)))
                self.graph.add((weather_uri, WEATHER.hasVisibility, Literal(float(row['visibility']), datatype=XSD.float)))
                
                # Sensor triples
                self.graph.add((sensor_uri, RDF.type, SENSOR.WeatherSensor))
                
        print(f"Converted {min(5000, i+1)} weather records to RDF")
        
    def convert_air_quality_data(self, csv_file):
        """Convert air quality data to RDF triples"""
        print(f"Converting air quality data from {csv_file}...")
        
        filepath = Path("output") / csv_file
        if not filepath.exists():
            print(f"File {filepath} not found!")
            return
            
        with open(filepath, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for i, row in enumerate(reader):
                if i >= 5000:  # Limit for performance
                    break
                    
                # Create URIs
                air_uri = URIRef(f"{AIR}air_{row['timestamp'].replace(' ', '_').replace(':', '-')}_{row['location_id']}")
                location_uri = URIRef(f"{LOCATION}{row['location_id']}")
                sensor_uri = URIRef(f"{SENSOR}{row['sensor_id']}")
                
                # Air quality triples
                self.graph.add((air_uri, RDF.type, AIR.AirQualityMeasurement))
                self.graph.add((air_uri, AIR.measuredAt, location_uri))
                self.graph.add((air_uri, AIR.measuredBy, sensor_uri))
                self.graph.add((air_uri, AIR.hasTimestamp, Literal(row['timestamp'], datatype=XSD.dateTime)))
                self.graph.add((air_uri, AIR.hasPM25, Literal(float(row['pm25']), datatype=XSD.float)))
                self.graph.add((air_uri, AIR.hasCO, Literal(float(row['co']), datatype=XSD.float)))
                self.graph.add((air_uri, AIR.hasNO2, Literal(float(row['no2']), datatype=XSD.float)))
                self.graph.add((air_uri, AIR.hasO3, Literal(float(row['o3']), datatype=XSD.float)))
                self.graph.add((air_uri, AIR.hasQualityLevel, Literal(row['quality_level'], datatype=XSD.string)))
                
                # Sensor triples
                self.graph.add((sensor_uri, RDF.type, SENSOR.AirQualitySensor))
                
        print(f"Converted {min(5000, i+1)} air quality records to RDF")
        
    def convert_infrastructure_data(self, csv_file):
        """Convert infrastructure data to RDF triples"""
        print(f"Converting infrastructure data from {csv_file}...")
        
        filepath = Path("output") / csv_file
        if not filepath.exists():
            print(f"File {filepath} not found!")
            return
            
        with open(filepath, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for i, row in enumerate(reader):
                if i >= 5000:  # Limit for performance
                    break
                    
                # Create URIs
                infra_uri = URIRef(f"{INFRA}infra_{row['timestamp'].replace(' ', '_').replace(':', '-')}_{row['location_id']}")
                location_uri = URIRef(f"{LOCATION}{row['location_id']}")
                sensor_uri = URIRef(f"{SENSOR}{row['sensor_id']}")
                
                # Infrastructure triples
                self.graph.add((infra_uri, RDF.type, INFRA.InfrastructureElement))
                self.graph.add((infra_uri, INFRA.hasLocation, location_uri))
                self.graph.add((infra_uri, INFRA.measuredBy, sensor_uri))
                self.graph.add((infra_uri, INFRA.hasTimestamp, Literal(row['timestamp'], datatype=XSD.dateTime)))
                self.graph.add((infra_uri, INFRA.hasStructuralHealth, Literal(float(row['structural_health']), datatype=XSD.float)))
                
                # Add conditional properties
                if row.get('vibration') and row['vibration'] != '':
                    self.graph.add((infra_uri, INFRA.hasVibration, Literal(float(row['vibration']), datatype=XSD.float)))
                if row.get('load_capacity') and row['load_capacity'] != '':
                    self.graph.add((infra_uri, INFRA.hasLoadCapacity, Literal(float(row['load_capacity']), datatype=XSD.float)))
                if row.get('air_quality_tunnel') and row['air_quality_tunnel'] != '':
                    self.graph.add((infra_uri, INFRA.hasAirQualityTunnel, Literal(float(row['air_quality_tunnel']), datatype=XSD.float)))
                if row.get('lighting_status') and row['lighting_status'] != '':
                    self.graph.add((infra_uri, INFRA.hasLightingStatus, Literal(row['lighting_status'], datatype=XSD.string)))
                if row.get('traffic_light_status') and row['traffic_light_status'] != '':
                    self.graph.add((infra_uri, INFRA.hasTrafficLightStatus, Literal(row['traffic_light_status'], datatype=XSD.string)))
                
                # Location type
                self.graph.add((location_uri, LOCATION.hasType, Literal(row['location_type'], datatype=XSD.string)))
                
                # Sensor triples
                self.graph.add((sensor_uri, RDF.type, SENSOR.InfrastructureSensor))
                
        print(f"Converted {min(5000, i+1)} infrastructure records to RDF")
        
    def add_digital_twin_relationships(self):
        """Add relationships between different data types"""
        print("Adding digital twin relationships...")
        
        # Add relationships between traffic and weather
        # (This would require more complex reasoning, simplified here)
        self.graph.add((TRAFFIC.TrafficEvent, WEATHER.affectsTraffic, WEATHER.WeatherCondition))
        self.graph.add((WEATHER.WeatherCondition, WEATHER.affectsVisibility, TRAFFIC.TrafficEvent))
        
        # Add relationships between traffic and air quality
        self.graph.add((TRAFFIC.Vehicle, TRAFFIC.affectsAirQuality, AIR.AirQualityMeasurement))
        self.graph.add((AIR.AirQualityMeasurement, AIR.affectsTraffic, TRAFFIC.TrafficEvent))
        
        # Add relationships between infrastructure and traffic
        self.graph.add((INFRA.InfrastructureElement, INFRA.affectsTraffic, TRAFFIC.TrafficEvent))
        
        # Add digital twin monitoring relationships
        self.graph.add((DT.DigitalTwin, DT.monitors, TRAFFIC.TrafficSystem))
        self.graph.add((DT.DigitalTwin, DT.monitors, WEATHER.WeatherSystem))
        self.graph.add((DT.DigitalTwin, DT.monitors, AIR.AirQualitySystem))
        self.graph.add((DT.DigitalTwin, DT.monitors, INFRA.InfrastructureSystem))
        
        print("Digital twin relationships added")
        
    def convert_all_data(self):
        """Convert all data types to RDF"""
        print("🚀 Starting comprehensive RDF conversion for Digital Twin...")
        
        # Convert each data type
        self.convert_traffic_data("smart_traffic_data.csv")
        self.convert_weather_data("weather_data.csv")
        self.convert_air_quality_data("air_quality_data.csv")
        self.convert_infrastructure_data("infrastructure_data.csv")
        
        # Add relationships
        self.add_digital_twin_relationships()
        
        print("✅ All data converted to RDF successfully!")
        
    def save_rdf(self, filename="digital_twin_knowledge_graph.ttl"):
        """Save RDF graph to file"""
        output_path = Path("output") / filename
        self.graph.serialize(destination=output_path, format="turtle")
        print(f"💾 RDF knowledge graph saved to {output_path}")
        
        # Print statistics
        print(f"📊 Graph Statistics:")
        print(f"   Total Triples: {len(self.graph)}")
        print(f"   Unique Subjects: {len(set(s for s, p, o in self.graph))}")
        print(f"   Unique Predicates: {len(set(p for s, p, o in self.graph))}")
        print(f"   Unique Objects: {len(set(o for s, p, o in self.graph))}")
        
    def get_graph_summary(self):
        """Get summary of the RDF graph"""
        subjects = set(s for s, p, o in self.graph)
        predicates = set(p for s, p, o in self.graph)
        objects = set(o for s, p, o in self.graph)
        
        return {
            "total_triples": len(self.graph),
            "unique_subjects": len(subjects),
            "unique_predicates": len(predicates),
            "unique_objects": len(objects),
            "namespaces": list(self.graph.namespaces())
        }

def main():
    """Main function to run the digital twin RDFizer"""
    print("🌐 Digital Twin RDFizer for Smart Traffic System")
    print("=" * 60)
    
    # Create RDFizer instance
    rdfizer = DigitalTwinRDFizer()
    
    # Convert all data
    rdfizer.convert_all_data()
    
    # Save RDF graph
    rdfizer.save_rdf()
    
    # Print summary
    summary = rdfizer.get_graph_summary()
    print(f"\n🎯 RDF Conversion Complete!")
    print(f"📈 Knowledge Graph Created with {summary['total_triples']} triples")
    print(f"🔗 Ready for MAS system processing and intelligent queries!")

if __name__ == "__main__":
    main()

