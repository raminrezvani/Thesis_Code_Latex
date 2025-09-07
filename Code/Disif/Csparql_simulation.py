
import csv
from rdflib import Graph, Namespace
from rdflib.namespace import XSD, RDF, RDFS
from datetime import datetime, timedelta
from pathlib import Path
import time

# Define namespaces
TRAFFIC = Namespace("http://example.org/traffic#")
CITY = Namespace("http://example.org/cityOnto#")

# Simulated C-SPARQL query executor
class CSPARQLSimulator:
    def __init__(self, rdf_file):
        self.graph = Graph()
        self.graph.parse(rdf_file, format="turtle")
        self.rdf_file = rdf_file

    def query_high_speed_vehicles(self, window_size_seconds=60, step_seconds=10, speed_threshold=80.0):
        print("Executing Query 1: High Speed Vehicles")
        end_time = datetime.now()
        start_time = end_time - timedelta(seconds=window_size_seconds)
        
        # Simulate sliding window by filtering triples based on timestamp
        query = """
        PREFIX traffic: <http://example.org/traffic#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?vehicle ?speed ?location
        WHERE {
            ?vehicle a traffic:Vehicle ;
                     traffic:hasSpeed ?speed ;
                     traffic:atLocation ?location ;
                     traffic:hasTimestamp ?timestamp .
            FILTER (?speed > %f && ?timestamp >= "%s"^^xsd:dateTime && ?timestamp <= "%s"^^xsd:dateTime)
        }
        """ % (speed_threshold, start_time.isoformat(), end_time.isoformat())
        
        results = self.graph.query(query)
        for row in results:
            print(f"Vehicle: {row.vehicle}, Speed: {row.speed}, Location: {row.location}")

    def query_vehicle_count_per_location(self, window_size_seconds=300, step_seconds=30):
        print("Executing Query 2: Vehicle Count Per Location")
        end_time = datetime.now()
        start_time = end_time - timedelta(seconds=window_size_seconds)
        
        query = """
        PREFIX traffic: <http://example.org/traffic#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?location (COUNT(?vehicle) AS ?vehicleCount)
        WHERE {
            ?vehicle a traffic:Vehicle ;
                     traffic:atLocation ?location ;
                     traffic:hasTimestamp ?timestamp .
            FILTER (?timestamp >= "%s"^^xsd:dateTime && ?timestamp <= "%s"^^xsd:dateTime)
        }
        GROUP BY ?location
        """ % (start_time.isoformat(), end_time.isoformat())
        
        results = self.graph.query(query)
        for row in results:
            print(f"Location: {row.location}, Vehicle Count: {row.vehicleCount}")

    def query_congestion_events(self, window_size_seconds=120, step_seconds=20):
        print("Executing Query 3: Congestion Events")
        end_time = datetime.now()
        start_time = end_time - timedelta(seconds=window_size_seconds)
        
        query = """
        PREFIX traffic: <http://example.org/traffic#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?vehicle ?location ?timestamp
        WHERE {
            ?vehicle a traffic:Vehicle ;
                     traffic:hasEventType "Congestion"^^xsd:string ;
                     traffic:atLocation ?location ;
                     traffic:hasTimestamp ?timestamp .
            FILTER (?timestamp >= "%s"^^xsd:dateTime && ?timestamp <= "%s"^^xsd:dateTime)
        }
        """ % (start_time.isoformat(), end_time.isoformat())
        
        results = self.graph.query(query)
        for row in results:
            print(f"Vehicle: {row.vehicle}, Location: {row.location}, Timestamp: {row.timestamp}")

# Main function to simulate C-SPARQL execution
def main():
    rdf_file = Path("output") / "traffic_data_rdf.ttl"
    if not rdf_file.exists():
        print(f"Error: RDF file {rdf_file} not found. Please run the RDFizer first.")
        return
    
    simulator = CSPARQLSimulator(rdf_file)
    
    # Simulate continuous query execution
    print("Starting C-SPARQL query simulation...")
    for _ in range(3):  # Simulate 3 sliding window iterations
        simulator.query_high_speed_vehicles()
        simulator.query_vehicle_count_per_location()
        simulator.query_congestion_events()
        time.sleep(5)  # Simulate step interval
        print("\n--- Next window iteration ---\n")

if __name__ == "__main__":
    main()