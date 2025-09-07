import csv
from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD, RDFS
from pathlib import Path
import datetime

# Define namespaces for the traffic ontology
TRAFFIC = Namespace("http://example.org/traffic#")
CITY = Namespace("http://example.org/cityOnto#")

# Function to initialize RDF graph with ontology
#using the TRAFFIC and CITY ontologies
def init_graph():  # این همون آنتولوژی است که تعریف شده است اینطوری
    g = Graph()
    # Bind namespaces
    g.bind("traffic", TRAFFIC)
    g.bind("city", CITY)
    g.bind("rdfs", RDFS)
    # Define ontology classes
    g.add((TRAFFIC.Vehicle, RDF.type, RDFS.Class))
    g.add((TRAFFIC.Sensor, RDF.type, RDFS.Class))
    g.add((TRAFFIC.Location, RDF.type, RDFS.Class))
    g.add((TRAFFIC.Event, RDF.type, RDFS.Class))
    # Define ontology properties
    g.add((TRAFFIC.hasSpeed, RDF.type, RDF.Property))
    g.add((TRAFFIC.atLocation, RDF.type, RDF.Property))
    g.add((TRAFFIC.detectedBy, RDF.type, RDF.Property))
    g.add((TRAFFIC.hasTimestamp, RDF.type, RDF.Property))
    g.add((TRAFFIC.hasEventType, RDF.type, RDF.Property))
    return g

# Function to convert CSV data to RDF triples
def csv_to_rdf(input_file, output_file="traffic_data_rdf.ttl"):
    g = init_graph()
    input_path = Path("output") / input_file
    
    with open(input_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader):
            if i >= 10000:  # Only process first 10 rows
                break
            # Create unique URIs for each entity
            vehicle_uri = URIRef(f"{TRAFFIC}vehicle/{row['vehicle_id']}")
            sensor_uri = URIRef(f"{TRAFFIC}sensor/{row['sensor_id']}")
            location_uri = URIRef(f"{TRAFFIC}location/{row['location_id']}")
            event_uri = URIRef(f"{TRAFFIC}event/{row['vehicle_id']}_{row['timestamp'].replace(' ', '_').replace(':', '-')}")
            
            # Add triples for Vehicle
            g.add((vehicle_uri, RDF.type, TRAFFIC.Vehicle))
            g.add((vehicle_uri, TRAFFIC.hasSpeed, Literal(float(row['speed']), datatype=XSD.float)))
            g.add((vehicle_uri, TRAFFIC.atLocation, location_uri))
            g.add((vehicle_uri, TRAFFIC.detectedBy, sensor_uri))
            g.add((vehicle_uri, TRAFFIC.hasTimestamp, Literal(row['timestamp'], datatype=XSD.dateTime)))
            g.add((vehicle_uri, TRAFFIC.hasEventType, Literal(row['event_type'], datatype=XSD.string)))
            
            # Add triples for Sensor
            g.add((sensor_uri, RDF.type, TRAFFIC.Sensor))
            
            # Add triples for Location
            g.add((location_uri, RDF.type, TRAFFIC.Location))
            g.add((location_uri, CITY.hasLatitude, Literal(float(row['latitude']), datatype=XSD.float)))
            g.add((location_uri, CITY.hasLongitude, Literal(float(row['longitude']), datatype=XSD.float)))
            
            # Add triples for Event
            g.add((event_uri, RDF.type, TRAFFIC.Event))
            g.add((event_uri, TRAFFIC.hasEventType, Literal(row['event_type'], datatype=XSD.string)))
            g.add((event_uri, TRAFFIC.occursAt, vehicle_uri))
    
    # Serialize RDF graph to Turtle format
    output_path = Path("output") / output_file
    g.serialize(destination=output_path, format="turtle")
    print(f"RDF data saved to {output_path}")

# Main function
def main():
    print("Converting traffic data to RDF...")
    input_file = "traffic_data.csv"  # Input from previous script
    csv_to_rdf(input_file)
    print("RDF conversion completed.")

if __name__ == "__main__":
    main()