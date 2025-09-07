import random
import datetime
import queue
import time
from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD, RDFS

# Define namespaces for the traffic ontology
TRAFFIC = Namespace("http://example.org/traffic#")
CITY = Namespace("http://example.org/cityOnto#")

class EdgeLayerQueueAgent:
    def __init__(self, shared_queue):
        self.data_queue = queue.Queue()
        self.graph = Graph()
        self.shared_queue = shared_queue  # Queue to send RDF triples to FogAgent

    # Step 1: Generate synthetic traffic data and put it in a queue
    def generate_traffic_data(self):
        print("Generating synthetic traffic data and adding to queue...")
        LOCATIONS = [
            {"id": "INT1", "name": "Intersection_1", "lat": 35.6895, "lon": 51.3890},
            {"id": "INT2", "name": "Intersection_2", "lat": 35.7000, "lon": 51.4000},
            {"id": "HWY1", "name": "Highway_1", "lat": 35.7100, "lon": 51.4100},
            {"id": "HWY2", "name": "Highway_2", "lat": 35.7200, "lon": 51.4200}
        ]
        SENSOR_IDS = [f"SENSOR_{i}" for i in range(1, 21)]
        EVENT_TYPES = ["Normal", "Congestion", "Incident"]
        NUM_VEHICLES = 1000
        SIMULATION_MINUTES = 5  # 5 minutes to align with query window
        RECORDS_PER_MINUTE = 1000  # Enough records for results

        def random_timestamp():
            now = datetime.datetime.now()
            start_time = now - datetime.timedelta(minutes=SIMULATION_MINUTES)
            time_diff = random.randint(0, SIMULATION_MINUTES * 60)
            return (start_time + datetime.timedelta(seconds=time_diff)).strftime("%Y-%m-%d %H:%M:%S")

        def get_speed_and_event(hour):
            if 7 <= hour <= 9 or 16 <= hour <= 18:
                speed = random.uniform(10, 50)
                event_probs = [0.3, 0.6, 0.1]
            else:
                speed = random.uniform(30, 100)
                event_probs = [0.7, 0.2, 0.1]
            return speed, random.choices(EVENT_TYPES, event_probs)[0]

        for _ in range(SIMULATION_MINUTES * RECORDS_PER_MINUTE):
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
            self.data_queue.put(record)

        print(f"Generated {self.data_queue.qsize()} records and added to queue.")
        return self.data_queue.qsize()

    # Step 2: Convert queued data to RDF triples
    def queue_to_rdf(self):
        print("Converting queued data to RDF...")
        self.graph.bind("traffic", TRAFFIC)
        self.graph.bind("city", CITY)
        self.graph.bind("rdfs", RDFS)
        
        # Define ontology classes
        self.graph.add((TRAFFIC.Vehicle, RDF.type, RDFS.Class))
        self.graph.add((TRAFFIC.Sensor, RDF.type, RDFS.Class))
        self.graph.add((TRAFFIC.Location, RDF.type, RDFS.Class))
        self.graph.add((TRAFFIC.Event, RDF.type, RDFS.Class))
        self.graph.add((TRAFFIC.Observation, RDF.type, RDFS.Class))
        # Define ontology properties
        self.graph.add((TRAFFIC.hasSpeed, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.atLocation, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.detectedBy, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasTimestamp, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasEventType, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasObservationType, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasVehicleCount, RDF.type, RDF.Property))

        while not self.data_queue.empty():
            row = self.data_queue.get()
            vehicle_uri = URIRef(f"{TRAFFIC}vehicle/{row['vehicle_id']}")
            sensor_uri = URIRef(f"{TRAFFIC}sensor/{row['sensor_id']}")
            location_uri = URIRef(f"{TRAFFIC}location/{row['location_id']}")
            event_uri = URIRef(f"{TRAFFIC}event/{row['vehicle_id']}_{row['timestamp'].replace(' ', '_').replace(':', '-')}")
            
            self.graph.add((vehicle_uri, RDF.type, TRAFFIC.Vehicle))
            self.graph.add((vehicle_uri, TRAFFIC.hasSpeed, Literal(float(row['speed']), datatype=XSD.float)))
            self.graph.add((vehicle_uri, TRAFFIC.atLocation, location_uri))
            self.graph.add((vehicle_uri, TRAFFIC.detectedBy, sensor_uri))
            self.graph.add((vehicle_uri, TRAFFIC.hasTimestamp, Literal(row['timestamp'], datatype=XSD.dateTime)))
            self.graph.add((vehicle_uri, TRAFFIC.hasEventType, Literal(row['event_type'], datatype=XSD.string)))
            
            self.graph.add((sensor_uri, RDF.type, TRAFFIC.Sensor))
            
            self.graph.add((location_uri, RDF.type, TRAFFIC.Location))
            self.graph.add((location_uri, CITY.hasLatitude, Literal(float(row['latitude']), datatype=XSD.float)))
            self.graph.add((location_uri, CITY.hasLongitude, Literal(float(row['longitude']), datatype=XSD.float)))
            
            self.graph.add((event_uri, RDF.type, TRAFFIC.Event))
            self.graph.add((event_uri, TRAFFIC.hasEventType, Literal(row['event_type'], datatype=XSD.string)))
            self.graph.add((event_uri, TRAFFIC.occursAt, vehicle_uri))

        print(f"Converted {len(self.graph)} triples to RDF graph.")

    # Step 3: Execute C-SPARQL queries and send vehicle_count_per_location results as RDF to shared queue
    def execute_csparql_queries(self, iterations=3):
        print("Executing C-SPARQL queries for object refinement...")
        
        def query_high_speed_vehicles(window_size_seconds=60, step_seconds=10, speed_threshold=80.0):
            print("Query 1: High Speed Vehicles")
            end_time = datetime.datetime.now()
            start_time = end_time - datetime.timedelta(seconds=window_size_seconds)
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
            if not results:
                print("Query 1: No high-speed vehicles found.")
            # for row in results:
            #     print(f"Vehicle: {row.vehicle}, Speed: {row.speed}, Location: {row.location}")

        def query_vehicle_count_per_location(window_size_seconds=300, step_seconds=30):
            print("Query 2: Vehicle Count Per Location")
            end_time = datetime.datetime.now()
            start_time = end_time - datetime.timedelta(seconds=window_size_seconds)
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
            triples_batch = []
            if not results:
                print("Query 2: No vehicle count results found.")
            for row in results:
                observation_uri = URIRef(f"{TRAFFIC}observation/vehicle_count_{str(row.location).split('/')[-1]}_{end_time.isoformat().replace(':', '-')}")
                triples = [
                    (observation_uri, RDF.type, TRAFFIC.Observation),
                    (observation_uri, TRAFFIC.hasObservationType, Literal("VehicleCount", datatype=XSD.string)),
                    (observation_uri, TRAFFIC.atLocation, URIRef(row.location)),
                    (observation_uri, TRAFFIC.hasVehicleCount, Literal(int(row.vehicleCount), datatype=XSD.integer)),
                    (observation_uri, TRAFFIC.hasTimestamp, Literal(end_time.isoformat(), datatype=XSD.dateTime))
                ]
                triples_batch.extend(triples)
                print(f"Query 2: Location: {row.location}, Vehicle Count: {row.vehicleCount}, Timestamp: {end_time.isoformat()}")
            
            if triples_batch:
                print(f"EdgeAgent: Sending {len(triples_batch)} RDF triples to shared queue:")
                for triple in triples_batch:
                    print(f"  {triple}")
                self.shared_queue.put(triples_batch)
            else:
                print("EdgeAgent: No RDF triples to send for vehicle count.")

        def query_congestion_events(window_size_seconds=120, step_seconds=20):
            print("Query 3: Congestion Events")
            end_time = datetime.datetime.now()
            start_time = end_time - datetime.timedelta(seconds=window_size_seconds)
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
            if not results:
                print("Query 3: No congestion events found.")
            # for row in results:
            #     print(f"Vehicle: {row.vehicle}, Location: {row.location}, Timestamp: {row.timestamp}")

        for i in range(iterations):
            print(f"\n--- Iteration {i+1} ---")
            query_high_speed_vehicles()
            query_vehicle_count_per_location()
            query_congestion_events()
            time.sleep(5)  # Simulate step interval

    # Main workflow to run all steps
    def run(self):
        # Step 1: Generate data and add to queue
        num_records = self.generate_traffic_data()
        
        # Step 2: Convert queued data to RDF
        self.queue_to_rdf()
        
        # Step 3: Execute C-SPARQL queries
        self.execute_csparql_queries()