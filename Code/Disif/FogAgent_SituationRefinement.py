import queue
import time
import datetime
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import XSD, RDF, RDFS

# Define namespaces
TRAFFIC = Namespace("http://example.org/traffic#")
CITY = Namespace("http://example.org/cityOnto#")

class FogLayerAgent:
    def __init__(self, shared_queue):
        self.shared_queue = shared_queue
        self.graph = Graph()
        self.graph.bind("traffic", TRAFFIC)
        self.graph.bind("city", CITY)
        # Define ontology classes and properties
        self.graph.add((TRAFFIC.Observation, RDF.type, RDFS.Class))
        self.graph.add((TRAFFIC.hasObservationType, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasVehicleCount, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasSituationType, RDF.type, RDF.Property))

    def process_queue(self, iterations=300):  # Match EdgeAgent iterations
        print("FogAgent: Listening to shared queue for situation refinement...")
        for i in range(iterations):
            print(f"\nFogAgent: --- Iteration {i+1} ---")
            try:
                # Get RDF triples from the shared queue
                received_triples = 0
                while True:
                    try:
                        triples = self.shared_queue.get(timeout=10)
                        print(f"FogAgent: Received {len(triples)} triples in iteration {i+1}:")
                        for triple in triples:
                            self.graph.add(triple)
                            print(f"  {triple}")
                            received_triples += 1
                        self.shared_queue.task_done()
                    except queue.Empty:
                        print("FogAgent: Queue empty, proceeding to query...")
                        break
                
                print(f"FogAgent: Total triples in graph: {len(self.graph)}")
                # Execute C-SPARQL query for situation refinement
                self.execute_csparql_query()
                time.sleep(2)
            except Exception as e:
                print(f"FogAgent: Error processing queue: {e}")
                time.sleep(1)

    def execute_csparql_query(self):
        print("FogAgent: Executing C-SPARQL query for traffic jam detection...")
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(minutes=15)  # 15-minute window for reference
        # Debug query to inspect matching triples
        debug_query = """
        PREFIX traffic: <http://example.org/traffic#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?observation ?location ?vehicleCount ?timestamp
        WHERE {
            ?observation a traffic:Observation ;
                         traffic:hasObservationType "VehicleCount"^^xsd:string ;
                         traffic:atLocation ?location ;
                         traffic:hasVehicleCount ?vehicleCount ;
                         traffic:hasTimestamp ?timestamp .
        }
        """
        results = self.graph.query(debug_query)
        if not results:
            print("FogAgent: No matching triples found in debug query.")
        else:
            print("FogAgent: Matching triples found:")
            for row in results:
                print(f"  Observation: {row.observation}, Location: {row.location}, VehicleCount: {row.vehicleCount}, Timestamp: {row.timestamp}")

        # Aggregate query without timestamp filter for testing
        aggregate_query = """
        PREFIX traffic: <http://example.org/traffic#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?location (SUM(?vehicleCount) AS ?totalVehicleCount)
        WHERE {
            ?observation a traffic:Observation ;
                         traffic:hasObservationType "VehicleCount"^^xsd:string ;
                         traffic:atLocation ?location ;
                         traffic:hasVehicleCount ?vehicleCount .
        }
        GROUP BY ?location
        """
        results = self.graph.query(aggregate_query)
        if not results:
            print("FogAgent: No results in aggregate query.")
        else:
            print("FogAgent: Aggregate query results:")
            for row in results:
                print(f"  Location: {row.location}, TotalVehicleCount: {row.totalVehicleCount}")

        # Traffic jam detection with HAVING clause
        traffic_jam_query = """
        PREFIX traffic: <http://example.org/traffic#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?location (SUM(?vehicleCount) AS ?totalVehicleCount)
        WHERE {
            ?observation a traffic:Observation ;
                         traffic:hasObservationType "VehicleCount"^^xsd:string ;
                         traffic:atLocation ?location ;
                         traffic:hasVehicleCount ?vehicleCount .
        }
        GROUP BY ?location
        HAVING (?totalVehicleCount >= 0)
        """
        results = self.graph.query(traffic_jam_query)
        if not results:
            print("FogAgent: No traffic jams detected in this iteration.")
        for row in results:
            location = str(row.location)
            total_vehicle_count = int(row.totalVehicleCount)
            print(f"Traffic Jam Detected at {location}: Total Vehicle Count = {total_vehicle_count}")
            situation_uri = URIRef(f"{TRAFFIC}situation/traffic_jam_{location.split('/')[-1]}_{int(time.time())}")
            self.graph.add((situation_uri, TRAFFIC.hasSituationType, Literal("TrafficJam", datatype=XSD.string)))
            self.graph.add((situation_uri, TRAFFIC.atLocation, URIRef(location)))
            self.graph.add((situation_uri, TRAFFIC.hasTimestamp, Literal(end_time.isoformat(), datatype=XSD.dateTime)))
            self.graph.add((situation_uri, TRAFFIC.hasVehicleCount, Literal(total_vehicle_count, datatype=XSD.integer)))

    def run(self):
        self.process_queue()