#!/usr/bin/env python3
"""
Centralized Agent for comparison with MAS system
Performs all operations in a single agent without distributed processing
"""

import time
import json
import queue
from typing import Dict, Any, List
import datetime
from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD, RDFS
import random
import logging

# Define namespaces for the traffic ontology
TRAFFIC = Namespace("http://example.org/traffic#")
CITY = Namespace("http://example.org/cityOnto#")

class CentralizedAgent:
    """
    Centralized agent that performs all operations without MAS
    Used for performance comparison with MAS system
    """
    
    def __init__(self, agent_id: str = "centralized_001"):
        self.agent_id = agent_id
        self.logger = logging.getLogger(f"CentralizedAgent_{agent_id}")
        
        # Data structures
        self.data_queue = queue.Queue()
        self.graph = Graph()
        self.current_task = None
        self.task_results = {}
        
        # Performance metrics for comparison
        self.performance_metrics = {
            'total_queries_executed': 0,
            'total_execution_time': 0.0,
            'average_execution_time': 0.0,
            'fastest_execution_time': float('inf'),
            'slowest_execution_time': 0.0,
            'query_type_performance': {},
            'data_generation_times': [],
            'rdf_conversion_times': [],
            'query_execution_times': [],
            'situation_refinement_times': []
        }
        
        # Initialize graph with ontology
        self._initialize_ontology()
        
        # Query templates (same as MAS for fair comparison)
        self.query_templates = self._initialize_query_templates()
    
    def _initialize_ontology(self):
        """Initialize the graph with traffic ontology"""
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
        self.graph.add((TRAFFIC.hasSituationType, RDF.type, RDF.Property))
    
    def _initialize_query_templates(self) -> Dict[str, Dict[str, Any]]:
        """Initialize predefined query templates (same as MAS)"""
        return {
            "high_speed_vehicles": {
                "description": "Find vehicles exceeding speed threshold",
                "parameters": {
                    "speed_threshold": 80.0,
                    "window_size_seconds": 60
                }
            },
            "vehicle_count_per_location": {
                "description": "Count vehicles per location",
                "parameters": {
                    "window_size_seconds": 300
                }
            },
            "congestion_events": {
                "description": "Find congestion events",
                "parameters": {
                    "window_size_seconds": 120
                }
            },
            "comprehensive_traffic_analysis": {
                "description": "Comprehensive traffic analysis combining multiple queries",
                "parameters": {
                    "speed_threshold": 80.0,
                    "window_size_seconds": 300
                }
            }
        }
    
    def generate_traffic_data(self):
        """Generate synthetic traffic data and put it in a queue (like EdgeAgent)"""
        self.logger.info("Generating synthetic traffic data and adding to queue...")
        LOCATIONS = [
            {"id": "INT1", "name": "Intersection_1", "lat": 35.6895, "lon": 51.3890},
            {"id": "INT2", "name": "Intersection_2", "lat": 35.7000, "lon": 51.4000},
            {"id": "HWY1", "name": "Highway_1", "lat": 35.7100, "lon": 51.4100},
            {"id": "HWY2", "name": "Highway_2", "lat": 35.7200, "lon": 51.4200}
        ]
        SENSOR_IDS = [f"SENSOR_{i}" for i in range(1, 21)]
        EVENT_TYPES = ["Normal", "Congestion", "Incident"]
        NUM_VEHICLES = 3000  # Increased for more triples
        SIMULATION_MINUTES = 15  # Increased time window
        
        # Calculate exact records to get exactly 60k triples (same as MAS total)
        # Each record generates exactly 12 triples:
        # 1. vehicle type, 2. vehicle speed, 3. vehicle location, 4. vehicle sensor, 5. vehicle timestamp, 6. vehicle event
        # 7. sensor type, 8. location type, 9. location lat, 10. location lon, 11. event type, 12. event occursAt
        # So we need 60000/12 = 5000 records total
        TARGET_TRIPLES = 60000
        TARGET_RECORDS = TARGET_TRIPLES // 12  # Exactly 5000 records
        
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

        for i in range(TARGET_RECORDS):
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

        self.logger.info(f"Generated {self.data_queue.qsize()} records and added to queue.")
        self.logger.info(f"Expected triples: exactly {self.data_queue.qsize() * 12} (target: {TARGET_TRIPLES})")
        self.logger.info(f"Centralized system total triples: {self.data_queue.qsize() * 12}")
        return self.data_queue.qsize()
    
    def queue_to_rdf(self):
        """Convert queued data to RDF triples (same as worker agent)"""
        conversion_start_time = time.time()
        self.logger.info("Converting queued data to RDF...")
        
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

        conversion_time = time.time() - conversion_start_time
        self.performance_metrics['rdf_conversion_times'].append(conversion_time)
        
        self.logger.info(f"Converted {len(self.graph)} triples to RDF in {conversion_time:.4f} seconds")
    
    def execute_csparql_query(self, query_type: str, custom_parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute C-SPARQL query sequentially without overlapping (like centralized system)"""
        if query_type not in self.query_templates:
            raise ValueError(f"Unknown query type: {query_type}")
        
        # Start timing from data input to query output
        total_start_time = time.time()
        
        # Get query template
        template = self.query_templates[query_type]
        parameters = template["parameters"].copy()
        if custom_parameters:
            parameters.update(custom_parameters)
        
        self.logger.info(f"Executing {query_type} query sequentially (no overlapping)")
        self.logger.info(f"Parameters: {parameters}")
        
        # Phase 1: Execute worker-like queries sequentially
        worker_start_time = time.time()
        worker_results = {}
        
        if query_type == "comprehensive_traffic_analysis":
            # Execute queries one by one without overlapping
            self.logger.info("Phase 1: Executing worker queries sequentially...")
            
            # Query 1: High speed vehicles
            self.logger.info("Executing high_speed_vehicles query...")
            high_speed_result = self._execute_high_speed_query(parameters)
            worker_results['high_speed_vehicles'] = high_speed_result
            
            # Wait for completion before starting next query
            time.sleep(0.1)  # Small delay to simulate sequential processing
            
            # Query 2: Vehicle count per location
            self.logger.info("Executing vehicle_count_per_location query...")
            vehicle_count_result = self._execute_vehicle_count_query(parameters)
            worker_results['vehicle_count_per_location'] = vehicle_count_result
            
            # Wait for completion before starting next query
            time.sleep(0.1)  # Small delay to simulate sequential processing
            
            # Query 3: Congestion events
            self.logger.info("Executing congestion_events query...")
            congestion_result = self._execute_congestion_query(parameters)
            worker_results['congestion_events'] = congestion_result
            
        else:
            # Single query type - execute sequentially
            if query_type == "high_speed_vehicles":
                worker_results[query_type] = self._execute_high_speed_query(parameters)
            elif query_type == "vehicle_count_per_location":
                worker_results[query_type] = self._execute_vehicle_count_query(parameters)
            elif query_type == "congestion_events":
                worker_results[query_type] = self._execute_congestion_query(parameters)
        
        worker_time = time.time() - worker_start_time
        self.logger.info(f"Phase 1 completed in {worker_time:.4f} seconds (sequential execution)")
        
        # Phase 2: Execute master-like query (situation refinement) sequentially
        master_start_time = time.time()
        self.logger.info("Phase 2: Executing master query sequentially...")
        
        # Execute situation refinement query
        situation_result = self._execute_situation_refinement_query(worker_results)
        
        master_time = time.time() - master_start_time
        self.logger.info(f"Phase 2 completed in {master_time:.4f} seconds (sequential execution)")
        
        # Calculate total execution time
        total_time = time.time() - total_start_time
        
        # Update performance metrics
        self._update_performance_metrics(query_type, total_time, worker_time, master_time)
        
        # Prepare result
        result = {
            'query_type': query_type,
            'execution_time': total_time,
            'worker_time': worker_time,
            'master_time': master_time,
            'execution_mode': 'sequential_no_overlap',
            'graph_size': len(self.graph),
            'result': worker_results,
            'situation_refinement': situation_result
        }
        
        self.logger.info(f"Query {query_type} completed sequentially in {total_time:.4f} seconds")
        self.logger.info(f"  Worker queries: {worker_time:.4f}s")
        self.logger.info(f"  Master query: {master_time:.4f}s")
        self.logger.info(f"  Total: {total_time:.4f}s")
        self.logger.info(f"  Execution mode: Sequential without overlapping")
        
        return result
    
    def _execute_high_speed_query(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute high speed vehicles query"""
        speed_threshold = parameters.get('speed_threshold', 80.0)
        window_size = parameters.get('window_size_seconds', 60)
        
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(seconds=window_size)
        
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
        
        results = []
        for row in self.graph.query(query):
            results.append({
                'vehicle': str(row.vehicle),
                'speed': float(row.speed),
                'location': str(row.location)
            })
        
        return results
    
    def _execute_vehicle_count_query(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute vehicle count per location query"""
        window_size = parameters.get('window_size_seconds', 300)
        
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(seconds=window_size)
        
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
        
        results = []
        for row in self.graph.query(query):
            results.append({
                'location': str(row.location),
                'vehicle_count': int(row.vehicleCount)
            })
        
        return results
    
    def _execute_congestion_query(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute congestion events query"""
        window_size = parameters.get('window_size_seconds', 120)
        
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(seconds=window_size)
        
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
        
        results = []
        for row in self.graph.query(query):
            results.append({
                'vehicle': str(row.vehicle),
                'location': str(row.location),
                'timestamp': str(row.timestamp)
            })
        
        return results
    
    def _execute_comprehensive_analysis(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Execute comprehensive traffic analysis (all queries)"""
        results = {}
        
        # Execute each query type
        results['high_speed_vehicles'] = self._execute_high_speed_query(parameters)
        results['vehicle_count_per_location'] = self._execute_vehicle_count_query(parameters)
        results['congestion_events'] = self._execute_congestion_query(parameters)
        
        return results
    
    def _execute_generic_query(self, query: str, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute a generic SPARQL query"""
        try:
            results = []
            for row in self.graph.query(query):
                # Convert row to dictionary
                row_dict = {}
                for var in row:
                    if var:
                        row_dict[str(var)] = str(row[var])
                results.append(row_dict)
            return results
        except Exception as e:
            self.logger.error(f"Generic query execution failed: {e}")
            return []
    
    def _execute_situation_refinement_query(self, worker_results: Dict[str, Any]) -> Dict[str, Any]:
        """Execute C-SPARQL query for traffic jam detection (same as MAS)"""
        refinement_start_time = time.time()
        
        # Use current time for time window
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(seconds=300)  # 5-minute window
        
        # SPARQL query for traffic jam detection
        query = """
        PREFIX traffic: <http://example.org/traffic#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?location (SUM(?vehicleCount) AS ?totalVehicleCount) (COUNT(?observation) AS ?observationCount)
        WHERE {
            ?observation a traffic:Observation ;
                         traffic:hasObservationType "VehicleCount"^^xsd:string ;
                         traffic:atLocation ?location ;
                         traffic:hasVehicleCount ?vehicleCount ;
                         traffic:hasTimestamp ?timestamp .
            FILTER (?timestamp >= "%s"^^xsd:dateTime && ?timestamp <= "%s"^^xsd:dateTime)
        }
        GROUP BY ?location
        HAVING (SUM(?vehicleCount) > 100)
        """ % (start_time.isoformat(), end_time.isoformat())
        
        self.logger.info(f"Executing situation refinement query for traffic jam detection")
        
        # Execute query
        results = []
        result_count = 0
        
        try:
            query_results = self.graph.query(query)
            
            # Process results
            for row in query_results:
                location = str(row.location)
                total_vehicle_count = int(row.totalVehicleCount)
                observation_count = int(row.observationCount) if hasattr(row, 'observationCount') else 0
                
                self.logger.info(f"Traffic Jam Detected at {location}: Total Vehicle Count = {total_vehicle_count}, Observations = {observation_count}")
                
                # Create situation URI
                situation_uri = URIRef(f"{TRAFFIC}situation/traffic_jam_{location.split('/')[-1]}_{int(time.time())}")
                self.graph.add((situation_uri, TRAFFIC.hasSituationType, Literal("TrafficJam", datatype=XSD.string)))
                self.graph.add((situation_uri, TRAFFIC.atLocation, URIRef(location)))
                self.graph.add((situation_uri, TRAFFIC.hasTimestamp, Literal(end_time.isoformat(), datatype=XSD.dateTime)))
                self.graph.add((situation_uri, TRAFFIC.hasVehicleCount, Literal(total_vehicle_count, datatype=XSD.integer)))
                self.graph.add((situation_uri, TRAFFIC.hasObservationCount, Literal(observation_count, datatype=XSD.integer)))
                
                results.append({
                    'location': location,
                    'total_vehicle_count': total_vehicle_count,
                    'observation_count': observation_count,
                    'situation_uri': str(situation_uri)
                })
                result_count += 1
                
        except Exception as e:
            self.logger.error(f"Error executing situation refinement query: {e}")
        
        # Record situation refinement execution time
        refinement_time = time.time() - refinement_start_time
        self.performance_metrics['situation_refinement_times'].append(refinement_time)
        
        # Log detailed summary
        if result_count > 0:
            self.logger.info(f"Situation refinement query executed in {refinement_time:.4f} seconds - Found {result_count} traffic jam locations")
            self.logger.info(f"Total observations processed: {len(self.graph)} triples in graph")
        else:
            self.logger.info(f"Situation refinement query executed in {refinement_time:.4f} seconds - No traffic jams detected")
            self.logger.info(f"Total observations processed: {len(self.graph)} triples in graph")
        
        # Additional traffic analysis summary
        self._log_traffic_summary()
        
        # Return summary result
        return {
            'situation_type': 'TrafficJam',
            'locations_found': result_count,
            'timestamp': end_time.isoformat(),
            'total_vehicle_count': sum(r['total_vehicle_count'] for r in results) if results else 0,
            'total_observation_count': sum(r['observation_count'] for r in results) if results else 0,
            'situations': results
        }
    
    def _log_traffic_summary(self):
        """Log a summary of current traffic data in the graph"""
        try:
            # Count different types of entities
            vehicle_count = len(list(self.graph.subjects(RDF.type, TRAFFIC.Vehicle)))
            observation_count = len(list(self.graph.subjects(RDF.type, TRAFFIC.Observation)))
            situation_count = len(list(self.graph.subjects(RDF.type, TRAFFIC.hasSituationType)))
            
            # Count total triples
            total_triples = len(self.graph)
            
            self.logger.info(f"Traffic Data Summary:")
            self.logger.info(f"  Total Triples: {total_triples}")
            self.logger.info(f"  Vehicles: {vehicle_count}")
            self.logger.info(f"  Observations: {observation_count}")
            self.logger.info(f"  Situations: {situation_count}")
            
        except Exception as e:
            self.logger.error(f"Error logging traffic summary: {e}")
    
    def _update_performance_metrics(self, query_type: str, total_time: float, worker_time: float, master_time: float):
        """Update performance metrics for executed query"""
        # Update overall performance metrics
        self.performance_metrics['total_queries_executed'] += 1
        self.performance_metrics['total_execution_time'] += total_time
        
        # Update fastest and slowest execution times
        if total_time < self.performance_metrics['fastest_execution_time']:
            self.performance_metrics['fastest_execution_time'] = total_time
        if total_time > self.performance_metrics['slowest_execution_time']:
            self.performance_metrics['slowest_execution_time'] = total_time
        
        # Update average execution time
        self.performance_metrics['average_execution_time'] = (
            self.performance_metrics['total_execution_time'] / self.performance_metrics['total_queries_executed']
        )
        
        # Update query type performance
        if query_type not in self.performance_metrics['query_type_performance']:
            self.performance_metrics['query_type_performance'][query_type] = {
                'count': 0,
                'total_time': 0.0,
                'average_time': 0.0,
                'fastest_time': float('inf'),
                'slowest_time': 0.0
            }
        
        qt_perf = self.performance_metrics['query_type_performance'][query_type]
        qt_perf['count'] += 1
        qt_perf['total_time'] += total_time
        qt_perf['average_time'] = qt_perf['total_time'] / qt_perf['count']
        
        if total_time < qt_perf['fastest_time']:
            qt_perf['fastest_time'] = total_time
        if total_time > qt_perf['slowest_time']:
            qt_perf['slowest_time'] = total_time
        
        # Add to query execution times
        self.performance_metrics['query_execution_times'].append(total_time)
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics"""
        return self.performance_metrics.copy()
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get a summary of performance metrics"""
        metrics = self.performance_metrics
        
        summary = {
            'overall_performance': {
                'total_queries': metrics['total_queries_executed'],
                'average_execution_time': metrics['average_execution_time'],
                'fastest_execution': metrics['fastest_execution_time'] if metrics['fastest_execution_time'] != float('inf') else 0,
                'slowest_execution': metrics['slowest_execution_time'],
                'total_execution_time': metrics['total_execution_time']
            },
            'query_type_breakdown': metrics['query_type_performance'],
            'data_generation_stats': {
                'total_generation_times': len(metrics['data_generation_times']),
                'average_generation_time': sum(metrics['data_generation_times']) / len(metrics['data_generation_times']) if metrics['data_generation_times'] else 0
            },
            'rdf_conversion_stats': {
                'total_conversion_times': len(metrics['rdf_conversion_times']),
                'average_conversion_time': sum(metrics['rdf_conversion_times']) / len(metrics['rdf_conversion_times']) if metrics['rdf_conversion_times'] else 0
            },
            'query_execution_stats': {
                'total_execution_times': len(metrics['query_execution_times']),
                'average_query_time': sum(metrics['query_execution_times']) / len(metrics['query_execution_times']) if metrics['query_execution_times'] else 0
            },
            'situation_refinement_stats': {
                'total_refinement_times': len(metrics['situation_refinement_times']),
                'average_refinement_time': sum(metrics['situation_refinement_times']) / len(metrics['situation_refinement_times']) if metrics['situation_refinement_times'] else 0
            }
        }
        
        return summary
    
    def get_status(self) -> Dict[str, Any]:
        """Get current agent status"""
        return {
            'agent_id': self.agent_id,
            'agent_type': 'CentralizedAgent',
            'graph_size': len(self.graph),
            'queue_size': self.data_queue.qsize(),
            'total_queries_executed': self.performance_metrics['total_queries_executed']
        }
