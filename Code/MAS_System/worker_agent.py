import random
import datetime
import queue
import time
import threading
from typing import Dict, Any, List
from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD, RDFS
from base_agent import BaseAgent

# Define namespaces for the traffic ontology
TRAFFIC = Namespace("http://example.org/traffic#")
CITY = Namespace("http://example.org/cityOnto#")

class WorkerAgent(BaseAgent):
    """
    Worker agent that executes C-SPARQL sub-queries (similar to EdgeAgent_ObjectRefinement)
    """
    
    def __init__(self, agent_id: str, shared_queue=None):
        super().__init__(agent_id, "WorkerAgent")
        self.shared_queue = shared_queue
        self.data_queue = queue.Queue()  # Add data_queue for local data processing
        self.graph = Graph()
        self.current_task = None
        self.task_results = {}
        
        # Performance metrics
        self.performance_metrics = {
            'total_queries_executed': 0,
            'total_execution_time': 0.0,
            'average_execution_time': 0.0,
            'fastest_execution_time': float('inf'),
            'slowest_execution_time': 0.0,
            'query_type_performance': {}
        }
        
        # Initialize graph with ontology
        self._initialize_ontology()
        
        # Initialize query templates
        self.query_templates = self._initialize_query_templates()
        
        # Register message handlers
        self.register_handler("execute_query", self._handle_execute_query)
        self.register_handler("generate_data", self._handle_generate_data)
        self.register_handler("get_status", self._handle_get_status)
    
    def _initialize_query_templates(self) -> Dict[str, Dict[str, Any]]:
        """Initialize predefined query templates"""
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
    
    def _run_logic(self):
        """Worker agent specific logic with overlapping operations (like EdgeAgent)"""
        # Process incoming messages
        self._process_messages()
        
        # Generate traffic data continuously with overlapping windows
        if not hasattr(self, '_last_data_generation') or time.time() - self._last_data_generation > 5:
            self._generate_traffic_data_periodic()
            self._last_data_generation = time.time()
        
        # Process shared queue periodically
        self._process_shared_queue()
        
        # Process current task if available
        if self.current_task:
            self._process_current_task()
    
    def _generate_traffic_data_periodic(self):
        """Generate traffic data periodically with overlapping time windows"""
        try:
            self.logger.info("Periodic traffic data generation with overlapping windows...")
            
            # Generate data for current time window
            self.generate_traffic_data()
            
            # Start background thread for next window to create overlap
            if not hasattr(self, '_background_data_thread') or not self._background_data_thread.is_alive():
                self._background_data_thread = threading.Thread(target=self._background_data_generation)
                self._background_data_thread.daemon = True
                self._background_data_thread.start()
                
        except Exception as e:
            self.logger.error(f"Error in periodic data generation: {e}")
    
    def _background_data_generation(self):
        """Background thread for continuous data generation with overlapping windows"""
        try:
            while self.is_running:
                time.sleep(5)  # Wait for next time window
                if self.is_running:
                    self.logger.info("Background data generation for overlapping window...")
                    self.generate_traffic_data()
                    self.queue_to_rdf()
                    
                    # Send data to master agent immediately for overlapping processing
                    if hasattr(self, 'master_agent') and self.master_agent:
                        self._send_data_to_master()
                        
        except Exception as e:
            self.logger.error(f"Error in background data generation: {e}")
    
    def _send_data_to_master(self):
        """Send generated data to master agent for overlapping processing"""
        try:
            if hasattr(self, 'shared_queue') and self.shared_queue:
                # Send data summary to master
                data_summary = {
                    'type': 'data_ready',
                    'sender': self.agent_id,
                    'timestamp': time.time(),
                    'data': {
                        'graph_size': len(self.graph),
                        'triples_count': len(self.graph),
                        'worker_id': self.agent_id
                    }
                }
                self.shared_queue.put(data_summary)
                self.logger.info(f"Sent data summary to master for overlapping processing")
                
        except Exception as e:
            self.logger.error(f"Error sending data to master: {e}")
    
    def execute_query_parallel(self, query_type: str, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute query with parallel processing and overlapping operations"""
        if query_type not in self.query_templates:
            raise ValueError(f"Unknown query type: {query_type}")
        
        # Start timing
        query_start_time = time.time()
        
        # Get parameters
        if parameters is None:
            parameters = self.query_templates[query_type]["parameters"]
        
        self.logger.info(f"Executing {query_type} query with parallel processing and overlapping")
        self.logger.info(f"Parameters: {parameters}")
        
        # Execute query based on type with parallel processing
        if query_type == "comprehensive_traffic_analysis":
            result = self._execute_comprehensive_analysis_parallel(parameters)
        elif query_type == "high_speed_vehicles":
            result = self._execute_high_speed_query_parallel(parameters)
        elif query_type == "vehicle_count_per_location":
            result = self._execute_vehicle_count_query_parallel(parameters)
        elif query_type == "congestion_events":
            result = self._execute_congestion_query_parallel(parameters)
        else:
            result = self._execute_generic_query(query_type, parameters)
        
        # Calculate execution time
        execution_time = time.time() - query_start_time
        
        # Send result to master immediately for overlapping processing
        self._send_result_to_master(query_type, result, execution_time)
        
        return {
            'query_type': query_type,
            'parameters': parameters,
            'result': result,
            'execution_time': execution_time,
            'worker_id': self.agent_id,
            'execution_mode': 'parallel_with_overlap'
        }
    
    def _execute_comprehensive_analysis_parallel(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Execute comprehensive analysis with parallel processing"""
        import threading
        import concurrent.futures
        
        self.logger.info("Executing comprehensive analysis with parallel processing...")
        
        # Create thread pool for parallel execution
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Submit all queries for parallel execution
            future_high_speed = executor.submit(self._execute_high_speed_query_parallel, parameters)
            future_vehicle_count = executor.submit(self._execute_vehicle_count_query_parallel, parameters)
            future_congestion = executor.submit(self._execute_congestion_query_parallel, parameters)
            
            # Collect results as they complete
            results = {}
            results['high_speed_vehicles'] = future_high_speed.result()
            results['vehicle_count_per_location'] = future_vehicle_count.result()
            results['congestion_events'] = future_congestion.result()
        
        self.logger.info("Comprehensive analysis completed with parallel processing")
        return results
    
    def _execute_high_speed_query_parallel(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute high speed vehicles query with parallel processing"""
        speed_threshold = parameters.get('speed_threshold', 80.0)
        window_size = parameters.get('window_size_seconds', 60)
        
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(seconds=window_size)
        
        # Use parallel processing for large datasets
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
        
        # Execute query with parallel result processing
        results = []
        query_results = self.graph.query(query)
        
        # Process results in parallel if dataset is large
        if len(query_results) > 1000:
            import concurrent.futures
            
            def process_row(row):
                return {
                    'vehicle': str(row.vehicle),
                    'speed': float(row.speed),
                    'location': str(row.location)
                }
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(process_row, row) for row in query_results]
                results = [future.result() for future in concurrent.futures.as_completed(futures)]
        else:
            # Sequential processing for small datasets
            for row in query_results:
                results.append({
                    'vehicle': str(row.vehicle),
                    'speed': float(row.speed),
                    'location': str(row.location)
                })
        
        return results
    
    def _execute_vehicle_count_query_parallel(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute vehicle count per location query with parallel processing"""
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
    
    def _execute_congestion_query_parallel(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute congestion events query with parallel processing"""
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
    
    def _handle_generate_data(self, sender_id: str, message: Dict[str, Any]):
        """Handle data generation request"""
        self.logger.info(f"Received data generation request from {sender_id}")
        self.generate_traffic_data()
        self.queue_to_rdf()
    
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
        
        # Calculate exact records to get exactly 30k triples per worker (60k total for 2 workers)
        # Each record generates exactly 12 triples:
        # 1. vehicle type, 2. vehicle speed, 3. vehicle location, 4. vehicle sensor, 5. vehicle timestamp, 6. vehicle event
        # 7. sensor type, 8. location type, 9. location lat, 10. location lon, 11. event type, 12. event occursAt
        # So we need 30000/12 = 2500 records per worker
        TARGET_TRIPLES_PER_WORKER = 30000
        TARGET_RECORDS = TARGET_TRIPLES_PER_WORKER // 12  # Exactly 2500 records per worker
        
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
        self.logger.info(f"Expected triples: exactly {self.data_queue.qsize() * 12} (target: {TARGET_TRIPLES_PER_WORKER} per worker)")
        self.logger.info(f"Total MAS triples: {self.data_queue.qsize() * 12 * 2} (2 workers)")
        return self.data_queue.qsize()

    def queue_to_rdf(self):
        """Convert queued data to RDF triples (like EdgeAgent)"""
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

        self.logger.info(f"Converted {len(self.graph)} triples to RDF graph.")
    
    def _process_messages(self):
        """Process incoming messages"""
        try:
            while not self.message_queue.empty():
                message = self.message_queue.get()
                self._handle_message(message)
        except Exception as e:
            self.logger.error(f"Error processing messages: {e}")
    
    def _handle_message(self, message: Dict[str, Any]):
        """Handle incoming message"""
        try:
            message_type = message.get('type')
            sender_id = message.get('sender')
            
            if message_type == 'execute_query':
                self._handle_execute_query(sender_id, message)
            elif message_type == 'generate_data':
                self._handle_generate_data(sender_id, message)
            elif message_type == 'worker_registered':
                self._handle_worker_registered(sender_id, message)
            else:
                self.logger.warning(f"Unknown message type: {message_type}")
                
        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
    
    def _handle_execute_query(self, sender_id: str, message: Dict[str, Any]):
        """Handle execute query message"""
        try:
            task_id = message.get('task_id')
            query_data = message.get('data', {})
            query_type = query_data.get('query')
            parameters = query_data.get('parameters', {})
            
            self.logger.info(f"Executing query {query_type} with task ID {task_id}")
            
            # Execute query using parallel processing
            result = self.execute_query_parallel(query_type, parameters)
            
            # Send acknowledgment to master
            self.send_message_to_master({
                'type': 'task_acknowledged',
                'task_id': task_id,
                'sender': self.agent_id,
                'timestamp': time.time(),
                'data': 'Query acknowledged and started'
            })
            
            # Send completion result to master
            self.send_message_to_master({
                'type': 'task_completed',
                'task_id': task_id,
                'sender': self.agent_id,
                'timestamp': time.time(),
                'data': result
            })
            
            self.logger.info(f"Query {query_type} completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            # Send error response to master
            self.send_message_to_master({
                'type': 'task_error',
                'task_id': message.get('task_id'),
                'sender': self.agent_id,
                'timestamp': time.time(),
                'data': f'Error: {str(e)}'
            })
    
    def _handle_worker_registered(self, sender_id: str, message: Dict[str, Any]):
        """Handle worker registration confirmation"""
        self.logger.info(f"Worker registration confirmed by {sender_id}")
    
    def _process_shared_queue(self):
        """Process shared queue for data sharing"""
        try:
            # This method can be used for additional data processing if needed
            pass
        except Exception as e:
            self.logger.error(f"Error processing shared queue: {e}")
    
    def _handle_get_status(self, sender_id: str, message: Dict[str, Any]):
        """Handle status request"""
        status = self.get_status()
        status['current_task'] = self.current_task
        status['graph_size'] = len(self.graph) if self.graph else 0
        
        response = {
            'type': 'status_response',
            'sender': self.agent_id,
            'timestamp': time.time(),
            'data': status
        }
        
        # Send status response
        self.logger.info(f"Status requested by {sender_id}")
    
    def _process_current_task(self):
        """Process the current task"""
        if not self.current_task:
            return
        
        try:
            # Execute the C-SPARQL query
            query = self.current_task['query']
            parameters = self.current_task['parameters']
            
            self.logger.info(f"Executing query for task {self.current_task['task_id']}")
            
            # Execute query based on type
            if 'high_speed' in query.lower():
                result = self._execute_high_speed_query(parameters)
            elif 'vehicle_count' in query.lower():
                result = self._execute_vehicle_count_query(parameters)
            elif 'congestion' in query.lower():
                result = self._execute_congestion_query(parameters)
            else:
                result = self._execute_generic_query(query, parameters)
            
            # Mark task as completed
            self.current_task['completed'] = True
            self.current_task['result'] = result
            self.current_task['completion_time'] = time.time()
            
            # Store result
            self.task_results[self.current_task['task_id']] = self.current_task.copy()
            
            self.logger.info(f"Task {self.current_task['task_id']} completed successfully")
            
            # Send result to master agent
            result_message = {
                'type': 'task_completed',
                'task_id': self.current_task['task_id'],
                'sender': self.agent_id,
                'timestamp': time.time(),
                'data': {
                    'result': result,
                    'execution_time': self.current_task['completion_time'] - self.current_task['received_time']
                }
            }
            
            # This would send the result back to the master agent
            self.logger.info(f"Task result ready for {self.current_task['sender_id']}")
            
        except Exception as e:
            self.current_task['error'] = str(e)
            self.current_task['completed'] = True
            self.logger.error(f"Task {self.current_task['task_id']} failed: {e}")
    
    def _execute_high_speed_query(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute high speed vehicles query (like EdgeAgent)"""
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
        """Execute vehicle count per location query (like EdgeAgent)"""
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
        triples_batch = []
        
        for row in self.graph.query(query):
            results.append({
                'location': str(row.location),
                'vehicle_count': int(row.vehicleCount)
            })
            
            # Create RDF triples for master agent (like EdgeAgent)
            observation_uri = URIRef(f"{TRAFFIC}observation/vehicle_count_{str(row.location).split('/')[-1]}_{end_time.isoformat().replace(':', '-')}")
            triples = [
                (observation_uri, RDF.type, TRAFFIC.Observation),
                (observation_uri, TRAFFIC.hasObservationType, Literal("VehicleCount", datatype=XSD.string)),
                (observation_uri, TRAFFIC.atLocation, URIRef(row.location)),
                (observation_uri, TRAFFIC.hasVehicleCount, Literal(int(row.vehicleCount), datatype=XSD.integer)),
                (observation_uri, TRAFFIC.hasTimestamp, Literal(end_time.isoformat(), datatype=XSD.dateTime))
            ]
            triples_batch.extend(triples)
        
        # Send RDF triples to shared queue for master agent
        if triples_batch and hasattr(self, 'shared_queue'):
            self.logger.info(f"Sending {len(triples_batch)} RDF triples to shared queue")
            self.shared_queue.put(triples_batch)
        
        return results
    
    def _execute_congestion_query(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute congestion events query (like EdgeAgent)"""
        window_size = parameters.get('window_size_seconds', 120)
        
        end_time = datetime.datetime.now()
        start_time = end_time - datetime.timedelta(seconds=window_size)
        
        query = """
        PREFIX traffic: <http://example.org/traffic#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?vehicle ?location ?timestamp
        WHERE {
            ?vehicle a traffic:Agent ;
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
    
    def get_task_result(self, task_id: str) -> Dict[str, Any]:
        """Get result of a specific task"""
        return self.task_results.get(task_id, {})
    
    def clear_completed_tasks(self):
        """Clear completed tasks from memory"""
        completed_tasks = [tid for tid, task in self.task_results.items() if task.get('completed', False)]
        for tid in completed_tasks:
            del self.task_results[tid]
        self.logger.info(f"Cleared {len(completed_tasks)} completed tasks")

    def send_message(self, target_agent, message: Dict[str, Any]):
        """Send a message to another agent"""
        try:
            if hasattr(target_agent, 'receive_message'):
                target_agent.receive_message(self.agent_id, message)
                self.logger.info(f"Sent message to {target_agent.agent_id}: {message['type']}")
            else:
                # Try to find the agent by ID if target_agent is a string
                if isinstance(target_agent, str):
                    # This would need to be implemented with agent discovery
                    self.logger.warning(f"Cannot send message to {target_agent}: agent not found")
                else:
                    self.logger.error(f"Target agent {target_agent} has no receive_message method")
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")
    
    def send_message_to_master(self, message: Dict[str, Any]):
        """Send a message to the master agent"""
        try:
            # Find master agent through coordinator or shared queue
            if hasattr(self, 'master_agent') and self.master_agent:
                self.send_message(self.master_agent, message)
            else:
                # Store message for later delivery
                self.logger.info(f"Storing message for master agent: {message['type']}")
                if not hasattr(self, 'pending_messages'):
                    self.pending_messages = []
                self.pending_messages.append(message)
        except Exception as e:
            self.logger.error(f"Error sending message to master: {e}")

    def _send_result_to_master(self, query_type: str, result: Any, execution_time: float):
        """Send query result to master agent immediately for overlapping processing"""
        try:
            if hasattr(self, 'master_agent') and self.master_agent:
                result_message = {
                    'type': 'query_result_ready',
                    'sender': self.agent_id,
                    'timestamp': time.time(),
                    'data': {
                        'query_type': query_type,
                        'result': result,
                        'execution_time': execution_time,
                        'worker_id': self.agent_id
                    }
                }
                self.master_agent.receive_message(self.agent_id, result_message)
                self.logger.info(f"Sent {query_type} result to master for overlapping processing")
                
        except Exception as e:
            self.logger.error(f"Error sending result to master: {e}")
