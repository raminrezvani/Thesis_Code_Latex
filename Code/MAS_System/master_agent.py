import time
import json
import queue
import threading
import concurrent.futures
from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import XSD, RDF, RDFS
from base_agent import BaseAgent

# Define namespaces
TRAFFIC = Namespace("http://example.org/traffic#")
CITY = Namespace("http://example.org/cityOnto#")

class MasterAgent(BaseAgent):
    """
    Master agent that coordinates C-SPARQL query execution across worker agents (similar to FogAgent_SituationRefinement)
    """
    
    def __init__(self, agent_id: str):
        super().__init__(agent_id, "MasterAgent")
        self.worker_agents = []
        self.active_tasks = {}
        self.completed_tasks = {}
        self.query_templates = self._initialize_query_templates()
        self.shared_queue = queue.Queue()
        self.graph = Graph()
        
        # Performance metrics for query execution time
        self.performance_metrics = {
            'total_queries_executed': 0,
            'total_execution_time': 0.0,
            'average_execution_time': 0.0,
            'fastest_execution_time': float('inf'),
            'slowest_execution_time': 0.0,
            'query_type_performance': {},
            'worker_performance': {},
            'queue_processing_times': [],
            'situation_refinement_times': [],
            'distribution_times': [] # Added for overlapping execution
        }
        
        # Initialize graph with ontology
        self._initialize_ontology()
        
        # Register message handlers
        self.register_handler("task_acknowledged", self._handle_task_acknowledged)
        self.register_handler("task_completed", self._handle_task_completed)
        self.register_handler("register_worker", self._handle_register_worker)
        self.register_handler("process_queue", self._handle_process_queue)
    
    def _initialize_ontology(self):
        """Initialize the graph with traffic ontology (like FogAgent)"""
        self.graph.bind("traffic", TRAFFIC)
        self.graph.bind("city", CITY)
        
        # Define ontology classes and properties
        self.graph.add((TRAFFIC.Observation, RDF.type, RDFS.Class))
        self.graph.add((TRAFFIC.hasObservationType, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasVehicleCount, RDF.type, RDF.Property))
        self.graph.add((TRAFFIC.hasSituationType, RDF.type, RDF.Property))
    
    def _initialize_query_templates(self) -> Dict[str, Dict[str, Any]]:
        """Initialize predefined query templates"""
        return {
            "high_speed_vehicles": {
                "description": "Find vehicles exceeding speed threshold",
                "parameters": {
                    "speed_threshold": 80.0,
                    "window_size_seconds": 60
                },
                "worker_distribution": "parallel"  # Can be parallel or sequential
            },
            "vehicle_count_per_location": {
                "description": "Count vehicles per location",
                "parameters": {
                    "window_size_seconds": 300
                },
                "worker_distribution": "parallel"
            },
            "congestion_events": {
                "description": "Find congestion events",
                "parameters": {
                    "window_size_seconds": 120
                },
                "worker_distribution": "parallel"
            },
            "comprehensive_traffic_analysis": {
                "description": "Comprehensive traffic analysis combining multiple queries",
                "parameters": {
                    "speed_threshold": 80.0,
                    "window_size_seconds": 300
                },
                "worker_distribution": "parallel"
            }
        }
    
    def _run_logic(self):
        """Master agent specific logic with parallel and overlapping processing (like FogAgent)"""
        # Process incoming messages
        self._process_messages()
        
        # Process shared queue in parallel (don't wait for all data)
        self._process_shared_queue_parallel()
        
        # Check for completed tasks and merge results if needed
        self._check_and_merge_results()
        
        # Execute situation refinement if we have enough data
        self._check_and_execute_situation_refinement()
    
    def _process_messages(self):
        """Process incoming messages from message queue"""
        try:
            while not self.message_queue.empty():
                try:
                    sender_id, message = self.message_queue.get_nowait()
                    self._process_message(sender_id, message)
                except queue.Empty:
                    break
        except Exception as e:
            self.logger.error(f"Error processing messages: {e}")
    
    def _process_message(self, sender_id: str, message: Dict[str, Any]):
        """Process a single message"""
        try:
            message_type = message.get('type', 'unknown')
            
            if message_type in self.message_handlers:
                self.message_handlers[message_type](sender_id, message)
            else:
                self.logger.warning(f"No handler for message type: {message_type}")
                
        except Exception as e:
            self.logger.error(f"Error processing message {message_type}: {e}")
    
    def _process_shared_queue_parallel(self):
        """Process shared queue in parallel without waiting for all data"""
        try:
            # Process all available data immediately (non-blocking)
            processed_count = 0
            while not self.shared_queue.empty():
                try:
                    data = self.shared_queue.get_nowait()
                    processed_count += 1
                    
                    if isinstance(data, dict) and data.get('type') == 'data_ready':
                        # Handle data ready message immediately
                        self.logger.info(f"Received data ready message from {data.get('sender', 'unknown')}")
                        data_summary = data.get('data', {})
                        self.logger.info(f"Data summary: {data_summary}")
                        
                        # Start processing this data immediately in background
                        self._process_worker_data_parallel(data_summary)
                        
                    elif isinstance(data, list):
                        # Handle RDF triples immediately
                        self.logger.info(f"Received {len(data)} RDF triples from queue")
                        for triple in data:
                            if len(triple) == 3:
                                self.graph.add(triple)
                        
                        # Start situation refinement if we have enough data
                        if len(self.graph) > 1000:  # Threshold for starting situation refinement
                            self._start_situation_refinement_parallel()
                            
                    else:
                        # Handle other data types
                        self.logger.info(f"Received data from queue: {type(data)}")
                        
                except queue.Empty:
                    break
                    
            if processed_count > 0:
                self.logger.info(f"Processed {processed_count} data items from queue in parallel")
                
        except Exception as e:
            self.logger.error(f"Error processing queue in parallel: {e}")
    
    def _process_worker_data_parallel(self, data_summary: Dict[str, Any]):
        """Process worker data in parallel without waiting"""
        try:
            worker_id = data_summary.get('worker_id', 'unknown')
            graph_size = data_summary.get('graph_size', 0)
            triples_count = data_summary.get('triples_count', 0)
            
            self.logger.info(f"Processing data from {worker_id}: {triples_count} triples")
            
            # Start background processing for this worker's data
            processing_thread = threading.Thread(
                target=self._process_worker_data_background,
                args=(worker_id, data_summary)
            )
            processing_thread.daemon = True
            processing_thread.start()
            
            # Don't wait for completion - continue processing other data
            
        except Exception as e:
            self.logger.error(f"Error processing worker data in parallel: {e}")
    
    def _process_worker_data_background(self, worker_id: str, data_summary: Dict[str, Any]):
        """Process worker data in background thread"""
        try:
            # Start timing
            start_time = time.time()
            self.logger.info(f"Background processing started for {worker_id}")
            
            # Simulate some processing time
            time.sleep(0.1)
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Update worker performance metrics
            if worker_id not in self.performance_metrics['worker_performance']:
                self.performance_metrics['worker_performance'][worker_id] = {
                    'data_processed': 0,
                    'triples_received': 0,
                    'processing_times': [],
                    'total_processing_time': 0.0,
                    'average_processing_time': 0.0
                }
            
            worker_perf = self.performance_metrics['worker_performance'][worker_id]
            worker_perf['data_processed'] += 1
            worker_perf['triples_received'] += data_summary.get('triples_count', 0)
            worker_perf['processing_times'].append(processing_time)
            worker_perf['total_processing_time'] += processing_time
            worker_perf['average_processing_time'] = worker_perf['total_processing_time'] / worker_perf['data_processed']
            
            self.logger.info(f"Background processing completed for {worker_id} in {processing_time:.4f} seconds")
            self.logger.info(f"  Triples processed: {data_summary.get('triples_count', 0)}")
            self.logger.info(f"  Total processing time: {worker_perf['total_processing_time']:.4f}s")
            self.logger.info(f"  Average processing time: {worker_perf['average_processing_time']:.4f}s")
            
        except Exception as e:
            self.logger.error(f"Error in background data processing for {worker_id}: {e}")
    
    def _start_situation_refinement_parallel(self):
        """Start situation refinement in parallel if not already running"""
        try:
            # Check if situation refinement is already running
            if hasattr(self, '_situation_refinement_running') and self._situation_refinement_running:
                return
            
            # Check if we have enough data to start situation refinement
            if len(self.graph) < 1000:
                return
            
            self.logger.info(f"Starting situation refinement in parallel with {len(self.graph)} triples")
            
            # Mark as running
            self._situation_refinement_running = True
            
            # Start situation refinement in background thread
            refinement_thread = threading.Thread(
                target=self._execute_situation_refinement_parallel
            )
            refinement_thread.daemon = True
            refinement_thread.start()
            
        except Exception as e:
            self.logger.error(f"Error starting situation refinement in parallel: {e}")
            self._situation_refinement_running = False
    
    def _execute_situation_refinement_parallel(self):
        """Execute situation refinement in parallel thread"""
        try:
            self.logger.info("Executing situation refinement in parallel thread")
            
            # Start timing
            start_time = time.time()
            
            # Execute situation refinement query
            result = self._execute_situation_refinement_query()
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Update performance metrics
            self.performance_metrics['situation_refinement_times'].append(execution_time)
            
            self.logger.info(f"Parallel situation refinement completed in {execution_time:.4f} seconds")
            
            # Mark as not running
            self._situation_refinement_running = False
            
        except Exception as e:
            self.logger.error(f"Error in parallel situation refinement: {e}")
            self._situation_refinement_running = False
    
    def _check_and_execute_situation_refinement(self):
        """Check if we should execute situation refinement based on available data"""
        try:
            # Only start if we have enough data and not already running
            if (len(self.graph) > 1000 and 
                (not hasattr(self, '_situation_refinement_running') or not self._situation_refinement_running)):
                
                self._start_situation_refinement_parallel()
                
        except Exception as e:
            self.logger.error(f"Error checking situation refinement: {e}")
    
    def _handle_process_queue(self, sender_id: str, message: Dict[str, Any]):
        """Handle process queue message"""
        try:
            self.logger.info(f"Processing shared queue as requested by {sender_id}")
            self._process_shared_queue()
        except Exception as e:
            self.logger.error(f"Error processing queue: {e}")
    
    def _process_shared_queue(self):
        """Process shared queue for data from worker agents"""
        try:
            if not self.shared_queue.empty():
                # Get data from queue
                data = self.shared_queue.get_nowait()
                
                if isinstance(data, dict) and data.get('type') == 'data_ready':
                    # Handle data ready message
                    self.logger.info(f"Received data ready message from {data.get('sender', 'unknown')}")
                    self.logger.info(f"Data summary: {data.get('data', {})}")
                elif isinstance(data, list):
                    # Handle RDF triples
                    self.logger.info(f"Received {len(data)} RDF triples from queue")
                    for triple in data:
                        if len(triple) == 3:
                            self.graph.add(triple)
                else:
                    # Handle other data types
                    self.logger.info(f"Received data from queue: {type(data)}")
                    
        except queue.Empty:
            pass  # No data in queue
        except Exception as e:
            self.logger.error(f"Error processing queue: {e}")
    
    def _execute_situation_refinement_query(self, worker_results: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute C-SPARQL query for traffic jam detection (like FogAgent)"""
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
    
    def register_worker_agent(self, worker_agent):
        """Register a worker agent with the master"""
        if worker_agent not in self.worker_agents:
            self.worker_agents.append(worker_agent)
            self.logger.info(f"Registered worker agent: {worker_agent.agent_id}")
            
            # Send registration confirmation
            self.send_message(worker_agent, {
                'type': 'worker_registered',
                'sender': self.agent_id,
                'timestamp': time.time(),
                'data': f"Worker {worker_agent.agent_id} successfully registered"
            })
    
    def _handle_register_worker(self, sender_id: str, message: Dict[str, Any]):
        """Handle worker registration request"""
        # This would typically be called when a worker agent starts up
        self.logger.info(f"Worker registration request from {sender_id}")
    
    def execute_csparql_query(self, query_type: str, custom_parameters: Dict[str, Any] = None) -> str:
        """Execute a C-SPARQL query by breaking it down and distributing to workers"""
        if query_type not in self.query_templates:
            raise ValueError(f"Unknown query type: {query_type}")
        
        # Start timing the query execution
        query_start_time = time.time()
        
        # Generate unique task ID
        task_id = f"master_task_{int(time.time())}"
        
        # Get query template
        template = self.query_templates[query_type]
        parameters = template["parameters"].copy()
        if custom_parameters:
            parameters.update(custom_parameters)
        
        # Break down query into sub-queries
        sub_queries = self._break_down_query(query_type, parameters)
        
        # Create master task with timing information
        self.active_tasks[task_id] = {
            'task_id': task_id,
            'query_type': query_type,
            'parameters': parameters,
            'sub_tasks': sub_queries,
            'worker_assignments': {},
            'results': {},
            'status': 'distributing',
            'created_time': query_start_time,
            'start_time': query_start_time,
            'distribution_start_time': None,
            'distribution_end_time': None,
            'execution_start_time': None,
            'execution_end_time': None,
            'merging_start_time': None,
            'merging_end_time': None,
            'completed': False
        }
        
        self.logger.info(f"Created master task {task_id} with {len(sub_queries)} sub-queries")
        
        # Distribute sub-queries to workers
        self._distribute_sub_queries(task_id, query_type, parameters)
        
        return task_id
    
    def _break_down_query(self, query_type: str, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Break down a C-SPARQL query into independent sub-queries"""
        sub_queries = []
        
        if query_type == "comprehensive_traffic_analysis":
            # Break down into multiple independent queries
            sub_queries = [
                {
                    'query_id': f"sub_{query_type}_1",
                    'query_type': 'high_speed_vehicles',
                    'query': 'high_speed_vehicles',
                    'parameters': {
                        'speed_threshold': parameters.get('speed_threshold', 80.0),
                        'window_size_seconds': parameters.get('window_size_seconds', 300)
                    },
                    'description': 'High speed vehicles analysis'
                },
                {
                    'query_id': f"sub_{query_type}_2",
                    'query_type': 'vehicle_count_per_location',
                    'query': 'vehicle_count_per_location',
                    'parameters': {
                        'window_size_seconds': parameters.get('window_size_seconds', 300)
                    },
                    'description': 'Vehicle count per location analysis'
                },
                {
                    'query_id': f"sub_{query_type}_3",
                    'query_type': 'congestion_events',
                    'query': 'congestion_events',
                    'parameters': {
                        'window_size_seconds': parameters.get('window_size_seconds', 300)
                    },
                    'description': 'Congestion events analysis'
                }
            ]
        else:
            # Single query type
            sub_queries = [{
                'query_id': f"sub_{query_type}_1",
                'query_type': query_type,
                'query': query_type,
                'parameters': parameters,
                'description': self.query_templates[query_type]['description']
            }]
        
        return sub_queries
    
    def _distribute_sub_queries(self, task_id: str, query_type: str, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Distribute sub-queries to workers with overlapping execution"""
        try:
            self.logger.info(f"Distributing sub-queries for {query_type} with overlapping execution")
            
            # Create overlapping execution plan
            execution_plan = self._create_overlapping_execution_plan(query_type, parameters)
            
            # Start timing for distribution
            distribution_start_time = time.time()
            
            # Execute sub-queries in parallel with overlapping
            sub_queries = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.worker_agents)) as executor:
                # Submit all sub-queries for parallel execution
                future_to_query = {}
                
                for phase, queries in execution_plan.items():
                    for query_info in queries:
                        # Submit query to worker
                        future = executor.submit(self._execute_sub_query_with_overlap, query_info, task_id)
                        future_to_query[future] = query_info
                        sub_queries.append(query_info)
                
                # Process results as they complete (overlapping processing)
                completed_results = []
                for future in concurrent.futures.as_completed(future_to_query):
                    query_info = future_to_query[future]
                    try:
                        result = future.result()
                        completed_results.append(result)
                        
                        # Process result immediately for overlapping
                        self._process_worker_result_immediately(result, task_id)
                        
                    except Exception as e:
                        self.logger.error(f"Sub-query {query_info['query_id']} failed: {e}")
                        completed_results.append({
                            'query_id': query_info['query_id'],
                            'status': 'failed',
                            'error': str(e)
                        })
            
            # Calculate distribution time
            distribution_time = time.time() - distribution_start_time
            self.logger.info(f"All sub-queries for task {task_id} distributed to workers in {distribution_time:.4f} seconds")
            self.logger.info(f"Overlapping execution plan: {execution_plan}")
            
            # Update performance metrics
            self.performance_metrics['distribution_times'].append(distribution_time)
            
            return sub_queries
            
        except Exception as e:
            self.logger.error(f"Error distributing sub-queries: {e}")
            return []
    
    def _create_overlapping_execution_plan(self, query_type: str, parameters: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Create execution plan with overlapping operations"""
        execution_plan = {}
        
        if query_type == "comprehensive_traffic_analysis":
            # Phase 1: Start high-speed and vehicle count queries simultaneously
            execution_plan['phase_1'] = [
                {
                    'query_id': 'sub_high_speed_1',
                    'query_type': 'high_speed_vehicles',
                    'query': 'high_speed_vehicles',
                    'parameters': parameters,
                    'description': 'Find vehicles exceeding speed threshold',
                    'sub_task_id': f'master_task_{int(time.time())}_sub_high_speed_1',
                    'assigned_worker': 'worker_001',
                    'time_window': 5  # 5-second window
                },
                {
                    'query_id': 'sub_vehicle_count_1',
                    'query_type': 'vehicle_count_per_location',
                    'query': 'vehicle_count_per_location',
                    'parameters': parameters,
                    'description': 'Count vehicles per location',
                    'sub_task_id': f'master_task_{int(time.time())}_sub_vehicle_count_1',
                    'assigned_worker': 'worker_002',
                    'time_window': 5  # 5-second window
                }
            ]
            
            # Phase 2: Start congestion query while previous queries are running
            execution_plan['phase_2'] = [
                {
                    'query_id': 'sub_congestion_1',
                    'query_type': 'congestion_events',
                    'query': 'congestion_events',
                    'parameters': parameters,
                    'description': 'Find congestion events',
                    'sub_task_id': f'master_task_{int(time.time())}_sub_congestion_1',
                    'assigned_worker': 'worker_001',
                    'time_window': 10  # 10-second window (overlaps with phase 1)
                }
            ]
            
        else:
            # For single queries, create single phase with overlapping
            execution_plan['phase_1'] = [
                {
                    'query_id': f'sub_{query_type}_1',
                    'query_type': query_type,
                    'query': query_type,
                    'parameters': parameters,
                    'description': f'Execute {query_type} query',
                    'sub_task_id': f'master_task_{int(time.time())}_sub_{query_type}_1',
                    'assigned_worker': 'worker_001',
                    'time_window': 5  # 5-second window
                }
            ]
        
        return execution_plan
    
    def _execute_sub_query_with_overlap(self, query_info: Dict[str, Any], task_id: str) -> Dict[str, Any]:
        """Execute a sub-query with overlapping processing"""
        try:
            worker_id = query_info['assigned_worker']
            worker = self._find_worker_by_id(worker_id)
            
            if not worker:
                raise Exception(f"Worker {worker_id} not found")
            
            # Send query to worker
            query_message = {
                'type': 'execute_query',
                'task_id': query_info['sub_task_id'],
                'sender': self.agent_id,
                'timestamp': time.time(),
                'data': {
                    'query': query_info['query'],
                    'parameters': query_info['parameters']
                }
            }
            
            worker.receive_message(self.agent_id, query_message)
            
            # Wait for result with timeout
            start_time = time.time()
            timeout = query_info.get('time_window', 5) + 2  # Add buffer time
            
            while time.time() - start_time < timeout:
                # Check if result is ready
                if hasattr(worker, 'last_query_result') and worker.last_query_result:
                    result = worker.last_query_result
                    worker.last_query_result = None  # Clear for next query
                    return {
                        'query_id': query_info['query_id'],
                        'status': 'completed',
                        'result': result,
                        'worker_id': worker_id,
                        'execution_time': time.time() - start_time
                    }
                
                time.sleep(0.1)  # Small delay
            
            # Timeout
            return {
                'query_id': query_info['query_id'],
                'status': 'timeout',
                'worker_id': worker_id,
                'execution_time': timeout
            }
            
        except Exception as e:
            self.logger.error(f"Error executing sub-query {query_info['query_id']}: {e}")
            return {
                'query_id': query_info['query_id'],
                'status': 'failed',
                'error': str(e),
                'worker_id': query_info.get('assigned_worker', 'unknown')
            }
    
    def _process_worker_result_immediately(self, result: Dict[str, Any], task_id: str):
        """Process worker result immediately for overlapping operations"""
        try:
            if result['status'] == 'completed':
                self.logger.info(f"Processing result from {result['worker_id']} immediately for overlapping")
                
                # Add result to task results
                if task_id not in self.active_tasks:
                    self.active_tasks[task_id] = {
                        'status': 'in_progress',
                        'sub_queries': {},
                        'start_time': time.time()
                    }
                
                self.active_tasks[task_id]['sub_queries'][result['query_id']] = result
                
                # Check if we can start situation refinement with partial results
                self._check_and_start_situation_refinement(task_id)
                
        except Exception as e:
            self.logger.error(f"Error processing worker result immediately: {e}")
    
    def _check_and_start_situation_refinement(self, task_id: str):
        """Check if we can start situation refinement with partial results"""
        try:
            if task_id not in self.active_tasks:
                return
            
            task = self.active_tasks[task_id]
            completed_queries = [q for q in task['sub_queries'].values() if q['status'] == 'completed']
            
            # Start situation refinement if we have at least 2 completed queries
            if len(completed_queries) >= 2:
                self.logger.info(f"Starting situation refinement with {len(completed_queries)} completed queries")
                
                # Start situation refinement in background thread
                refinement_thread = threading.Thread(
                    target=self._execute_situation_refinement_with_overlap,
                    args=(task_id, completed_queries)
                )
                refinement_thread.daemon = True
                refinement_thread.start()
                
        except Exception as e:
            self.logger.error(f"Error checking situation refinement: {e}")
    
    def _execute_situation_refinement_with_overlap(self, task_id: str, completed_queries: List[Dict[str, Any]]):
        """Execute situation refinement with overlapping processing"""
        try:
            self.logger.info(f"Executing situation refinement with overlapping for task {task_id}")
            
            # Start timing
            start_time = time.time()
            
            # Execute situation refinement query
            result = self._execute_situation_refinement_query(completed_queries)
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Update task status
            if task_id in self.active_tasks:
                self.active_tasks[task_id]['situation_refinement'] = {
                    'status': 'completed',
                    'result': result,
                    'execution_time': execution_time
                }
                
                # Check if task is complete
                self._finalize_task_with_overlap(task_id)
            
            # Update performance metrics
            self.performance_metrics['situation_refinement_times'].append(execution_time)
            
        except Exception as e:
            self.logger.error(f"Error in situation refinement with overlap: {e}")
    
    def _finalize_task_with_overlap(self, task_id: str):
        """Finalize task with overlapping processing results"""
        try:
            if task_id not in self.active_tasks:
                return
            
            task = self.active_tasks[task_id]
            
            # Check if all components are complete
            if (len(task['sub_queries']) >= 2 and 
                'situation_refinement' in task and 
                task['situation_refinement']['status'] == 'completed'):
                
                self.logger.info(f"Finalizing task {task_id} with overlapping results")
                
                # Merge results
                merged_result = self._merge_results_with_overlap(task)
                
                # Mark task as completed
                task['status'] = 'completed'
                task['merged_result'] = merged_result
                task['completion_time'] = time.time()
                
                # Move to completed tasks
                self.completed_tasks[task_id] = task
                del self.active_tasks[task_id]
                
                # Update performance metrics
                total_time = task['completion_time'] - task['start_time']
                self._calculate_task_performance(task_id, total_time, merged_result)
                
        except Exception as e:
            self.logger.error(f"Error finalizing task with overlap: {e}")
    
    def _merge_results_with_overlap(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Merge results from overlapping operations"""
        try:
            merged_result = {
                'query_type': 'comprehensive_traffic_analysis',
                'execution_mode': 'parallel_with_overlap',
                'results': {},
                'situation_refinement': task.get('situation_refinement', {}),
                'execution_summary': {
                    'total_execution_time': task['completion_time'] - task['start_time'],
                    'overlapping_operations': True,
                    'parallel_processing': True
                }
            }
            
            # Add sub-query results
            for query_id, result in task['sub_queries'].items():
                if result['status'] == 'completed':
                    merged_result['results'][query_id] = {
                        'data': result.get('result', []),
                        'execution_time': result.get('execution_time', 0),
                        'worker_id': result.get('worker_id', 'unknown')
                    }
            
            return merged_result
            
        except Exception as e:
            self.logger.error(f"Error merging results with overlap: {e}")
            return {}
    
    def _find_worker_by_id(self, worker_id: str):
        """Find worker agent by ID"""
        for worker in self.worker_agents:
            if worker.agent_id == worker_id:
                return worker
        return None
    
    def _handle_task_acknowledged(self, sender_id: str, message: Dict[str, Any]):
        """Handle task acknowledgment from worker"""
        sub_task_id = message.get('task_id')
        
        # Find the master task and update status
        for task_id, task in self.active_tasks.items():
            if sub_task_id in task['worker_assignments']:
                task['worker_assignments'][sub_task_id]['status'] = 'acknowledged'
                task['worker_assignments'][sub_task_id]['acknowledged_time'] = time.time()
                self.logger.info(f"Sub-task {sub_task_id} acknowledged by worker {sender_id}")
                break
    
    def _handle_task_completed(self, sender_id: str, message: Dict[str, Any]):
        """Handle task completion from worker"""
        sub_task_id = message.get('task_id')
        result_data = message.get('data', {})
        
        # Find the master task and store result
        for task_id, task in self.active_tasks.items():
            if sub_task_id in task['worker_assignments']:
                # Update worker assignment status
                task['worker_assignments'][sub_task_id]['status'] = 'completed'
                task['worker_assignments'][sub_task_id]['completed_time'] = time.time()
                
                # Store result
                task['results'][sub_task_id] = result_data
                
                self.logger.info(f"Sub-task {sub_task_id} completed by worker {sender_id}")
                
                # Check if all sub-tasks are completed
                if self._are_all_sub_tasks_completed(task_id):
                    self._finalize_task(task_id)
                break
    
    def _are_all_sub_tasks_completed(self, task_id: str) -> bool:
        """Check if all sub-tasks for a master task are completed"""
        task = self.active_tasks[task_id]
        for assignment in task['worker_assignments'].values():
            if assignment['status'] != 'completed':
                return False
        return True
    
    def _finalize_task(self, task_id: str):
        """Finalize a master task by merging all sub-task results"""
        task = self.active_tasks[task_id]
        task['status'] = 'merging'
        
        # Record merging start time
        task['merging_start_time'] = time.time()
        
        # Merge results from all sub-task results
        merged_result = self._merge_sub_task_results(task)
        
        # Record merging end time and completion time
        task['merging_end_time'] = time.time()
        task['execution_end_time'] = time.time()
        task['status'] = 'completed'
        task['completed'] = True
        task['completion_time'] = time.time()
        task['merged_result'] = merged_result
        
        # Calculate performance metrics
        self._calculate_task_performance(task_id, task)
        
        # Move to completed tasks
        self.completed_tasks[task_id] = task.copy()
        del self.active_tasks[task_id]
        
        self.logger.info(f"Master task {task_id} completed and results merged")
    
    def _merge_sub_task_results(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Merge results from multiple sub-tasks"""
        merged_result = {
            'query_type': task['query_type'],
            'parameters': task['parameters'],
            'execution_summary': {
                'total_sub_tasks': len(task['worker_assignments']),
                'total_execution_time': 0,
                'worker_performance': {}
            },
            'results': {}
        }
        
        # Aggregate results and performance metrics
        for sub_task_id, assignment in task['worker_assignments'].items():
            sub_query = assignment['sub_query']
            result = task['results'].get(sub_task_id, {})
            
            # Add sub-query result
            merged_result['results'][sub_query['query_type']] = {
                'description': sub_query['description'],
                'data': result.get('result', []),
                'execution_time': result.get('execution_time', 0)
            }
            
            # Update execution summary
            merged_result['execution_summary']['total_execution_time'] += result.get('execution_time', 0)
            merged_result['execution_summary']['worker_performance'][assignment['worker_id']] = {
                'sub_task_id': sub_task_id,
                'execution_time': result.get('execution_time', 0),
                'status': assignment['status']
            }
        
        return merged_result
    
    def _check_and_merge_results(self):
        """Check for completed tasks and merge results if needed"""
        # This is called periodically in the main loop
        pass
    
    def _cleanup_old_tasks(self):
        """Clean up old completed tasks"""
        current_time = time.time()
        tasks_to_remove = []
        
        for task_id, task in self.completed_tasks.items():
            # Remove tasks older than 1 hour
            if current_time - task.get('completion_time', 0) > 3600:
                tasks_to_remove.append(task_id)
        
        for task_id in tasks_to_remove:
            del self.completed_tasks[task_id]
        
        if tasks_to_remove:
            self.logger.info(f"Cleaned up {len(tasks_to_remove)} old completed tasks")
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Get status of a specific task"""
        if task_id in self.active_tasks:
            return self.active_tasks[task_id]
        elif task_id in self.completed_tasks:
            return self.completed_tasks[task_id]
        else:
            return {'error': 'Task not found'}
    
    def get_all_tasks_status(self) -> Dict[str, Any]:
        """Get status of all tasks"""
        return {
            'active_tasks': len(self.active_tasks),
            'completed_tasks': len(self.completed_tasks),
            'registered_workers': len(self.worker_agents),
            'active_tasks_details': {tid: task['status'] for tid, task in self.active_tasks.items()}
        }
    
    def get_worker_agents(self) -> List[str]:
        """Get list of registered worker agent IDs"""
        return [worker.agent_id for worker in self.worker_agents]
    
    def _calculate_task_performance(self, task_id: str, total_time: float, merged_result: Dict[str, Any]):
        """Calculate and update performance metrics for a completed task"""
        try:
            # Calculate various timing metrics
            distribution_time = self.performance_metrics['distribution_times'][-1] if self.performance_metrics['distribution_times'] else 0
            execution_time = total_time - distribution_time
            merging_time = self.performance_metrics['situation_refinement_times'][-1] if self.performance_metrics['situation_refinement_times'] else 0
            
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
            query_type = merged_result.get('query_type', 'unknown')
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
            
            # Update worker performance
            for sub_task_id, assignment in self.active_tasks[task_id]['worker_assignments'].items():
                worker_id = assignment['worker_id']
                if worker_id not in self.performance_metrics['worker_performance']:
                    self.performance_metrics['worker_performance'][worker_id] = {
                        'tasks_completed': 0,
                        'total_execution_time': 0.0,
                        'average_execution_time': 0.0
                    }
                
                worker_perf = self.performance_metrics['worker_performance'][worker_id]
                worker_perf['tasks_completed'] += 1
                
                # Calculate worker execution time if available
                if 'completed_time' in assignment and 'sent_time' in assignment:
                    worker_exec_time = assignment['completed_time'] - assignment['sent_time']
                    worker_perf['total_execution_time'] += worker_exec_time
                    worker_perf['average_execution_time'] = worker_perf['total_execution_time'] / worker_perf['tasks_completed']
            
            # Log performance summary
            self.logger.info(f"Task {task_id} Performance Summary:")
            self.logger.info(f"  Total Time: {total_time:.4f}s")
            self.logger.info(f"  Distribution Time: {distribution_time:.4f}s")
            self.logger.info(f"  Execution Time: {execution_time:.4f}s")
            self.logger.info(f"  Merging Time: {merging_time:.4f}s")
            self.logger.info(f"  Query Type: {query_type}")
            
        except Exception as e:
            self.logger.error(f"Error calculating performance metrics for task {task_id}: {e}")
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics"""
        return self.performance_metrics.copy()
    
    def get_query_performance_summary(self) -> Dict[str, Any]:
        """Get a summary of query performance metrics"""
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
            'worker_performance': metrics['worker_performance'],
            'queue_processing_stats': {
                'total_processing_times': len(metrics['queue_processing_times']),
                'average_queue_time': sum(metrics['queue_processing_times']) / len(metrics['queue_processing_times']) if metrics['queue_processing_times'] else 0
            },
            'situation_refinement_stats': {
                'total_refinement_times': len(metrics['situation_refinement_times']),
                'average_refinement_time': sum(metrics['situation_refinement_times']) / len(metrics['situation_refinement_times']) if metrics['situation_refinement_times'] else 0
            }
        }
        
        return summary
