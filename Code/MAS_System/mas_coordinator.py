import time
import threading
import queue
from typing import Dict, Any, List
from pathlib import Path
import logging

from master_agent import MasterAgent
from worker_agent import WorkerAgent

class MASCoordinator:
    """
    Coordinator for the Multi-Agent System
    Manages creation, registration, and coordination of all agents
    """
    
    def __init__(self, rdf_file_path: str = None):
        self.master_agent = None
        self.worker_agents = []
        self.is_running = False
        self.logger = logging.getLogger("MASCoordinator")
        
        # Number of worker agents
        self.num_workers = 2
        
        # Default RDF file path
        if rdf_file_path is None:
            rdf_file_path = Path("output") / "traffic_data_rdf.ttl"
        
        self.rdf_file_path = rdf_file_path
        
        # Agent configuration
        self.agent_config = {
            'master_id': 'master_001',
            'worker_ids': ['worker_001', 'worker_002'],
            'auto_start': True
        }
        
        # Shared queue for communication between agents
        self.shared_queue = queue.Queue()
    
    def initialize_system(self) -> bool:
        """Initialize the MAS system"""
        try:
            self.logger.info("Initializing MAS system...")
            
            # Create shared queue
            self.shared_queue = queue.Queue()
            
            # Create master agent
            self.master_agent = MasterAgent("master_001")
            self.master_agent.shared_queue = self.shared_queue
            
            # Create worker agents
            self.worker_agents = []
            for i in range(self.num_workers):
                worker_id = f"worker_{i+1:03d}"
                worker = WorkerAgent(worker_id, self.shared_queue)
                worker.master_agent = self.master_agent  # Connect worker to master
                self.worker_agents.append(worker)
            
            # Register worker agents with master
            for worker in self.worker_agents:
                self.master_agent.register_worker_agent(worker)
            
            self.logger.info(f"MAS system initialized with {self.num_workers} worker agents")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize MAS system: {e}")
            return False
    
    def start_system(self):
        """Start all agents in the system"""
        if not self.master_agent or not self.worker_agents:
            self.logger.error("System not initialized. Call initialize_system() first.")
            return False
        
        try:
            self.logger.info("Starting MAS system...")
            
            # Start master agent
            self.master_agent.start()
            self.logger.info(f"Started master agent: {self.master_agent.agent_id}")
            
            # Start worker agents
            for worker in self.worker_agents:
                worker.start()
                self.logger.info(f"Started worker agent: {worker.agent_id}")
            
            self.is_running = True
            self.logger.info("MAS system started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start MAS system: {e}")
            return False
    
    def stop_system(self):
        """Stop all agents in the system"""
        try:
            self.logger.info("Stopping MAS system...")
            
            # Stop master agent
            if self.master_agent:
                self.master_agent.stop()
                self.logger.info(f"Stopped master agent: {self.master_agent.agent_id}")
            
            # Stop worker agents
            for worker in self.worker_agents:
                worker.stop()
                self.logger.info(f"Stopped worker agent: {worker.agent_id}")
            
            self.is_running = False
            self.logger.info("MAS system stopped successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to stop MAS system: {e}")
            return False
    
    def execute_query(self, query_type: str, custom_parameters: Dict[str, Any] = None) -> str:
        """Execute a C-SPARQL query through the MAS system"""
        if not self.is_running:
            self.logger.error("System not running. Call start_system() first.")
            return None
        
        try:
            task_id = self.master_agent.execute_csparql_query(query_type, custom_parameters)
            self.logger.info(f"Query execution started with task ID: {task_id}")
            return task_id
        except Exception as e:
            self.logger.error(f"Failed to execute query: {e}")
            return None
    
    def wait_for_task_completion(self, task_id: str, timeout_seconds: int = 120) -> bool:
        """Wait for a task to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            if self.master_agent and hasattr(self.master_agent, 'get_task_status'):
                task_status = self.master_agent.get_task_status(task_id)
                if task_status and task_status.get('status') == 'completed':
                    return True
            time.sleep(1)
        return False
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Get status of a specific task"""
        if self.master_agent and hasattr(self.master_agent, 'get_task_status'):
            return self.master_agent.get_task_status(task_id)
        return {}
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary from master agent"""
        if self.master_agent and hasattr(self.master_agent, 'get_performance_metrics'):
            return self.master_agent.get_performance_metrics()
        return {}
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get overall system status"""
        return {
            'is_running': self.is_running,
            'master_agent': self.master_agent.agent_id if self.master_agent else None,
            'worker_count': len(self.worker_agents),
            'worker_ids': [w.agent_id for w in self.worker_agents] if self.worker_agents else []
        }
    
    def run_demo(self):
        """Run a demonstration of the MAS system"""
        self.logger.info("Starting MAS system demonstration...")
        
        # Initialize and start system
        if not self.initialize_system():
            return False
        
        if not self.start_system():
            return False
        
        # Wait a moment for agents to start
        time.sleep(2)
        
        # Execute a comprehensive query
        self.logger.info("Executing comprehensive traffic analysis query...")
        task_id = self.execute_query("comprehensive_traffic_analysis", {
            'speed_threshold': 90.0,
            'window_size_seconds': 600
        })
        
        if task_id:
            # Wait for completion
            if self.wait_for_task_completion(task_id, timeout_seconds=60):
                # Get final result
                final_status = self.get_task_status(task_id)
                if 'merged_result' in final_status:
                    self.logger.info("Query execution completed successfully!")
                    self.logger.info(f"Total execution time: {final_status['merged_result']['execution_summary']['total_execution_time']:.2f} seconds")
                    
                    # Print some results
                    for query_type, result in final_status['merged_result']['results'].items():
                        self.logger.info(f"{query_type}: {len(result['data'])} results in {result['execution_time']:.2f} seconds")
                else:
                    self.logger.error("Task completed but no merged result available")
            else:
                self.logger.error("Task did not complete within timeout")
        
        # Show system status
        system_status = self.get_system_status()
        self.logger.info(f"System status: {system_status['master_agent']['tasks']['active_tasks']} active tasks, {system_status['master_agent']['tasks']['completed_tasks']} completed tasks")
        
        # Show performance metrics
        performance_summary = self.get_performance_summary()
        if performance_summary:
            self.logger.info("Performance Metrics Summary:")
            overall = performance_summary.get('overall_performance', {})
            self.logger.info(f"  Total Queries: {overall.get('total_queries', 0)}")
            self.logger.info(f"  Average Execution Time: {overall.get('average_execution_time', 0):.4f}s")
            self.logger.info(f"  Fastest Execution: {overall.get('fastest_execution', 0):.4f}s")
            self.logger.info(f"  Slowest Execution: {overall.get('slowest_execution', 0):.4f}s")
            
            # Show query type breakdown
            query_types = performance_summary.get('query_type_breakdown', {})
            if query_types:
                self.logger.info("  Query Type Performance:")
                for qtype, perf in query_types.items():
                    self.logger.info(f"    {qtype}: {perf.get('count', 0)} queries, avg: {perf.get('average_time', 0):.4f}s")
        
        return True
    
    def cleanup(self):
        """Clean up system resources"""
        self.stop_system()
        self.logger.info("MAS system cleanup completed")


def main():
    """Main function to run the MAS system"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run MAS system
    coordinator = MASCoordinator()
    
    try:
        # Run demonstration
        success = coordinator.run_demo()
        
        if success:
            print("\n" + "="*50)
            print("MAS System Demonstration Completed Successfully!")
            print("="*50)
        else:
            print("\n" + "="*50)
            print("MAS System Demonstration Failed!")
            print("="*50)
        
        # Keep system running for a bit to see results
        print("\nSystem will continue running for 10 seconds...")
        time.sleep(10)
        
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
    finally:
        coordinator.cleanup()
        print("MAS System shutdown complete")


if __name__ == "__main__":
    main()
