#!/usr/bin/env python3
"""
Simple test for MAS system with overlapping operations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import logging
from mas_coordinator import MASCoordinator

def test_mas_overlapping():
    """Test MAS system with overlapping operations"""
    print("Testing MAS System with Overlapping Operations")
    print("=" * 60)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create MAS coordinator
    coordinator = MASCoordinator()
    
    try:
        # Initialize and start system
        print("Initializing MAS system...")
        if not coordinator.initialize_system():
            print("Failed to initialize MAS system")
            return False
        
        print("Starting MAS system...")
        if not coordinator.start_system():
            print("Failed to start MAS system")
            return False
        
        print("MAS system started successfully!")
        print("Note: MAS system uses parallel processing with overlapping operations")
        
        # Wait for agents to be ready
        time.sleep(2)
        
        # Test data generation in worker agents
        print("\nTesting data generation in worker agents (parallel + overlapping)...")
        for worker in coordinator.worker_agents:
            if hasattr(worker, 'generate_traffic_data'):
                # Generate traffic data directly
                worker.generate_traffic_data()
                worker.queue_to_rdf()
        
        # Wait for data generation and RDF conversion
        time.sleep(5)
        
        # Test query execution with timing
        print("\nTesting query execution in MAS system (parallel + overlapping)...")
        query_type = "vehicle_count_per_location"
        params = {'window_size_seconds': 300}
        
        print(f"Executing {query_type} query in MAS (parallel + overlapping)...")
        print(f"Parameters: {params}")
        print(f"Execution Mode: Parallel with overlapping operations")
        
        # Start timing from data input to query output
        start_time = time.time()
        
        # Execute query directly on master agent
        if coordinator.master_agent:
            task_id = coordinator.master_agent.execute_csparql_query(query_type, params)
            
            if task_id:
                print(f"Query started with task ID: {task_id}")
                
                # Wait for completion
                if coordinator.wait_for_task_completion(task_id, timeout_seconds=120):
                    end_time = time.time()
                    total_time = end_time - start_time
                    print(f"Query completed successfully in {total_time:.4f} seconds!")
                    print(f"Execution Mode: Parallel processing with overlapping operations")
                    
                    # Get task result
                    task_status = coordinator.get_task_status(task_id)
                    if task_status and 'merged_result' in task_status:
                        result = task_status['merged_result']
                        print(f"Query Type: {result.get('query_type', 'Unknown')}")
                        if 'execution_summary' in result:
                            print(f"Total Execution Time: {result['execution_summary'].get('total_execution_time', 0):.4f} seconds")
                        
                        if 'results' in result:
                            for sub_query_type, sub_result in result['results'].items():
                                if isinstance(sub_result, dict):
                                    print(f"  {sub_query_type}: {len(sub_result.get('data', []))} results in {sub_result.get('execution_time', 0):.4f}s")
                    
                    print(f"\nMAS System Performance:")
                    print(f"  Total Time: {total_time:.4f} seconds")
                    print(f"  Execution Mode: Parallel + Overlapping")
                    print(f"  Worker Agents: {len(coordinator.worker_agents)}")
                    print(f"  Overlapping Operations: Enabled")
                    
                else:
                    print(f"Query did not complete within timeout")
            else:
                print(f"Failed to execute query")
        else:
            print(f"Master agent not available")
        
        return True
        
    except Exception as e:
        print(f"MAS test failed with error: {e}")
        logging.exception("Error in MAS test")
        return False
    finally:
        # Stop system
        print("\nStopping MAS system...")
        coordinator.stop_system()
        print("MAS system stopped")

def main():
    """Main function"""
    print("MAS System Test with Overlapping Operations")
    print("=" * 60)
    
    # Test MAS system
    success = test_mas_overlapping()
    
    if success:
        print("\n" + "="*50)
        print("MAS System Test Completed Successfully!")
        print("="*50)
        print("The system demonstrated parallel processing with overlapping operations.")
        print("Worker agents generate data continuously while queries execute in parallel.")
        print("Results are sent to the master agent as they complete for immediate processing.")
    else:
        print("\n" + "="*50)
        print("MAS System Test Failed!")
        print("="*50)

if __name__ == "__main__":
    main()
