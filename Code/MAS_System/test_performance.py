#!/usr/bin/env python3
"""
Performance Test for the MAS System
Tests query execution time and performance metrics
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import logging
from mas_coordinator import MASCoordinator

def test_performance():
    """Test the performance metrics of the MAS system"""
    print("MAS System Performance Test")
    print("=" * 50)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create MAS coordinator
    coordinator = MASCoordinator()
    
    try:
        # Initialize system
        print("Initializing MAS system...")
        if not coordinator.initialize_system():
            print("Failed to initialize MAS system")
            return
        
        # Start system
        print("Starting MAS system...")
        if not coordinator.start_system():
            print("Failed to start MAS system")
            return
        
        print("System started successfully")
        print()
        
        # Test different query types with timing
        query_types = [
            'high_speed_vehicles',
            'vehicle_count_per_location', 
            'congestion_events'
        ]
        
        query_results = {}
        
        for i, query_type in enumerate(query_types):
            print(f"Test {i+1}: Executing {query_type} query...")
            
            # Start timing
            start_time = time.time()
            
            # Execute query
            task_id = coordinator.execute_query(query_type, {
                'speed_threshold': 80.0 + (i * 10),  # Different thresholds
                'window_size_seconds': 300 + (i * 60)  # Different window sizes
            })
            
            if task_id:
                print(f"  Task ID: {task_id}")
                
                # Wait for completion
                if coordinator.wait_for_task_completion(task_id, timeout_seconds=120):
                    end_time = time.time()
                    total_time = end_time - start_time
                    
                    print(f"  Query completed in {total_time:.4f} seconds")
                    
                    # Get task result
                    task_status = coordinator.get_task_status(task_id)
                    if 'merged_result' in task_status:
                        print(f"  Results: {len(task_status['merged_result'].get('results', {}))} query results")
                    
                    query_results[query_type] = {
                        'task_id': task_id,
                        'execution_time': total_time,
                        'status': 'completed'
                    }
                else:
                    print(f"  Query did not complete within timeout")
                    query_results[query_type] = {
                        'task_id': task_id,
                        'execution_time': None,
                        'status': 'timeout'
                    }
            else:
                print(f"  Failed to execute query")
                query_results[query_type] = {
                    'task_id': None,
                    'execution_time': None,
                    'status': 'failed'
                }
            
            print()
        
        # Display performance metrics
        print("Performance Metrics Summary")
        print("-" * 30)
        
        performance_summary = coordinator.get_performance_summary()
        if performance_summary:
            overall = performance_summary.get('overall_performance', {})
            print(f"Overall Performance:")
            print(f"  Total Queries Executed: {overall.get('total_queries', 0)}")
            print(f"  Average Execution Time: {overall.get('average_execution_time', 0):.4f} seconds")
            print(f"  Fastest Execution: {overall.get('fastest_execution', 0):.4f} seconds")
            print(f"  Slowest Execution: {overall.get('slowest_execution', 0):.4f} seconds")
            print(f"  Total Execution Time: {overall.get('total_execution_time', 0):.4f} seconds")
            
            print(f"\nQuery Type Breakdown:")
            query_types_perf = performance_summary.get('query_type_breakdown', {})
            for qtype, perf in query_types_perf.items():
                print(f"  {qtype}:")
                print(f"    Count: {perf.get('count', 0)}")
                print(f"    Average Time: {perf.get('average_time', 0):.4f}s")
                print(f"    Fastest: {perf.get('fastest_time', 0):.4f}s")
                print(f"    Slowest: {perf.get('slowest_time', 0):.4f}s")
            
            print(f"\nWorker Performance:")
            worker_perf = performance_summary.get('worker_performance', {})
            for worker_id, perf in worker_perf.items():
                print(f"  {worker_id}:")
                print(f"    Tasks Completed: {perf.get('tasks_completed', 0)}")
                print(f"    Average Execution Time: {perf.get('average_execution_time', 0):.4f}s")
            
            print(f"\nQueue Processing Stats:")
            queue_stats = performance_summary.get('queue_processing_stats', {})
            print(f"  Total Processing Times: {queue_stats.get('total_processing_times', 0)}")
            print(f"  Average Queue Time: {queue_stats.get('average_queue_time', 0):.4f}s")
            
            print(f"\nSituation Refinement Stats:")
            refinement_stats = performance_summary.get('situation_refinement_stats', {})
            print(f"  Total Refinement Times: {refinement_stats.get('total_refinement_times', 0)}")
            print(f"  Average Refinement Time: {refinement_stats.get('average_refinement_time', 0):.4f}s")
        
        # Display individual query results
        print(f"\nIndividual Query Results:")
        print("-" * 30)
        for query_type, result in query_results.items():
            print(f"{query_type}:")
            print(f"  Status: {result['status']}")
            if result['execution_time']:
                print(f"  Execution Time: {result['execution_time']:.4f}s")
            if result['task_id']:
                print(f"  Task ID: {result['task_id']}")
            print()
        
        # Display system status
        print("System Status:")
        print("-" * 30)
        system_status = coordinator.get_system_status()
        print(f"System Running: {system_status.get('system_running', False)}")
        print(f"Total Agents: {system_status.get('total_agents', 0)}")
        
        master_info = system_status.get('master_agent', {})
        if master_info:
            print(f"Master Agent: {master_info.get('id', 'Unknown')}")
            tasks = master_info.get('tasks', {})
            print(f"Active Tasks: {tasks.get('active_tasks', 0)}")
            print(f"Completed Tasks: {tasks.get('completed_tasks', 0)}")
        
        print(f"Worker Agents: {len(system_status.get('worker_agents', {}))}")
        
    except Exception as e:
        print(f"Performance test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Stop system
        print("\nStopping system...")
        coordinator.stop_system()
        print("System stopped")

def main():
    """Main function"""
    test_performance()

if __name__ == "__main__":
    main()
