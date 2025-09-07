#!/usr/bin/env python3
"""
Simple test for the updated MAS system
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import logging
from mas_coordinator import MASCoordinator

def main():
    """Test the updated MAS system"""
    print("Testing Updated MAS System")
    print("=" * 40)
    
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
        
        print("MAS system started successfully!")
        
        # Wait for agents to be ready
        time.sleep(2)
        
        # Test data generation in worker agents
        print("\nTesting data generation in worker agents...")
        for worker in coordinator.worker_agents:
            if hasattr(worker, 'shared_queue'):
                # Send generate data message
                worker.send_message(worker, {
                    'type': 'generate_data',
                    'sender': 'test',
                    'timestamp': time.time(),
                    'data': {}
                })
        
        # Wait for data generation
        time.sleep(3)
        
        # Test multiple query executions for performance metrics
        print("\nTesting multiple query executions for performance metrics...")
        
        query_tests = [
            ("vehicle_count_per_location", {'window_size_seconds': 300}),
            ("high_speed_vehicles", {'speed_threshold': 80.0, 'window_size_seconds': 60}),
            ("congestion_events", {'window_size_seconds': 120})
        ]
        
        for i, (query_type, params) in enumerate(query_tests):
            print(f"\nTest {i+1}: Executing {query_type} query...")
            print(f"Parameters: {params}")
            
            # Start timing
            start_time = time.time()
            
            task_id = coordinator.execute_query(query_type, params)
            
            if task_id:
                print(f"Query started with task ID: {task_id}")
                
                # Wait for completion
                if coordinator.wait_for_task_completion(task_id, timeout_seconds=30):
                    end_time = time.time()
                    total_time = end_time - start_time
                    print(f"Query completed successfully in {total_time:.4f} seconds!")
                    
                    # Get result
                    status = coordinator.get_task_status(task_id)
                    if 'merged_result' in status:
                        result = status['merged_result']
                        print(f"Query Type: {result['query_type']}")
                        print(f"Total Execution Time: {result['execution_summary']['total_execution_time']:.4f} seconds")
                        
                        for sub_query_type, sub_result in result['results'].items():
                            print(f"  {sub_query_type}: {len(sub_result['data'])} results in {sub_result['execution_time']:.4f}s")
                else:
                    print("Query did not complete within timeout")
            else:
                print("Failed to start query")
        
        # Test comprehensive query for better performance metrics
        print(f"\nTest {len(query_tests) + 1}: Executing comprehensive traffic analysis...")
        comprehensive_params = {
            'speed_threshold': 85.0,
            'window_size_seconds': 600
        }
        print(f"Parameters: {comprehensive_params}")
        
        start_time = time.time()
        comprehensive_task_id = coordinator.execute_query("comprehensive_traffic_analysis", comprehensive_params)
        
        if comprehensive_task_id:
            print(f"Comprehensive query started with task ID: {comprehensive_task_id}")
            
            if coordinator.wait_for_task_completion(comprehensive_task_id, timeout_seconds=60):
                end_time = time.time()
                total_time = end_time - start_time
                print(f"Comprehensive query completed successfully in {total_time:.4f} seconds!")
                
                status = coordinator.get_task_status(comprehensive_task_id)
                if 'merged_result' in status:
                    result = status['merged_result']
                    print(f"Comprehensive Query Results:")
                    print(f"  Total Sub-tasks: {result['execution_summary']['total_sub_tasks']}")
                    print(f"  Total Execution Time: {result['execution_summary']['total_execution_time']:.4f} seconds")
                    
                    for sub_query_type, sub_result in result['results'].items():
                        print(f"  {sub_query_type}: {len(sub_result['data'])} results in {sub_result['execution_time']:.4f}s")
            else:
                print("Comprehensive query did not complete within timeout")
        else:
            print("Failed to start comprehensive query")
        
        # Show system status
        print("\nSystem Status:")
        system_status = coordinator.get_system_status()
        print(f"  System Running: {system_status['system_running']}")
        print(f"  Total Agents: {system_status['total_agents']}")
        print(f"  Master Agent: {system_status['master_agent']['id']}")
        print(f"  Worker Agents: {list(system_status['worker_agents'].keys())}")
        
        # Show performance metrics
        print("\nPerformance Metrics:")
        print("-" * 40)
        performance_summary = coordinator.get_performance_summary()
        if performance_summary:
            overall = performance_summary.get('overall_performance', {})
            print(f"Overall Performance:")
            print(f"  Total Queries Executed: {overall.get('total_queries', 0)}")
            print(f"  Average Execution Time: {overall.get('average_execution_time', 0):.4f} seconds")
            print(f"  Fastest Execution: {overall.get('fastest_execution', 0):.4f} seconds")
            print(f"  Slowest Execution: {overall.get('slowest_execution', 0):.4f} seconds")
            print(f"  Total Execution Time: {overall.get('total_execution_time', 0):.4f} seconds")
            
            # Show query type breakdown
            query_types = performance_summary.get('query_type_breakdown', {})
            if query_types:
                print(f"\nQuery Type Performance:")
                for qtype, perf in query_types.items():
                    print(f"  {qtype}:")
                    print(f"    Count: {perf.get('count', 0)}")
                    print(f"    Average Time: {perf.get('average_time', 0):.4f}s")
                    print(f"    Fastest: {perf.get('fastest_time', 0):.4f}s")
                    print(f"    Slowest: {perf.get('slowest_time', 0):.4f}s")
            
            # Show worker performance
            worker_perf = performance_summary.get('worker_performance', {})
            if worker_perf:
                print(f"\nWorker Performance:")
                for worker_id, perf in worker_perf.items():
                    print(f"  {worker_id}:")
                    print(f"    Tasks Completed: {perf.get('tasks_completed', 0)}")
                    print(f"    Average Execution Time: {perf.get('average_execution_time', 0):.4f}s")
            
            # Show queue processing stats
            queue_stats = performance_summary.get('queue_processing_stats', {})
            if queue_stats:
                print(f"\nQueue Processing Stats:")
                print(f"  Total Processing Times: {queue_stats.get('total_processing_times', 0)}")
                print(f"  Average Queue Time: {queue_stats.get('average_queue_time', 0):.4f}s")
            
            # Show situation refinement stats
            refinement_stats = performance_summary.get('situation_refinement_stats', {})
            if refinement_stats:
                print(f"\nSituation Refinement Stats:")
                print(f"  Total Refinement Times: {refinement_stats.get('total_refinement_times', 0)}")
                print(f"  Average Refinement Time: {refinement_stats.get('average_refinement_time', 0):.4f}s")
        else:
            print("  No performance metrics available yet")
        
        # Keep system running for a bit
        print("\nSystem will continue running for 10 seconds...")
        time.sleep(10)
        
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        logging.exception("Error in test")
    finally:
        print("\nShutting down MAS system...")
        coordinator.cleanup()
        print("MAS system shutdown complete")

if __name__ == "__main__":
    main()
