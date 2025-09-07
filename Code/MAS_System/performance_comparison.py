#!/usr/bin/env python3
"""
Performance Comparison Test between MAS and Centralized Approaches
Compares query execution time from data input to query output
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import time
import logging
from mas_coordinator import MASCoordinator
from centralized_agent import CentralizedAgent

def test_mas_performance():
    """Test MAS system performance with parallel processing"""
    print("Testing MAS System Performance (Parallel Processing)")
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
            return None
        
        print("Starting MAS system...")
        if not coordinator.start_system():
            print("Failed to start MAS system")
            return None
        
        print("MAS system started successfully!")
        print("Note: MAS system uses parallel processing with overlapping operations")
        
        # Wait for agents to be ready
        time.sleep(2)
        
        # Test data generation in worker agents
        print("\nTesting data generation in worker agents (parallel)...")
        for worker in coordinator.worker_agents:
            if hasattr(worker, 'generate_traffic_data'):
                # Generate traffic data directly
                worker.generate_traffic_data()
                worker.queue_to_rdf()
        
        # Wait for data generation and RDF conversion
        time.sleep(5)
        
        # Test query execution with timing
        print("\nTesting query execution in MAS system (parallel processing)...")
        query_tests = [
            ("vehicle_count_per_location", {'window_size_seconds': 300}),
            ("high_speed_vehicles", {'speed_threshold': 80.0, 'window_size_seconds': 60}),
            ("congestion_events", {'window_size_seconds': 120}),
            ("comprehensive_traffic_analysis", {'speed_threshold': 85.0, 'window_size_seconds': 600})
        ]
        
        mas_results = {}
        
        for i, (query_type, params) in enumerate(query_tests):
            print(f"\nTest {i+1}: Executing {query_type} query in MAS (parallel)...")
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
                        print(f"Execution Mode: Parallel processing with worker agents")
                        
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
                        
                        mas_results[query_type] = {
                            'task_id': task_id,
                            'total_time': total_time,
                            'status': 'completed',
                            'execution_mode': 'parallel'
                        }
                    else:
                        print(f"Query did not complete within timeout")
                        mas_results[query_type] = {
                            'task_id': task_id,
                            'total_time': None,
                            'status': 'timeout',
                            'execution_mode': 'parallel'
                        }
                else:
                    print(f"Failed to execute query")
                    mas_results[query_type] = {
                        'task_id': None,
                        'total_time': None,
                        'status': 'failed',
                        'execution_mode': 'parallel'
                    }
            else:
                print(f"Master agent not available")
                mas_results[query_type] = {
                    'task_id': None,
                    'total_time': None,
                    'status': 'failed',
                    'execution_mode': 'parallel'
                }
        
        # Get performance metrics
        performance_summary = coordinator.get_performance_summary()
        
        return {
            'results': mas_results,
            'performance': performance_summary,
            'system_status': coordinator.get_system_status()
        }
        
    except Exception as e:
        print(f"MAS test failed with error: {e}")
        logging.exception("Error in MAS test")
        return None
    finally:
        # Stop system
        print("\nStopping MAS system...")
        coordinator.stop_system()
        print("MAS system stopped")

def test_centralized_performance():
    """Test centralized agent performance with sequential processing"""
    print("\nTesting Centralized Agent Performance (Sequential Processing)")
    print("=" * 60)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create centralized agent
    agent = CentralizedAgent()
    
    try:
        print("Centralized agent created successfully!")
        print("Note: Centralized system uses sequential processing without overlapping")
        
        # Test data generation
        print("\nTesting data generation in centralized agent (sequential)...")
        agent.generate_traffic_data()
        
        # Test RDF conversion
        print("\nTesting RDF conversion (sequential)...")
        agent.queue_to_rdf()
        
        # Test query execution with timing
        print("\nTesting query execution in centralized agent (sequential processing)...")
        query_tests = [
            ("vehicle_count_per_location", {'window_size_seconds': 300}),
            ("high_speed_vehicles", {'speed_threshold': 80.0, 'window_size_seconds': 60}),
            ("congestion_events", {'window_size_seconds': 120}),
            ("comprehensive_traffic_analysis", {'speed_threshold': 85.0, 'window_size_seconds': 600})
        ]
        
        centralized_results = {}
        
        for i, (query_type, params) in enumerate(query_tests):
            print(f"\nTest {i+1}: Executing {query_type} query in centralized agent (sequential)...")
            print(f"Parameters: {params}")
            print(f"Execution Mode: Sequential (worker queries first, then master query)")
            
            # Start timing from data input to query output
            start_time = time.time()
            
            # Execute query
            result = agent.execute_csparql_query(query_type, params)
            
            end_time = time.time()
            total_time = end_time - start_time
            
            if result:
                print(f"Query completed successfully in {total_time:.4f} seconds!")
                print(f"Query Type: {result['query_type']}")
                print(f"Total Execution Time: {result['execution_time']:.4f} seconds")
                print(f"Worker Queries Time: {result['worker_time']:.4f} seconds")
                print(f"Master Query Time: {result['master_time']:.4f} seconds")
                print(f"Graph Size: {result['graph_size']} triples")
                print(f"Execution Mode: {result['execution_mode']}")
                
                if isinstance(result['result'], dict):
                    for sub_query_type, sub_result in result['result'].items():
                        print(f"  {sub_query_type}: {len(sub_result)} results")
                else:
                    print(f"  Results: {len(result['result'])} items")
                
                centralized_results[query_type] = {
                    'total_time': total_time,
                    'execution_time': result['execution_time'],
                    'worker_time': result['worker_time'],
                    'master_time': result['master_time'],
                    'status': 'completed',
                    'graph_size': result['graph_size'],
                    'execution_mode': result['execution_mode']
                }
            else:
                print(f"Query failed")
                centralized_results[query_type] = {
                    'total_time': total_time,
                    'execution_time': None,
                    'worker_time': None,
                    'master_time': None,
                    'status': 'failed',
                    'graph_size': 0,
                    'execution_mode': 'sequential'
                }
        
        # Get performance metrics
        performance_summary = agent.get_performance_summary()
        
        return {
            'results': centralized_results,
            'performance': performance_summary,
            'agent_status': agent.get_status()
        }
        
    except Exception as e:
        print(f"Centralized test failed with error: {e}")
        logging.exception("Error in centralized test")
        return None

def compare_performance(mas_results, centralized_results):
    """Compare performance between MAS and centralized approaches"""
    print("\n" + "="*70)
    print("PERFORMANCE COMPARISON: MAS (PARALLEL + OVERLAPPING) vs CENTRALIZED (SEQUENTIAL)")
    print("="*70)
    
    if not mas_results or not centralized_results:
        print("Cannot compare: One or both tests failed")
        return
    
    print("\nQuery Execution Time Comparison:")
    print("-" * 70)
    print(f"{'Query Type':<30} {'MAS (Parallel)':<15} {'Centralized (Seq)':<18} {'Difference':<15}")
    print("-" * 70)
    
    total_mas_time = 0
    total_centralized_time = 0
    successful_comparisons = 0
    
    for query_type in mas_results['results'].keys():
        if query_type in centralized_results['results']:
            mas_time = mas_results['results'][query_type].get('total_time', 0)
            centralized_time = centralized_results['results'][query_type].get('total_time', 0)
            
            if mas_time is not None and centralized_time is not None:
                difference = centralized_time - mas_time
                percentage = (difference / centralized_time * 100) if centralized_time > 0 else 0
                
                print(f"{query_type:<30} {mas_time:<15.4f} {centralized_time:<18.4f} {difference:<15.4f} ({percentage:+.1f}%)")
                
                total_mas_time += mas_time
                total_centralized_time += centralized_time
                successful_comparisons += 1
            else:
                print(f"{query_type:<30} {'N/A':<15} {'N/A':<18} {'N/A':<15}")
    
    print("-" * 70)
    if successful_comparisons > 0:
        avg_mas_time = total_mas_time / successful_comparisons
        avg_centralized_time = total_centralized_time / successful_comparisons
        total_difference = total_centralized_time - total_mas_time
        total_percentage = (total_difference / total_centralized_time * 100) if total_centralized_time > 0 else 0
        
        print(f"{'AVERAGE':<30} {avg_mas_time:<15.4f} {avg_centralized_time:<18.4f} {total_difference:<15.4f} ({total_percentage:+.1f}%)")
        print(f"{'TOTAL':<30} {total_mas_time:<15.4f} {total_centralized_time:<18.4f} {total_difference:<15.4f} ({total_percentage:+.1f}%)")
        
        print(f"\nPerformance Analysis:")
        if total_difference > 0:
            print(f"  ✅ MAS system (Parallel + Overlapping) is {total_difference:.4f} seconds FASTER ({total_percentage:.1f}% improvement)")
            print(f"  📊 Parallel processing with overlapping operations provides better performance")
            print(f"  🔄 Overlapping operations allow continuous data processing while queries execute")
        else:
            print(f"  ❌ Centralized system (Sequential) is {abs(total_difference):.4f} seconds FASTER ({abs(total_percentage):.1f}% improvement)")
            print(f"  📊 Sequential processing without overlapping operations")
            print(f"  ⚠️  This suggests the MAS system needs optimization")
        
        print(f"\nExecution Mode Analysis:")
        print(f"  🔄 MAS System: Parallel processing with overlapping operations")
        print(f"    - Multiple queries executed simultaneously")
        print(f"    - Continuous data generation in background threads")
        print(f"    - Results sent to master as they complete")
        print(f"    - Situation refinement starts with partial results")
        print(f"    - Expected: Faster total execution time")
        
        print(f"\n  📋 Centralized System: Sequential processing in single agent")
        print(f"    - Worker queries executed first, then master query")
        print(f"    - No overlapping operations")
        print(f"    - Single-threaded execution")
        print(f"    - Sequential time windows (wait for completion)")
        print(f"    - All operations in one agent")
    
    # Compare detailed performance metrics
    print(f"\nDetailed Performance Metrics:")
    print("-" * 50)
    
    mas_perf = mas_results['performance']
    centralized_perf = centralized_results['performance']
    
    if mas_perf and centralized_perf:
        mas_overall = mas_perf.get('overall_performance', {})
        centralized_overall = centralized_perf.get('overall_performance', {})
        
        print(f"MAS System (Parallel + Overlapping):")
        print(f"  Total Queries: {mas_overall.get('total_queries', 0)}")
        print(f"  Average Execution Time: {mas_overall.get('average_execution_time', 0):.4f}s")
        print(f"  Fastest Execution: {mas_overall.get('fastest_execution', 0):.4f}s")
        print(f"  Slowest Execution: {mas_overall.get('slowest_execution', 0):.4f}s")
        
        print(f"\nCentralized System (Sequential):")
        print(f"  Total Queries: {centralized_overall.get('total_queries', 0)}")
        print(f"  Average Execution Time: {centralized_overall.get('average_execution_time', 0):.4f}s")
        print(f"  Fastest Execution: {centralized_overall.get('fastest_execution', 0):.4f}s")
        print(f"  Slowest Execution: {centralized_overall.get('slowest_execution', 0):.4f}s")
        
        # Show timing breakdown for centralized system
        print(f"\nCentralized System Timing Breakdown:")
        for query_type, result in centralized_results['results'].items():
            if result.get('worker_time') is not None and result.get('master_time') is not None:
                print(f"  {query_type}:")
                print(f"    Worker Queries: {result['worker_time']:.4f}s (sequential)")
                print(f"    Master Query: {result['master_time']:.4f}s (sequential)")
                print(f"    Total: {result['total_time']:.4f}s (no overlap)")
                print(f"    Execution Mode: {result['execution_mode']}")
        
        # Show timing breakdown for MAS system
        print(f"\nMAS System Timing Breakdown:")
        for query_type, result in mas_results['results'].items():
            if result.get('total_time') is not None:
                print(f"  {query_type}:")
                print(f"    Total Time: {result['total_time']:.4f}s (parallel + overlapping)")
                print(f"    Execution Mode: {result.get('execution_mode', 'parallel')}")
                print(f"    Status: {result.get('status', 'unknown')}")
    
    # Show expected behavior explanation
    print(f"\nExpected Behavior Explanation:")
    print("-" * 40)
    print(f"  🔄 MAS System (Parallel + Overlapping):")
    print(f"    - Worker queries run simultaneously")
    print(f"    - Data generation continues in background")
    print(f"    - Results sent to master as they complete")
    print(f"    - Situation refinement starts with partial results")
    print(f"    - Expected: Faster total execution time")
    
    print(f"\n  📋 Centralized System (Sequential):")
    print(f"    - Worker queries run one after another")
    print(f"    - Wait for each query to complete")
    print(f"    - Master query starts only after all workers complete")
    print(f"    - No overlapping operations")
    print(f"    - Expected: Slower total execution time")
    
    print(f"\n  📊 Performance Expectation:")
    print(f"    - MAS should be faster due to overlapping operations")
    print(f"    - If MAS is slower, check for:")
    print(f"      * Communication overhead between agents")
    print(f"      * Thread synchronization issues")
    print(f"      * Data transfer bottlenecks")
    print(f"      * Agent coordination overhead")

def main():
    """Main function to run performance comparison"""
    print("MAS vs Centralized Performance Comparison Test")
    print("=" * 60)
    print("This test compares query execution time from data input to query output")
    print("between Multi-Agent System (MAS) and Centralized approaches.")
    print()
    

    # Test centralized system
    centralized_results = test_centralized_performance()

            # Test MAS system
    mas_results = test_mas_performance()



    
    # Compare performance
    compare_performance(mas_results, centralized_results)
    
    print(f"\n" + "="*60)
    print("PERFORMANCE COMPARISON COMPLETED")
    print("="*60)

if __name__ == "__main__":
    main()
