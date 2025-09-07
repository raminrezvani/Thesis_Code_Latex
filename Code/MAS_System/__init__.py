"""
Multi-Agent System (MAS) for C-SPARQL Query Processing

This package provides a distributed system architecture where:
- A master agent breaks down C-SPARQL queries into sub-queries
- Worker agents execute the sub-queries independently
- Results are merged back in the master agent

Classes:
    - BaseAgent: Abstract base class for all agents
    - MasterAgent: Coordinates query execution and result merging
    - WorkerAgent: Executes C-SPARQL sub-queries
    - MASCoordinator: Manages the entire system

Usage:
    from MAS_System import MASCoordinator
    
    coordinator = MASCoordinator()
    coordinator.initialize_system()
    coordinator.start_system()
    
    # Execute a query
    task_id = coordinator.execute_query("comprehensive_traffic_analysis")
    
    # Wait for completion
    coordinator.wait_for_task_completion(task_id)
    
    # Get results
    status = coordinator.get_task_status(task_id)
"""

from .base_agent import BaseAgent
from .master_agent import MasterAgent
from .worker_agent import WorkerAgent
from .mas_coordinator import MASCoordinator

__version__ = "1.0.0"
__author__ = "MAS System Developer"

__all__ = [
    'BaseAgent',
    'MasterAgent', 
    'WorkerAgent',
    'MASCoordinator'
]
