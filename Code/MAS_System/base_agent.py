import threading
import time
import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from queue import Queue
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class BaseAgent(ABC):
    """
    Base class for all agents in the Multi-Agent System
    """
    
    def __init__(self, agent_id: str, agent_type: str):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.message_queue = Queue()
        self.is_running = False
        self.thread = None
        self.logger = logging.getLogger(f"{self.agent_type}_{agent_id}")
        
        # Message handlers
        self.message_handlers = {}
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default message handlers"""
        self.register_handler("ping", self._handle_ping)
        self.register_handler("stop", self._handle_stop)
    
    def register_handler(self, message_type: str, handler_func):
        """Register a message handler for a specific message type"""
        self.message_handlers[message_type] = handler_func
    
    def send_message(self, target_agent, message: Dict[str, Any]):
        """Send a message to another agent"""
        if hasattr(target_agent, 'receive_message'):
            target_agent.receive_message(self.agent_id, message)
            self.logger.info(f"Sent message to {target_agent.agent_id}: {message['type']}")
        else:
            self.logger.error(f"Target agent {target_agent} has no receive_message method")
    
    def receive_message(self, sender_id: str, message: Dict[str, Any]):
        """Receive a message from another agent"""
        message_type = message.get('type', 'unknown')
        self.logger.info(f"Received message from {sender_id}: {message_type}")
        
        # Add message to queue for processing
        self.message_queue.put((sender_id, message))
    
    def _handle_ping(self, sender_id: str, message: Dict[str, Any]):
        """Handle ping messages"""
        response = {
            'type': 'pong',
            'sender': self.agent_id,
            'timestamp': time.time(),
            'data': f"Agent {self.agent_id} is alive"
        }
        # Find the sender agent and send response
        # This would need to be implemented based on agent discovery mechanism
        self.logger.info(f"Responded to ping from {sender_id}")
    
    def _handle_stop(self, sender_id: str, message: Dict[str, Any]):
        """Handle stop messages"""
        self.logger.info(f"Received stop message from {sender_id}")
        self.stop()
    
    def start(self):
        """Start the agent"""
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self._run_loop)
            self.thread.daemon = True
            self.thread.start()
            self.logger.info(f"Agent {self.agent_id} started")
    
    def stop(self):
        """Stop the agent"""
        self.is_running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=1.0)
        self.logger.info(f"Agent {self.agent_id} stopped")
    
    def _run_loop(self):
        """Main message processing loop"""
        while self.is_running:
            try:
                # Process messages from queue
                if not self.message_queue.empty():
                    sender_id, message = self.message_queue.get(timeout=0.1)
                    self._process_message(sender_id, message)
                
                # Run agent-specific logic
                self._run_logic()
                
                time.sleep(0.1)  # Small delay to prevent busy waiting
                
            except Exception as e:
                self.logger.error(f"Error in agent loop: {e}")
                time.sleep(1)  # Wait before retrying
    
    def _process_message(self, sender_id: str, message: Dict[str, Any]):
        """Process a received message"""
        message_type = message.get('type', 'unknown')
        
        if message_type in self.message_handlers:
            try:
                self.message_handlers[message_type](sender_id, message)
            except Exception as e:
                self.logger.error(f"Error handling message {message_type}: {e}")
        else:
            self.logger.warning(f"No handler for message type: {message_type}")
    
    @abstractmethod
    def _run_logic(self):
        """Agent-specific logic to run in the main loop"""
        pass
    
    def get_status(self) -> Dict[str, Any]:
        """Get current agent status"""
        return {
            'agent_id': self.agent_id,
            'agent_type': self.agent_type,
            'is_running': self.is_running,
            'queue_size': self.message_queue.qsize(),
            'thread_alive': self.thread.is_alive() if self.thread else False
        }
