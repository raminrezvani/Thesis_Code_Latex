import queue
import threading
from EdgeAgent_ObjectRefinement import EdgeLayerQueueAgent
from FogAgent_SituationRefinement import FogLayerAgent

def main():
    # Shared queue for communication between Edge and Fog agents
    shared_queue = queue.Queue()

    # Initialize agents
    edge_agent = EdgeLayerQueueAgent(shared_queue)
    fog_agent = FogLayerAgent(shared_queue)

    # Run agents in separate threads
    edge_thread = threading.Thread(target=edge_agent.run)
    fog_thread = threading.Thread(target=fog_agent.run)

    # Start threads
    edge_thread.start()
    fog_thread.start()

    # Wait for edge agent to complete
    # edge_thread.join()

    # Signal fog agent to stop after edge agent is done
    # fog_thread.join(timeout=10)

if __name__ == "__main__":
    main()