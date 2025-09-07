import threading
import time
from typing import Iterable, Iterator, List, Tuple

try:
	from .bus import BoundedBus, Triple
	from .queries import q1_vehicle_count_observations, q2_situation_refinement_over_graph, append_triples_to_graph
except ImportError:
	from bus import BoundedBus, Triple  # type: ignore
	from queries import q1_vehicle_count_observations, q2_situation_refinement_over_graph, append_triples_to_graph  # type: ignore
from rdflib import Graph


def run_distributed(
	triples: Iterable[Triple],
	bus_capacity: int,
	q2_threshold: int = 100,
	worker_batch_size: int = 1000,
	master_batch_size: int = 5000,
	master_poll_ms: int = 1,
) -> Tuple[Iterator[Triple], float]:
	"""Distributed execution: worker runs q1 -> BUS, master runs q2 <- BUS.

	This uses two threads to simulate separate agents.
	If bus_capacity is -1, uses effectively unlimited capacity.
	"""
	# Use unlimited capacity for distributed mode if specified
	effective_capacity = bus_capacity if bus_capacity > 0 else 1000000000  # 1 billion for unlimited
	bus = BoundedBus(effective_capacity)
	q2_results: List[Triple] = []
	transfer_seconds: float = 0.0

	def worker() -> None:
		try:
			batch: List[Triple] = []
			for t in q1_vehicle_count_observations(triples):
				batch.append(t)
				if len(batch) >= max(1, worker_batch_size):
					bus.put_many(batch)
					batch.clear()
			if batch:
				bus.put_many(batch)
		finally:
			bus.close()

	def master() -> None:
		nonlocal transfer_seconds
		obs_graph = Graph()
		while True:
			batch: List[Triple] = []
			# Try to get at least one item (with small timeout)
			item = bus.get(timeout=max(0.001, master_poll_ms / 1000.0))
			if item is None:
				break
			batch.append(item)
			# Drain more to reach desired batch size
			while len(batch) < max(1, master_batch_size):
				more = bus.drain_all()
				if not more:
					break
				batch.extend(more)
			start_t = time.perf_counter()
			append_triples_to_graph(obs_graph, batch)
			transfer_seconds += time.perf_counter() - start_t

		for out in q2_situation_refinement_over_graph(obs_graph, threshold=q2_threshold):
			q2_results.append(out)

	wt = threading.Thread(target=worker, name="worker-q1", daemon=True)
	mt = threading.Thread(target=master, name="master-q2", daemon=True)
	wt.start()
	mt.start()
	wt.join()
	mt.join()

	return iter(q2_results), transfer_seconds


