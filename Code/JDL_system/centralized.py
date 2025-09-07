from typing import Iterable, Iterator, Tuple
import threading
import time

try:
	from .bus import BoundedBus, Triple
	from .queries import q1_vehicle_count_observations, q2_situation_refinement_over_graph, append_triples_to_graph
except ImportError:
	from bus import BoundedBus, Triple  # type: ignore
	from queries import q1_vehicle_count_observations, q2_situation_refinement_over_graph, append_triples_to_graph  # type: ignore
from rdflib import Graph


def run_centralized(
	triples: Iterable[Triple],
	bus_capacity: int,
	q2_threshold: int = 100,
	heavy_q2_memory_mb: int = 0,
	heavy_q2_delay_ms: int = 0,
	master_poll_ms: int = 30,
) -> Tuple[Iterator[Triple], float]:
	"""Centralized execution with overlap and heavy q2 to create backpressure.

	Worker thread runs q1 and pushes to BUS. Master thread consumes slowly
	(simulated by delay and RAM allocation), accumulates into a graph, then runs q2 once.
	"""
	bus = BoundedBus(bus_capacity)
	obs_graph = Graph()
	q2_results: list[Triple] = []
	transfer_seconds: float = 0.0

	def worker() -> None:
		try:
			for t in q1_vehicle_count_observations(triples):
				bus.put(t)  # This will block if BUS is full
		finally:
			bus.close()

	def master() -> None:
		nonlocal transfer_seconds
		memory_hog = []
		if heavy_q2_memory_mb > 0:
			try:
				memory_hog.append(bytearray(heavy_q2_memory_mb * 1024 * 1024))
			except Exception:
				pass
		while True:
			batch = bus.drain_all()
			if not batch:
				item = bus.get(timeout=max(0.001, master_poll_ms / 1000.0))
				if item is None:
					break
				else:
					batch = [item]
			
			# Apply delay per triple to make it much slower
			for triple in batch:
				if heavy_q2_delay_ms > 0:
					time.sleep(heavy_q2_delay_ms / 1000.0)
				start_t = time.perf_counter()
				append_triples_to_graph(obs_graph, [triple])
				transfer_seconds += time.perf_counter() - start_t

		for out in q2_situation_refinement_over_graph(obs_graph, threshold=q2_threshold):
			q2_results.append(out)

	wt = threading.Thread(target=worker, name="central-worker-q1", daemon=True)
	mt = threading.Thread(target=master, name="central-master-q2", daemon=True)
	wt.start()
	mt.start()
	wt.join()
	mt.join()

	return iter(q2_results), transfer_seconds


