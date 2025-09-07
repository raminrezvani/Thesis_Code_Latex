import argparse
from typing import List, Tuple

from .centralized import run_centralized
from .data_generator import generate_synthetic_triples
from .distributed import run_distributed
from .utils import measure_run


def run_experiment(num_raw_triples: int, bus_capacity: int, q2_threshold: int = 100) -> None:
	print(f"Running experiment with {num_raw_triples} raw triples, BUS capacity={bus_capacity}, threshold={q2_threshold}")

	# Centralized first
	triples_iter = generate_synthetic_triples(num_raw_triples)
	with measure_run() as metrics_factory:
		centralized_out = list(run_centralized(triples_iter, bus_capacity, q2_threshold=q2_threshold))
		centralized_metrics = metrics_factory()
	print(f"Centralized: duration={centralized_metrics.duration_seconds:.3f}s, peak_mem={centralized_metrics.peak_memory_bytes/1e6:.2f}MB, out={len(centralized_out)} triples")

	# Distributed second
	triples_iter = generate_synthetic_triples(num_raw_triples)
	with measure_run() as metrics_factory:
		distributed_out = list(run_distributed(triples_iter, bus_capacity, q2_threshold=q2_threshold))
		distributed_metrics = metrics_factory()
	print(f"Distributed: duration={distributed_metrics.duration_seconds:.3f}s, peak_mem={distributed_metrics.peak_memory_bytes/1e6:.2f}MB, out={len(distributed_out)} triples")

	# Simple comparison
	better_time = "Distributed" if distributed_metrics.duration_seconds < centralized_metrics.duration_seconds else "Centralized"
	better_mem = "Distributed" if distributed_metrics.peak_memory_bytes < centralized_metrics.peak_memory_bytes else "Centralized"
	print(f"Faster: {better_time}")
	print(f"Lower memory: {better_mem}")


def main() -> None:
	parser = argparse.ArgumentParser(description="Run centralized vs distributed experiment")
	parser.add_argument("--num", type=int, default=60000, help="Number of raw triples to generate")
	parser.add_argument("--bus", type=int, default=10000, help="BUS capacity in triples")
	parser.add_argument("--threshold", type=int, default=100, help="q2 situation refinement threshold")
	args = parser.parse_args()
	run_experiment(args.num, args.bus, q2_threshold=args.threshold)


if __name__ == "__main__":
	main()


