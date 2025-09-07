import os, sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import argparse
import json
from typing import List, Tuple

from data_generator import generate_synthetic_triples
from centralized import run_centralized
from distributed import run_distributed
from utils import measure_run


Triple = Tuple[str, str, str]


def print_triples(label: str, triples: List[Triple], limit: int = 20) -> None:
	print(f"\n=== {label} (count={len(triples)}) ===")
	to_show = triples if limit <= 0 else triples[:limit]
	for s, p, o in to_show:
		print(f"{s} {p} {o}")
	if limit > 0 and len(triples) > limit:
		print(f"... ({len(triples) - limit} more)")


def save_triples(path: str, triples: List[Triple]) -> None:
	with open(path, "w", encoding="utf-8") as f:
		for s, p, o in triples:
			f.write(f"{s}\t{p}\t{json.dumps(o, ensure_ascii=False)}\n")


def main() -> None:
	parser = argparse.ArgumentParser(description="Run centralized and distributed modes and show outputs")
	parser.add_argument("--num", type=int, default=30000, help="Number of raw triples to generate")
	parser.add_argument("--central-bus", type=int, default=100, help="BUS capacity for centralized mode")
	parser.add_argument("--distrib-bus", type=int, default=-1, help="BUS capacity for distributed mode (-1 for unlimited)")
	parser.add_argument("--threshold", type=int, default=100, help="q2 situation refinement threshold")
	parser.add_argument("--central-q2-delay-ms", type=int, default=1000, help="Simulated per-batch delay in centralized q2 consumer")
	parser.add_argument("--central-q2-mem-mb", type=int, default=500, help="Simulated extra RAM (MB) used by centralized q2 consumer")
	parser.add_argument("--central-master-poll-ms", type=int, default=1, help="Centralized master poll interval in ms before giving up")
	parser.add_argument("--print", dest="do_print", action="store_true", help="Print resulting triples to console")
	parser.add_argument("--print-limit", type=int, default=50, help="Max triples to print per mode (<=0 for all)")
	parser.add_argument("--save-central", type=str, default="", help="Path to save centralized output triples")
	parser.add_argument("--save-distrib", type=str, default="", help="Path to save distributed output triples")
	# Distributed tuning
	parser.add_argument("--worker-batch", type=int, default=15000, help="Worker batch size for BUS puts")
	parser.add_argument("--master-batch", type=int, default=60000, help="Master desired batch size per drain")
	parser.add_argument("--master-poll-ms", type=int, default=5, help="Master poll interval in ms before giving up")
	args = parser.parse_args()

	print(
		f"Generating {args.num} raw triples ... (threshold={args.threshold}, bus={args.central_bus}, "
		f"central-heavy: delay={args.central_q2_delay_ms}ms, mem={args.central_q2_mem_mb}MB, poll={args.central_master_poll_ms}ms)"
	)
	triples_iter = generate_synthetic_triples(args.num)
	with measure_run() as m:
		central_iter, central_transfer = run_centralized(
			triples_iter,
			args.central_bus,
			q2_threshold=args.threshold,
			heavy_q2_memory_mb=args.central_q2_mem_mb,
			heavy_q2_delay_ms=args.central_q2_delay_ms,
			master_poll_ms=args.central_master_poll_ms,
		)
		central_out = list(central_iter)
		central_metrics = m()
	print(f"Centralized done: duration={central_metrics.duration_seconds:.3f}s, transfer={central_transfer:.3f}s, peak_mem={central_metrics.peak_memory_bytes/1e6:.2f}MB, triples={len(central_out)}")
	if args.do_print:
		print_triples("Centralized output", central_out, args.print_limit)
	if args.save_central:
		save_triples(args.save_central, central_out)
		print(f"Saved centralized output to {args.save_central}")

	distrib_bus_display = "unlimited" if args.distrib_bus == -1 else str(args.distrib_bus)
	print(
		f"\nGenerating {args.num} raw triples ... (threshold={args.threshold}, bus={distrib_bus_display}, distributed-fast, "
		f"worker-batch={args.worker_batch}, master-batch={args.master_batch}, poll={args.master_poll_ms}ms)"
	)
	triples_iter = generate_synthetic_triples(args.num)
	with measure_run() as m:
		distrib_iter, distrib_transfer = run_distributed(
			triples_iter,
			args.distrib_bus,
			q2_threshold=args.threshold,
			worker_batch_size=args.worker_batch,
			master_batch_size=args.master_batch,
			master_poll_ms=args.master_poll_ms,
		)
		distrib_out = list(distrib_iter)
		distrib_metrics = m()
	print(f"Distributed done: duration={distrib_metrics.duration_seconds:.3f}s, transfer={distrib_transfer:.3f}s, peak_mem={distrib_metrics.peak_memory_bytes/1e6:.2f}MB, triples={len(distrib_out)}")
	if args.do_print:
		print_triples("Distributed output", distrib_out, args.print_limit)
	if args.save_distrib:
		save_triples(args.save_distrib, distrib_out)
		print(f"Saved distributed output to {args.save_distrib}")

	# Comparison
	print("\n=== Comparison ===")
	print(f"Faster: {'Distributed' if distrib_metrics.duration_seconds < central_metrics.duration_seconds else 'Centralized'}")
	print(f"Lower memory: {'Distributed' if distrib_metrics.peak_memory_bytes < central_metrics.peak_memory_bytes else 'Centralized'}")
	central_bus_display = str(args.central_bus)
	distrib_bus_display = "unlimited" if args.distrib_bus == -1 else str(args.distrib_bus)
	print(f"BUS usage: Centralized={central_bus_display}, Distributed={distrib_bus_display}")
	
	# Performance difference
	time_diff = central_metrics.duration_seconds - distrib_metrics.duration_seconds
	time_improvement = (time_diff / central_metrics.duration_seconds) * 100
	print(f"Time difference: {time_diff:.3f}s ({time_improvement:.1f}% improvement)")


if __name__ == "__main__":
	main()