import time
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Generator, Iterable, Iterator, Tuple


@dataclass
class RunMetrics:
	duration_seconds: float
	peak_memory_bytes: int


@contextmanager
def measure_run() -> Generator[Callable[[], RunMetrics], None, None]:
	start = time.perf_counter()
	tracemalloc.start()
	yield lambda: RunMetrics(
		duration_seconds=time.perf_counter() - start,
		peak_memory_bytes=tracemalloc.get_traced_memory()[1],
	)
	tracemalloc.stop()


