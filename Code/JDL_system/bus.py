import threading
from collections import deque
from typing import Deque, Iterable, List, Optional, Tuple

Triple = Tuple[str, str, str]


class BoundedBus:
	"""A thread-safe bounded BUS measured in number of triples.

	The BUS stores triples output by q1 and consumed by q2. Capacity is the
	number of triples it can hold concurrently. Producers block when full,
	consumers block when empty.
	"""

	def __init__(self, capacity_triples: int) -> None:
		if capacity_triples <= 0:
			raise ValueError("capacity_triples must be > 0")
		self._capacity: int = capacity_triples
		self._queue: Deque[Triple] = deque()
		self._cv = threading.Condition()
		self._closed: bool = False

	@property
	def capacity(self) -> int:
		return self._capacity

	def put(self, triple: Triple) -> None:
		with self._cv:
			while not self._closed and len(self._queue) >= self._capacity:
				self._cv.wait()
			if self._closed:
				raise RuntimeError("BUS is closed")
			self._queue.append(triple)
			self._cv.notify_all()

	def try_put(self, triple: Triple) -> bool:
		"""Attempt to put without blocking. Returns True on success, False if full or closed."""
		with self._cv:
			if self._closed or len(self._queue) >= self._capacity:
				return False
			self._queue.append(triple)
			self._cv.notify_all()
			return True

	def put_many(self, triples: Iterable[Triple]) -> None:
		for t in triples:
			self.put(t)

	def get(self, timeout: Optional[float] = None) -> Optional[Triple]:
		with self._cv:
			if timeout is None:
				while not self._closed and len(self._queue) == 0:
					self._cv.wait()
			else:
				# Fallback to simple wait without precise timeout to avoid complexity.
				while not self._closed and len(self._queue) == 0:
					self._cv.wait(timeout)
			if len(self._queue) == 0:
				return None
			item = self._queue.popleft()
			self._cv.notify_all()
			return item

	def drain_all(self) -> List[Triple]:
		with self._cv:
			items = list(self._queue)
			self._queue.clear()
			self._cv.notify_all()
			return items

	def close(self) -> None:
		with self._cv:
			self._closed = True
			self._cv.notify_all()

	def size(self) -> int:
		with self._cv:
			return len(self._queue)


