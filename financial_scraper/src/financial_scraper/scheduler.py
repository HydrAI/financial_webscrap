"""Priority queue scheduler with URL fingerprinting for deduplication."""

import hashlib
import heapq
from dataclasses import dataclass, field
from typing import Any


@dataclass(order=False)
class Request:
    """A schedulable fetch request."""

    url: str
    priority: int = 0
    meta: dict[str, Any] = field(default_factory=dict)

    def fingerprint(self) -> str:
        """Normalised URL hash for deduplication."""
        normalised = self.url.lower().rstrip("/")
        return hashlib.sha256(normalised.encode()).hexdigest()


class Scheduler:
    """Priority queue with URL fingerprinting for dedup.

    Higher ``priority`` values are processed first.
    """

    def __init__(self):
        self._queue: list[tuple[int, int, Request]] = []
        self._seen: set[str] = set()
        self._counter = 0

    def push(self, url: str, priority: int = 0, **meta) -> bool:
        """Add a URL to the queue. Returns False if already seen."""
        req = Request(url=url, priority=priority, meta=meta)
        fp = req.fingerprint()
        if fp in self._seen:
            return False
        self._seen.add(fp)
        # Negate priority so heapq (min-heap) pops highest priority first
        heapq.heappush(self._queue, (-priority, self._counter, req))
        self._counter += 1
        return True

    def pop(self) -> Request | None:
        """Remove and return the highest-priority request, or None if empty."""
        if not self._queue:
            return None
        _, _, req = heapq.heappop(self._queue)
        return req

    def is_seen(self, url: str) -> bool:
        normalised = url.lower().rstrip("/")
        fp = hashlib.sha256(normalised.encode()).hexdigest()
        return fp in self._seen

    def __len__(self) -> int:
        return len(self._queue)

    def __bool__(self) -> bool:
        return bool(self._queue)
