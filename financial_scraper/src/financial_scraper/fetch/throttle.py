"""Per-domain adaptive rate limiters.

- ``DomainThrottler``: async version for the aiohttp fetch pipeline.
- ``SyncDomainThrottler``: thread-safe sync version for the transcript pipeline.
"""

import asyncio
import threading
import time
from collections import defaultdict

from aiolimiter import AsyncLimiter

# Status codes that indicate blocking / overload
BLOCKED_CODES = frozenset({401, 403, 407, 429, 444, 500, 502, 503, 504})


class DomainThrottler:
    """Per-domain adaptive rate limiting (async).

    Each domain gets its own AsyncLimiter instance.
    Delays adapt based on server responses.
    """

    def __init__(self, base_rate: float = 1.0, max_delay: float = 30.0,
                 max_per_domain: int = 3):
        self._base_rate = base_rate
        self._max_delay = max_delay
        self._max_per_domain = max_per_domain
        self._limiters: dict[str, AsyncLimiter] = {}
        self._extra_delays: dict[str, float] = defaultdict(float)
        self._semaphores: dict[str, asyncio.Semaphore] = {}

    def _get_limiter(self, domain: str) -> AsyncLimiter:
        if domain not in self._limiters:
            self._limiters[domain] = AsyncLimiter(1, 1.0 / self._base_rate)
            self._semaphores[domain] = asyncio.Semaphore(self._max_per_domain)
        return self._limiters[domain]

    async def acquire(self, domain: str):
        """Acquire rate limit + semaphore for domain. Blocks until allowed."""
        limiter = self._get_limiter(domain)
        await self._semaphores[domain].acquire()
        async with limiter:
            extra = self._extra_delays.get(domain, 0)
            if extra > 0:
                await asyncio.sleep(extra)

    def release(self, domain: str):
        """Release domain semaphore after request completes."""
        if domain in self._semaphores:
            self._semaphores[domain].release()

    def report_success(self, domain: str):
        """Halve extra delay on success (minimum 0)."""
        if domain in self._extra_delays:
            self._extra_delays[domain] = max(0, self._extra_delays[domain] / 2)

    def report_failure(self, domain: str, status: int,
                       retry_after: float | None = None):
        """Increase delay based on failure type."""
        current = self._extra_delays.get(domain, 1.0)
        if current == 0:
            current = 1.0
        if status == 429:
            if retry_after is not None:
                self._extra_delays[domain] = min(retry_after, self._max_delay)
            else:
                self._extra_delays[domain] = min(current * 2, self._max_delay)
        elif status == 403:
            self._extra_delays[domain] = min(current * 1.5, self._max_delay)
        elif status >= 500:
            self._extra_delays[domain] = min(current * 1.25, self._max_delay)


class SyncDomainThrottler:
    """Thread-safe per-domain rate limiter for synchronous code.

    Uses ``threading.Semaphore`` for per-domain concurrency limits and
    adaptive delays that increase on failure and decrease on success.
    """

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        max_per_domain: int = 2,
    ):
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._max_per_domain = max_per_domain

        self._lock = threading.Lock()
        self._semaphores: dict[str, threading.Semaphore] = {}
        self._delays: dict[str, float] = {}
        self._last_request: dict[str, float] = {}

    def _ensure_domain(self, domain: str):
        if domain not in self._semaphores:
            self._semaphores[domain] = threading.Semaphore(self._max_per_domain)
            self._delays[domain] = self._base_delay

    def acquire(self, domain: str):
        """Block until the domain's rate limit allows a request."""
        with self._lock:
            self._ensure_domain(domain)
            sem = self._semaphores[domain]

        sem.acquire()

        # Enforce minimum delay between requests to this domain
        with self._lock:
            delay = self._delays.get(domain, self._base_delay)
            last = self._last_request.get(domain, 0)
            elapsed = time.monotonic() - last
            wait = max(0, delay - elapsed)

        if wait > 0:
            time.sleep(wait)

        with self._lock:
            self._last_request[domain] = time.monotonic()

    def release(self, domain: str):
        """Release the domain semaphore."""
        with self._lock:
            if domain in self._semaphores:
                self._semaphores[domain].release()

    def report_success(self, domain: str):
        """Decrease delay on success (halve, minimum base_delay)."""
        with self._lock:
            current = self._delays.get(domain, self._base_delay)
            self._delays[domain] = max(self._base_delay, current * 0.75)

    def report_failure(self, domain: str, status: int):
        """Increase delay based on failure status code."""
        with self._lock:
            current = self._delays.get(domain, self._base_delay)
            if current == 0:
                current = self._base_delay
            if status == 429:
                self._delays[domain] = min(current * 3, self._max_delay)
            elif status in (401, 403):
                self._delays[domain] = min(current * 2, self._max_delay)
            elif status >= 500:
                self._delays[domain] = min(current * 1.5, self._max_delay)

    def get_delay(self, domain: str) -> float:
        """Current delay for a domain (for logging/debugging)."""
        with self._lock:
            return self._delays.get(domain, self._base_delay)
