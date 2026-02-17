"""Per-domain adaptive rate limiter using aiolimiter's leaky bucket."""

import asyncio
from collections import defaultdict

from aiolimiter import AsyncLimiter


class DomainThrottler:
    """Per-domain adaptive rate limiting.

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
        current = self._extra_delays.get(domain, 1.0) or 1.0
        if status == 429:
            if retry_after is not None:
                self._extra_delays[domain] = min(retry_after, self._max_delay)
            else:
                self._extra_delays[domain] = min(current * 2, self._max_delay)
        elif status == 403:
            self._extra_delays[domain] = min(current * 1.5, self._max_delay)
        elif status >= 500:
            self._extra_delays[domain] = min(current * 1.25, self._max_delay)
