"""Track progress for resume capability.

Supports interval-based saves to reduce I/O, and uses orjson for faster
serialization when available.
"""

import logging
import os
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# Use orjson for faster JSON when available, fallback to stdlib json
try:
    import orjson

    def _json_dumps(data) -> bytes:
        return orjson.dumps(data)

    def _json_loads(raw: bytes | str):
        return orjson.loads(raw)

except ImportError:
    import json as _json

    def _json_dumps(data) -> bytes:  # type: ignore[misc]
        return _json.dumps(data).encode("utf-8")

    def _json_loads(raw: bytes | str):  # type: ignore[misc]
        return _json.loads(raw)


class Checkpoint:
    """Saves after each completed query. Atomic writes."""

    def __init__(self, path: Path):
        self.path = Path(path).resolve()
        self.completed_queries: set[str] = set()
        self.fetched_urls: set[str] = set()
        self.failed_urls: dict[str, int] = {}
        self.stats: dict[str, int] = {
            "total_queries": 0,
            "total_pages": 0,
            "total_words": 0,
            "failed_fetches": 0,
            "failed_extractions": 0,
        }
        self._last_save_time: float = 0.0

    def save(self):
        data = {
            "completed_queries": list(self.completed_queries),
            "fetched_urls": list(self.fetched_urls),
            "failed_urls": self.failed_urls,
            "stats": self.stats,
        }
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "wb") as f:
            f.write(_json_dumps(data))
            f.flush()
            os.fsync(f.fileno())
        # Windows: os.replace can fail if target is briefly locked (e.g. antivirus).
        for attempt in range(5):
            try:
                os.replace(tmp, self.path)
                self._last_save_time = time.monotonic()
                logger.debug(
                    "Checkpoint saved: %d fetched, %d failed",
                    len(self.fetched_urls), len(self.failed_urls),
                )
                return
            except OSError:
                if attempt < 4:
                    time.sleep(0.2 * (attempt + 1))
        # Last resort: direct write (non-atomic but avoids crash)
        self.path.write_bytes(tmp.read_bytes())
        self._last_save_time = time.monotonic()
        logger.debug(
            "Checkpoint saved (fallback): %d fetched, %d failed",
            len(self.fetched_urls), len(self.failed_urls),
        )
        try:
            tmp.unlink()
        except OSError:
            pass

    def save_if_due(self, interval_seconds: int = 300):
        """Save only if *interval_seconds* have elapsed since the last save."""
        now = time.monotonic()
        if now - self._last_save_time >= interval_seconds:
            self.save()

    def load(self):
        if not self.path.exists():
            return
        raw = self.path.read_bytes()
        data = _json_loads(raw)
        self.completed_queries = set(data.get("completed_queries", []))
        self.fetched_urls = set(data.get("fetched_urls", []))
        self.failed_urls = data.get("failed_urls", {})
        self.stats = data.get("stats", self.stats)

    def reset_queries(self):
        """Clear completed queries and stats but keep URL history."""
        self.completed_queries.clear()
        self.stats = {
            "total_queries": 0,
            "total_pages": 0,
            "total_words": 0,
            "failed_fetches": 0,
            "failed_extractions": 0,
        }
        self.save()

    def is_query_done(self, query: str) -> bool:
        return query in self.completed_queries

    def mark_query_done(self, query: str):
        self.completed_queries.add(query)
        self.stats["total_queries"] += 1
        self.save()

    def is_url_fetched(self, url: str) -> bool:
        return url in self.fetched_urls

    def mark_url_fetched(self, url: str):
        self.fetched_urls.add(url)

    def mark_url_failed(self, url: str):
        self.failed_urls[url] = self.failed_urls.get(url, 0) + 1
        self.stats["failed_fetches"] += 1

    def should_retry(self, url: str, max_retries: int = 3) -> bool:
        return self.failed_urls.get(url, 0) < max_retries
