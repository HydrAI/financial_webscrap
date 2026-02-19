"""Track progress for resume capability."""

import json
import os
from pathlib import Path


class Checkpoint:
    """Saves after each completed query. Atomic writes."""

    def __init__(self, path: Path):
        self.path = Path(path)
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

    def save(self):
        data = {
            "completed_queries": list(self.completed_queries),
            "fetched_urls": list(self.fetched_urls),
            "failed_urls": self.failed_urls,
            "stats": self.stats,
        }
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, self.path)

    def load(self):
        if not self.path.exists():
            return
        with open(self.path) as f:
            data = json.load(f)
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
