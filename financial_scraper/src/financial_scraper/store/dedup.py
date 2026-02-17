"""URL + content hash deduplication with persistence."""

import hashlib
import json
from pathlib import Path
from urllib.parse import urlparse, urldefrag


class Deduplicator:
    """URL + content hash deduplication."""

    def __init__(self):
        self._seen_urls: set[str] = set()
        self._seen_content: set[str] = set()

    def _normalize_url(self, url: str) -> str:
        url = urldefrag(url)[0]  # remove fragment
        url = url.lower().rstrip("/")
        return url

    def _hash_url(self, url: str) -> str:
        normalized = self._normalize_url(url)
        return hashlib.sha256(normalized.encode()).hexdigest()

    def _hash_content(self, content: str) -> str:
        return hashlib.sha256(content[:2000].encode()).hexdigest()

    def is_duplicate_url(self, url: str) -> bool:
        h = self._hash_url(url)
        return h in self._seen_urls

    def is_duplicate_content(self, content: str) -> bool:
        h = self._hash_content(content)
        return h in self._seen_content

    def mark_seen(self, url: str, content: str):
        self._seen_urls.add(self._hash_url(url))
        if content:
            self._seen_content.add(self._hash_content(content))

    def content_hash(self, content: str) -> str:
        return self._hash_content(content)

    def save(self, path: Path):
        data = {
            "urls": list(self._seen_urls),
            "content": list(self._seen_content),
        }
        with open(path, "w") as f:
            json.dump(data, f)

    def load(self, path: Path):
        if not path.exists():
            return
        with open(path) as f:
            data = json.load(f)
        self._seen_urls = set(data.get("urls", []))
        self._seen_content = set(data.get("content", []))
