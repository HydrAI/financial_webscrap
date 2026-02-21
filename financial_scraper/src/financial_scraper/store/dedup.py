"""URL + content hash deduplication with persistence."""

import hashlib
import json
from pathlib import Path
from urllib.parse import urlparse, urldefrag

try:
    from datasketch import MinHash, MinHashLSH

    _HAS_DATASKETCH = True
except ImportError:
    _HAS_DATASKETCH = False


class Deduplicator:
    """URL + content hash deduplication."""

    NUM_PERM = 128
    LSH_THRESHOLD = 0.85
    SHINGLE_SIZE = 3

    def __init__(self):
        self._seen_urls: set[str] = set()
        self._seen_content: set[str] = set()
        # Fuzzy dedup (gracefully disabled when datasketch missing)
        self._lsh: object | None = None
        self._minhashes: dict[str, "MinHash"] = {}
        self._doc_counter: int = 0
        if _HAS_DATASKETCH:
            self._lsh = MinHashLSH(threshold=self.LSH_THRESHOLD, num_perm=self.NUM_PERM)

    def _normalize_url(self, url: str) -> str:
        url = urldefrag(url)[0]  # remove fragment
        url = url.lower().rstrip("/")
        return url

    def _hash_url(self, url: str) -> str:
        normalized = self._normalize_url(url)
        return hashlib.sha256(normalized.encode()).hexdigest()

    def _hash_content(self, content: str) -> str:
        return hashlib.sha256(content[:2000].encode()).hexdigest()

    def _minhash_content(self, content: str) -> "MinHash | None":
        if not _HAS_DATASKETCH:
            return None
        words = content.split()
        m = MinHash(num_perm=self.NUM_PERM)
        if len(words) < self.SHINGLE_SIZE:
            # For very short content, hash the whole thing as one shingle
            m.update(" ".join(words).encode("utf-8"))
        else:
            for i in range(len(words) - self.SHINGLE_SIZE + 1):
                shingle = " ".join(words[i : i + self.SHINGLE_SIZE])
                m.update(shingle.encode("utf-8"))
        return m

    def is_duplicate_url(self, url: str) -> bool:
        h = self._hash_url(url)
        return h in self._seen_urls

    def is_duplicate_content(self, content: str) -> bool:
        h = self._hash_content(content)
        if h in self._seen_content:
            return True
        # Fuzzy check via MinHash LSH
        if self._lsh is not None and content.strip():
            m = self._minhash_content(content)
            if m is not None:
                matches = self._lsh.query(m)
                if matches:
                    return True
        return False

    def mark_seen(self, url: str, content: str):
        self._seen_urls.add(self._hash_url(url))
        if content:
            self._seen_content.add(self._hash_content(content))
            # Insert into LSH index
            if self._lsh is not None:
                m = self._minhash_content(content)
                if m is not None:
                    key = str(self._doc_counter)
                    self._lsh.insert(key, m)
                    self._minhashes[key] = m
                    self._doc_counter += 1

    def content_hash(self, content: str) -> str:
        return self._hash_content(content)

    def save(self, path: Path):
        data = {
            "urls": list(self._seen_urls),
            "content": list(self._seen_content),
        }
        if self._minhashes:
            data["minhash"] = {
                k: m.hashvalues.tobytes().hex() for k, m in self._minhashes.items()
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
        # Restore MinHash state
        minhash_data = data.get("minhash", {})
        if minhash_data and _HAS_DATASKETCH:
            import numpy as np

            self._lsh = MinHashLSH(threshold=self.LSH_THRESHOLD, num_perm=self.NUM_PERM)
            self._minhashes = {}
            self._doc_counter = 0
            for key, hex_digest in minhash_data.items():
                m = MinHash(num_perm=self.NUM_PERM)
                m.hashvalues = np.frombuffer(bytes.fromhex(hex_digest), dtype=np.uint64).copy()
                self._lsh.insert(key, m)
                self._minhashes[key] = m
                self._doc_counter = max(self._doc_counter, int(key) + 1)
