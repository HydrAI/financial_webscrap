"""Thread-safe proxy rotation with pluggable strategies.

Adapted from Scrapling's proxy rotation pattern. Supports cyclic (round-robin),
random, and weighted (prefer working proxies) strategies.
"""

from __future__ import annotations

import json
import logging
import random
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

_MAX_CONSECUTIVE_FAILURES = 5


class ProxyRotator:
    """Thread-safe proxy rotator.

    Args:
        proxies: List of proxy URLs (e.g. ``["http://user:pass@host:port", ...]``).
        strategy: ``"cyclic"`` (round-robin), ``"random"``, or ``"weighted"``
            (prefer proxies with fewer recent failures).
    """

    def __init__(
        self,
        proxies: list[str],
        strategy: str = "cyclic",
        max_failures: int = _MAX_CONSECUTIVE_FAILURES,
    ):
        if not proxies:
            raise ValueError("At least one proxy is required")

        self._all_proxies = list(proxies)
        self._active: list[str] = list(proxies)
        self._strategy = strategy
        self._max_failures = max_failures

        self._index = 0
        self._lock = threading.Lock()

        # Tracking
        self._consecutive_failures: dict[str, int] = defaultdict(int)
        self._successes: dict[str, int] = defaultdict(int)

    def next(self) -> str:
        """Return the next proxy URL according to the configured strategy.

        Raises ``RuntimeError`` if all proxies have been removed.
        """
        with self._lock:
            if not self._active:
                raise RuntimeError("All proxies exhausted (all hit max failures)")
            if self._strategy == "random":
                return random.choice(self._active)
            if self._strategy == "weighted":
                return self._weighted_pick()
            # cyclic (default)
            proxy = self._active[self._index % len(self._active)]
            self._index += 1
            return proxy

    def report_success(self, proxy: str):
        """Mark a proxy as working (resets its failure counter)."""
        with self._lock:
            self._consecutive_failures[proxy] = 0
            self._successes[proxy] += 1

    def report_error(self, proxy: str):
        """Record a failure. Removes the proxy after *max_failures* consecutive errors."""
        with self._lock:
            self._consecutive_failures[proxy] += 1
            if self._consecutive_failures[proxy] >= self._max_failures:
                if proxy in self._active:
                    self._active.remove(proxy)
                    logger.warning(
                        "Proxy removed after %d consecutive failures: %s "
                        "(%d active remaining)",
                        self._max_failures,
                        _redact(proxy),
                        len(self._active),
                    )

    def add_proxies(self, proxies: list[str]):
        """Add new proxies, deduplicating against existing ones. Thread-safe."""
        with self._lock:
            existing = set(self._all_proxies)
            added = 0
            for p in proxies:
                if p not in existing:
                    self._all_proxies.append(p)
                    self._active.append(p)
                    existing.add(p)
                    added += 1
            if added:
                logger.info("Added %d new proxies (%d total active)", added, len(self._active))
            return added

    def reset(self):
        """Restore all proxies (including previously removed ones)."""
        with self._lock:
            self._active = list(self._all_proxies)
            self._consecutive_failures.clear()
            self._successes.clear()
            self._index = 0

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    # ---- internals --------------------------------------------------------

    def _weighted_pick(self) -> str:
        """Pick a proxy weighted by success rate (more successes → higher weight)."""
        weights = []
        for p in self._active:
            s = self._successes.get(p, 0)
            f = self._consecutive_failures.get(p, 0)
            # Weight: base 1 + successes, penalised by consecutive failures
            weights.append(max(1, 1 + s - f * 2))
        return random.choices(self._active, weights=weights, k=1)[0]


def load_proxies(path: str) -> list[str]:
    """Load proxy URLs from a file (one per line, ``#`` comments allowed)."""
    proxies = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                proxies.append(line)
    return proxies


def _redact(proxy: str) -> str:
    """Redact credentials from a proxy URL for logging."""
    if "@" in proxy:
        scheme_rest = proxy.split("://", 1)
        if len(scheme_rest) == 2:
            return f"{scheme_rest[0]}://***@{scheme_rest[1].split('@', 1)[-1]}"
    return proxy


# ---------------------------------------------------------------------------
# Auto-fetch free proxies
# ---------------------------------------------------------------------------

_PROXY_SOURCES = [
    (
        "proxyscrape",
        "https://api.proxyscrape.com/v4/free-proxy-list/get"
        "?request=display_proxies&proxy_format=protocolipport"
        "&format=json&limit=40&protocol=http&timeout=5000&anonymity=elite,anonymous",
    ),
    (
        "geonode",
        "https://proxylist.geonode.com/api/proxy-list"
        "?protocols=http%2Chttps&limit=30&sort_by=lastChecked&sort_type=desc&speed=fast",
    ),
    (
        "thespeedx",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    ),
]


def _parse_proxy_response(name: str, text: str) -> list[str]:
    """Parse proxy list from a source response."""
    proxies: list[str] = []
    if name == "proxyscrape":
        data = json.loads(text)
        for p in data.get("proxies", []):
            ip, port = p.get("ip", ""), p.get("port", "")
            proto = p.get("protocol", "http")
            if ip and port:
                proxies.append(f"{proto}://{ip}:{port}")
    elif name == "geonode":
        data = json.loads(text)
        for p in data.get("data", []):
            ip, port = p.get("ip", ""), p.get("port", "")
            protocols = p.get("protocols", ["http"])
            proto = "https" if "https" in protocols else "http"
            if ip and port:
                proxies.append(f"{proto}://{ip}:{port}")
    elif name == "thespeedx":
        for line in text.strip().split("\n")[:50]:
            line = line.strip()
            if line and ":" in line:
                proxies.append(f"http://{line}")
    return proxies


def _check_proxy(proxy_url: str) -> str | None:
    """Validate a proxy by reaching httpbin. Returns proxy URL or None."""
    from .curl_client import CurlSession

    try:
        s = CurlSession(browser="chrome", proxy=proxy_url)
        resp = s.get("https://httpbin.org/ip", timeout=5)
        s.close()
        if resp.status_code == 200:
            return proxy_url
    except Exception:
        pass
    return None


def auto_fetch_proxies(
    rotator: ProxyRotator | None = None,
    max_validated: int = 5,
    validate_workers: int = 8,
) -> ProxyRotator | None:
    """Fetch and validate free proxies from public sources.

    If *rotator* is provided, validated proxies are added to it and it is
    returned.  Otherwise a new ``ProxyRotator`` (weighted strategy) is created.

    Returns ``None`` only if zero proxies validated successfully.
    """
    from .curl_client import CurlSession

    logger.info("Fetching free proxies...")
    candidates: list[str] = []
    session = CurlSession(browser="chrome")

    for name, url in _PROXY_SOURCES:
        # Skip the GitHub fallback list if we already have enough candidates
        if name == "thespeedx" and candidates:
            continue
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                parsed = _parse_proxy_response(name, resp.text)
                candidates.extend(parsed)
                logger.debug("  %s: %d proxies", name, len(parsed))
        except Exception as exc:
            logger.debug("  %s failed: %s", name, exc)

    session.close()

    # Deduplicate
    candidates = list(dict.fromkeys(candidates))
    logger.info("Fetched %d proxy candidates, validating...", len(candidates))

    if not candidates:
        logger.warning("No proxy candidates found from any source")
        return rotator

    # Validate concurrently
    working: list[str] = []
    with ThreadPoolExecutor(max_workers=validate_workers) as executor:
        futures = {executor.submit(_check_proxy, p): p for p in candidates[:40]}
        for future in as_completed(futures, timeout=30):
            try:
                result = future.result()
                if result:
                    working.append(result)
                    if len(working) >= max_validated:
                        break
            except Exception:
                pass

    logger.info("Validated %d working proxies out of %d candidates", len(working), len(candidates))

    if not working:
        return rotator

    if rotator is not None:
        rotator.add_proxies(working)
        return rotator

    return ProxyRotator(working, strategy="weighted")
