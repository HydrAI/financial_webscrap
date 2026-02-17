"""Async HTTP client with fingerprint management and per-domain throttling."""

import asyncio
import logging
from dataclasses import dataclass
from urllib.parse import urlparse

import aiohttp
import chardet

from ..config import ScraperConfig
from .fingerprints import get_fingerprint_for_domain, ALL_FINGERPRINTS
from .throttle import DomainThrottler
from .robots import RobotChecker
from .tor import TorManager

logger = logging.getLogger(__name__)


@dataclass
class FetchResult:
    url: str
    status: int
    html: str | None
    content_type: str
    content_bytes: bytes | None
    error: str | None
    response_headers: dict | None


class FetchClient:
    """Async HTTP client with fingerprint management, per-domain throttling, and Tor support."""

    def __init__(self, config: ScraperConfig, throttler: DomainThrottler,
                 robot_checker: RobotChecker,
                 tor_manager: TorManager | None = None):
        self._config = config
        self._throttler = throttler
        self._robot_checker = robot_checker
        self._tor = tor_manager
        self._session: aiohttp.ClientSession | None = None
        self._global_semaphore = asyncio.Semaphore(config.max_concurrent_total)

    async def __aenter__(self):
        if self._tor and self._tor.is_available:
            from aiohttp_socks import ProxyConnector
            connector = ProxyConnector.from_url(
                self._tor.get_proxy_url(),
                limit=self._config.max_concurrent_total,
                limit_per_host=self._config.max_concurrent_per_domain,
                ttl_dns_cache=300,
            )
        else:
            connector = aiohttp.TCPConnector(
                limit=self._config.max_concurrent_total,
                limit_per_host=self._config.max_concurrent_per_domain,
                ttl_dns_cache=300,
                enable_cleanup_closed=True,
            )
        self._session = aiohttp.ClientSession(connector=connector)
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()

    async def fetch(self, url: str) -> FetchResult:
        """Fetch a URL with fingerprinting, rate limiting, robots compliance."""
        domain = urlparse(url).netloc.lower()

        # 1. Check robots.txt
        if self._config.respect_robots:
            if not await self._robot_checker.is_allowed(url, self._session):
                return FetchResult(url=url, status=0, html=None, content_type="",
                                   content_bytes=None, error="robots.txt disallowed",
                                   response_headers=None)

        # 2. Get fingerprint
        fp = get_fingerprint_for_domain(domain)
        headers = fp.to_headers()
        headers["Connection"] = "keep-alive"

        # 3. Acquire throttle + semaphore
        await self._throttler.acquire(domain)
        try:
            async with self._global_semaphore:
                return await self._do_fetch(url, domain, headers, fp)
        finally:
            self._throttler.release(domain)

    async def _do_fetch(self, url: str, domain: str,
                        headers: dict, fp, retry: bool = True) -> FetchResult:
        try:
            timeout = aiohttp.ClientTimeout(total=self._config.fetch_timeout)
            async with self._session.get(url, headers=headers, timeout=timeout,
                                         allow_redirects=True, ssl=False) as resp:
                ct = resp.headers.get("Content-Type", "").lower()
                resp_headers = dict(resp.headers)

                if resp.status == 200:
                    self._throttler.report_success(domain)
                    if "application/pdf" in ct or url.lower().endswith(".pdf"):
                        data = await resp.read()
                        return FetchResult(url=url, status=200, html=None,
                                           content_type=ct, content_bytes=data,
                                           error=None, response_headers=resp_headers)
                    else:
                        html = await self._decode(resp)
                        return FetchResult(url=url, status=200, html=html,
                                           content_type=ct, content_bytes=None,
                                           error=None, response_headers=resp_headers)

                # Handle 403/429
                if resp.status in (403, 429):
                    retry_after = resp.headers.get("Retry-After")
                    ra_val = float(retry_after) if retry_after and retry_after.isdigit() else None
                    self._throttler.report_failure(domain, resp.status, ra_val)

                    if retry:
                        if ra_val:
                            await asyncio.sleep(ra_val)
                        # Retry with different fingerprint
                        idx = (ALL_FINGERPRINTS.index(fp) + 1) % len(ALL_FINGERPRINTS)
                        new_fp = ALL_FINGERPRINTS[idx]
                        new_headers = new_fp.to_headers()
                        new_headers["Connection"] = "keep-alive"
                        return await self._do_fetch(url, domain, new_headers,
                                                    new_fp, retry=False)

                return FetchResult(url=url, status=resp.status, html=None,
                                   content_type=ct, content_bytes=None,
                                   error=f"HTTP {resp.status}",
                                   response_headers=resp_headers)

        except Exception as e:
            return FetchResult(url=url, status=0, html=None, content_type="",
                               content_bytes=None, error=str(e),
                               response_headers=None)

    async def _decode(self, resp: aiohttp.ClientResponse) -> str:
        try:
            return await resp.text()
        except UnicodeDecodeError:
            raw = await resp.read()
            detected = chardet.detect(raw)
            enc = detected.get("encoding", "utf-8") or "utf-8"
            return raw.decode(enc, errors="ignore")

    async def fetch_batch(self, urls: list[str]) -> list[FetchResult]:
        """Fetch multiple URLs concurrently, respecting all limits."""
        tasks = [asyncio.create_task(self.fetch(url)) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out = []
        for r in results:
            if isinstance(r, Exception):
                out.append(FetchResult(url="", status=0, html=None, content_type="",
                                       content_bytes=None, error=str(r),
                                       response_headers=None))
            else:
                out.append(r)
        return out
