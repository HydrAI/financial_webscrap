"""DuckDuckGo search with retry + rate limiting + news support."""

import asyncio
import logging
import random
import sys
import time
from dataclasses import dataclass

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ..config import ScraperConfig
from ..fetch.tor import TorManager

# CRITICAL: Windows asyncio compatibility for curl-cffi used by duckduckgo-search
if sys.platform.lower().startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SearchResult:
    url: str
    title: str
    snippet: str
    search_rank: int
    query: str


class DDGSearcher:
    """DuckDuckGo search with robust rate limit handling and Tor integration."""

    def __init__(self, config: ScraperConfig,
                 tor_manager: TorManager | None = None):
        self._config = config
        self._tor = tor_manager
        self._consecutive_ratelimits = 0
        self._last_search_time = 0.0

    def _get_proxy(self) -> str | None:
        if self._tor and self._tor.is_available:
            return self._tor.get_ddgs_proxy()
        return self._config.proxy

    def _get_ddgs_class(self):
        try:
            from duckduckgo_search import DDGS
            return DDGS
        except ImportError:
            from ddgs import DDGS
            return DDGS

    def _do_search_inner(self, query: str, max_results: int) -> list[dict]:
        """Execute DDG search. May raise RatelimitException."""
        DDGS = self._get_ddgs_class()
        proxy = self._get_proxy()

        with DDGS(proxy=proxy) as ddgs:
            if self._config.search_type == "news":
                raw = list(ddgs.news(
                    query,
                    max_results=max_results,
                    region=self._config.ddg_region,
                    timelimit=self._config.ddg_timelimit,
                ))
            else:
                raw = list(ddgs.text(
                    query,
                    max_results=max_results,
                    region=self._config.ddg_region,
                    safesearch="off",
                    timelimit=self._config.ddg_timelimit,
                    backend=self._config.ddg_backend,
                ))
        return raw

    def _do_search_with_retry(self, query: str, max_results: int) -> list[dict]:
        """Search with tenacity retry on ratelimit."""
        from duckduckgo_search.exceptions import RatelimitException

        for attempt in range(3):
            try:
                return self._do_search_inner(query, max_results)
            except RatelimitException:
                logger.warning(f"DDG ratelimit on attempt {attempt + 1}/3 for '{query[:40]}...'")
                if self._tor and self._tor.is_available:
                    self._tor.on_ratelimit()
                wait = 10 * (2 ** attempt)  # 10s, 20s, 40s
                time.sleep(wait)
            except Exception as e:
                logger.warning(f"DDG search error: {e}")
                if attempt < 2:
                    time.sleep(2 ** (attempt + 1))
        return []

    def search(self, query: str, max_results: int) -> list[SearchResult]:
        """Full search with pre-delay and rate limit tracking."""
        # Pre-delay
        delay = random.uniform(self._config.search_delay_min,
                               self._config.search_delay_max)
        if self._consecutive_ratelimits > 0:
            delay += self._consecutive_ratelimits * 15
            logger.info(f"Extra cooldown: {self._consecutive_ratelimits * 15}s "
                        f"(consecutive ratelimits: {self._consecutive_ratelimits})")

        elapsed = time.time() - self._last_search_time
        if elapsed < delay:
            time.sleep(delay - elapsed)

        # Tor circuit renewal check
        if self._tor and self._tor.is_available and self._tor.should_renew():
            self._tor.renew_circuit()

        self._last_search_time = time.time()

        raw = self._do_search_with_retry(query, max_results)

        if raw:
            self._consecutive_ratelimits = max(0, self._consecutive_ratelimits - 1)
            if self._tor:
                self._tor.on_search_completed()
        else:
            self._consecutive_ratelimits += 1

        results = []
        for i, r in enumerate(raw):
            url = r.get("href") or r.get("url", "")
            if not url:
                continue
            results.append(SearchResult(
                url=url,
                title=r.get("title", ""),
                snippet=r.get("body", "") or r.get("snippet", ""),
                search_rank=i + 1,
                query=query,
            ))

        return results

    def search_news(self, query: str, max_results: int) -> list[SearchResult]:
        """Alias that forces news search mode."""
        original = self._config.search_type
        # Temporarily override - since config is frozen, we work around it
        old_type = self._config.search_type
        try:
            object.__setattr__(self._config, 'search_type', 'news')
            return self.search(query, max_results)
        finally:
            object.__setattr__(self._config, 'search_type', old_type)
