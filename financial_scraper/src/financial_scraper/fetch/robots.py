"""robots.txt compliance with caching."""

import logging
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp

logger = logging.getLogger(__name__)


class RobotChecker:
    """Cached robots.txt parser with async fetching."""

    def __init__(self, user_agent: str = "Mozilla/5.0"):
        self._cache: dict[str, RobotFileParser | None] = {}
        self._user_agent = user_agent

    async def _fetch_robots(self, domain_url: str,
                            session: aiohttp.ClientSession) -> RobotFileParser | None:
        robots_url = f"{domain_url}/robots.txt"
        try:
            async with session.get(
                robots_url,
                timeout=aiohttp.ClientTimeout(total=5),
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    text = await resp.text(errors="ignore")
                    parser = RobotFileParser()
                    parser.parse(text.splitlines())
                    return parser
        except Exception:
            pass
        return None

    async def is_allowed(self, url: str, session: aiohttp.ClientSession) -> bool:
        """Check if URL is allowed by robots.txt. Permissive default on errors."""
        parsed = urlparse(url)
        domain_url = f"{parsed.scheme}://{parsed.netloc}"

        if domain_url not in self._cache:
            self._cache[domain_url] = await self._fetch_robots(domain_url, session)

        parser = self._cache[domain_url]
        if parser is None:
            return True  # permissive default
        try:
            return parser.can_fetch(self._user_agent, url)
        except Exception:
            return True

    def get_crawl_delay(self, domain: str) -> float | None:
        """Return Crawl-delay directive if present."""
        for key, parser in self._cache.items():
            if domain in key and parser is not None:
                try:
                    delay = parser.crawl_delay(self._user_agent)
                    return float(delay) if delay else None
                except Exception:
                    return None
        return None
