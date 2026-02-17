"""Tests for financial_scraper.fetch.robots."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.robotparser import RobotFileParser

from financial_scraper.fetch.robots import RobotChecker


def _mock_session_with_robots(text, status=200):
    """Create a mock session whose .get() returns an async CM with .status and .text()."""
    resp = MagicMock()
    resp.status = status
    resp.text = AsyncMock(return_value=text)

    # Make resp work as async context manager
    cm = AsyncMock()
    cm.__aenter__.return_value = resp
    cm.__aexit__.return_value = False

    session = MagicMock()
    session.get.return_value = cm
    return session


class TestIsAllowed:
    def test_allowed_url(self):
        checker = RobotChecker()
        robots_txt = "User-agent: *\nDisallow: /private\n"
        session = _mock_session_with_robots(robots_txt)
        result = asyncio.run(checker.is_allowed("https://example.com/public", session))
        assert result is True

    def test_disallowed_url(self):
        checker = RobotChecker()
        robots_txt = "User-agent: *\nDisallow: /private\n"
        session = _mock_session_with_robots(robots_txt)
        result = asyncio.run(checker.is_allowed("https://example.com/private/page", session))
        assert result is False

    def test_caches_robots(self):
        checker = RobotChecker()
        robots_txt = "User-agent: *\nDisallow:\n"
        session = _mock_session_with_robots(robots_txt)
        asyncio.run(checker.is_allowed("https://example.com/a", session))
        asyncio.run(checker.is_allowed("https://example.com/b", session))
        # Only one fetch call (cached)
        assert session.get.call_count == 1

    def test_permissive_on_fetch_failure(self):
        checker = RobotChecker()
        session = MagicMock()
        session.get.side_effect = Exception("Network error")
        result = asyncio.run(checker.is_allowed("https://example.com/page", session))
        assert result is True

    def test_permissive_on_404(self):
        checker = RobotChecker()
        session = _mock_session_with_robots("", status=404)
        result = asyncio.run(checker.is_allowed("https://example.com/page", session))
        assert result is True


class TestGetCrawlDelay:
    def test_no_cached_parser_returns_none(self):
        checker = RobotChecker()
        assert checker.get_crawl_delay("example.com") is None

    def test_returns_delay_when_set(self):
        checker = RobotChecker()
        robots_txt = "User-agent: *\nCrawl-delay: 5\nDisallow:\n"
        session = _mock_session_with_robots(robots_txt)
        asyncio.run(checker.is_allowed("https://example.com/page", session))
        delay = checker.get_crawl_delay("example.com")
        assert delay == 5.0

    def test_returns_none_when_no_delay(self):
        checker = RobotChecker()
        robots_txt = "User-agent: *\nDisallow:\n"
        session = _mock_session_with_robots(robots_txt)
        asyncio.run(checker.is_allowed("https://example.com/page", session))
        delay = checker.get_crawl_delay("example.com")
        assert delay is None
