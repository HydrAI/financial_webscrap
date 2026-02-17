"""Tests for financial_scraper.search.duckduckgo."""

from unittest.mock import MagicMock, patch

from financial_scraper.config import ScraperConfig
from financial_scraper.search.duckduckgo import DDGSearcher, SearchResult


class TestSearchResult:
    def test_construction(self):
        r = SearchResult(
            url="https://example.com",
            title="Test",
            snippet="A snippet",
            search_rank=1,
            query="test query",
        )
        assert r.url == "https://example.com"
        assert r.search_rank == 1
        assert r.query == "test query"


class TestDDGSearcherSearch:
    def _make_searcher(self, **config_overrides):
        defaults = {
            "search_delay_min": 0.0,
            "search_delay_max": 0.0,
        }
        defaults.update(config_overrides)
        cfg = ScraperConfig(**defaults)
        return DDGSearcher(cfg)

    @patch("financial_scraper.search.duckduckgo.time")
    @patch("financial_scraper.search.duckduckgo.random")
    def test_successful_search(self, mock_random, mock_time):
        mock_random.uniform.return_value = 0.0
        mock_time.time.return_value = 1000.0
        mock_time.sleep = MagicMock()

        searcher = self._make_searcher()

        raw_results = [
            {"href": "https://example.com/1", "title": "Result 1", "body": "Snippet 1"},
            {"href": "https://example.com/2", "title": "Result 2", "body": "Snippet 2"},
        ]

        with patch.object(searcher, "_do_search_with_retry", return_value=raw_results):
            results = searcher.search("test query", 5)

        assert len(results) == 2
        assert results[0].url == "https://example.com/1"
        assert results[0].search_rank == 1
        assert results[1].search_rank == 2
        assert results[0].query == "test query"

    @patch("financial_scraper.search.duckduckgo.time")
    @patch("financial_scraper.search.duckduckgo.random")
    def test_empty_results(self, mock_random, mock_time):
        mock_random.uniform.return_value = 0.0
        mock_time.time.return_value = 1000.0
        mock_time.sleep = MagicMock()

        searcher = self._make_searcher()

        with patch.object(searcher, "_do_search_with_retry", return_value=[]):
            results = searcher.search("test query", 5)

        assert results == []


class TestGetProxy:
    def test_no_tor_no_proxy(self):
        cfg = ScraperConfig(proxy=None, use_tor=False)
        searcher = DDGSearcher(cfg)
        assert searcher._get_proxy() is None

    def test_no_tor_with_proxy(self):
        cfg = ScraperConfig(proxy="socks5://127.0.0.1:9050")
        searcher = DDGSearcher(cfg)
        assert searcher._get_proxy() == "socks5://127.0.0.1:9050"

    def test_tor_available(self):
        cfg = ScraperConfig()
        mock_tor = MagicMock()
        mock_tor.is_available = True
        mock_tor.get_ddgs_proxy.return_value = "socks5://127.0.0.1:9150"
        searcher = DDGSearcher(cfg, tor_manager=mock_tor)
        assert searcher._get_proxy() == "socks5://127.0.0.1:9150"

    def test_tor_not_available_falls_back(self):
        cfg = ScraperConfig(proxy="http://proxy:8080")
        mock_tor = MagicMock()
        mock_tor.is_available = False
        searcher = DDGSearcher(cfg, tor_manager=mock_tor)
        assert searcher._get_proxy() == "http://proxy:8080"
