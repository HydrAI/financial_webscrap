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


class TestDoSearchWithRetry:
    def _make_searcher(self, **config_overrides):
        defaults = {"search_delay_min": 0.0, "search_delay_max": 0.0}
        defaults.update(config_overrides)
        return DDGSearcher(ScraperConfig(**defaults))

    @patch("financial_scraper.search.duckduckgo.time")
    def test_ratelimit_retries_then_succeeds(self, mock_time):
        mock_time.sleep = MagicMock()
        mock_time.time.return_value = 1000.0

        searcher = self._make_searcher()
        from duckduckgo_search.exceptions import RatelimitException

        call_count = 0
        def side_effect(query, max_results):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RatelimitException("rate limited")
            return [{"href": "https://example.com", "title": "T", "body": "B"}]

        with patch.object(searcher, "_do_search_inner", side_effect=side_effect):
            result = searcher._do_search_with_retry("test", 5)

        assert len(result) == 1

    @patch("financial_scraper.search.duckduckgo.time")
    def test_all_retries_exhausted(self, mock_time):
        mock_time.sleep = MagicMock()
        mock_time.time.return_value = 1000.0

        searcher = self._make_searcher()
        from duckduckgo_search.exceptions import RatelimitException

        with patch.object(searcher, "_do_search_inner",
                         side_effect=RatelimitException("rate limited")):
            result = searcher._do_search_with_retry("test", 5)

        assert result == []

    @patch("financial_scraper.search.duckduckgo.time")
    def test_generic_exception_retries(self, mock_time):
        mock_time.sleep = MagicMock()
        mock_time.time.return_value = 1000.0

        searcher = self._make_searcher()
        call_count = 0
        def side_effect(query, max_results):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("network error")
            return [{"href": "https://example.com", "title": "T", "body": "B"}]

        with patch.object(searcher, "_do_search_inner", side_effect=side_effect):
            result = searcher._do_search_with_retry("test", 5)

        assert len(result) == 1


class TestDoSearchInner:
    def _make_searcher(self, **config_overrides):
        defaults = {"search_delay_min": 0.0, "search_delay_max": 0.0}
        defaults.update(config_overrides)
        return DDGSearcher(ScraperConfig(**defaults))

    def test_text_search(self):
        searcher = self._make_searcher(search_type="text")
        mock_ddgs = MagicMock()
        mock_ddgs.__enter__ = MagicMock(return_value=mock_ddgs)
        mock_ddgs.__exit__ = MagicMock(return_value=False)
        mock_ddgs.text.return_value = [{"href": "https://a.com", "title": "T", "body": "B"}]

        with patch.object(searcher, "_get_ddgs_class", return_value=lambda **kw: mock_ddgs):
            result = searcher._do_search_inner("query", 5)

        assert len(result) == 1
        mock_ddgs.text.assert_called_once()

    def test_news_search(self):
        searcher = self._make_searcher(search_type="news")
        mock_ddgs = MagicMock()
        mock_ddgs.__enter__ = MagicMock(return_value=mock_ddgs)
        mock_ddgs.__exit__ = MagicMock(return_value=False)
        mock_ddgs.news.return_value = [{"url": "https://a.com", "title": "T", "body": "B"}]

        with patch.object(searcher, "_get_ddgs_class", return_value=lambda **kw: mock_ddgs):
            result = searcher._do_search_inner("query", 5)

        assert len(result) == 1
        mock_ddgs.news.assert_called_once()


class TestSearchWithTor:
    @patch("financial_scraper.search.duckduckgo.time")
    @patch("financial_scraper.search.duckduckgo.random")
    def test_tor_circuit_renewal(self, mock_random, mock_time):
        mock_random.uniform.return_value = 0.0
        mock_time.time.return_value = 1000.0
        mock_time.sleep = MagicMock()

        mock_tor = MagicMock()
        mock_tor.is_available = True
        mock_tor.should_renew.return_value = True

        cfg = ScraperConfig(search_delay_min=0.0, search_delay_max=0.0)
        searcher = DDGSearcher(cfg, tor_manager=mock_tor)

        with patch.object(searcher, "_do_search_with_retry",
                         return_value=[{"href": "https://a.com", "title": "T", "body": "B"}]):
            searcher.search("test", 5)

        mock_tor.renew_circuit.assert_called_once()
        mock_tor.on_search_completed.assert_called_once()

    @patch("financial_scraper.search.duckduckgo.time")
    @patch("financial_scraper.search.duckduckgo.random")
    def test_url_field_fallback(self, mock_random, mock_time):
        """Test that 'url' field is used when 'href' is missing (news results)."""
        mock_random.uniform.return_value = 0.0
        mock_time.time.return_value = 1000.0
        mock_time.sleep = MagicMock()

        cfg = ScraperConfig(search_delay_min=0.0, search_delay_max=0.0)
        searcher = DDGSearcher(cfg)

        raw = [{"url": "https://news.com/1", "title": "News", "snippet": "Body"}]
        with patch.object(searcher, "_do_search_with_retry", return_value=raw):
            results = searcher.search("test", 5)

        assert len(results) == 1
        assert results[0].url == "https://news.com/1"

    @patch("financial_scraper.search.duckduckgo.time")
    @patch("financial_scraper.search.duckduckgo.random")
    def test_consecutive_ratelimit_tracking(self, mock_random, mock_time):
        mock_random.uniform.return_value = 0.0
        mock_time.time.return_value = 1000.0
        mock_time.sleep = MagicMock()

        cfg = ScraperConfig(search_delay_min=0.0, search_delay_max=0.0)
        searcher = DDGSearcher(cfg)

        # Empty result WITHOUT ratelimit does NOT increment counter
        with patch.object(searcher, "_do_search_with_retry", return_value=[]):
            searcher.search("test", 5)
        assert searcher._consecutive_ratelimits == 0

        # Actual ratelimit increments counter
        def fake_retry_ratelimit(query, max_results):
            searcher._hit_ratelimit = True
            return []
        with patch.object(searcher, "_do_search_with_retry", side_effect=fake_retry_ratelimit):
            searcher.search("test2", 5)
        assert searcher._consecutive_ratelimits == 1

        # Successful result resets counter to 0
        def fake_retry_success(query, max_results):
            searcher._hit_ratelimit = False
            return [{"href": "https://a.com", "title": "T", "body": "B"}]
        with patch.object(searcher, "_do_search_with_retry", side_effect=fake_retry_success):
            searcher.search("test3", 5)
        assert searcher._consecutive_ratelimits == 0


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
