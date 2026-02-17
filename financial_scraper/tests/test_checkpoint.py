"""Tests for financial_scraper.checkpoint."""

from financial_scraper.checkpoint import Checkpoint


class TestQueryTracking:
    def test_not_done_before_mark(self, tmp_path):
        cp = Checkpoint(tmp_path / "cp.json")
        assert cp.is_query_done("test query") is False

    def test_done_after_mark(self, tmp_path):
        cp = Checkpoint(tmp_path / "cp.json")
        cp.mark_query_done("test query")
        assert cp.is_query_done("test query") is True


class TestURLTracking:
    def test_not_fetched_initially(self, tmp_path):
        cp = Checkpoint(tmp_path / "cp.json")
        assert cp.is_url_fetched("https://example.com") is False

    def test_fetched_after_mark(self, tmp_path):
        cp = Checkpoint(tmp_path / "cp.json")
        cp.mark_url_fetched("https://example.com")
        assert cp.is_url_fetched("https://example.com") is True


class TestFailureTracking:
    def test_mark_failed_increments(self, tmp_path):
        cp = Checkpoint(tmp_path / "cp.json")
        url = "https://example.com/fail"
        cp.mark_url_failed(url)
        assert cp.failed_urls[url] == 1
        cp.mark_url_failed(url)
        assert cp.failed_urls[url] == 2

    def test_should_retry_respects_max(self, tmp_path):
        cp = Checkpoint(tmp_path / "cp.json")
        url = "https://example.com/fail"
        assert cp.should_retry(url, max_retries=2) is True
        cp.mark_url_failed(url)
        assert cp.should_retry(url, max_retries=2) is True
        cp.mark_url_failed(url)
        assert cp.should_retry(url, max_retries=2) is False


class TestPersistence:
    def test_save_load_roundtrip(self, tmp_path):
        path = tmp_path / "cp.json"
        cp = Checkpoint(path)
        cp.mark_url_fetched("https://example.com/1")
        cp.mark_url_failed("https://example.com/bad")
        cp.mark_query_done("query1")  # calls save() internally

        cp2 = Checkpoint(path)
        cp2.load()
        assert cp2.is_query_done("query1") is True
        assert cp2.is_url_fetched("https://example.com/1") is True
        assert cp2.failed_urls["https://example.com/bad"] == 1

    def test_load_missing_file_is_noop(self, tmp_path):
        cp = Checkpoint(tmp_path / "nonexistent.json")
        cp.load()  # should not raise
        assert cp.is_query_done("anything") is False


class TestStats:
    def test_stats_increment_on_query_done(self, tmp_path):
        cp = Checkpoint(tmp_path / "cp.json")
        cp.mark_query_done("q1")
        cp.mark_query_done("q2")
        assert cp.stats["total_queries"] == 2

    def test_stats_increment_on_failed_fetch(self, tmp_path):
        cp = Checkpoint(tmp_path / "cp.json")
        cp.mark_url_failed("https://example.com/1")
        cp.mark_url_failed("https://example.com/2")
        assert cp.stats["failed_fetches"] == 2
