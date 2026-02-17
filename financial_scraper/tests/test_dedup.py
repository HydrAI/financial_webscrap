"""Tests for financial_scraper.store.dedup."""

from financial_scraper.store.dedup import Deduplicator


class TestURLDedup:
    def test_new_url_not_duplicate(self):
        d = Deduplicator()
        assert d.is_duplicate_url("https://example.com/page") is False

    def test_after_mark_seen_is_duplicate(self):
        d = Deduplicator()
        d.mark_seen("https://example.com/page", "some content")
        assert d.is_duplicate_url("https://example.com/page") is True

    def test_fragment_stripped(self):
        d = Deduplicator()
        d.mark_seen("https://example.com/page#section", "content")
        assert d.is_duplicate_url("https://example.com/page") is True

    def test_case_folded(self):
        d = Deduplicator()
        d.mark_seen("https://EXAMPLE.COM/Page", "content")
        assert d.is_duplicate_url("https://example.com/page") is True

    def test_trailing_slash_stripped(self):
        d = Deduplicator()
        d.mark_seen("https://example.com/page/", "content")
        assert d.is_duplicate_url("https://example.com/page") is True


class TestContentDedup:
    def test_same_content_is_duplicate(self):
        d = Deduplicator()
        content = "A" * 100
        d.mark_seen("https://a.com", content)
        assert d.is_duplicate_content(content) is True

    def test_different_content_not_duplicate(self):
        d = Deduplicator()
        d.mark_seen("https://a.com", "content A")
        assert d.is_duplicate_content("content B") is False

    def test_differ_only_after_2000_chars_still_duplicate(self):
        d = Deduplicator()
        base = "X" * 2000
        d.mark_seen("https://a.com", base + "AAAA")
        assert d.is_duplicate_content(base + "BBBB") is True


class TestPersistence:
    def test_save_load_roundtrip(self, tmp_path):
        path = tmp_path / "dedup.json"
        d = Deduplicator()
        d.mark_seen("https://example.com/1", "content one")
        d.mark_seen("https://example.com/2", "content two")
        d.save(path)

        d2 = Deduplicator()
        d2.load(path)
        assert d2.is_duplicate_url("https://example.com/1") is True
        assert d2.is_duplicate_url("https://example.com/2") is True
        assert d2.is_duplicate_content("content one") is True

    def test_load_missing_file_is_noop(self, tmp_path):
        path = tmp_path / "nonexistent.json"
        d = Deduplicator()
        d.load(path)  # should not raise
        assert d.is_duplicate_url("https://example.com") is False
