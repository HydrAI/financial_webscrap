"""Tests for financial_scraper.store.dedup."""

import pytest
from financial_scraper.store.dedup import Deduplicator, _HAS_DATASKETCH

needs_datasketch = pytest.mark.skipif(
    not _HAS_DATASKETCH, reason="datasketch not installed"
)


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


@needs_datasketch
class TestFuzzyDedup:
    """MinHash LSH near-duplicate detection tests."""

    BASE_ARTICLE = (
        "Apple Inc reported record quarterly revenue of $124 billion "
        "driven by strong iPhone sales and services growth. CEO Tim Cook "
        "said the company saw broad-based strength across its product "
        "lineup during the holiday quarter. The tech giant posted earnings "
        "per share of $2.10, beating analyst expectations of $1.89. "
        "Revenue from the services segment reached an all-time high of "
        "$20.8 billion. The company returned over $30 billion to "
        "shareholders during the quarter through dividends and share "
        "repurchases. Apple expects continued momentum in the current "
        "quarter with strong demand across all geographic segments. "
        "Greater China revenue grew 2% year over year to $23.9 billion "
        "marking a return to growth in the region. The Americas segment "
        "contributed $49.3 billion in revenue representing the largest "
        "geographic segment for the company. Wearables Home and "
        "Accessories brought in $11.9 billion during the quarter. "
        "Apple now has over 2.2 billion active devices worldwide and "
        "its installed base continues to grow at a record pace. The "
        "company also announced plans to invest heavily in artificial "
        "intelligence capabilities throughout its product ecosystem. "
        "Gross margin for the quarter came in at 46.6 percent exceeding "
        "guidance and analyst expectations. Operating cash flow was "
        "strong at $34 billion for the quarter."
    )

    def _make_near_duplicate(self, base: str) -> str:
        """Simulate a syndicated rewrite — swap a couple words."""
        text = base.replace("tech giant", "technology company")
        text = text.replace("strong demand", "robust demand")
        return text

    def test_near_duplicate_detected(self):
        d = Deduplicator()
        original = self.BASE_ARTICLE
        rewrite = self._make_near_duplicate(original)
        d.mark_seen("https://a.com/article", original)
        assert d.is_duplicate_content(rewrite) is True

    def test_dissimilar_not_flagged(self):
        d = Deduplicator()
        d.mark_seen("https://a.com/article", self.BASE_ARTICLE)
        unrelated = (
            "Tesla shares dropped 5% following reports of a recall "
            "affecting 200,000 vehicles due to a software issue with "
            "the autopilot system. The NHTSA launched a formal "
            "investigation into the matter. Competitors in the EV "
            "space including Rivian and Lucid saw their shares rise "
            "on the news as investors rotated into alternative names."
        )
        assert d.is_duplicate_content(unrelated) is False

    def test_exact_dedup_still_works_with_fuzzy(self):
        d = Deduplicator()
        content = "Exact same content for testing"
        d.mark_seen("https://a.com", content)
        # Exact match caught by SHA256 layer
        assert d.is_duplicate_content(content) is True
        # Different content not a duplicate
        assert d.is_duplicate_content("Completely different text") is False

    def test_save_load_preserves_minhash(self, tmp_path):
        path = tmp_path / "dedup.json"
        d = Deduplicator()
        d.mark_seen("https://a.com/article", self.BASE_ARTICLE)
        d.save(path)

        d2 = Deduplicator()
        d2.load(path)
        rewrite = self._make_near_duplicate(self.BASE_ARTICLE)
        assert d2.is_duplicate_content(rewrite) is True

    def test_short_and_empty_content_no_crash(self):
        d = Deduplicator()
        # Empty content
        assert d.is_duplicate_content("") is False
        # Single word
        d.mark_seen("https://a.com", "hello")
        assert d.is_duplicate_content("hello") is True
        # Two words (below shingle size)
        d.mark_seen("https://b.com", "short text")
        assert d.is_duplicate_content("short text") is True
