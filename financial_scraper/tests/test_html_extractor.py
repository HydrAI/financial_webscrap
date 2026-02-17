"""Tests for financial_scraper.extract.html."""

from unittest.mock import patch

from financial_scraper.config import ScraperConfig
from financial_scraper.extract.html import ExtractionResult, HTMLExtractor


def _make_extractor(**config_overrides):
    defaults = {"min_word_count": 10, "favor_precision": True}
    defaults.update(config_overrides)
    return HTMLExtractor(ScraperConfig(**defaults))


class TestSuccessfulExtraction:
    @patch("trafilatura.bare_extraction")
    def test_returns_correct_fields(self, mock_extract):
        text = " ".join(["word"] * 50)
        mock_extract.return_value = {
            "text": text,
            "title": "Test Title",
            "author": "Author Name",
            "date": "2024-06-15",
        }
        ext = _make_extractor()
        result = ext.extract("<html>content</html>", "https://example.com")
        assert result.title == "Test Title"
        assert result.author == "Author Name"
        assert result.date == "2024-06-15"
        assert result.extraction_method == "trafilatura"
        assert result.word_count > 0


class TestFailedExtraction:
    @patch("trafilatura.bare_extraction")
    def test_none_result_gives_failed(self, mock_extract):
        mock_extract.return_value = None
        ext = _make_extractor()
        result = ext.extract("<html></html>", "https://example.com")
        assert result.text == ""
        assert result.word_count == 0
        assert result.extraction_method == "failed"


class TestFallback:
    @patch("trafilatura.bare_extraction")
    def test_fallback_when_below_min_word_count(self, mock_extract):
        short_text = "only three words"
        long_text = " ".join(["word"] * 50)
        mock_extract.side_effect = [
            {"text": short_text, "title": "T", "author": None, "date": None},
            {"text": long_text, "title": "T2", "author": None, "date": None},
        ]
        ext = _make_extractor(min_word_count=10)
        result = ext.extract("<html>x</html>", "https://example.com")
        assert result.extraction_method == "trafilatura_fallback"
        assert result.word_count > 10

    @patch("trafilatura.bare_extraction")
    def test_fallback_not_used_if_fewer_words(self, mock_extract):
        text_a = " ".join(["word"] * 5)
        text_b = " ".join(["word"] * 3)
        mock_extract.side_effect = [
            {"text": text_a, "title": "T", "author": None, "date": None},
            {"text": text_b, "title": "T2", "author": None, "date": None},
        ]
        ext = _make_extractor(min_word_count=10)
        result = ext.extract("<html>x</html>", "https://example.com")
        # Should keep original since fallback has fewer words
        assert result.extraction_method == "trafilatura"
