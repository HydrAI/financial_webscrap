"""Tests for financial_scraper.extract.pdf."""

import io
from unittest.mock import MagicMock, patch

from financial_scraper.extract.pdf import PDFExtractor


def _make_mock_pdf(pages_text, metadata=None):
    """Create a mock pdfplumber PDF context manager."""
    mock_pdf = MagicMock()
    mock_pdf.metadata = metadata or {}
    mock_pages = []
    for text in pages_text:
        page = MagicMock()
        page.extract_text.return_value = text
        mock_pages.append(page)
    mock_pdf.pages = mock_pages
    mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
    mock_pdf.__exit__ = MagicMock(return_value=False)
    return mock_pdf


class TestSuccessfulExtraction:
    @patch("pdfplumber.open")
    def test_multi_page(self, mock_open):
        mock_pdf = _make_mock_pdf(["Page one text.", "Page two text."])
        mock_open.return_value = mock_pdf
        ext = PDFExtractor()
        result = ext.extract(b"fake-pdf-bytes", "https://example.com/report.pdf")
        assert "Page one text" in result.text
        assert "Page two text" in result.text
        assert result.extraction_method == "pdfplumber"
        assert result.word_count > 0

    @patch("pdfplumber.open")
    def test_title_from_metadata(self, mock_open):
        mock_pdf = _make_mock_pdf(["Some text."], metadata={"Title": "My Report"})
        mock_open.return_value = mock_pdf
        ext = PDFExtractor()
        result = ext.extract(b"bytes", "https://example.com/doc.pdf")
        assert result.title == "My Report"

    @patch("pdfplumber.open")
    def test_title_fallback_to_url_slug(self, mock_open):
        mock_pdf = _make_mock_pdf(["Some text."], metadata={})
        mock_open.return_value = mock_pdf
        ext = PDFExtractor()
        result = ext.extract(b"bytes", "https://example.com/annual_report.pdf")
        assert result.title == "annual_report"


class TestEmptyPDF:
    @patch("pdfplumber.open")
    def test_no_text_on_any_page(self, mock_open):
        mock_pdf = _make_mock_pdf([None, None])
        mock_open.return_value = mock_pdf
        ext = PDFExtractor()
        result = ext.extract(b"bytes", "https://example.com/empty.pdf")
        assert result.text == ""
        assert result.word_count == 0
        assert result.extraction_method == "failed"


class TestExceptionHandling:
    @patch("pdfplumber.open")
    def test_pdfplumber_exception(self, mock_open):
        mock_open.side_effect = Exception("Corrupt PDF")
        ext = PDFExtractor()
        result = ext.extract(b"bytes", "https://example.com/bad.pdf")
        assert result.text == ""
        assert result.extraction_method == "failed"
