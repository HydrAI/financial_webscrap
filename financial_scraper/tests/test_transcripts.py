"""Tests for financial_scraper.transcripts module."""

import json
import pytest

from financial_scraper.transcripts.config import TranscriptConfig
from financial_scraper.transcripts.discovery import (
    _parse_transcript_url,
    TranscriptInfo,
)
from financial_scraper.transcripts.extract import (
    extract_transcript,
    _extract_json_ld,
    _extract_ticker_from_jsonld,
    TranscriptResult,
)


# ---------------------------------------------------------------------------
# Discovery tests
# ---------------------------------------------------------------------------

class TestParseTranscriptURL:
    def test_standard_url(self):
        url = (
            "https://www.fool.com/earnings/call-transcripts/"
            "2025/01/30/apple-aapl-q1-2025-earnings-call-transcript/"
        )
        info = _parse_transcript_url(url)
        assert info is not None
        assert info.ticker == "AAPL"
        assert info.quarter == "Q1"
        assert info.year == 2025
        assert info.pub_date == "2025-01-30"

    def test_url_with_multi_word_company(self):
        url = (
            "https://www.fool.com/earnings/call-transcripts/"
            "2025/04/15/extra-space-storage-exr-q1-2025-earnings-call-transcript/"
        )
        info = _parse_transcript_url(url)
        assert info is not None
        assert info.ticker == "EXR"
        assert info.quarter == "Q1"
        assert info.year == 2025

    def test_url_without_quarter_returns_none(self):
        url = (
            "https://www.fool.com/earnings/call-transcripts/"
            "2025/02/10/apple-aapl-2025-earnings-call-transcript/"
        )
        info = _parse_transcript_url(url)
        assert info is None

    def test_non_transcript_url_returns_none(self):
        url = "https://www.fool.com/investing/2025/01/01/some-article/"
        info = _parse_transcript_url(url)
        assert info is None


class TestTranscriptConfig:
    def test_defaults(self):
        cfg = TranscriptConfig()
        assert cfg.tickers == ()
        assert cfg.year is None
        assert cfg.concurrent == 5

    def test_frozen(self):
        cfg = TranscriptConfig(tickers=("AAPL",))
        with pytest.raises(AttributeError):
            cfg.tickers = ("MSFT",)


# ---------------------------------------------------------------------------
# Extract tests
# ---------------------------------------------------------------------------

MOCK_TRANSCRIPT_HTML = """
<html>
<head>
<script type="application/ld+json">
{
  "@type": "NewsArticle",
  "headline": "Apple (AAPL) Q1 2025 Earnings Call Transcript",
  "datePublished": "2025-01-30T18:00:00Z",
  "about": [
    {"tickerSymbol": "NASDAQ AAPL"}
  ]
}
</script>
</head>
<body>
<div class="article-body">
  <h2>DATE</h2>
  <p>January 30, 2025</p>

  <h2>CALL PARTICIPANTS</h2>
  <ul>
    <li>Tim Cook -- Chief Executive Officer</li>
    <li>Luca Maestri -- Chief Financial Officer</li>
    <li>Analyst One -- Goldman Sachs</li>
  </ul>

  <h2>Full Conference Call Transcript</h2>
  <p>Tim Cook: Good afternoon, everyone. Welcome to our fiscal first quarter
  earnings call. We had a strong quarter with revenue of $120 billion.</p>
  <p>Luca Maestri: Thanks Tim. Let me walk through the financials in detail.
  Revenue was up 5% year over year.</p>
  <p>We will now begin the question-and-answer session.</p>
  <p>Analyst One: Thanks for taking my question. Can you talk about iPhone demand?</p>
  <p>Tim Cook: Absolutely. iPhone demand was strong across all geographies.</p>

  <h2>Premium Investing</h2>
  <p>This is premium content.</p>
</div>
</body>
</html>
"""


class TestExtractTranscript:
    def test_basic_extraction(self):
        result = extract_transcript(MOCK_TRANSCRIPT_HTML)
        assert result is not None
        assert result.ticker == "AAPL"
        assert result.date == "2025-01-30"
        assert "revenue" in result.full_text.lower()

    def test_participants_extracted(self):
        result = extract_transcript(MOCK_TRANSCRIPT_HTML)
        assert len(result.participants) == 3
        assert "Tim Cook -- Chief Executive Officer" in result.participants

    def test_speakers_extracted(self):
        result = extract_transcript(MOCK_TRANSCRIPT_HTML)
        assert "Tim Cook" in result.speakers

    def test_qa_split(self):
        result = extract_transcript(MOCK_TRANSCRIPT_HTML)
        assert result.prepared_remarks
        assert result.qa_section
        assert "question-and-answer" in result.qa_section.lower()
        assert "strong quarter" in result.prepared_remarks.lower()

    def test_quarter_year_from_headline(self):
        result = extract_transcript(MOCK_TRANSCRIPT_HTML)
        assert result.quarter == "Q1"
        assert result.year == 2025

    def test_no_article_body_returns_none(self):
        html = "<html><body><p>No transcript here.</p></body></html>"
        result = extract_transcript(html)
        assert result is None


class TestExtractJsonLD:
    def test_extracts_news_article(self):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(MOCK_TRANSCRIPT_HTML, "lxml")
        data = _extract_json_ld(soup)
        assert data["@type"] == "NewsArticle"
        assert "Apple" in data["headline"]

    def test_no_jsonld_returns_empty(self):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup("<html><body></body></html>", "lxml")
        data = _extract_json_ld(soup)
        assert data == {}


class TestExtractTicker:
    def test_nasdaq_format(self):
        data = {"about": [{"tickerSymbol": "NASDAQ AAPL"}]}
        assert _extract_ticker_from_jsonld(data) == "AAPL"

    def test_nyse_format(self):
        data = {"about": [{"tickerSymbol": "NYSE EXR"}]}
        assert _extract_ticker_from_jsonld(data) == "EXR"

    def test_empty_about(self):
        assert _extract_ticker_from_jsonld({"about": []}) == ""

    def test_no_about(self):
        assert _extract_ticker_from_jsonld({}) == ""
