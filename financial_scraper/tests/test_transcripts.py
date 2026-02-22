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
    _extract_speakers_from_text,
    _extract_speakers_from_elements,
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
# Extract tests — older HTML format (CALL PARTICIPANTS as <ul>,
# "Full Conference Call Transcript" heading, "Name:" speaker pattern)
# ---------------------------------------------------------------------------

MOCK_TRANSCRIPT_OLD = """
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

# ---------------------------------------------------------------------------
# Extract tests — live HTML format (Prepared Remarks / Questions & Answers
# headings, "Name -- Title" speaker pattern, <p> participants)
# ---------------------------------------------------------------------------

MOCK_TRANSCRIPT_LIVE = """
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
<div class="article-body transcript-content">
  <p>Apple(AAPL+1.54%)Q1 2025 Earnings CallJan 30, 2025, 5:00 p.m. ET</p>

  <h2>Contents:</h2>
  <ul>
    <li>Prepared Remarks</li>
    <li>Questions and Answers</li>
    <li>Call Participants</li>
  </ul>

  <h2>Prepared Remarks:</h2>
  <p><strong>Suhasini Chandramouli</strong> -- <em>Director, Investor Relations</em></p>
  <p>Good afternoon, everyone. Welcome to Apple's fiscal first quarter 2025
  earnings call. We had a strong quarter with revenue of $120 billion.</p>
  <p><strong>Timothy Donald Cook</strong> -- <em>Chief Executive Officer</em></p>
  <p>Thank you, Suhasini. We are thrilled to report outstanding results.</p>
  <p><strong>Kevan Parekh</strong> -- <em>Chief Financial Officer</em></p>
  <p>Thank you, Tim. Revenue was up 5% year over year driven by strong iPhone demand.</p>

  <h2>Questions & Answers:</h2>
  <p><strong>Operator</strong></p>
  <p>We will now begin the question-and-answer session.</p>
  <p><strong>Erik Woodring</strong> -- <em>Analyst</em></p>
  <p>Thanks for taking my question. Can you talk about iPhone demand?</p>
  <p><strong>Timothy Donald Cook</strong> -- <em>Chief Executive Officer</em></p>
  <p>Absolutely. iPhone demand was strong across all geographies.</p>

  <h2>Call participants:</h2>
  <p><strong>Suhasini Chandramouli</strong> -- <em>Director, Investor Relations</em></p>
  <p><strong>Timothy Donald Cook</strong> -- <em>Chief Executive Officer</em></p>
  <p><strong>Kevan Parekh</strong> -- <em>Chief Financial Officer</em></p>
  <p><strong>Erik Woodring</strong> -- <em>Analyst</em></p>
  <p><a href="#">More AAPL analysis</a></p>
</div>
</body>
</html>
"""


class TestExtractTranscriptOldFormat:
    """Tests for the older HTML format (CALL PARTICIPANTS, Full Conference Call Transcript)."""

    def test_basic_extraction(self):
        result = extract_transcript(MOCK_TRANSCRIPT_OLD)
        assert result is not None
        assert result.ticker == "AAPL"
        assert result.date == "2025-01-30"
        assert "revenue" in result.full_text.lower()

    def test_participants_extracted(self):
        result = extract_transcript(MOCK_TRANSCRIPT_OLD)
        assert len(result.participants) == 3
        assert "Tim Cook -- Chief Executive Officer" in result.participants

    def test_speakers_extracted(self):
        result = extract_transcript(MOCK_TRANSCRIPT_OLD)
        assert "Tim Cook" in result.speakers

    def test_qa_split(self):
        result = extract_transcript(MOCK_TRANSCRIPT_OLD)
        assert result.prepared_remarks
        assert result.qa_section
        assert "question-and-answer" in result.qa_section.lower()
        assert "strong quarter" in result.prepared_remarks.lower()

    def test_quarter_year_from_headline(self):
        result = extract_transcript(MOCK_TRANSCRIPT_OLD)
        assert result.quarter == "Q1"
        assert result.year == 2025

    def test_no_article_body_returns_none(self):
        html = "<html><body><p>No transcript here.</p></body></html>"
        result = extract_transcript(html)
        assert result is None


class TestExtractTranscriptLiveFormat:
    """Tests for the live Motley Fool HTML format (Prepared Remarks, Q&A, <p> participants)."""

    def test_basic_extraction(self):
        result = extract_transcript(MOCK_TRANSCRIPT_LIVE)
        assert result is not None
        assert result.ticker == "AAPL"
        assert result.date == "2025-01-30"
        assert "revenue" in result.full_text.lower()

    def test_participants_from_p_tags(self):
        result = extract_transcript(MOCK_TRANSCRIPT_LIVE)
        assert len(result.participants) >= 4
        assert any("Timothy Donald Cook" in p for p in result.participants)
        assert any("Erik Woodring" in p for p in result.participants)

    def test_speakers_dash_format(self):
        result = extract_transcript(MOCK_TRANSCRIPT_LIVE)
        assert "Timothy Donald Cook" in result.speakers
        assert "Kevan Parekh" in result.speakers
        assert "Erik Woodring" in result.speakers

    def test_no_false_positive_speakers(self):
        result = extract_transcript(MOCK_TRANSCRIPT_LIVE)
        # "Image source" should not appear as a speaker
        assert "Image source" not in result.speakers
        # "More AAPL analysis" link text should not appear
        assert "More AAPL analysis" not in result.speakers

    def test_qa_split_from_h2(self):
        result = extract_transcript(MOCK_TRANSCRIPT_LIVE)
        assert result.prepared_remarks
        assert result.qa_section
        assert "thrilled" in result.prepared_remarks.lower()
        assert "erik woodring" in result.qa_section.lower()

    def test_prepared_remarks_no_qa_content(self):
        result = extract_transcript(MOCK_TRANSCRIPT_LIVE)
        # Prepared remarks should not contain Q&A content
        assert "erik woodring" not in result.prepared_remarks.lower()

    def test_quarter_year_from_headline(self):
        result = extract_transcript(MOCK_TRANSCRIPT_LIVE)
        assert result.quarter == "Q1"
        assert result.year == 2025


class TestExtractSpeakersFromText:
    """Text-based speaker extraction (fallback for older format)."""

    def test_colon_format(self):
        text = "Tim Cook: Hello everyone.\nLuca Maestri: Thank you."
        speakers = _extract_speakers_from_text(text)
        assert "Tim Cook" in speakers
        assert "Luca Maestri" in speakers

    def test_filters_single_word_names(self):
        """Single-word names like 'Duration:', 'Operator:' should be filtered."""
        text = "Duration: 45 minutes\nOperator: We will begin."
        speakers = _extract_speakers_from_text(text)
        assert len(speakers) == 0

    def test_filters_short_names(self):
        text = "OK: something\nAB: test"
        speakers = _extract_speakers_from_text(text)
        assert len(speakers) == 0

    def test_filters_sentence_fragments(self):
        """Sentences before colons should not match as speakers."""
        text = "And I think: this is great.\nI do believe: yes."
        speakers = _extract_speakers_from_text(text)
        assert len(speakers) == 0

    def test_operator_without_colon_not_matched(self):
        text = "Operator\nWe will now begin."
        speakers = _extract_speakers_from_text(text)
        assert "Operator" not in speakers


class TestExtractSpeakersFromElements:
    """HTML-based speaker extraction (live Motley Fool format)."""

    def test_extracts_from_strong_tags(self):
        from bs4 import BeautifulSoup
        html = """
        <div>
          <p><strong>Tim Cook</strong> -- <em>CEO</em></p>
          <p>Hello everyone, welcome to our call.</p>
          <p><strong>Luca Maestri</strong> -- <em>CFO</em></p>
          <p>Thank you, Tim.</p>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        elements = list(soup.find("div").children)
        tags = [el for el in elements if hasattr(el, "name")]
        speakers = _extract_speakers_from_elements(tags)
        assert "Tim Cook" in speakers
        assert "Luca Maestri" in speakers

    def test_ignores_long_paragraphs(self):
        from bs4 import BeautifulSoup
        html = """
        <div>
          <p><strong>We've</strong> announced that we're going to open four new stores there. We also -- the iPhone was the top-selling model in all regions this quarter.</p>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        elements = list(soup.find("div").children)
        tags = [el for el in elements if hasattr(el, "name")]
        speakers = _extract_speakers_from_elements(tags)
        assert len(speakers) == 0

    def test_operator_extracted(self):
        from bs4 import BeautifulSoup
        html = '<div><p><strong>Operator</strong></p></div>'
        soup = BeautifulSoup(html, "lxml")
        tags = [el for el in soup.find("div").children if hasattr(el, "name")]
        speakers = _extract_speakers_from_elements(tags)
        assert "Operator" in speakers


class TestExtractJsonLD:
    def test_extracts_news_article(self):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(MOCK_TRANSCRIPT_OLD, "lxml")
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
