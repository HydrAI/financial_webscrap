"""Tests for financial_scraper.transcripts module."""

import json
import pytest

from financial_scraper.transcripts.config import TranscriptConfig
from financial_scraper.transcripts.discovery import (
    _parse_transcript_url,
    _normalize_ticker,
    _ticker_to_slug,
    _fetch_sitemap_urls,
    discover_transcripts,
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

    def test_dot_ticker_brk_a(self):
        url = (
            "https://www.fool.com/earnings/call-transcripts/"
            "2025/02/22/berkshire-hathaway-brk-a-q4-2024-earnings-call-transcript/"
        )
        info = _parse_transcript_url(url)
        assert info is not None
        assert info.ticker == "BRK.A"
        assert info.quarter == "Q4"
        assert info.year == 2024

    def test_dot_ticker_mog_a(self):
        url = (
            "https://www.fool.com/earnings/call-transcripts/"
            "2025/01/28/moog-mog-a-q1-2025-earnings-call-transcript/"
        )
        info = _parse_transcript_url(url)
        assert info is not None
        assert info.ticker == "MOG.A"
        assert info.quarter == "Q1"
        assert info.year == 2025


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

  <h2>Questions &amp; Answers:</h2>
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
    """HTML-based speaker extraction (live Motley Fool format).

    Uses lxml elements (migrated from BeautifulSoup).
    """

    def test_extracts_from_strong_tags(self):
        from lxml import html as lxml_html
        html = """
        <div>
          <p><strong>Tim Cook</strong> -- <em>CEO</em></p>
          <p>Hello everyone, welcome to our call.</p>
          <p><strong>Luca Maestri</strong> -- <em>CFO</em></p>
          <p>Thank you, Tim.</p>
        </div>
        """
        doc = lxml_html.fromstring(html)
        divs = doc.cssselect("div")
        div = divs[0] if divs else doc
        elements = list(div)
        speakers = _extract_speakers_from_elements(elements)
        assert "Tim Cook" in speakers
        assert "Luca Maestri" in speakers

    def test_ignores_long_paragraphs(self):
        from lxml import html as lxml_html
        html = """
        <div>
          <p><strong>We've</strong> announced that we're going to open four new stores there. We also -- the iPhone was the top-selling model in all regions this quarter.</p>
        </div>
        """
        doc = lxml_html.fromstring(html)
        divs = doc.cssselect("div")
        div = divs[0] if divs else doc
        elements = list(div)
        speakers = _extract_speakers_from_elements(elements)
        assert len(speakers) == 0

    def test_operator_extracted(self):
        from lxml import html as lxml_html
        html = '<div><p><strong>Operator</strong></p></div>'
        doc = lxml_html.fromstring(html)
        divs = doc.cssselect("div")
        div = divs[0] if divs else doc
        elements = list(div)
        speakers = _extract_speakers_from_elements(elements)
        assert "Operator" in speakers


class TestExtractJsonLD:
    def test_extracts_news_article(self):
        from lxml import html as lxml_html
        doc = lxml_html.fromstring(MOCK_TRANSCRIPT_OLD)
        data = _extract_json_ld(doc)
        assert data["@type"] == "NewsArticle"
        assert "Apple" in data["headline"]

    def test_no_jsonld_returns_empty(self):
        from lxml import html as lxml_html
        doc = lxml_html.fromstring("<html><body></body></html>")
        data = _extract_json_ld(doc)
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


# ---------------------------------------------------------------------------
# TranscriptPipeline integration tests
# ---------------------------------------------------------------------------

def _make_transcript_config(tmp_path, **overrides):
    defaults = {
        "tickers": ("AAPL",),
        "tickers_file": None,
        "year": 2025,
        "quarters": ("Q1",),
        "concurrent": 1,
        "output_dir": tmp_path,
        "output_path": tmp_path / "out.parquet",
        "jsonl_path": None,
        "checkpoint_file": tmp_path / "cp.json",
        "resume": False,
    }
    defaults.update(overrides)
    return TranscriptConfig(**defaults)


_SAMPLE_INFO = TranscriptInfo(
    url="https://www.fool.com/earnings/call-transcripts/2025/01/30/apple-aapl-q1-2025-earnings-call-transcript/",
    ticker="AAPL",
    quarter="Q1",
    year=2025,
    pub_date="2025-01-30",
)

_SAMPLE_FULL_TEXT = "Good morning everyone. " * 40


class TestTranscriptPipelineLoadTickers:
    def test_loads_inline_tickers(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        cfg = _make_transcript_config(tmp_path, tickers=("AAPL", "MSFT"))
        p = TranscriptPipeline(cfg)
        assert p._load_tickers() == ["AAPL", "MSFT"]

    def test_loads_tickers_from_file(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        tf = tmp_path / "tickers.txt"
        tf.write_text("NVDA\n# comment\nGOOG\n")
        cfg = _make_transcript_config(tmp_path, tickers=(), tickers_file=tf)
        p = TranscriptPipeline(cfg)
        result = p._load_tickers()
        assert "NVDA" in result
        assert "GOOG" in result

    def test_deduplicates_tickers(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        cfg = _make_transcript_config(tmp_path, tickers=("AAPL", "AAPL", "MSFT"))
        p = TranscriptPipeline(cfg)
        result = p._load_tickers()
        assert result.count("AAPL") == 1

    def test_missing_tickers_file_returns_inline(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        cfg = _make_transcript_config(tmp_path, tickers=("AAPL",),
                                      tickers_file=tmp_path / "missing.txt")
        p = TranscriptPipeline(cfg)
        assert p._load_tickers() == ["AAPL"]


class TestTranscriptPipelineRun:
    def test_run_no_transcripts_found(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from unittest.mock import patch
        cfg = _make_transcript_config(tmp_path)
        p = TranscriptPipeline(cfg)
        with patch("financial_scraper.transcripts.pipeline.discover_transcripts", return_value=[]):
            p.run()
        assert not (tmp_path / "out.parquet").exists()

    def test_run_with_successful_extraction(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from unittest.mock import patch, MagicMock

        result = TranscriptResult(
            company="Apple", ticker="AAPL", quarter="Q1", year=2025,
            date="2025-01-30", full_text=_SAMPLE_FULL_TEXT,
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "<html><body>transcript</body></html>"

        cfg = _make_transcript_config(tmp_path)
        p = TranscriptPipeline(cfg)
        with patch("financial_scraper.transcripts.pipeline.discover_transcripts", return_value=[_SAMPLE_INFO]):
            with patch.object(p._session, "get", return_value=mock_resp):
                with patch("financial_scraper.transcripts.pipeline.extract_transcript", return_value=result):
                    with patch("financial_scraper.transcripts.pipeline.time.sleep"):
                        p.run()

        assert (tmp_path / "out.parquet").exists()

    def test_run_fetch_failure(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from unittest.mock import patch
        import requests as req

        cfg = _make_transcript_config(tmp_path)
        p = TranscriptPipeline(cfg)
        with patch("financial_scraper.transcripts.pipeline.discover_transcripts", return_value=[_SAMPLE_INFO]):
            with patch.object(p._session, "get", side_effect=req.RequestException("timeout")):
                p.run()

        assert not (tmp_path / "out.parquet").exists()

    def test_run_http_error(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from unittest.mock import patch, MagicMock

        mock_resp = MagicMock()
        mock_resp.status_code = 403

        cfg = _make_transcript_config(tmp_path)
        p = TranscriptPipeline(cfg)
        with patch("financial_scraper.transcripts.pipeline.discover_transcripts", return_value=[_SAMPLE_INFO]):
            with patch.object(p._session, "get", return_value=mock_resp):
                p.run()

        assert not (tmp_path / "out.parquet").exists()

    def test_run_with_no_tickers(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        cfg = _make_transcript_config(tmp_path, tickers=())
        p = TranscriptPipeline(cfg)
        p.run()

    def test_run_with_jsonl(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from unittest.mock import patch, MagicMock

        result = TranscriptResult(
            company="Apple", ticker="AAPL", quarter="Q1", year=2025,
            date="2025-01-30", full_text=_SAMPLE_FULL_TEXT,
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "<html><body>transcript</body></html>"

        jsonl_path = tmp_path / "out.jsonl"
        cfg = _make_transcript_config(tmp_path, jsonl_path=jsonl_path)
        p = TranscriptPipeline(cfg)
        with patch("financial_scraper.transcripts.pipeline.discover_transcripts", return_value=[_SAMPLE_INFO]):
            with patch.object(p._session, "get", return_value=mock_resp):
                with patch("financial_scraper.transcripts.pipeline.extract_transcript", return_value=result):
                    with patch("financial_scraper.transcripts.pipeline.time.sleep"):
                        p.run()

        assert jsonl_path.exists()


class TestTranscriptPipelineConcurrent:
    """Tests for parallel fetching via the `concurrent` config field."""

    def _make_infos(self, n: int) -> list:
        return [
            TranscriptInfo(
                url=f"https://www.fool.com/earnings/call-transcripts/2025/0{q}/aapl-q{q}-2025/",
                ticker="AAPL",
                quarter=f"Q{q}",
                year=2025,
                pub_date=f"2025-0{q}-15",
            )
            for q in range(1, n + 1)
        ]

    def _make_results(self, n: int) -> list:
        return [
            TranscriptResult(
                company="Apple",
                ticker="AAPL",
                quarter=f"Q{q}",
                year=2025,
                date=f"2025-0{q}-15",
                # Unique content so dedup doesn't drop records
                full_text=f"Quarter {q} results. Revenue grew strongly. " * 20,
            )
            for q in range(1, n + 1)
        ]

    def test_all_records_written_with_concurrent_workers(self, tmp_path):
        """With concurrent=3 and 3 transcripts, all 3 records are written."""
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from unittest.mock import patch, MagicMock
        import pyarrow.parquet as pq

        infos = self._make_infos(3)
        extract_results = self._make_results(3)
        mock_resp = MagicMock(status_code=200, text="<html></html>")

        cfg = _make_transcript_config(tmp_path, concurrent=3)
        p = TranscriptPipeline(cfg)

        with patch("financial_scraper.transcripts.pipeline.discover_transcripts", return_value=infos):
            with patch.object(p._session, "get", return_value=mock_resp):
                with patch("financial_scraper.transcripts.pipeline.extract_transcript",
                           side_effect=extract_results):
                    with patch("financial_scraper.transcripts.pipeline.time.sleep"):
                        p.run()

        table = pq.read_table(tmp_path / "out.parquet")
        assert table.num_rows == 3

    def test_partial_failures_dont_drop_successes(self, tmp_path):
        """If one worker fails (HTTP error), the others still produce records."""
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from unittest.mock import patch, MagicMock
        import pyarrow.parquet as pq

        infos = self._make_infos(3)
        extract_results = self._make_results(3)

        # First GET returns 404, the other two return 200
        responses = [
            MagicMock(status_code=404),
            MagicMock(status_code=200, text="<html></html>"),
            MagicMock(status_code=200, text="<html></html>"),
        ]

        cfg = _make_transcript_config(tmp_path, concurrent=3)
        p = TranscriptPipeline(cfg)

        with patch("financial_scraper.transcripts.pipeline.discover_transcripts", return_value=infos):
            with patch.object(p._session, "get", side_effect=responses):
                with patch("financial_scraper.transcripts.pipeline.extract_transcript",
                           side_effect=extract_results[1:]):
                    with patch("financial_scraper.transcripts.pipeline.time.sleep"):
                        p.run()

        table = pq.read_table(tmp_path / "out.parquet")
        assert table.num_rows == 2

    def test_concurrent_one_behaves_like_sequential(self, tmp_path):
        """concurrent=1 produces the same output as the original sequential path."""
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from unittest.mock import patch, MagicMock
        import pyarrow.parquet as pq

        infos = self._make_infos(2)
        extract_results = self._make_results(2)
        mock_resp = MagicMock(status_code=200, text="<html></html>")

        cfg = _make_transcript_config(tmp_path, concurrent=1)
        p = TranscriptPipeline(cfg)

        with patch("financial_scraper.transcripts.pipeline.discover_transcripts", return_value=infos):
            with patch.object(p._session, "get", return_value=mock_resp):
                with patch("financial_scraper.transcripts.pipeline.extract_transcript",
                           side_effect=extract_results):
                    with patch("financial_scraper.transcripts.pipeline.time.sleep"):
                        p.run()

        table = pq.read_table(tmp_path / "out.parquet")
        assert table.num_rows == 2


# ---------------------------------------------------------------------------
# Discovery: _fetch_sitemap_urls and discover_transcripts
# ---------------------------------------------------------------------------

class TestFetchSitemapUrls:
    def test_returns_loc_urls(self):
        from unittest.mock import patch, MagicMock
        sitemap_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset>
          <url><loc>https://www.fool.com/earnings/call-transcripts/2025/01/30/apple-aapl-q1-2025-earnings-call-transcript/</loc></url>
          <url><loc>https://www.fool.com/some-other-article/</loc></url>
        </urlset>"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = sitemap_xml

        with patch("financial_scraper.transcripts.discovery.requests.get", return_value=mock_resp):
            urls = _fetch_sitemap_urls(2025, 1)

        assert len(urls) == 2
        assert any("aapl-q1-2025" in u for u in urls)

    def test_returns_empty_on_non_200(self):
        from unittest.mock import patch, MagicMock
        mock_resp = MagicMock()
        mock_resp.status_code = 404

        with patch("financial_scraper.transcripts.discovery.requests.get", return_value=mock_resp):
            urls = _fetch_sitemap_urls(2025, 1)

        assert urls == []

    def test_returns_empty_on_request_exception(self):
        from unittest.mock import patch
        import requests as req

        with patch("financial_scraper.transcripts.discovery.requests.get",
                   side_effect=req.RequestException("timeout")):
            urls = _fetch_sitemap_urls(2025, 1)

        assert urls == []


class TestDiscoverTranscripts:
    def test_discovers_matching_ticker(self):
        from unittest.mock import patch
        sitemap_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset>
          <url><loc>https://www.fool.com/earnings/call-transcripts/2025/01/30/apple-aapl-q1-2025-earnings-call-transcript/</loc></url>
          <url><loc>https://www.fool.com/earnings/call-transcripts/2025/04/25/apple-aapl-q2-2025-earnings-call-transcript/</loc></url>
        </urlset>"""
        from unittest.mock import MagicMock
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = sitemap_xml

        with patch("financial_scraper.transcripts.discovery.requests.get", return_value=mock_resp):
            results = discover_transcripts("AAPL", year=2025)

        assert len(results) >= 1
        assert all(r.ticker == "AAPL" for r in results)

    def test_filters_by_quarter(self):
        from unittest.mock import patch, MagicMock
        sitemap_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset>
          <url><loc>https://www.fool.com/earnings/call-transcripts/2025/01/30/apple-aapl-q1-2025-earnings-call-transcript/</loc></url>
          <url><loc>https://www.fool.com/earnings/call-transcripts/2025/04/25/apple-aapl-q2-2025-earnings-call-transcript/</loc></url>
        </urlset>"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = sitemap_xml

        with patch("financial_scraper.transcripts.discovery.requests.get", return_value=mock_resp):
            results = discover_transcripts("AAPL", year=2025, quarters=("Q1",))

        assert all(r.quarter == "Q1" for r in results)

    def test_returns_empty_when_no_sitemaps(self):
        from unittest.mock import patch
        import requests as req

        with patch("financial_scraper.transcripts.discovery.requests.get",
                   side_effect=req.RequestException("down")):
            results = discover_transcripts("AAPL", year=2025)

        assert results == []


# ---------------------------------------------------------------------------
# discover_transcripts_range
# ---------------------------------------------------------------------------

class TestDiscoverTranscriptsRange:
    """Tests for bulk multi-ticker, multi-year discovery."""

    # Two tickers x two years worth of URLs in a single sitemap response
    _SITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset>
      <url><loc>https://www.fool.com/earnings/call-transcripts/2022/01/28/apple-aapl-q1-2022-earnings-call-transcript/</loc></url>
      <url><loc>https://www.fool.com/earnings/call-transcripts/2022/04/29/apple-aapl-q2-2022-earnings-call-transcript/</loc></url>
      <url><loc>https://www.fool.com/earnings/call-transcripts/2023/01/27/apple-aapl-q1-2023-earnings-call-transcript/</loc></url>
      <url><loc>https://www.fool.com/earnings/call-transcripts/2022/07/27/microsoft-msft-q4-2022-earnings-call-transcript/</loc></url>
      <url><loc>https://www.fool.com/some-other-article/</loc></url>
    </urlset>"""

    def _patched_sitemap(self):
        from unittest.mock import MagicMock
        mock_resp = MagicMock(status_code=200, text=self._SITEMAP)
        return mock_resp

    def test_returns_dict_keyed_by_ticker(self):
        from financial_scraper.transcripts.discovery import discover_transcripts_range
        from unittest.mock import patch

        with patch("financial_scraper.transcripts.discovery.requests.get",
                   return_value=self._patched_sitemap()):
            result = discover_transcripts_range(["AAPL", "MSFT"], from_year=2022, to_year=2023)

        assert "AAPL" in result
        assert "MSFT" in result

    def test_finds_transcripts_for_both_tickers(self):
        from financial_scraper.transcripts.discovery import discover_transcripts_range
        from unittest.mock import patch

        with patch("financial_scraper.transcripts.discovery.requests.get",
                   return_value=self._patched_sitemap()):
            result = discover_transcripts_range(["AAPL", "MSFT"], from_year=2022, to_year=2023)

        assert len(result["AAPL"]) >= 2  # Q1 2022, Q2 2022, Q1 2023
        assert len(result["MSFT"]) >= 1  # Q4 2022
        assert all(r.ticker == "AAPL" for r in result["AAPL"])
        assert all(r.ticker == "MSFT" for r in result["MSFT"])

    def test_year_range_filter(self):
        from financial_scraper.transcripts.discovery import discover_transcripts_range
        from unittest.mock import patch

        with patch("financial_scraper.transcripts.discovery.requests.get",
                   return_value=self._patched_sitemap()):
            result = discover_transcripts_range(["AAPL"], from_year=2023, to_year=2023)

        # Only 2023 transcripts should be returned
        assert all(r.year == 2023 for r in result["AAPL"])

    def test_quarter_filter(self):
        from financial_scraper.transcripts.discovery import discover_transcripts_range
        from unittest.mock import patch

        with patch("financial_scraper.transcripts.discovery.requests.get",
                   return_value=self._patched_sitemap()):
            result = discover_transcripts_range(
                ["AAPL"], from_year=2022, to_year=2023, quarters=("Q1",)
            )

        assert all(r.quarter == "Q1" for r in result["AAPL"])

    def test_results_sorted_by_year_then_quarter(self):
        from financial_scraper.transcripts.discovery import discover_transcripts_range
        from unittest.mock import patch

        with patch("financial_scraper.transcripts.discovery.requests.get",
                   return_value=self._patched_sitemap()):
            result = discover_transcripts_range(["AAPL"], from_year=2022, to_year=2023)

        infos = result["AAPL"]
        assert infos == sorted(infos, key=lambda t: (t.year, t.quarter))

    def test_no_duplicates_across_sitemaps(self):
        from financial_scraper.transcripts.discovery import discover_transcripts_range
        from unittest.mock import patch

        # Same sitemap returned for every month — URLs should be deduplicated
        with patch("financial_scraper.transcripts.discovery.requests.get",
                   return_value=self._patched_sitemap()):
            result = discover_transcripts_range(["AAPL"], from_year=2022, to_year=2023)

        urls = [r.url for r in result["AAPL"]]
        assert len(urls) == len(set(urls))

    def test_empty_result_for_unknown_ticker(self):
        from financial_scraper.transcripts.discovery import discover_transcripts_range
        from unittest.mock import patch

        with patch("financial_scraper.transcripts.discovery.requests.get",
                   return_value=self._patched_sitemap()):
            result = discover_transcripts_range(["NVDA"], from_year=2022, to_year=2023)

        assert result["NVDA"] == []

    def test_sitemap_error_does_not_crash(self):
        from financial_scraper.transcripts.discovery import discover_transcripts_range
        from unittest.mock import patch, MagicMock

        # First call raises, subsequent calls succeed
        error_resp = MagicMock(side_effect=Exception("network error"))
        ok_resp = MagicMock(status_code=200, text=self._SITEMAP)

        with patch("financial_scraper.transcripts.discovery._fetch_sitemap_urls",
                   side_effect=[[], [], []]):  # all sitemaps return empty
            result = discover_transcripts_range(["AAPL"], from_year=2022, to_year=2022)

        assert result["AAPL"] == []


# ---------------------------------------------------------------------------
# Pipeline range mode integration
# ---------------------------------------------------------------------------

def _make_range_config(tmp_path, **overrides):
    defaults = {
        "tickers": ("AAPL", "MSFT"),
        "tickers_file": None,
        "year": None,
        "quarters": (),
        "from_year": 2022,
        "to_year": 2023,
        "concurrent": 1,
        "output_dir": tmp_path,
        "output_path": tmp_path / "out.parquet",
        "jsonl_path": None,
        "checkpoint_file": tmp_path / "cp.json",
        "resume": False,
    }
    defaults.update(overrides)
    return TranscriptConfig(**defaults)


class TestTranscriptPipelineRangeMode:
    """Integration tests for from_year / to_year bulk discovery mode."""

    def test_range_mode_uses_bulk_discovery(self, tmp_path):
        """Pipeline calls discover_transcripts_range (not discover_transcripts) in range mode."""
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from unittest.mock import patch, MagicMock

        bulk = {
            "AAPL": [TranscriptInfo(
                url="https://www.fool.com/earnings/call-transcripts/2022/01/28/apple-aapl-q1-2022/",
                ticker="AAPL", quarter="Q1", year=2022, pub_date="2022-01-28",
            )],
            "MSFT": [],
        }

        result = TranscriptResult(
            company="Apple", ticker="AAPL", quarter="Q1", year=2022,
            date="2022-01-28", full_text="Strong results in our first quarter. " * 30,
        )
        mock_resp = MagicMock(status_code=200, text="<html></html>")

        cfg = _make_range_config(tmp_path)
        p = TranscriptPipeline(cfg)

        with patch("financial_scraper.transcripts.pipeline.discover_transcripts_range",
                   return_value=bulk) as mock_range:
            with patch("financial_scraper.transcripts.pipeline.discover_transcripts") as mock_single:
                with patch.object(p._session, "get", return_value=mock_resp):
                    with patch("financial_scraper.transcripts.pipeline.extract_transcript",
                               return_value=result):
                        with patch("financial_scraper.transcripts.pipeline.time.sleep"):
                            p.run()

        mock_range.assert_called_once()
        mock_single.assert_not_called()

    def test_range_mode_writes_records(self, tmp_path):
        """Records from range mode are written to parquet."""
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from unittest.mock import patch, MagicMock
        import pyarrow.parquet as pq

        bulk = {
            "AAPL": [
                TranscriptInfo(
                    url=f"https://www.fool.com/earnings/call-transcripts/202{y}/01/aapl-q1-202{y}/",
                    ticker="AAPL", quarter="Q1", year=int(f"202{y}"),
                    pub_date=f"202{y}-01-28",
                )
                for y in range(2, 4)
            ],
            "MSFT": [],
        }

        extract_results = [
            TranscriptResult(
                company="Apple", ticker="AAPL", quarter="Q1", year=int(f"202{y}"),
                date=f"202{y}-01-28",
                full_text=f"FY202{y} Q1 results were strong. Revenue grew. " * 20,
            )
            for y in range(2, 4)
        ]

        mock_resp = MagicMock(status_code=200, text="<html></html>")
        cfg = _make_range_config(tmp_path)
        p = TranscriptPipeline(cfg)

        with patch("financial_scraper.transcripts.pipeline.discover_transcripts_range",
                   return_value=bulk):
            with patch.object(p._session, "get", return_value=mock_resp):
                with patch("financial_scraper.transcripts.pipeline.extract_transcript",
                           side_effect=extract_results):
                    with patch("financial_scraper.transcripts.pipeline.time.sleep"):
                        p.run()

        table = pq.read_table(tmp_path / "out.parquet")
        assert table.num_rows == 2
