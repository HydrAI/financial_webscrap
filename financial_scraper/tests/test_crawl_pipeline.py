"""Tests for financial_scraper.crawl — config, strategy, pipeline."""

import argparse
import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow.parquet as pq
import pytest

from financial_scraper.crawl.config import CrawlConfig, apply_stealth
from financial_scraper.crawl.pipeline import CrawlPipeline
from financial_scraper.main import build_crawl_config, _resolve_output_paths
from financial_scraper.store.output import make_source_file_tag


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class FakeCrawlResult:
    """Minimal stand-in for crawl4ai CrawlResult."""
    url: str
    html: str
    success: bool = True
    status_code: int = 200
    metadata: dict = None
    response_headers: dict = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.response_headers is None:
            self.response_headers = {}


_ARTICLE_COUNTER = 0


def _make_article_html(extra: str = "") -> str:
    """Generate unique article HTML to avoid trafilatura dedup cache."""
    global _ARTICLE_COUNTER
    _ARTICLE_COUNTER += 1
    return f"""
<html><head><title>Q4 Earnings Report {_ARTICLE_COUNTER}</title></head><body>
<article>
<h1>Q4 Earnings Report {_ARTICLE_COUNTER}</h1>
<p>The company reported strong quarterly results with revenue growth
across all segments in period {_ARTICLE_COUNTER}. Operating income improved significantly compared
to the prior year period. Management highlighted continued investment
in technology and product development as key drivers of future growth.
The board approved a new share repurchase program reflecting confidence
in the business outlook. Analysts noted the company exceeded consensus
estimates for both revenue and earnings per share this quarter. {extra}</p>
</article></body></html>
"""


def _make_crawl_config(tmp_path, **overrides) -> CrawlConfig:
    """Build a test CrawlConfig pointing to tmp_path."""
    urls_file = tmp_path / "urls.txt"
    if not urls_file.exists():
        urls_file.write_text("https://example.com\n")

    defaults = dict(
        urls_file=urls_file,
        min_word_count=10,
        output_dir=tmp_path,
        output_path=tmp_path / "crawl_test.parquet",
        checkpoint_file=tmp_path / ".crawl_checkpoint.json",
    )
    defaults.update(overrides)
    return CrawlConfig(**defaults)


# ---------------------------------------------------------------------------
# CrawlConfig
# ---------------------------------------------------------------------------

class TestCrawlConfig:
    def test_defaults(self):
        cfg = CrawlConfig()
        assert cfg.max_depth == 2
        assert cfg.max_pages == 50
        assert cfg.semaphore_count == 2
        assert cfg.min_word_count == 100
        assert cfg.check_robots_txt is True
        assert cfg.stealth is False

    def test_frozen(self):
        cfg = CrawlConfig()
        with pytest.raises(AttributeError):
            cfg.max_depth = 5

    def test_apply_stealth(self):
        cfg = CrawlConfig(stealth=True, semaphore_count=4)
        result = apply_stealth(cfg)
        assert result.semaphore_count == 1  # overridden
        assert result.stealth is True

    def test_apply_stealth_noop(self):
        cfg = CrawlConfig(stealth=False, semaphore_count=4)
        result = apply_stealth(cfg)
        assert result.semaphore_count == 4  # unchanged


# ---------------------------------------------------------------------------
# make_source_file_tag for crawl mode
# ---------------------------------------------------------------------------

class TestSourceFileTagCrawl:
    def test_crawl_mode(self):
        tag = make_source_file_tag("reuters.com", "2026-01-15", "crawl")
        assert "crawl" in tag
        assert "reuters_com" in tag
        assert "2026Q1" in tag
        assert tag.endswith(".parquet")

    def test_crawl_no_ddg(self):
        tag = make_source_file_tag("example.com", "2026-06-01", "crawl")
        assert "ddg" not in tag
        assert "crawl" in tag


# ---------------------------------------------------------------------------
# Load seed URLs
# ---------------------------------------------------------------------------

class TestLoadSeedUrls:
    def test_load_from_file(self, tmp_path):
        urls_file = tmp_path / "urls.txt"
        urls_file.write_text("https://a.com\nhttps://b.com\nhttps://c.com\n")
        cfg = _make_crawl_config(tmp_path, urls_file=urls_file)
        pipeline = CrawlPipeline(cfg)
        urls = pipeline._load_seed_urls()
        assert urls == ["https://a.com", "https://b.com", "https://c.com"]

    def test_skip_comments_and_blanks(self, tmp_path):
        urls_file = tmp_path / "urls.txt"
        urls_file.write_text("# comment\nhttps://a.com\n\n# another\nhttps://b.com\n")
        cfg = _make_crawl_config(tmp_path, urls_file=urls_file)
        pipeline = CrawlPipeline(cfg)
        urls = pipeline._load_seed_urls()
        assert urls == ["https://a.com", "https://b.com"]

    def test_missing_file(self, tmp_path):
        cfg = _make_crawl_config(
            tmp_path, urls_file=tmp_path / "nonexistent.txt"
        )
        pipeline = CrawlPipeline(cfg)
        urls = pipeline._load_seed_urls()
        assert urls == []


# ---------------------------------------------------------------------------
# Exclusions
# ---------------------------------------------------------------------------

class TestCrawlExclusions:
    def test_excluded_domain_skipped(self, tmp_path):
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("badsite.com\n")
        cfg = _make_crawl_config(tmp_path, exclude_file=exclude_file)
        pipeline = CrawlPipeline(cfg)
        pipeline._exclusions = pipeline._load_exclusions()

        assert pipeline._is_excluded_domain("https://badsite.com/page") is True
        assert pipeline._is_excluded_domain("https://goodsite.com/page") is False

    def test_subdomain_excluded(self, tmp_path):
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("example.com\n")
        cfg = _make_crawl_config(tmp_path, exclude_file=exclude_file)
        pipeline = CrawlPipeline(cfg)
        pipeline._exclusions = pipeline._load_exclusions()

        assert pipeline._is_excluded_domain("https://sub.example.com/page") is True


# ---------------------------------------------------------------------------
# Extract domain
# ---------------------------------------------------------------------------

class TestExtractDomain:
    def test_simple(self):
        assert CrawlPipeline._extract_domain("https://reuters.com/page") == "reuters.com"

    def test_strips_www(self):
        assert CrawlPipeline._extract_domain("https://www.reuters.com/page") == "reuters.com"

    def test_preserves_subdomain(self):
        assert CrawlPipeline._extract_domain("https://finance.yahoo.com/q") == "finance.yahoo.com"


# ---------------------------------------------------------------------------
# Checkpoint resume
# ---------------------------------------------------------------------------

class TestCrawlCheckpoint:
    @pytest.mark.asyncio
    async def test_resume_skips_done(self, tmp_path):
        """Seed URLs already in checkpoint are skipped."""
        urls_file = tmp_path / "urls.txt"
        urls_file.write_text("https://done.com\nhttps://new.com\n")

        # Pre-populate checkpoint
        cp_file = tmp_path / ".crawl_checkpoint.json"
        cp_data = {
            "completed_queries": ["https://done.com"],
            "fetched_urls": [],
            "failed_urls": {},
            "stats": {
                "total_queries": 1, "total_pages": 0,
                "total_words": 0, "failed_fetches": 0,
                "failed_extractions": 0,
            },
        }
        cp_file.write_text(json.dumps(cp_data))

        cfg = _make_crawl_config(
            tmp_path, urls_file=urls_file,
            checkpoint_file=cp_file, resume=True,
        )

        fake_result = FakeCrawlResult(
            url="https://new.com",
            html=_make_article_html(),
        )

        with patch("financial_scraper.crawl.pipeline.AsyncWebCrawler") as MockCrawler:
            instance = AsyncMock()
            instance.arun.return_value = [fake_result]
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockCrawler.return_value = instance

            with patch("financial_scraper.crawl.pipeline.build_browser_config"):
                with patch("financial_scraper.crawl.pipeline.build_crawl_strategy"):
                    with patch("financial_scraper.crawl.pipeline.build_crawler_config"):
                        pipeline = CrawlPipeline(cfg)
                        await pipeline.run()

            # Only "new.com" should be crawled
            assert instance.arun.call_count == 1
            call_url = instance.arun.call_args[1].get("url") or instance.arun.call_args[0][0]
            assert "new.com" in call_url


# ---------------------------------------------------------------------------
# Full pipeline with mock crawl4ai
# ---------------------------------------------------------------------------

class TestCrawlPipeline:
    @pytest.mark.asyncio
    async def test_basic_crawl_extracts_and_stores(self, tmp_path):
        """Mock crawl4ai, verify extraction and parquet output."""
        urls_file = tmp_path / "urls.txt"
        urls_file.write_text("https://example.com\n")

        parquet_path = tmp_path / "crawl_out.parquet"
        cfg = _make_crawl_config(
            tmp_path,
            urls_file=urls_file,
            output_path=parquet_path,
        )

        fake_results = [
            FakeCrawlResult(url="https://example.com", html=_make_article_html()),
            FakeCrawlResult(url="https://example.com/page2", html=_make_article_html()),
        ]

        with patch("financial_scraper.crawl.pipeline.AsyncWebCrawler") as MockCrawler:
            instance = AsyncMock()
            instance.arun.return_value = fake_results
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockCrawler.return_value = instance

            with patch("financial_scraper.crawl.pipeline.build_browser_config"):
                with patch("financial_scraper.crawl.pipeline.build_crawl_strategy"):
                    with patch("financial_scraper.crawl.pipeline.build_crawler_config"):
                        pipeline = CrawlPipeline(cfg)
                        await pipeline.run()

        # Verify parquet was written
        assert parquet_path.exists()
        table = pq.read_table(parquet_path)
        assert len(table) >= 1
        # Company should be the seed domain
        companies = table.column("company").to_pylist()
        assert all(c == "example.com" for c in companies)
        # source_file should contain "crawl"
        source_files = table.column("source_file").to_pylist()
        assert all("crawl" in sf for sf in source_files)

    @pytest.mark.asyncio
    async def test_failed_crawl_results_skipped(self, tmp_path):
        """Pages with success=False are skipped."""
        urls_file = tmp_path / "urls.txt"
        urls_file.write_text("https://example.com\n")

        parquet_path = tmp_path / "crawl_out.parquet"
        cfg = _make_crawl_config(
            tmp_path,
            urls_file=urls_file,
            output_path=parquet_path,
        )

        fake_results = [
            FakeCrawlResult(url="https://example.com", html="", success=False),
        ]

        with patch("financial_scraper.crawl.pipeline.AsyncWebCrawler") as MockCrawler:
            instance = AsyncMock()
            instance.arun.return_value = fake_results
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockCrawler.return_value = instance

            with patch("financial_scraper.crawl.pipeline.build_browser_config"):
                with patch("financial_scraper.crawl.pipeline.build_crawl_strategy"):
                    with patch("financial_scraper.crawl.pipeline.build_crawler_config"):
                        pipeline = CrawlPipeline(cfg)
                        await pipeline.run()

        # No parquet should be created (no successful extractions)
        assert not parquet_path.exists()

    @pytest.mark.asyncio
    async def test_single_result_not_list(self, tmp_path):
        """arun returning a single CrawlResult (not wrapped in list) works."""
        urls_file = tmp_path / "urls.txt"
        urls_file.write_text("https://single-test.com\n")

        parquet_path = tmp_path / "crawl_out.parquet"
        cfg = _make_crawl_config(
            tmp_path,
            urls_file=urls_file,
            output_path=parquet_path,
        )

        # Return a single result, not a list
        fake_result = FakeCrawlResult(url="https://single-test.com", html=_make_article_html())

        with patch("financial_scraper.crawl.pipeline.AsyncWebCrawler") as MockCrawler:
            instance = AsyncMock()
            instance.arun.return_value = fake_result  # not a list
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockCrawler.return_value = instance

            with patch("financial_scraper.crawl.pipeline.build_browser_config"):
                with patch("financial_scraper.crawl.pipeline.build_crawl_strategy"):
                    with patch("financial_scraper.crawl.pipeline.build_crawler_config"):
                        pipeline = CrawlPipeline(cfg)
                        await pipeline.run()

        assert parquet_path.exists()

    @pytest.mark.asyncio
    async def test_crawl_exception_handled(self, tmp_path):
        """If arun raises, the seed is marked done and we continue."""
        urls_file = tmp_path / "urls.txt"
        urls_file.write_text("https://bad.com\nhttps://good.com\n")

        parquet_path = tmp_path / "crawl_out.parquet"
        cfg = _make_crawl_config(
            tmp_path,
            urls_file=urls_file,
            output_path=parquet_path,
        )

        good_result = FakeCrawlResult(url="https://good.com", html=_make_article_html())

        call_count = 0

        async def side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            url = kwargs.get("url", "")
            if "bad.com" in url:
                raise RuntimeError("Connection failed")
            return [good_result]

        with patch("financial_scraper.crawl.pipeline.AsyncWebCrawler") as MockCrawler:
            instance = AsyncMock()
            instance.arun.side_effect = side_effect
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockCrawler.return_value = instance

            with patch("financial_scraper.crawl.pipeline.build_browser_config"):
                with patch("financial_scraper.crawl.pipeline.build_crawl_strategy"):
                    with patch("financial_scraper.crawl.pipeline.build_crawler_config"):
                        pipeline = CrawlPipeline(cfg)
                        await pipeline.run()

        # Both seeds were attempted
        assert call_count == 2
        # good.com should have produced output
        assert parquet_path.exists()

    @pytest.mark.asyncio
    async def test_jsonl_output(self, tmp_path):
        """JSONL writer is used when configured."""
        urls_file = tmp_path / "urls.txt"
        urls_file.write_text("https://jsonl-test.com\n")

        parquet_path = tmp_path / "crawl_out.parquet"
        jsonl_path = tmp_path / "crawl_out.jsonl"
        cfg = _make_crawl_config(
            tmp_path,
            urls_file=urls_file,
            output_path=parquet_path,
            jsonl_path=jsonl_path,
        )

        fake_result = FakeCrawlResult(url="https://jsonl-test.com", html=_make_article_html())

        with patch("financial_scraper.crawl.pipeline.AsyncWebCrawler") as MockCrawler:
            instance = AsyncMock()
            instance.arun.return_value = [fake_result]
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockCrawler.return_value = instance

            with patch("financial_scraper.crawl.pipeline.build_browser_config"):
                with patch("financial_scraper.crawl.pipeline.build_crawl_strategy"):
                    with patch("financial_scraper.crawl.pipeline.build_crawler_config"):
                        pipeline = CrawlPipeline(cfg)
                        await pipeline.run()

        assert jsonl_path.exists()
        lines = jsonl_path.read_text().strip().split("\n")
        assert len(lines) >= 1
        data = json.loads(lines[0])
        assert data["company"] == "jsonl-test.com"


# ---------------------------------------------------------------------------
# build_crawl_config (CLI args -> CrawlConfig)
# ---------------------------------------------------------------------------

def _make_crawl_args(**overrides):
    defaults = {
        "urls_file": "urls.txt",
        "output_dir": None,
        "max_depth": 2,
        "max_pages": 50,
        "semaphore_count": 2,
        "min_words": 100,
        "target_language": None,
        "no_favor_precision": False,
        "date_from": None,
        "date_to": None,
        "jsonl": False,
        "markdown": False,
        "exclude_file": None,
        "checkpoint": ".crawl_checkpoint.json",
        "resume": False,
        "no_robots": False,
        "stealth": False,
        "pdf_extractor": "auto",
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestBuildCrawlConfig:
    def test_basic(self, tmp_path):
        args = _make_crawl_args(output_dir=str(tmp_path))
        cfg = build_crawl_config(args)
        assert cfg.max_depth == 2
        assert cfg.max_pages == 50
        assert cfg.urls_file == Path("urls.txt")

    def test_custom_depth_and_pages(self, tmp_path):
        args = _make_crawl_args(
            output_dir=str(tmp_path), max_depth=3, max_pages=100,
        )
        cfg = build_crawl_config(args)
        assert cfg.max_depth == 3
        assert cfg.max_pages == 100

    def test_stealth(self, tmp_path):
        args = _make_crawl_args(output_dir=str(tmp_path), stealth=True)
        cfg = build_crawl_config(args)
        assert cfg.stealth is True

    def test_no_robots(self, tmp_path):
        args = _make_crawl_args(output_dir=str(tmp_path), no_robots=True)
        cfg = build_crawl_config(args)
        assert cfg.check_robots_txt is False

    def test_output_paths_prefixed_crawl(self, tmp_path):
        args = _make_crawl_args(output_dir=str(tmp_path))
        cfg = build_crawl_config(args)
        assert "crawl_" in cfg.output_path.name


# ---------------------------------------------------------------------------
# Backward compatibility: resolve_output_paths with crawl prefix
# ---------------------------------------------------------------------------

class TestResolveOutputPathsCrawl:
    def test_crawl_prefix(self, tmp_path):
        args = _make_crawl_args(output_dir=str(tmp_path))
        _, out_path, _, _ = _resolve_output_paths(args, prefix="crawl")
        assert "crawl_" in out_path.name
        assert out_path.suffix == ".parquet"


# ---------------------------------------------------------------------------
# PDF Detection
# ---------------------------------------------------------------------------

class TestPdfDetection:
    def test_pdf_url_detected(self):
        assert CrawlPipeline._is_pdf("https://example.com/report.pdf", {}) is True

    def test_pdf_url_case_insensitive(self):
        assert CrawlPipeline._is_pdf("https://example.com/report.PDF", {}) is True

    def test_content_type_detected(self):
        headers = {"content-type": "application/pdf; charset=utf-8"}
        assert CrawlPipeline._is_pdf("https://example.com/doc", headers) is True

    def test_non_pdf_not_detected(self):
        assert CrawlPipeline._is_pdf("https://example.com/page.html", {}) is False

    def test_non_pdf_content_type(self):
        headers = {"content-type": "text/html"}
        assert CrawlPipeline._is_pdf("https://example.com/page", headers) is False


# ---------------------------------------------------------------------------
# PDF Download + Extract in Pipeline
# ---------------------------------------------------------------------------

class TestPdfDownloadAndExtract:
    @pytest.mark.asyncio
    async def test_pdf_routed_through_pdf_extractor(self, tmp_path):
        """PDF URLs should be downloaded and extracted via PDF extractor."""
        urls_file = tmp_path / "urls.txt"
        urls_file.write_text("https://example.com\n")

        parquet_path = tmp_path / "crawl_out.parquet"
        cfg = _make_crawl_config(
            tmp_path, urls_file=urls_file, output_path=parquet_path,
        )

        fake_result = FakeCrawlResult(
            url="https://example.com/report.pdf",
            html="",
            response_headers={"content-type": "application/pdf"},
        )

        fake_pdf_bytes = b"%PDF-1.4 fake content"
        mock_extraction = MagicMock()
        mock_extraction.extraction_method = "pdfplumber"
        mock_extraction.word_count = 200
        mock_extraction.text = "Extracted PDF content " * 20
        mock_extraction.title = "Report"
        mock_extraction.date = None

        with patch("financial_scraper.crawl.pipeline.AsyncWebCrawler") as MockCrawler:
            instance = AsyncMock()
            instance.arun.return_value = [fake_result]
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockCrawler.return_value = instance

            with patch("financial_scraper.crawl.pipeline.build_browser_config"), \
                 patch("financial_scraper.crawl.pipeline.build_crawl_strategy"), \
                 patch("financial_scraper.crawl.pipeline.build_crawler_config"):
                pipeline = CrawlPipeline(cfg)

                # Mock the PDF extractor and download
                pipeline._pdf_extractor = MagicMock()
                pipeline._pdf_extractor.extract.return_value = mock_extraction
                pipeline._download_pdf_bytes = AsyncMock(return_value=fake_pdf_bytes)

                await pipeline.run()

        pipeline._download_pdf_bytes.assert_called_once_with("https://example.com/report.pdf")
        pipeline._pdf_extractor.extract.assert_called_once_with(fake_pdf_bytes, "https://example.com/report.pdf")
        assert parquet_path.exists()


class TestPdfDownloadFailure:
    @pytest.mark.asyncio
    async def test_pdf_download_failure_graceful(self, tmp_path):
        """Failed PDF download should not crash the pipeline."""
        urls_file = tmp_path / "urls.txt"
        urls_file.write_text("https://example.com\n")

        parquet_path = tmp_path / "crawl_out.parquet"
        cfg = _make_crawl_config(
            tmp_path, urls_file=urls_file, output_path=parquet_path,
        )

        fake_result = FakeCrawlResult(
            url="https://example.com/missing.pdf",
            html="",
            response_headers={},
        )

        with patch("financial_scraper.crawl.pipeline.AsyncWebCrawler") as MockCrawler:
            instance = AsyncMock()
            instance.arun.return_value = [fake_result]
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockCrawler.return_value = instance

            with patch("financial_scraper.crawl.pipeline.build_browser_config"), \
                 patch("financial_scraper.crawl.pipeline.build_crawl_strategy"), \
                 patch("financial_scraper.crawl.pipeline.build_crawler_config"):
                pipeline = CrawlPipeline(cfg)
                pipeline._download_pdf_bytes = AsyncMock(return_value=None)

                await pipeline.run()

        # No output — PDF download failed
        assert not parquet_path.exists()


# ---------------------------------------------------------------------------
# get_pdf_extractor factory
# ---------------------------------------------------------------------------

class TestGetPdfExtractor:
    def test_pdfplumber_always_works(self):
        from financial_scraper.extract.pdf import get_pdf_extractor, PDFExtractor
        ext = get_pdf_extractor("pdfplumber")
        assert isinstance(ext, PDFExtractor)

    def test_auto_returns_extractor(self):
        from financial_scraper.extract.pdf import get_pdf_extractor, PDFExtractor
        ext = get_pdf_extractor("auto")
        # Should return either PDFExtractor or DoclingExtractor depending on env
        assert hasattr(ext, "extract")

    def test_auto_falls_back_without_docling(self):
        from financial_scraper.extract.pdf import PDFExtractor
        import financial_scraper.extract.pdf as pdf_mod
        original = pdf_mod.DOCLING_AVAILABLE
        try:
            pdf_mod.DOCLING_AVAILABLE = False
            ext = pdf_mod.get_pdf_extractor("auto")
            assert isinstance(ext, PDFExtractor)
        finally:
            pdf_mod.DOCLING_AVAILABLE = original

    def test_docling_raises_when_unavailable(self):
        import financial_scraper.extract.pdf as pdf_mod
        original = pdf_mod.DOCLING_AVAILABLE
        try:
            pdf_mod.DOCLING_AVAILABLE = False
            with pytest.raises(ImportError):
                pdf_mod.get_pdf_extractor("docling")
        finally:
            pdf_mod.DOCLING_AVAILABLE = original
