"""Tests for financial_scraper.pipeline."""

import asyncio
from collections import Counter
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from financial_scraper.config import ScraperConfig
from financial_scraper.pipeline import ScraperPipeline


def _make_pipeline(tmp_path, **config_overrides):
    defaults = {
        "queries_file": tmp_path / "queries.txt",
        "output_path": tmp_path / "out.parquet",
        "checkpoint_file": tmp_path / "cp.json",
        "search_delay_min": 0.0,
        "search_delay_max": 0.0,
        "min_word_count": 5,
    }
    defaults.update(config_overrides)
    return ScraperPipeline(ScraperConfig(**defaults))


class TestLoadQueries:
    def test_loads_lines(self, tmp_path):
        qf = tmp_path / "queries.txt"
        qf.write_text("query one\nquery two\n# comment\n\nquery three\n")
        p = _make_pipeline(tmp_path)
        result = p._load_queries()
        assert result == ["query one", "query two", "query three"]

    def test_missing_file_returns_empty(self, tmp_path):
        p = _make_pipeline(tmp_path, queries_file=tmp_path / "missing.txt")
        result = p._load_queries()
        assert result == []

    def test_skips_blank_lines_and_comments(self, tmp_path):
        qf = tmp_path / "queries.txt"
        qf.write_text("# header\n\n  \nreal query\n")
        p = _make_pipeline(tmp_path)
        result = p._load_queries()
        assert result == ["real query"]


class TestLoadExclusions:
    def test_loads_domains(self, tmp_path):
        ef = tmp_path / "exclude.txt"
        ef.write_text("https://bad.com/path\nhttp://www.evil.org\nnaked.io\n# comment\n")
        p = _make_pipeline(tmp_path, exclude_file=ef)
        result = p._load_exclusions()
        assert "bad.com" in result
        assert "evil.org" in result
        assert "naked.io" in result

    def test_no_exclude_file_returns_empty(self, tmp_path):
        p = _make_pipeline(tmp_path, exclude_file=None)
        result = p._load_exclusions()
        assert result == set()

    def test_missing_exclude_file_returns_empty(self, tmp_path):
        p = _make_pipeline(tmp_path, exclude_file=tmp_path / "missing.txt")
        result = p._load_exclusions()
        assert result == set()


class TestExtractDomain:
    def test_basic(self):
        assert ScraperPipeline._extract_domain("https://Example.COM/page") == "example.com"

    def test_with_port(self):
        assert ScraperPipeline._extract_domain("https://example.com:8080/p") == "example.com:8080"


class TestPrintSummary:
    def test_runs_without_error(self, tmp_path):
        p = _make_pipeline(tmp_path)
        p._method_counter = Counter({"trafilatura": 5})
        p._domain_counter = Counter({"example.com": 3})
        p._print_summary(total_records=5, total_queries=2)

    def test_with_date_filter_active(self, tmp_path):
        p = _make_pipeline(tmp_path, date_from="2024-01-01")
        p._print_summary(total_records=0, total_queries=1)

    def test_with_tor(self, tmp_path):
        p = _make_pipeline(tmp_path)
        mock_tor = MagicMock()
        mock_tor._circuits_renewed = 3
        p._tor = mock_tor
        p._print_summary(total_records=0, total_queries=1)


class TestRunNoQueries:
    def test_empty_queries_file(self, tmp_path):
        qf = tmp_path / "queries.txt"
        qf.write_text("# only comments\n")
        p = _make_pipeline(tmp_path)

        with patch("financial_scraper.pipeline.DDGSearcher"):
            asyncio.run(p.run())


class TestRunWithResults:
    def _run_pipeline(self, tmp_path, search_results, fetch_results,
                      extraction_result=None, pdf_extraction=None,
                      **config_overrides):
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult

        qf = tmp_path / "queries.txt"
        qf.write_text("test query\n")

        p = _make_pipeline(tmp_path, **config_overrides)

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = search_results

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            with patch("financial_scraper.pipeline.FetchClient") as MockClient:
                mock_client_instance = AsyncMock()
                mock_client_instance.fetch_batch.return_value = fetch_results
                MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_client_instance)
                MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

                if extraction_result:
                    with patch.object(p._extractor, "extract", return_value=extraction_result):
                        asyncio.run(p.run())
                elif pdf_extraction:
                    with patch.object(p._pdf_extractor, "extract", return_value=pdf_extraction):
                        asyncio.run(p.run())
                else:
                    asyncio.run(p.run())

        return p

    def test_full_pipeline_mocked(self, tmp_path):
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult
        from financial_scraper.extract.html import ExtractionResult

        html_content = " ".join(["word"] * 50)
        p = self._run_pipeline(
            tmp_path,
            search_results=[
                SearchResult(url="https://example.com/1", title="T1",
                             snippet="S1", search_rank=1, query="test query"),
            ],
            fetch_results=[
                FetchResult(url="https://example.com/1", status=200,
                            html=f"<html>{html_content}</html>",
                            content_type="text/html", content_bytes=None,
                            error=None, response_headers={}),
            ],
            extraction_result=ExtractionResult(
                text=html_content, title="Test Title", author=None,
                date="2024-06-15", word_count=50,
                extraction_method="trafilatura", language=None,
            ),
        )
        assert (tmp_path / "out.parquet").exists()

    def test_pipeline_with_failed_fetch(self, tmp_path):
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult

        p = self._run_pipeline(
            tmp_path,
            search_results=[
                SearchResult(url="https://example.com/1", title="T1",
                             snippet="S1", search_rank=1, query="test query"),
            ],
            fetch_results=[
                FetchResult(url="https://example.com/1", status=403,
                            html=None, content_type="", content_bytes=None,
                            error="HTTP 403", response_headers={}),
            ],
        )
        assert p._checkpoint.stats["failed_fetches"] >= 1

    def test_pipeline_skips_duplicate_urls(self, tmp_path):
        from financial_scraper.search.duckduckgo import SearchResult

        qf = tmp_path / "queries.txt"
        qf.write_text("test query\n")
        p = _make_pipeline(tmp_path)
        p._dedup.mark_seen("https://example.com/1", "content")

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = [
            SearchResult(url="https://example.com/1", title="T1",
                         snippet="S1", search_rank=1, query="test query"),
        ]

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            with patch("financial_scraper.pipeline.FetchClient") as MockClient:
                asyncio.run(p.run())
                MockClient.assert_not_called()

    def test_pipeline_pdf_extraction(self, tmp_path):
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult
        from financial_scraper.extract.html import ExtractionResult

        p = self._run_pipeline(
            tmp_path,
            search_results=[
                SearchResult(url="https://example.com/report.pdf", title="PDF",
                             snippet="S", search_rank=1, query="test query"),
            ],
            fetch_results=[
                FetchResult(url="https://example.com/report.pdf", status=200,
                            html=None, content_type="application/pdf",
                            content_bytes=b"fake-pdf", error=None, response_headers={}),
            ],
            pdf_extraction=ExtractionResult(
                text=" ".join(["word"] * 50), title="Report", author=None,
                date=None, word_count=50, extraction_method="pdfplumber",
                language=None,
            ),
        )
        assert p._method_counter["pdfplumber"] == 1

    def test_pipeline_with_exclusions(self, tmp_path):
        from financial_scraper.search.duckduckgo import SearchResult

        ef = tmp_path / "exclude.txt"
        ef.write_text("example.com\n")
        qf = tmp_path / "queries.txt"
        qf.write_text("test query\n")
        p = _make_pipeline(tmp_path, exclude_file=ef)

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = [
            SearchResult(url="https://example.com/1", title="T1",
                         snippet="S1", search_rank=1, query="test query"),
        ]

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            with patch("financial_scraper.pipeline.FetchClient") as MockClient:
                asyncio.run(p.run())
                MockClient.assert_not_called()

    def test_pipeline_no_search_results(self, tmp_path):
        qf = tmp_path / "queries.txt"
        qf.write_text("test query\n")
        p = _make_pipeline(tmp_path)

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = []

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            asyncio.run(p.run())

        assert p._checkpoint.is_query_done("test query")

    def test_pipeline_failed_extraction(self, tmp_path):
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult
        from financial_scraper.extract.html import ExtractionResult

        p = self._run_pipeline(
            tmp_path,
            search_results=[
                SearchResult(url="https://example.com/1", title="T1",
                             snippet="S1", search_rank=1, query="test query"),
            ],
            fetch_results=[
                FetchResult(url="https://example.com/1", status=200,
                            html="<html>short</html>", content_type="text/html",
                            content_bytes=None, error=None, response_headers={}),
            ],
            extraction_result=ExtractionResult(
                text="", title=None, author=None, date=None,
                word_count=0, extraction_method="failed", language=None,
            ),
        )
        assert p._checkpoint.stats["failed_extractions"] >= 1

    def test_pipeline_date_filter(self, tmp_path):
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult
        from financial_scraper.extract.html import ExtractionResult

        html_content = " ".join(["word"] * 50)
        p = self._run_pipeline(
            tmp_path,
            search_results=[
                SearchResult(url="https://example.com/1", title="T1",
                             snippet="S1", search_rank=1, query="test query"),
            ],
            fetch_results=[
                FetchResult(url="https://example.com/1", status=200,
                            html="<html>x</html>", content_type="text/html",
                            content_bytes=None, error=None, response_headers={}),
            ],
            extraction_result=ExtractionResult(
                text=html_content, title="T", author=None,
                date="2020-01-01", word_count=50,
                extraction_method="trafilatura", language=None,
            ),
            date_from="2024-01-01",
        )
        # Article from 2020 should be filtered out
        assert not (tmp_path / "out.parquet").exists()

    def test_pipeline_content_dedup(self, tmp_path):
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult
        from financial_scraper.extract.html import ExtractionResult

        content = " ".join(["word"] * 50)
        qf = tmp_path / "queries.txt"
        qf.write_text("test query\n")
        p = _make_pipeline(tmp_path)
        # Pre-mark the content as seen
        p._dedup._seen_content.add(p._dedup._hash_content(content))

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = [
            SearchResult(url="https://example.com/1", title="T1",
                         snippet="S1", search_rank=1, query="test query"),
        ]

        extraction = ExtractionResult(
            text=content, title="T", author=None, date=None,
            word_count=50, extraction_method="trafilatura", language=None,
        )

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            with patch("financial_scraper.pipeline.FetchClient") as MockClient:
                mock_client_instance = AsyncMock()
                mock_client_instance.fetch_batch.return_value = [
                    FetchResult(url="https://example.com/1", status=200,
                                html="<html>x</html>", content_type="text/html",
                                content_bytes=None, error=None, response_headers={}),
                ]
                MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_client_instance)
                MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

                with patch.object(p._extractor, "extract", return_value=extraction):
                    asyncio.run(p.run())

        # Content was duplicate, so no parquet file created
        assert not (tmp_path / "out.parquet").exists()

    def test_pipeline_with_jsonl(self, tmp_path):
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult
        from financial_scraper.extract.html import ExtractionResult

        jsonl_path = tmp_path / "out.jsonl"
        html_content = " ".join(["word"] * 50)

        qf = tmp_path / "queries.txt"
        qf.write_text("test query\n")
        p = _make_pipeline(tmp_path, jsonl_path=jsonl_path)

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = [
            SearchResult(url="https://example.com/1", title="T1",
                         snippet="S1", search_rank=1, query="test query"),
        ]

        extraction = ExtractionResult(
            text=html_content, title="T", author=None, date="2024-06-15",
            word_count=50, extraction_method="trafilatura", language=None,
        )

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            with patch("financial_scraper.pipeline.FetchClient") as MockClient:
                mock_client_instance = AsyncMock()
                mock_client_instance.fetch_batch.return_value = [
                    FetchResult(url="https://example.com/1", status=200,
                                html="<html>x</html>", content_type="text/html",
                                content_bytes=None, error=None, response_headers={}),
                ]
                MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_client_instance)
                MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

                with patch.object(p._extractor, "extract", return_value=extraction):
                    asyncio.run(p.run())

        assert jsonl_path.exists()

    def test_pipeline_resume(self, tmp_path):
        from financial_scraper.checkpoint import Checkpoint

        qf = tmp_path / "queries.txt"
        qf.write_text("query1\nquery2\n")
        cp_path = tmp_path / "cp.json"

        # Pre-create checkpoint with query1 done
        cp = Checkpoint(cp_path)
        cp.mark_query_done("query1")

        p = _make_pipeline(tmp_path, resume=True)

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = []

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            asyncio.run(p.run())

        # query1 was skipped, only query2 was searched
        assert mock_searcher.search.call_count == 1

    def test_pipeline_fetch_no_html_no_bytes(self, tmp_path):
        """Test when fetch returns 200 but no html and no content_bytes."""
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult

        p = self._run_pipeline(
            tmp_path,
            search_results=[
                SearchResult(url="https://example.com/1", title="T1",
                             snippet="S1", search_rank=1, query="test query"),
            ],
            fetch_results=[
                FetchResult(url="https://example.com/1", status=200,
                            html=None, content_type="text/html",
                            content_bytes=None, error=None, response_headers={}),
            ],
        )
        # No output file created since extraction couldn't proceed
        assert not (tmp_path / "out.parquet").exists()


class TestCrawlFeature:
    """Tests for the BFS deep-crawl feature."""

    def _setup_crawl_pipeline(self, tmp_path, *, crawl, crawl_depth=2,
                               max_pages_per_domain=50):
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult
        from financial_scraper.extract.html import ExtractionResult

        qf = tmp_path / "queries.txt"
        qf.write_text("test query\n")

        p = _make_pipeline(
            tmp_path, crawl=crawl, crawl_depth=crawl_depth,
            max_pages_per_domain=max_pages_per_domain,
        )

        html_with_links = (
            '<html><body>'
            '<a href="https://example.com/page2">Link</a>'
            '<p>' + " ".join(["word"] * 50) + '</p>'
            '</body></html>'
        )

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = [
            SearchResult(url="https://example.com/page1", title="T1",
                         snippet="S1", search_rank=1, query="test query"),
        ]

        extraction = ExtractionResult(
            text=" ".join(["word"] * 50), title="Title", author=None,
            date=None, word_count=50, extraction_method="trafilatura",
            language=None,
        )

        return p, mock_searcher, html_with_links, extraction, SearchResult, FetchResult

    def test_crawl_false_single_fetch(self, tmp_path):
        """crawl=False: fetch_batch called once even if HTML has links."""
        p, mock_searcher, html, extraction, SR, FR = self._setup_crawl_pipeline(
            tmp_path, crawl=False,
        )

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            with patch("financial_scraper.pipeline.FetchClient") as MockClient:
                mock_instance = AsyncMock()
                mock_instance.fetch_batch.return_value = [
                    FR(url="https://example.com/page1", status=200,
                       html=html, content_type="text/html",
                       content_bytes=None, error=None, response_headers={}),
                ]
                MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
                MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

                with patch.object(p._extractor, "extract", return_value=extraction):
                    asyncio.run(p.run())

                assert mock_instance.fetch_batch.call_count == 1

    def test_crawl_true_follows_links(self, tmp_path):
        """crawl=True: fetch_batch called twice (depth 0 + depth 1)."""
        p, mock_searcher, html, extraction, SR, FR = self._setup_crawl_pipeline(
            tmp_path, crawl=True, crawl_depth=1,
        )

        depth1_html = '<html><body><p>' + " ".join(["other"] * 50) + '</p></body></html>'

        call_count = [0]

        def fake_fetch_batch(urls):
            call_count[0] += 1
            if call_count[0] == 1:
                return [
                    FR(url=urls[0], status=200, html=html,
                       content_type="text/html", content_bytes=None,
                       error=None, response_headers={}),
                ]
            else:
                return [
                    FR(url=u, status=200, html=depth1_html,
                       content_type="text/html", content_bytes=None,
                       error=None, response_headers={})
                    for u in urls
                ]

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            with patch("financial_scraper.pipeline.FetchClient") as MockClient:
                mock_instance = AsyncMock()
                mock_instance.fetch_batch.side_effect = fake_fetch_batch
                MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
                MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

                with patch.object(p._extractor, "extract", return_value=extraction):
                    asyncio.run(p.run())

                assert mock_instance.fetch_batch.call_count == 2

    def test_crawl_respects_max_pages_per_domain(self, tmp_path):
        """crawl=True with max_pages_per_domain=1: no depth-1 fetch because cap hit."""
        p, mock_searcher, html, extraction, SR, FR = self._setup_crawl_pipeline(
            tmp_path, crawl=True, crawl_depth=1, max_pages_per_domain=1,
        )

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            with patch("financial_scraper.pipeline.FetchClient") as MockClient:
                mock_instance = AsyncMock()
                mock_instance.fetch_batch.return_value = [
                    FR(url="https://example.com/page1", status=200,
                       html=html, content_type="text/html",
                       content_bytes=None, error=None, response_headers={}),
                ]
                MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
                MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

                with patch.object(p._extractor, "extract", return_value=extraction):
                    asyncio.run(p.run())

                # Only depth-0 fetch; domain cap prevents depth-1
                assert mock_instance.fetch_batch.call_count == 1


class TestResetFeatures:
    def test_reset_queries_reprocesses_all(self, tmp_path):
        """--resume --reset-queries: all queries run again despite checkpoint."""
        from financial_scraper.checkpoint import Checkpoint

        qf = tmp_path / "queries.txt"
        qf.write_text("query1\nquery2\n")
        cp_path = tmp_path / "cp.json"

        # Pre-create checkpoint with both queries done
        cp = Checkpoint(cp_path)
        cp.mark_query_done("query1")
        cp.mark_query_done("query2")

        p = _make_pipeline(tmp_path, resume=True, reset_queries=True)

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = []

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            asyncio.run(p.run())

        # Both queries should be searched (not skipped)
        assert mock_searcher.search.call_count == 2

    def test_reset_queries_keeps_url_dedup(self, tmp_path):
        """--reset-queries: URL history preserved, already-fetched URLs skipped."""
        from financial_scraper.checkpoint import Checkpoint
        from financial_scraper.search.duckduckgo import SearchResult
        from financial_scraper.fetch.client import FetchResult

        qf = tmp_path / "queries.txt"
        qf.write_text("query1\n")
        cp_path = tmp_path / "cp.json"

        # Pre-create checkpoint with query1 done and a URL fetched
        cp = Checkpoint(cp_path)
        cp.mark_url_fetched("https://example.com/already-seen")
        cp.mark_query_done("query1")

        p = _make_pipeline(tmp_path, resume=True, reset_queries=True)

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = [
            SearchResult(url="https://example.com/already-seen", title="T1",
                         snippet="S1", search_rank=1, query="query1"),
        ]

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            with patch("financial_scraper.pipeline.FetchClient") as MockClient:
                mock_instance = AsyncMock()
                mock_instance.fetch_batch.return_value = []
                MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_instance)
                MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

                asyncio.run(p.run())

        # Search was called (query not skipped)
        assert mock_searcher.search.call_count == 1
        # But the URL was already fetched, so fetch_batch should not be called
        # (all URLs filtered as already seen)

    def test_resume_without_reset_skips_done_queries(self, tmp_path):
        """--resume without --reset-queries: done queries are skipped as before."""
        from financial_scraper.checkpoint import Checkpoint

        qf = tmp_path / "queries.txt"
        qf.write_text("query1\nquery2\n")
        cp_path = tmp_path / "cp.json"

        cp = Checkpoint(cp_path)
        cp.mark_query_done("query1")

        p = _make_pipeline(tmp_path, resume=True, reset_queries=False)

        mock_searcher = MagicMock()
        mock_searcher.search.return_value = []

        with patch("financial_scraper.pipeline.DDGSearcher", return_value=mock_searcher):
            asyncio.run(p.run())

        # Only query2 should be searched (query1 skipped)
        assert mock_searcher.search.call_count == 1
