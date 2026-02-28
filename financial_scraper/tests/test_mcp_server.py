"""Tests for the MCP server tools (all external dependencies mocked)."""

import pytest

pytest.importorskip("mcp", reason="mcp package not installed (optional dependency)")

from collections import OrderedDict
from unittest.mock import AsyncMock, MagicMock, patch

from financial_scraper.config import ScraperConfig
from financial_scraper.fetch.client import FetchResult
from financial_scraper.extract.html import ExtractionResult
from financial_scraper.search.duckduckgo import SearchResult
from financial_scraper.mcp import server as mcp_server


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_server_state():
    """Reset module-level state between tests."""
    mcp_server._fetch_cache.clear()
    mcp_server._extract_cache.clear()
    mcp_server._dedup = mcp_server.Deduplicator()
    mcp_server._config = ScraperConfig()
    yield


@pytest.fixture
def sample_search_results():
    return [
        SearchResult(url="https://example.com/1", title="Article 1",
                     snippet="Snippet 1", search_rank=1, query="test query"),
        SearchResult(url="https://example.com/2", title="Article 2",
                     snippet="Snippet 2", search_rank=2, query="test query"),
    ]


@pytest.fixture
def sample_fetch_result():
    return FetchResult(
        url="https://example.com/1",
        status=200,
        html="<html><body><p>Some content here</p></body></html>",
        content_type="text/html",
        content_bytes=None,
        error=None,
        response_headers={"Content-Type": "text/html"},
    )


@pytest.fixture
def sample_extraction():
    return ExtractionResult(
        text="This is extracted financial content " * 30,
        title="Article Title",
        author="Author Name",
        date="2025-06-15",
        word_count=150,
        extraction_method="trafilatura",
        language="en",
    )


# ---------------------------------------------------------------------------
# _build_config
# ---------------------------------------------------------------------------

class TestBuildConfig:
    def test_defaults(self):
        cfg = mcp_server._build_config()
        assert isinstance(cfg, ScraperConfig)
        assert cfg.fetch_timeout == 20

    def test_overrides(self):
        cfg = mcp_server._build_config(fetch_timeout=30, min_word_count=50)
        assert cfg.fetch_timeout == 30
        assert cfg.min_word_count == 50

    def test_none_ignored(self):
        cfg = mcp_server._build_config(fetch_timeout=None)
        assert cfg.fetch_timeout == 20  # default preserved


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class TestCachePut:
    def test_basic_put(self, sample_fetch_result):
        mcp_server._cache_put("https://example.com/1", sample_fetch_result)
        assert "https://example.com/1" in mcp_server._fetch_cache

    def test_lru_eviction(self, sample_fetch_result):
        for i in range(mcp_server._CACHE_MAX + 10):
            fr = FetchResult(url=f"https://example.com/{i}", status=200,
                             html="x", content_type="text/html",
                             content_bytes=None, error=None,
                             response_headers=None)
            mcp_server._cache_put(fr.url, fr)
        assert len(mcp_server._fetch_cache) == mcp_server._CACHE_MAX
        # First entries should be evicted
        assert "https://example.com/0" not in mcp_server._fetch_cache


# ---------------------------------------------------------------------------
# Tool: search
# ---------------------------------------------------------------------------

class TestSearchTool:
    @pytest.mark.asyncio
    async def test_search_returns_structured_results(self, sample_search_results):
        with patch.object(mcp_server, "DDGSearcher") as MockSearcher:
            instance = MockSearcher.return_value
            instance.search.return_value = sample_search_results

            results = await mcp_server.search(query="test query", max_results=2)

        assert len(results) == 2
        assert results[0]["url"] == "https://example.com/1"
        assert results[0]["title"] == "Article 1"
        assert results[0]["snippet"] == "Snippet 1"
        assert results[0]["search_rank"] == 1
        assert results[0]["query"] == "test query"

    @pytest.mark.asyncio
    async def test_search_empty_results(self):
        with patch.object(mcp_server, "DDGSearcher") as MockSearcher:
            instance = MockSearcher.return_value
            instance.search.return_value = []

            results = await mcp_server.search(query="nothing")

        assert results == []

    @pytest.mark.asyncio
    async def test_search_passes_config(self):
        with patch.object(mcp_server, "DDGSearcher") as MockSearcher:
            instance = MockSearcher.return_value
            instance.search.return_value = []

            await mcp_server.search(
                query="test", search_type="news", region="us-en", timelimit="w"
            )

        cfg_arg = MockSearcher.call_args[0][0]
        assert cfg_arg.search_type == "news"
        assert cfg_arg.ddg_region == "us-en"
        assert cfg_arg.ddg_timelimit == "w"


# ---------------------------------------------------------------------------
# Tool: fetch
# ---------------------------------------------------------------------------

class TestFetchTool:
    @pytest.mark.asyncio
    async def test_fetch_caches_results(self, sample_fetch_result):
        mock_client = AsyncMock()
        mock_client.fetch_batch.return_value = [sample_fetch_result]
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(mcp_server, "FetchClient", return_value=mock_client):
            with patch.object(mcp_server, "DomainThrottler"):
                with patch.object(mcp_server, "RobotChecker"):
                    results = await mcp_server.fetch(urls=["https://example.com/1"])

        assert len(results) == 1
        assert results[0]["url"] == "https://example.com/1"
        assert results[0]["status"] == 200
        assert results[0]["has_html"] is True
        assert results[0]["has_pdf_bytes"] is False
        assert "https://example.com/1" in mcp_server._fetch_cache

    @pytest.mark.asyncio
    async def test_fetch_error_still_cached(self):
        error_result = FetchResult(
            url="https://example.com/fail", status=0, html=None,
            content_type="", content_bytes=None, error="Connection refused",
            response_headers=None,
        )
        mock_client = AsyncMock()
        mock_client.fetch_batch.return_value = [error_result]
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch.object(mcp_server, "FetchClient", return_value=mock_client):
            with patch.object(mcp_server, "DomainThrottler"):
                with patch.object(mcp_server, "RobotChecker"):
                    results = await mcp_server.fetch(urls=["https://example.com/fail"])

        assert results[0]["error"] == "Connection refused"
        assert "https://example.com/fail" in mcp_server._fetch_cache


# ---------------------------------------------------------------------------
# Tool: extract
# ---------------------------------------------------------------------------

class TestExtractTool:
    @pytest.mark.asyncio
    async def test_extract_html(self, sample_fetch_result, sample_extraction):
        mcp_server._cache_put(sample_fetch_result.url, sample_fetch_result)

        with patch.object(mcp_server, "HTMLExtractor") as MockExtractor:
            MockExtractor.return_value.extract.return_value = sample_extraction
            results = await mcp_server.extract(urls=["https://example.com/1"])

        assert len(results) == 1
        assert results[0]["title"] == "Article Title"
        assert results[0]["word_count"] == 150
        assert results[0]["error"] is None

    @pytest.mark.asyncio
    async def test_extract_not_in_cache(self):
        results = await mcp_server.extract(urls=["https://not-fetched.com"])
        assert results[0]["error"] == "URL not in fetch cache - call fetch first"

    @pytest.mark.asyncio
    async def test_extract_fetch_error(self):
        error_fr = FetchResult(
            url="https://example.com/err", status=0, html=None,
            content_type="", content_bytes=None, error="Timeout",
            response_headers=None,
        )
        mcp_server._cache_put(error_fr.url, error_fr)

        results = await mcp_server.extract(urls=["https://example.com/err"])
        assert "Fetch failed" in results[0]["error"]

    @pytest.mark.asyncio
    async def test_extract_below_min_word_count(self, sample_fetch_result):
        mcp_server._cache_put(sample_fetch_result.url, sample_fetch_result)
        short = ExtractionResult(
            text="Short", title="T", author=None, date=None,
            word_count=5, extraction_method="trafilatura", language=None,
        )
        with patch.object(mcp_server, "HTMLExtractor") as MockExtractor:
            MockExtractor.return_value.extract.return_value = short
            results = await mcp_server.extract(
                urls=["https://example.com/1"], min_word_count=100,
            )

        assert "Below min_word_count" in results[0]["error"]

    @pytest.mark.asyncio
    async def test_extract_pdf(self, sample_extraction):
        pdf_fr = FetchResult(
            url="https://example.com/doc.pdf", status=200, html=None,
            content_type="application/pdf", content_bytes=b"%PDF-fake",
            error=None, response_headers=None,
        )
        mcp_server._cache_put(pdf_fr.url, pdf_fr)

        with patch.object(mcp_server, "PDFExtractor") as MockExtractor:
            MockExtractor.return_value.extract.return_value = sample_extraction
            results = await mcp_server.extract(urls=["https://example.com/doc.pdf"])

        assert results[0]["extraction_method"] == "trafilatura"
        assert results[0]["error"] is None

    @pytest.mark.asyncio
    async def test_extract_dedup(self, sample_fetch_result, sample_extraction):
        # Fetch two URLs with same content
        fr1 = sample_fetch_result
        fr2 = FetchResult(
            url="https://example.com/2", status=200,
            html="<html><body>Same</body></html>",
            content_type="text/html", content_bytes=None,
            error=None, response_headers=None,
        )
        mcp_server._cache_put(fr1.url, fr1)
        mcp_server._cache_put(fr2.url, fr2)

        with patch.object(mcp_server, "HTMLExtractor") as MockExtractor:
            MockExtractor.return_value.extract.return_value = sample_extraction
            # Extract first - succeeds
            results1 = await mcp_server.extract(urls=[fr1.url])
            # Extract second with identical content - deduplicated
            results2 = await mcp_server.extract(urls=[fr2.url])

        assert results1[0]["error"] is None
        assert results2[0]["error"] == "Duplicate content"


# ---------------------------------------------------------------------------
# Tool: scrape
# ---------------------------------------------------------------------------

class TestScrapeTool:
    @pytest.mark.asyncio
    async def test_scrape_full_pipeline(self, sample_search_results,
                                        sample_fetch_result, sample_extraction):
        with patch.object(mcp_server, "DDGSearcher") as MockSearcher, \
             patch.object(mcp_server, "FetchClient") as MockFetchClient, \
             patch.object(mcp_server, "HTMLExtractor") as MockHTMLExtractor, \
             patch.object(mcp_server, "DomainThrottler"), \
             patch.object(mcp_server, "RobotChecker"):

            # search
            MockSearcher.return_value.search.return_value = sample_search_results

            # fetch - return results for both URLs
            fr1 = sample_fetch_result
            fr2 = FetchResult(
                url="https://example.com/2", status=200,
                html="<html>body2</html>", content_type="text/html",
                content_bytes=None, error=None, response_headers=None,
            )
            mock_client = AsyncMock()
            mock_client.fetch_batch.return_value = [fr1, fr2]
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            MockFetchClient.return_value = mock_client

            # extract - different content so dedup doesn't filter
            ex1 = sample_extraction
            ex2 = ExtractionResult(
                text="Different financial content " * 30,
                title="Article 2", author=None, date=None,
                word_count=120, extraction_method="trafilatura", language="en",
            )
            MockHTMLExtractor.return_value.extract.side_effect = [ex1, ex2]

            result = await mcp_server.scrape(query="test query", max_results=2)

        assert result["query"] == "test query"
        assert result["results_found"] == 2
        assert result["articles_extracted"] == 2
        assert len(result["articles"]) == 2
        assert result["articles"][0]["title"] == "Article Title"

    @pytest.mark.asyncio
    async def test_scrape_empty_search(self):
        with patch.object(mcp_server, "DDGSearcher") as MockSearcher:
            MockSearcher.return_value.search.return_value = []
            result = await mcp_server.scrape(query="nothing")

        assert result["results_found"] == 0
        assert result["articles"] == []


# ---------------------------------------------------------------------------
# Tool: export_markdown
# ---------------------------------------------------------------------------

class TestExportMarkdownTool:
    @pytest.mark.asyncio
    async def test_export_from_extract_cache(self):
        mcp_server._extract_cache_put("https://example.com/1", {
            "url": "https://example.com/1",
            "title": "Oil Article",
            "company": "oil futures",
            "link": "https://example.com/1",
            "date": "2025-06-15",
            "source": "example.com",
            "full_text": "Full article content about oil.",
        })

        result = await mcp_server.export_markdown()

        assert result["article_count"] == 1
        assert "Oil Article" in result["markdown"]
        assert "oil futures" in result["markdown"]

    @pytest.mark.asyncio
    async def test_export_specific_urls(self):
        mcp_server._extract_cache_put("https://a.com", {
            "url": "https://a.com",
            "title": "Article A",
            "company": "query a",
            "link": "https://a.com",
            "date": "",
            "source": "a.com",
            "full_text": "Content A",
        })
        mcp_server._extract_cache_put("https://b.com", {
            "url": "https://b.com",
            "title": "Article B",
            "company": "query b",
            "link": "https://b.com",
            "date": "",
            "source": "b.com",
            "full_text": "Content B",
        })

        result = await mcp_server.export_markdown(urls=["https://a.com"])
        assert result["article_count"] == 1
        assert "Article A" in result["markdown"]
        assert "Article B" not in result["markdown"]

    @pytest.mark.asyncio
    async def test_export_unknown_url_returns_error(self):
        result = await mcp_server.export_markdown(urls=["https://unknown.com"])
        assert "error" in result

    @pytest.mark.asyncio
    async def test_export_empty_cache(self):
        result = await mcp_server.export_markdown()
        assert result["article_count"] == 0
        assert result["markdown"] == ""


# ---------------------------------------------------------------------------
# Tool: read_output
# ---------------------------------------------------------------------------

class TestReadOutputTool:
    @pytest.mark.asyncio
    async def test_read_parquet(self, tmp_path):
        import pandas as pd

        df = pd.DataFrame({
            "company": ["oil futures", "gold price"],
            "title": ["Article A", "Article B"],
            "link": ["https://a.com", "https://b.com"],
            "full_text": ["content a", "content b"],
        })
        path = tmp_path / "test.parquet"
        df.to_parquet(path)

        result = await mcp_server.read_output(file_path=str(path), limit=10)

        assert result["total_rows"] == 2
        assert result["returned_rows"] == 2
        assert "company" in result["columns"]
        assert len(result["rows"]) == 2
        assert result["rows"][0]["company"] == "oil futures"

    @pytest.mark.asyncio
    async def test_read_parquet_limit(self, tmp_path):
        import pandas as pd

        df = pd.DataFrame({"x": list(range(100))})
        path = tmp_path / "big.parquet"
        df.to_parquet(path)

        result = await mcp_server.read_output(file_path=str(path), limit=5)

        assert result["total_rows"] == 100
        assert result["returned_rows"] == 5

    @pytest.mark.asyncio
    async def test_read_parquet_as_markdown(self, tmp_path):
        import pandas as pd

        df = pd.DataFrame({
            "company": ["oil futures"],
            "title": ["Article A"],
            "link": ["https://a.com"],
            "full_text": ["Some article content here."],
            "source": ["a.com"],
            "date": [None],
            "snippet": ["Some article..."],
            "source_file": ["test.parquet"],
        })
        path = tmp_path / "test.parquet"
        df.to_parquet(path)

        result = await mcp_server.read_output(file_path=str(path), limit=10, as_markdown=True)

        assert result["total_rows"] == 1
        assert result["returned_rows"] == 1
        assert "markdown" in result
        assert "rows" not in result
        assert "# Financial Scraper Report" in result["markdown"]
