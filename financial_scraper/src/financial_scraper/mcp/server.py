"""MCP server exposing financial_scraper tools over stdio.

Tools: search, fetch, extract, scrape (convenience), read_output.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections import OrderedDict
from dataclasses import fields as dc_fields
from typing import Any

from mcp.server.fastmcp import FastMCP

from ..config import ScraperConfig
from ..extract.html import HTMLExtractor
from ..extract.pdf import PDFExtractor
from ..fetch.client import FetchClient, FetchResult
from ..fetch.robots import RobotChecker
from ..fetch.throttle import DomainThrottler
from ..search.duckduckgo import DDGSearcher
from ..store.dedup import Deduplicator
from ..store.markdown import format_records_md

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state (long-lived stdio process)
# ---------------------------------------------------------------------------
_config = ScraperConfig()
_fetch_cache: OrderedDict[str, FetchResult] = OrderedDict()
_extract_cache: OrderedDict[str, dict] = OrderedDict()
_dedup = Deduplicator()

_CACHE_MAX = 500

mcp = FastMCP(
    "financial-scraper",
    instructions="Ethical financial web scraper — search, fetch, extract articles",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_config(**overrides: Any) -> ScraperConfig:
    """Rebuild frozen ScraperConfig with selective overrides."""
    base = {f.name: getattr(_config, f.name) for f in dc_fields(_config)}
    base.update({k: v for k, v in overrides.items() if v is not None})
    return ScraperConfig(**base)


def _cache_put(url: str, result: FetchResult) -> None:
    """LRU-evicting cache for bridging fetch → extract."""
    _fetch_cache[url] = result
    _fetch_cache.move_to_end(url)
    while len(_fetch_cache) > _CACHE_MAX:
        _fetch_cache.popitem(last=False)


def _extract_cache_put(url: str, record: dict) -> None:
    """LRU-evicting cache for bridging extract → export_markdown."""
    _extract_cache[url] = record
    _extract_cache.move_to_end(url)
    while len(_extract_cache) > _CACHE_MAX:
        _extract_cache.popitem(last=False)


# ---------------------------------------------------------------------------
# Tool 1: search
# ---------------------------------------------------------------------------
@mcp.tool()
async def search(
    query: str,
    max_results: int = 10,
    search_type: str = "text",
    region: str = "wt-wt",
    timelimit: str | None = None,
) -> list[dict]:
    """Search DuckDuckGo for financial content.

    Returns structured results with url, title, snippet, and search rank.
    """
    cfg = _build_config(
        search_type=search_type,
        ddg_region=region,
        ddg_timelimit=timelimit,
    )
    searcher = DDGSearcher(cfg)
    results = await asyncio.to_thread(searcher.search, query, max_results)
    return [
        {
            "url": r.url,
            "title": r.title,
            "snippet": r.snippet,
            "search_rank": r.search_rank,
            "query": query,
        }
        for r in results
    ]


# ---------------------------------------------------------------------------
# Tool 2: fetch
# ---------------------------------------------------------------------------
@mcp.tool()
async def fetch(
    urls: list[str],
    timeout: int = 20,
    respect_robots: bool = True,
) -> list[dict]:
    """Fetch URLs with ethical rate limiting, fingerprints, and robots.txt compliance.

    Results are cached in memory so `extract` can process them without re-fetching.
    Does NOT return raw HTML (too large) — call `extract` to get clean text.
    """
    cfg = _build_config(fetch_timeout=timeout, respect_robots=respect_robots)
    throttler = DomainThrottler(max_per_domain=cfg.max_concurrent_per_domain)
    robot_checker = RobotChecker()

    async with FetchClient(cfg, throttler, robot_checker) as client:
        results = await client.fetch_batch(urls)

    out = []
    for fr in results:
        _cache_put(fr.url, fr)
        out.append({
            "url": fr.url,
            "status": fr.status,
            "content_type": fr.content_type,
            "error": fr.error,
            "has_html": fr.html is not None,
            "has_pdf_bytes": fr.content_bytes is not None,
            "html_length": len(fr.html) if fr.html else 0,
        })
    return out


# ---------------------------------------------------------------------------
# Tool 3: extract
# ---------------------------------------------------------------------------
@mcp.tool()
async def extract(
    urls: list[str],
    min_word_count: int = 100,
) -> list[dict]:
    """Extract clean text from previously fetched content.

    You must call `fetch` first — this tool looks up cached fetch results by URL.
    Returns title, author, date, full text, word count, and extraction method.
    """
    cfg = _build_config(min_word_count=min_word_count)
    html_extractor = HTMLExtractor(cfg)
    pdf_extractor = PDFExtractor()
    results = []

    for url in urls:
        fr = _fetch_cache.get(url)
        if fr is None:
            results.append({"url": url, "error": "URL not in fetch cache — call fetch first"})
            continue
        if fr.error:
            results.append({"url": url, "error": f"Fetch failed: {fr.error}"})
            continue

        try:
            if fr.content_bytes is not None:
                ex = pdf_extractor.extract(fr.content_bytes, url)
            elif fr.html is not None:
                ex = html_extractor.extract(fr.html, url)
            else:
                results.append({"url": url, "error": "No content available"})
                continue

            if ex.word_count < min_word_count:
                results.append({
                    "url": url,
                    "error": f"Below min_word_count ({ex.word_count} < {min_word_count})",
                    "word_count": ex.word_count,
                })
                continue

            if _dedup.is_duplicate_content(ex.text):
                results.append({"url": url, "error": "Duplicate content"})
                continue

            _dedup.mark_seen(url, ex.text)
            result_dict = {
                "url": url,
                "title": ex.title,
                "author": ex.author,
                "date": ex.date,
                "text": ex.text,
                "word_count": ex.word_count,
                "extraction_method": ex.extraction_method,
                "error": None,
            }
            results.append(result_dict)
            # Cache for export_markdown
            _extract_cache_put(url, result_dict)
        except Exception as exc:
            results.append({"url": url, "error": str(exc)})

    return results


# ---------------------------------------------------------------------------
# Tool 4: scrape (convenience: search → fetch → extract)
# ---------------------------------------------------------------------------
@mcp.tool()
async def scrape(
    query: str,
    max_results: int = 10,
    search_type: str = "text",
    min_word_count: int = 100,
    region: str = "wt-wt",
    timelimit: str | None = None,
) -> dict:
    """Full pipeline for a single query: search → fetch → extract.

    Returns articles with title, author, date, word count, snippet, and full text.
    """
    # 1. Search
    search_results = await search(
        query=query,
        max_results=max_results,
        search_type=search_type,
        region=region,
        timelimit=timelimit,
    )
    if not search_results:
        return {
            "query": query,
            "results_found": 0,
            "articles_extracted": 0,
            "articles": [],
        }

    urls = [r["url"] for r in search_results]
    snippets = {r["url"]: r["snippet"] for r in search_results}

    # 2. Fetch
    await fetch(urls=urls)

    # 3. Extract
    extractions = await extract(urls=urls, min_word_count=min_word_count)

    articles = []
    for ex in extractions:
        if ex.get("error"):
            continue
        article = {
            "url": ex["url"],
            "title": ex.get("title"),
            "author": ex.get("author"),
            "date": ex.get("date"),
            "word_count": ex.get("word_count"),
            "snippet": snippets.get(ex["url"], ""),
            "full_text": ex.get("text", ""),
        }
        articles.append(article)
        # Update extract cache with query context for export_markdown
        _extract_cache_put(ex["url"], {
            **ex,
            "company": query,
            "full_text": ex.get("text", ""),
            "link": ex["url"],
            "source": ex["url"],
        })

    return {
        "query": query,
        "results_found": len(search_results),
        "articles_extracted": len(articles),
        "articles": articles,
    }


# ---------------------------------------------------------------------------
# Tool 5: export_markdown
# ---------------------------------------------------------------------------
@mcp.tool()
async def export_markdown(
    urls: list[str] | None = None,
) -> dict:
    """Format previously extracted articles as markdown.

    If urls is provided, format only those articles.
    If urls is omitted, format all articles extracted in this session.
    """
    if urls is not None:
        missing = [u for u in urls if u not in _extract_cache]
        if missing:
            return {"error": f"URLs not in extract cache: {missing}"}
        records = [_extract_cache[u] for u in urls]
    else:
        records = list(_extract_cache.values())

    if not records:
        return {"markdown": "", "article_count": 0}

    # Normalise records for format_records_md (expects company, title, link, full_text, etc.)
    normalised = []
    for r in records:
        normalised.append({
            "company": r.get("company", ""),
            "title": r.get("title", ""),
            "link": r.get("link", r.get("url", "")),
            "date": r.get("date", ""),
            "source": r.get("source", ""),
            "full_text": r.get("full_text", r.get("text", "")),
        })

    md = format_records_md(normalised)
    return {"markdown": md, "article_count": len(normalised)}


# ---------------------------------------------------------------------------
# Tool 6: read_output
# ---------------------------------------------------------------------------
@mcp.tool()
async def read_output(
    file_path: str,
    limit: int = 50,
) -> dict:
    """Read a Parquet file from a previous CLI scrape run.

    Returns column names, total rows, and the first `limit` rows as dicts.
    """
    import pandas as pd

    df = pd.read_parquet(file_path)
    total = len(df)
    rows = df.head(limit).to_dict(orient="records")

    # Convert Timestamps to ISO strings for JSON serialization
    for row in rows:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()

    return {
        "total_rows": total,
        "returned_rows": len(rows),
        "columns": list(df.columns),
        "rows": rows,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def start_server() -> None:
    """Start the MCP server on stdio."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    mcp.run(transport="stdio")
