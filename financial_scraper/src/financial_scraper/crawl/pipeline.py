"""Crawl pipeline: crawl4ai deep-crawl -> extract -> store."""

import asyncio
import logging
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
from crawl4ai import AsyncWebCrawler

from .config import CrawlConfig, apply_stealth
from .strategy import build_browser_config, build_crawl_strategy, build_crawler_config
from ..extract.html import HTMLExtractor
from ..extract.clean import TextCleaner
from ..extract.date_filter import DateFilter
from ..extract.pdf import get_pdf_extractor
from ..store.dedup import Deduplicator
from ..store.output import ParquetWriter, JSONLWriter, make_source_file_tag
from ..store.markdown import MarkdownWriter
from ..checkpoint import Checkpoint

logger = logging.getLogger(__name__)


class CrawlPipeline:
    """Deep-crawl pipeline using crawl4ai."""

    def __init__(self, config: CrawlConfig):
        self._config = apply_stealth(config)
        self._extractor = HTMLExtractor(self._config)
        self._pdf_extractor = get_pdf_extractor(self._config.pdf_extractor)
        self._cleaner = TextCleaner()
        self._date_filter = DateFilter(self._config.date_from, self._config.date_to)
        self._dedup = Deduplicator()
        self._checkpoint = Checkpoint(self._config.checkpoint_file)
        self._parquet_writer = ParquetWriter(self._config.output_path)
        self._jsonl_writer = (
            JSONLWriter(self._config.jsonl_path) if self._config.jsonl_path else None
        )
        self._markdown_writer = (
            MarkdownWriter(self._config.markdown_path) if self._config.markdown_path else None
        )
        self._exclusions: set[str] = set()
        # Stats
        self._method_counter: Counter = Counter()
        self._domain_counter: Counter = Counter()

    async def run(self):
        """Execute the crawl pipeline."""
        # 1. Load exclusions, checkpoint
        self._exclusions = self._load_exclusions()
        if self._config.resume:
            self._checkpoint.load()
            logger.info(
                f"Resumed: {len(self._checkpoint.completed_queries)} seed URLs done"
            )

        # 2. Date filter log
        if self._date_filter.is_active:
            logger.info(f"Date filter: {self._config.date_from} to {self._config.date_to}")

        # 3. Load seed URLs
        seed_urls = self._load_seed_urls()
        if not seed_urls:
            logger.error("No seed URLs to process")
            return

        logger.info(f"Processing {len(seed_urls)} seed URLs")

        # 4. Crawl each seed URL
        total_records = 0
        browser_config = build_browser_config()

        async with AsyncWebCrawler(config=browser_config) as crawler:
            for si, seed_url in enumerate(seed_urls):
                domain = self._extract_domain(seed_url)

                if self._checkpoint.is_query_done(seed_url):
                    logger.info(
                        f"[{si+1}/{len(seed_urls)}] Skipping (already done): {seed_url}"
                    )
                    continue

                if self._is_excluded_domain(seed_url):
                    logger.info(
                        f"[{si+1}/{len(seed_urls)}] Skipping (excluded): {seed_url}"
                    )
                    self._checkpoint.mark_query_done(seed_url)
                    continue

                logger.info(f"\n[{si+1}/{len(seed_urls)}] Crawling: {seed_url}")

                # Build strategy + config for this seed
                strategy = build_crawl_strategy(
                    max_depth=self._config.max_depth,
                    max_pages=self._config.max_pages,
                )
                crawler_config = build_crawler_config(
                    strategy=strategy,
                    check_robots_txt=self._config.check_robots_txt,
                    semaphore_count=self._config.semaphore_count,
                )

                # Run crawl4ai
                try:
                    results = await crawler.arun(
                        url=seed_url,
                        config=crawler_config,
                    )
                except Exception:
                    logger.exception(f"  Crawl failed for {seed_url}")
                    self._checkpoint.mark_url_failed(seed_url)
                    self._checkpoint.mark_query_done(seed_url)
                    continue

                # Normalize: arun may return a single result or a list
                if not isinstance(results, list):
                    results = [results]

                logger.info(
                    f"  crawl4ai returned {len(results)} result(s)"
                )

                # 5. Extract content from each CrawlResult
                all_records: list[dict] = []
                total_success = 0
                total_failed = 0

                for cr in results:
                    url = cr.url

                    # Skip failed pages
                    if not cr.success:
                        logger.debug(
                            f"  Skipped (success=False): {url} "
                            f"status={getattr(cr, 'status_code', '?')} "
                            f"error={getattr(cr, 'error_message', '')}"
                        )
                        total_failed += 1
                        self._checkpoint.mark_url_failed(url)
                        continue

                    # Skip already-processed URLs
                    if self._checkpoint.is_url_fetched(url):
                        continue
                    self._checkpoint.mark_url_fetched(url)

                    # URL dedup
                    if self._dedup.is_duplicate_url(url):
                        continue

                    # PDF vs HTML extraction
                    response_headers = getattr(cr, "response_headers", None) or {}
                    if self._is_pdf(url, response_headers):
                        pdf_bytes = await self._download_pdf_bytes(url)
                        if not pdf_bytes:
                            total_failed += 1
                            continue
                        ex = await asyncio.to_thread(
                            self._pdf_extractor.extract, pdf_bytes, url
                        )
                    else:
                        html = cr.html or ""
                        if not html:
                            total_failed += 1
                            continue
                        ex = self._extractor.extract(html, url)

                    if ex.extraction_method == "failed" or ex.word_count < self._config.min_word_count:
                        self._checkpoint.stats["failed_extractions"] += 1
                        total_failed += 1
                        continue

                    # Post-extraction filters
                    if self._cleaner.is_ticker_page(ex.text):
                        logger.debug(f"  Skipped ticker/profile page: {url}")
                        self._checkpoint.stats["failed_extractions"] += 1
                        total_failed += 1
                        continue

                    if self._cleaner.is_nature_index_page(ex.text):
                        logger.debug(f"  Skipped Nature Index profile: {url}")
                        self._checkpoint.stats["failed_extractions"] += 1
                        total_failed += 1
                        continue

                    # Date filter
                    if self._date_filter.is_active and not self._date_filter.passes(ex.date):
                        continue

                    # Content dedup
                    if self._dedup.is_duplicate_content(ex.text):
                        continue

                    self._dedup.mark_seen(url, ex.text)
                    self._method_counter[ex.extraction_method] += 1
                    page_domain = self._extract_domain(url)
                    self._domain_counter[page_domain] += 1

                    # Build record
                    snippet = (ex.text[:300] + "...") if len(ex.text) > 300 else ex.text
                    record = {
                        "company": domain,
                        "title": ex.title or "",
                        "link": url,
                        "snippet": snippet,
                        "date": ex.date or "",
                        "source": page_domain,
                        "full_text": ex.text,
                        "source_file": make_source_file_tag(
                            domain, ex.date, "crawl"
                        ),
                    }
                    all_records.append(record)
                    total_success += 1
                    self._checkpoint.stats["total_pages"] += 1
                    self._checkpoint.stats["total_words"] += ex.word_count

                # Write batch for this seed URL
                if all_records:
                    self._parquet_writer.append(all_records)
                    if self._jsonl_writer:
                        self._jsonl_writer.append(all_records)
                    if self._markdown_writer:
                        self._markdown_writer.append(all_records)
                    total_records += len(all_records)

                avg_words = (
                    sum(len(r["full_text"].split()) for r in all_records) // len(all_records)
                    if all_records
                    else 0
                )
                logger.info(
                    f"  Seed done: {total_success} pages extracted, "
                    f"{total_failed} failed, avg {avg_words} words"
                )

                self._checkpoint.mark_query_done(seed_url)

        # 6. Summary
        self._print_summary(total_records, len(seed_urls))

    def _print_summary(self, total_records: int, total_seeds: int):
        logger.info("\n" + "=" * 60)
        logger.info("CRAWL SUMMARY")
        logger.info("=" * 60)
        logger.info(
            f"Seed URLs processed: {self._checkpoint.stats['total_queries']}/{total_seeds}"
        )
        logger.info(f"Total pages extracted: {self._checkpoint.stats['total_pages']}")
        logger.info(f"Total words: {self._checkpoint.stats['total_words']:,}")
        logger.info(f"Failed fetches: {self._checkpoint.stats['failed_fetches']}")
        logger.info(f"Failed extractions: {self._checkpoint.stats['failed_extractions']}")

        if self._method_counter:
            logger.info(f"Extraction methods: {dict(self._method_counter)}")
        if self._domain_counter:
            top = self._domain_counter.most_common(10)
            logger.info(f"Top domains: {dict(top)}")
        if self._date_filter.is_active:
            logger.info(f"Date filter stats: {self._date_filter.get_stats()}")

        logger.info(f"Output: {self._config.output_path}")
        logger.info("=" * 60)

    def _load_seed_urls(self) -> list[str]:
        """Load seed URLs from file, skipping comments and blanks."""
        path = Path(self._config.urls_file)
        if not path.exists():
            logger.error(f"URLs file not found: {path}")
            return []
        urls = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    urls.append(line)
        return urls

    def _load_exclusions(self) -> set[str]:
        """Load excluded domains from file."""
        if not self._config.exclude_file:
            return set()
        path = Path(self._config.exclude_file)
        if not path.exists():
            logger.warning(f"Exclusion file not found: {path}")
            return set()
        domains = set()
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip().lower()
                if line and not line.startswith("#"):
                    line = line.replace("https://", "").replace("http://", "")
                    line = line.replace("www.", "").split("/")[0]
                    domains.add(line)
        logger.info(f"Loaded {len(domains)} excluded domains")
        return domains

    def _is_excluded_domain(self, url: str) -> bool:
        """Check if URL's domain or base domain is in the exclusion set."""
        domain = self._extract_domain(url)
        if domain in self._exclusions:
            return True
        parts = domain.split(".")
        if len(parts) > 2:
            base = ".".join(parts[-2:])
            if base in self._exclusions:
                return True
        return False

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract domain from URL, stripping www. prefix."""
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc

    @staticmethod
    def _is_pdf(url: str, response_headers: dict) -> bool:
        """Check if URL points to a PDF (by extension or content-type)."""
        if urlparse(url).path.lower().endswith(".pdf"):
            return True
        content_type = response_headers.get("content-type", "")
        if "application/pdf" in content_type.lower():
            return True
        return False

    async def _download_pdf_bytes(self, url: str, timeout: int = 30) -> bytes | None:
        """Download PDF bytes directly via aiohttp."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=timeout)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"PDF download failed ({resp.status}): {url}")
                        return None
                    return await resp.read()
        except Exception as e:
            logger.warning(f"PDF download error for {url}: {e}")
            return None
