"""Orchestrator: search -> fetch -> extract -> store."""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from collections import Counter

from .config import ScraperConfig, apply_stealth
from .search.duckduckgo import DDGSearcher, SearchResult
from .fetch.client import FetchClient
from .fetch.throttle import DomainThrottler
from .fetch.robots import RobotChecker
from .fetch.tor import TorManager
from .extract.html import HTMLExtractor
from .extract.pdf import PDFExtractor
from .extract.clean import TextCleaner
from .extract.date_filter import DateFilter
from .extract.links import extract_links, filter_links_same_domain
from .store.dedup import Deduplicator
from .store.output import ParquetWriter, JSONLWriter, make_source_file_tag
from .store.markdown import MarkdownWriter
from .checkpoint import Checkpoint

logger = logging.getLogger(__name__)


class ScraperPipeline:
    """Main orchestrator: search -> fetch -> extract -> store."""

    def __init__(self, config: ScraperConfig):
        self._config = apply_stealth(config)
        self._tor: TorManager | None = None
        self._searcher: DDGSearcher | None = None
        self._extractor = HTMLExtractor(self._config)
        self._pdf_extractor = PDFExtractor()
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
        self._pages_extracted = 0
        self._domain_page_counts: Counter = Counter()
        # Stats
        self._method_counter: Counter = Counter()
        self._domain_counter: Counter = Counter()

    async def run(self):
        """Execute the full pipeline."""
        # 1. Tor setup
        if self._config.use_tor:
            self._tor = TorManager(
                socks_port=self._config.tor_socks_port,
                control_port=self._config.tor_control_port,
                password=self._config.tor_password,
                renew_every=self._config.tor_renew_every,
                renew_on_ratelimit=self._config.tor_renew_on_ratelimit,
            )
            available = await self._tor.check_availability()
            if not available:
                logger.warning("Tor not available, falling back to direct connection")
                self._tor = None

        # 2. Initialize searcher
        self._searcher = DDGSearcher(self._config, self._tor)

        # 3. Date filter log
        if self._date_filter.is_active:
            logger.info(f"Date filter: {self._config.date_from} to {self._config.date_to}")

        # 4. Load exclusions, checkpoint, dedup
        self._exclusions = self._load_exclusions()
        if self._config.resume:
            self._checkpoint.load()
            if self._config.reset_queries:
                prev = len(self._checkpoint.completed_queries)
                self._checkpoint.reset_queries()
                logger.info(f"Reset {prev} completed queries (URL history kept)")
            else:
                logger.info(f"Resumed: {len(self._checkpoint.completed_queries)} queries done")

        # 5. Load queries
        queries = self._load_queries()
        if not queries:
            logger.error("No queries to process")
            return

        logger.info(f"Processing {len(queries)} queries")

        # 6. Process each query
        total_records = 0
        for qi, query in enumerate(queries):
            if self._checkpoint.is_query_done(query):
                logger.info(f"[{qi+1}/{len(queries)}] Skipping (already done): '{query}'")
                continue

            logger.info(f"\n[{qi+1}/{len(queries)}] '{query}'")

            # Search
            search_results = self._searcher.search(
                query, self._config.max_results_per_query
            )

            if not search_results:
                logger.warning(f"No search results for '{query}'")
                self._checkpoint.mark_query_done(query)
                continue

            # Filter exclusions and dedup
            filtered = []
            excluded = 0
            already_seen = 0
            for sr in search_results:
                domain = self._extract_domain(sr.url)
                if domain in self._exclusions:
                    excluded += 1
                    continue
                if self._dedup.is_duplicate_url(sr.url):
                    already_seen += 1
                    continue
                if self._checkpoint.is_url_fetched(sr.url):
                    already_seen += 1
                    continue
                filtered.append(sr)

            logger.info(
                f"  {len(search_results)} results -> {len(filtered)} to fetch "
                f"({excluded} excluded, {already_seen} already seen)"
            )

            if not filtered:
                self._checkpoint.mark_query_done(query)
                continue

            # BFS crawl loop
            all_seen_urls: set[str] = set()
            # url -> query mapping so crawled pages keep the original query
            url_to_query: dict[str, str] = {}
            urls_to_fetch: list[str] = []
            for sr in filtered:
                urls_to_fetch.append(sr.url)
                url_to_query[sr.url] = sr.query
                all_seen_urls.add(sr.url)

            current_depth = 0
            max_depth = self._config.crawl_depth if self._config.crawl else 0
            all_records: list[dict] = []
            total_success = 0
            total_failed = 0

            while urls_to_fetch:
                depth_label = f"depth {current_depth}"
                logger.info(
                    f"  [{depth_label}] Fetching {len(urls_to_fetch)} URLs"
                )

                throttler = DomainThrottler(
                    max_per_domain=self._config.max_concurrent_per_domain
                )
                robot_checker = RobotChecker()

                async with FetchClient(
                    self._config, throttler, robot_checker, self._tor
                ) as client:
                    fetch_results = await client.fetch_batch(urls_to_fetch)

                next_depth_urls: list[str] = []

                for url, fr in zip(urls_to_fetch, fetch_results):
                    self._checkpoint.mark_url_fetched(url)
                    source_query = url_to_query.get(url, query)

                    if fr.error:
                        self._checkpoint.mark_url_failed(url)
                        total_failed += 1
                        continue

                    # Extract content
                    is_pdf = fr.content_bytes and (
                        "application/pdf" in fr.content_type
                        or url.lower().endswith(".pdf")
                    )
                    if is_pdf:
                        ex = self._pdf_extractor.extract(fr.content_bytes, url)
                    elif fr.html:
                        ex = self._extractor.extract(fr.html, url)
                    else:
                        total_failed += 1
                        continue

                    # Count this page for domain cap (before link extraction)
                    fetch_domain = self._extract_domain(url)
                    self._domain_page_counts[fetch_domain] += 1

                    # Crawl: extract links from HTML for next depth
                    if (
                        self._config.crawl
                        and current_depth < max_depth
                        and fr.html
                        and not is_pdf
                    ):
                        raw_links = extract_links(fr.html, url)
                        source_domain = self._extract_domain(url)
                        new_links = filter_links_same_domain(
                            raw_links,
                            source_domain,
                            self._exclusions,
                            all_seen_urls,
                            self._domain_page_counts,
                            self._config.max_pages_per_domain,
                        )
                        for link in new_links:
                            if self._checkpoint.is_url_fetched(link):
                                continue
                            if self._dedup.is_duplicate_url(link):
                                continue
                            # Hard cap: never queue more than max_pages_per_domain total crawl URLs per depth
                            if len(next_depth_urls) >= self._config.max_pages_per_domain:
                                break
                            all_seen_urls.add(link)
                            url_to_query[link] = source_query
                            next_depth_urls.append(link)

                    if ex.extraction_method == "failed" or ex.word_count < self._config.min_word_count:
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
                    domain = self._extract_domain(url)
                    self._domain_counter[domain] += 1

                    # Snippet: first 300 chars of content
                    snippet = (ex.text[:300] + "...") if len(ex.text) > 300 else ex.text

                    record = {
                        "company": source_query,
                        "title": ex.title or "",
                        "link": url,
                        "snippet": snippet,
                        "date": ex.date or "",
                        "source": domain,
                        "full_text": ex.text,
                        "source_file": make_source_file_tag(
                            source_query, ex.date, self._config.search_type
                        ),
                    }
                    all_records.append(record)
                    total_success += 1
                    self._checkpoint.stats["total_pages"] += 1
                    self._checkpoint.stats["total_words"] += ex.word_count

                if next_depth_urls:
                    logger.info(
                        f"  [{depth_label}] Discovered {len(next_depth_urls)} "
                        f"links for depth {current_depth + 1}"
                    )

                current_depth += 1
                urls_to_fetch = next_depth_urls

            # Write batch (all depths)
            if all_records:
                self._parquet_writer.append(all_records)
                if self._jsonl_writer:
                    self._jsonl_writer.append(all_records)
                if self._markdown_writer:
                    self._markdown_writer.append(all_records)
                total_records += len(all_records)

            avg_words = (
                sum(len(r["full_text"].split()) for r in all_records) // len(all_records)
                if all_records else 0
            )
            logger.info(
                f"  Query done: {total_success} new pages, {total_failed} failed, "
                f"avg {avg_words} words"
            )

            self._checkpoint.mark_query_done(query)

        # 7. Summary
        self._print_summary(total_records, len(queries))

    def _print_summary(self, total_records: int, total_queries: int):
        logger.info("\n" + "=" * 60)
        logger.info("SCRAPER SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Queries processed: {self._checkpoint.stats['total_queries']}/{total_queries}")
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
        if self._tor:
            logger.info(f"Tor circuits renewed: {self._tor._circuits_renewed}")

        logger.info(f"Output: {self._config.output_path}")
        logger.info("=" * 60)

    def _load_queries(self) -> list[str]:
        path = Path(self._config.queries_file)
        if not path.exists():
            logger.error(f"Queries file not found: {path}")
            return []
        queries = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    queries.append(line)
        return queries

    def _load_exclusions(self) -> set[str]:
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

    @staticmethod
    def _extract_domain(url: str) -> str:
        return urlparse(url).netloc.lower()
