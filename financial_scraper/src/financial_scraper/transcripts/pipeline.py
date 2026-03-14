"""Transcript pipeline: discover -> fetch -> extract -> store."""

import logging
import signal
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

from .config import TranscriptConfig
from .discovery import discover_transcripts, discover_transcripts_range, TranscriptInfo
from .extract import extract_transcript, TranscriptResult
from .sources.fmp import FMPSource
from ..store.dedup import Deduplicator
from ..store.output import ParquetWriter, JSONLWriter, _parse_date
from ..checkpoint import Checkpoint
from ..fetch.curl_client import CurlSession
from ..fetch.proxy import ProxyRotator, auto_fetch_proxies
from ..fetch.throttle import SyncDomainThrottler

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Checkpoint save interval (seconds) — avoid saving after every single URL
_CHECKPOINT_INTERVAL = 120


class TranscriptPipeline:
    """Discover, fetch, and extract earnings call transcripts."""

    def __init__(self, config: TranscriptConfig):
        self._config = config
        self._dedup = Deduplicator()
        self._checkpoint = Checkpoint(config.checkpoint_file)
        self._parquet = ParquetWriter(config.output_path)
        self._jsonl = JSONLWriter(config.jsonl_path) if config.jsonl_path else None
        self._session = CurlSession(headers={"User-Agent": USER_AGENT})
        self._fmp = FMPSource(api_key=config.fmp_api_key)
        self._throttler = SyncDomainThrottler(
            base_delay=1.0, max_delay=60.0, max_per_domain=2,
        )
        self._shutdown_requested = False
        self._browser = None
        self._proxy_rotator = None

        if self._fmp.available:
            logger.info("FMP fallback source enabled")

        # Proxy rotation
        if config.proxies_file:
            from ..fetch.proxy import load_proxies

            proxies = load_proxies(str(config.proxies_file))
            if proxies:
                self._proxy_rotator = ProxyRotator(proxies, strategy="weighted")
                logger.info(
                    "Proxy rotation enabled: %d proxies loaded", len(proxies)
                )

        # Browser fallback (lazy — only loaded if needed)
        if config.browser_fallback:
            try:
                from ..fetch.browser import BrowserFetcher

                self._browser = BrowserFetcher(headless=True)
                logger.info("Browser fallback enabled")
            except ImportError:
                logger.warning(
                    "Browser fallback requested but playwright not installed"
                )

    def run(self):
        """Execute the full transcript pipeline (synchronous)."""
        # Install graceful shutdown handler
        self._shutdown_requested = False
        original_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_sigint)

        try:
            self._run_inner()
        finally:
            signal.signal(signal.SIGINT, original_handler)
            self._session.close()
            if self._browser:
                self._browser.close()

    def _run_inner(self):
        # 1. Resume checkpoint
        if self._config.resume:
            self._checkpoint.load()
            logger.info(
                f"Resumed: {len(self._checkpoint.fetched_urls)} URLs already fetched"
            )

        # 2. Load tickers
        tickers = self._load_tickers()
        if not tickers:
            logger.error("No tickers to process")
            return

        logger.info(f"Processing {len(tickers)} ticker(s): {', '.join(tickers)}")

        # 3. Discovery — bulk (range mode) or per-ticker (single-year mode)
        if self._config.from_year is not None:
            cache_path = self._config.checkpoint_file.with_name("discovery_cache.json")
            bulk = discover_transcripts_range(
                tickers,
                from_year=self._config.from_year,
                to_year=self._config.to_year,  # guaranteed set by build_transcript_config
                quarters=self._config.quarters,
                cache_path=cache_path,
            )
        else:
            bulk = None

        total_records = 0
        stats: Counter = Counter()

        for ti, ticker in enumerate(tickers):
            if self._shutdown_requested:
                logger.warning("Shutdown requested — stopping after current ticker")
                break

            logger.info(f"\n[{ti+1}/{len(tickers)}] {ticker}")

            # Discovery (range mode pre-computed; single-year fetched per ticker)
            if bulk is not None:
                infos = bulk.get(ticker, [])
            else:
                infos = discover_transcripts(
                    ticker,
                    year=self._config.year,
                    quarters=self._config.quarters,
                )
            if not infos:
                logger.warning(f"  No transcripts found for {ticker}")
                continue

            stats["discovered"] += len(infos)

            # Filter already-fetched
            to_fetch = [
                info for info in infos
                if not self._checkpoint.is_url_fetched(info.url)
                and not self._dedup.is_duplicate_url(info.url)
            ]
            skipped = len(infos) - len(to_fetch)
            if skipped:
                logger.info(f"  Skipping {skipped} already-fetched transcript(s)")

            if not to_fetch:
                continue

            # Fetch and extract
            records = self._fetch_and_extract(ticker, to_fetch, stats)
            if records:
                self._parquet.append(records)
                if self._jsonl:
                    self._jsonl.append(records)
                total_records += len(records)

            self._checkpoint.save()

        # 4. Final checkpoint save + Summary
        self._checkpoint.save()
        logger.info("\n" + "=" * 60)
        logger.info("TRANSCRIPT SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Tickers processed: {len(tickers)}")
        logger.info(f"Transcripts discovered: {stats['discovered']}")
        logger.info(f"Transcripts fetched: {stats['fetched']}")
        logger.info(f"Transcripts extracted: {stats['extracted']}")
        logger.info(f"Failed: {stats['failed']}")
        logger.info(f"Records written: {total_records}")
        logger.info(f"Output: {self._config.output_path}")
        logger.info("=" * 60)

    def _fetch_and_extract(
        self, ticker: str, infos: list[TranscriptInfo], stats: Counter
    ) -> list[dict]:
        """Fetch transcript pages concurrently and extract content.

        Worker threads handle HTTP GET + extraction (pure I/O, no shared state).
        The main thread processes results: stats, checkpoint, dedup, record building.
        """
        session = self._session
        fmp = self._fmp
        throttler = self._throttler
        # Mutable container so auto-fetch can create/replace the rotator
        proxy_ref: list[ProxyRotator | None] = [self._proxy_rotator]
        _auto_fetch_lock = threading.Lock()
        browser = self._browser

        def _fetch_one(info: TranscriptInfo):
            """Worker: fetch and extract one transcript. Touches no shared state.

            Strategy:
              1. Try fool.com with up to 4 attempts (exponential backoff on 429).
              2. On permanent HTTP failure, try FMP as fallback if configured.
              3. On both failing, try browser fallback if configured.
            """
            logger.info(f"  Fetching {info.quarter} {info.year}: {info.url}")
            http_failed = False
            domain = urlparse(info.url).netloc.lower()
            proxy_rotator = proxy_ref[0]

            # Set proxy for this request if available
            if proxy_rotator:
                proxy = proxy_rotator.next()
                session.set_proxy(proxy)

            for attempt in range(4):
                # Acquire throttle before request
                throttler.acquire(domain)
                try:
                    resp = session.get(info.url, timeout=30)
                except Exception as e:
                    if proxy_rotator:
                        proxy_rotator.report_error(proxy)
                    logger.warning(f"  Failed to fetch {info.url}: {e}")
                    return info, None, "request_error"
                finally:
                    throttler.release(domain)

                if resp.status_code == 429:
                    throttler.report_failure(domain, 429)
                    if proxy_rotator:
                        proxy_rotator.report_error(proxy)

                    # Auto-fetch free proxies if we have none or very few
                    if not proxy_rotator or proxy_rotator.active_count < 2:
                        with _auto_fetch_lock:
                            proxy_rotator = proxy_ref[0]
                            if not proxy_rotator or proxy_rotator.active_count < 2:
                                proxy_rotator = auto_fetch_proxies(rotator=proxy_rotator)
                                proxy_ref[0] = proxy_rotator
                                self._proxy_rotator = proxy_rotator
                            else:
                                proxy_rotator = proxy_ref[0]

                    retry_after = min(int(resp.headers.get("Retry-After", 0)), 60)
                    wait = retry_after if retry_after > 0 else (10 * 2 ** attempt)
                    if attempt < 3:
                        logger.warning(
                            f"  HTTP 429 for {info.url} (attempt {attempt+1}/4), "
                            f"backing off {wait}s"
                        )
                        time.sleep(wait)
                        # Rotate proxy/browser for next attempt
                        if proxy_rotator:
                            proxy = proxy_rotator.next()
                            session.set_proxy(proxy)
                        session.rotate_browser()
                        continue
                    logger.warning(f"  HTTP 429 for {info.url} (max retries)")
                    http_failed = True
                    break

                if resp.status_code != 200:
                    throttler.report_failure(domain, resp.status_code)
                    logger.warning(f"  HTTP {resp.status_code} for {info.url}")
                    http_failed = True
                    break

                throttler.report_success(domain)
                if proxy_rotator:
                    proxy_rotator.report_success(proxy)

                result = extract_transcript(resp.text)
                if result is None or not result.full_text:
                    logger.warning(f"  Extraction failed for {info.url}")
                    return info, None, "extract_error"

                time.sleep(1.0)
                return info, result, "success"

            # Fool.com permanently failed — try FMP fallback
            if http_failed and fmp.available:
                logger.info(
                    f"  Trying FMP fallback for {info.ticker} {info.quarter} {info.year}"
                )
                # FMP uses its own HTTP client; pass a plain requests session
                import requests
                fmp_session = requests.Session()
                try:
                    fmp_result = fmp.get_transcript(
                        info.ticker, info.quarter, info.year, fmp_session
                    )
                    if fmp_result and fmp_result.full_text:
                        time.sleep(1.0)
                        return info, fmp_result, "success"
                finally:
                    fmp_session.close()

            # Browser fallback
            if http_failed and browser:
                logger.info(f"  Trying browser fallback for {info.url}")
                html = browser.fetch(info.url, wait_selector="div.article-body")
                if html:
                    result = extract_transcript(html)
                    if result and result.full_text:
                        return info, result, "success"

            return info, None, "http_error"

        records = []
        concurrent = max(1, self._config.concurrent)

        with ThreadPoolExecutor(max_workers=concurrent) as executor:
            futures = {executor.submit(_fetch_one, info): info for info in infos}
            for future in as_completed(futures):
                if self._shutdown_requested:
                    logger.warning("Shutdown requested — cancelling remaining fetches")
                    for f in futures:
                        f.cancel()
                    break

                try:
                    info, result, event = future.result()
                except Exception as e:
                    info = futures[future]
                    logger.error(f"  Unexpected error for {info.url}: {e}")
                    stats["failed"] += 1
                    continue

                if event == "request_error":
                    self._checkpoint.mark_url_failed(info.url)
                    stats["failed"] += 1
                    continue

                # HTTP GET completed (200 or not)
                stats["fetched"] += 1

                if event in ("http_error", "extract_error"):
                    self._checkpoint.mark_url_failed(info.url)
                    stats["failed"] += 1
                    continue

                # Successful extraction — shared state handled here (main thread only)
                self._checkpoint.mark_url_fetched(info.url)

                if self._dedup.is_duplicate_content(result.full_text):
                    logger.info("  Duplicate content, skipping")
                    continue

                self._dedup.mark_seen(info.url, result.full_text)
                stats["extracted"] += 1

                title = f"{ticker} {info.quarter} {info.year} Earnings Call Transcript"
                snippet = (
                    (result.full_text[:300] + "...")
                    if len(result.full_text) > 300
                    else result.full_text
                )
                source_file = f"{ticker}_transcript_{info.quarter}_{info.year}.parquet"
                records.append({
                    "company": ticker,
                    "title": title,
                    "link": info.url,
                    "snippet": snippet,
                    "date": result.date or info.pub_date,
                    "source": "fool.com",
                    "full_text": result.full_text,
                    "source_file": source_file,
                })

                # Interval-based checkpoint save
                self._checkpoint.save_if_due(_CHECKPOINT_INTERVAL)

        return records

    def _handle_sigint(self, sig, frame):
        """Handle Ctrl+C gracefully. Second Ctrl+C force-quits."""
        if self._shutdown_requested:
            raise KeyboardInterrupt  # second Ctrl+C = force quit
        logger.warning("Shutdown requested — finishing current ticker and saving checkpoint...")
        self._shutdown_requested = True
        self._checkpoint.save()

    def _load_tickers(self) -> list[str]:
        """Load ticker list from config (inline or file)."""
        tickers = list(self._config.tickers)

        if self._config.tickers_file:
            path = Path(self._config.tickers_file)
            if not path.exists():
                logger.error(f"Tickers file not found: {path}")
                return tickers
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip().upper()
                    if line and not line.startswith("#"):
                        tickers.append(line)

        # Deduplicate while preserving order
        seen: set[str] = set()
        unique = []
        for t in tickers:
            if t not in seen:
                seen.add(t)
                unique.append(t)
        return unique
