"""Transcript pipeline: discover -> fetch -> extract -> store."""

import logging
import time
from collections import Counter
from pathlib import Path

import requests

from .config import TranscriptConfig
from .discovery import discover_transcripts, TranscriptInfo
from .extract import extract_transcript, TranscriptResult
from ..store.dedup import Deduplicator
from ..store.output import ParquetWriter, JSONLWriter, _parse_date
from ..checkpoint import Checkpoint

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


class TranscriptPipeline:
    """Discover, fetch, and extract earnings call transcripts."""

    def __init__(self, config: TranscriptConfig):
        self._config = config
        self._dedup = Deduplicator()
        self._checkpoint = Checkpoint(config.checkpoint_file)
        self._parquet = ParquetWriter(config.output_path)
        self._jsonl = JSONLWriter(config.jsonl_path) if config.jsonl_path else None
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})

    def run(self):
        """Execute the full transcript pipeline (synchronous)."""
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

        # 3. Discover + fetch + extract per ticker
        total_records = 0
        stats: Counter = Counter()

        for ti, ticker in enumerate(tickers):
            logger.info(f"\n[{ti+1}/{len(tickers)}] {ticker}")

            # Discovery
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

        # 4. Summary
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
        """Fetch transcript pages and extract content."""
        records = []

        for info in infos:
            logger.info(f"  Fetching {info.quarter} {info.year}: {info.url}")

            try:
                resp = self._session.get(info.url, timeout=30)
                stats["fetched"] += 1
            except requests.RequestException as e:
                logger.warning(f"  Failed to fetch {info.url}: {e}")
                self._checkpoint.mark_url_failed(info.url)
                stats["failed"] += 1
                continue

            if resp.status_code != 200:
                logger.warning(f"  HTTP {resp.status_code} for {info.url}")
                self._checkpoint.mark_url_failed(info.url)
                stats["failed"] += 1
                continue

            self._checkpoint.mark_url_fetched(info.url)

            # Extract
            result = extract_transcript(resp.text)
            if result is None or not result.full_text:
                logger.warning(f"  Extraction failed for {info.url}")
                stats["failed"] += 1
                continue

            # Content dedup
            if self._dedup.is_duplicate_content(result.full_text):
                logger.info(f"  Duplicate content, skipping")
                continue

            self._dedup.mark_seen(info.url, result.full_text)
            stats["extracted"] += 1

            # Build record (merged_by_year schema)
            title = f"{ticker} {info.quarter} {info.year} Earnings Call Transcript"
            snippet = (result.full_text[:300] + "...") if len(result.full_text) > 300 else result.full_text
            source_file = f"{ticker}_transcript_{info.quarter}_{info.year}.parquet"

            record = {
                "company": ticker,
                "title": title,
                "link": info.url,
                "snippet": snippet,
                "date": result.date or info.pub_date,
                "source": "fool.com",
                "full_text": result.full_text,
                "source_file": source_file,
            }
            records.append(record)

            # Polite delay between requests
            time.sleep(1.5)

        return records

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
