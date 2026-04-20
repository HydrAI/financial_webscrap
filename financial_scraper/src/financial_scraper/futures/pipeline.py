"""Futures pipeline: discover -> fetch -> filter -> store."""

import json
import logging
import signal
import time
from collections import Counter
from datetime import datetime, timezone

import requests

from .config import FuturesConfig
from .model import FuturesContract
from .lme_fetcher import LmeFetcher
from .cme_fetcher import CmeFetcher
from .ice_fetcher import IceFetcher
from ..checkpoint import Checkpoint
from ..fetch.throttle import SyncDomainThrottler
from ..store.output import ParquetWriter, JSONLWriter

logger = logging.getLogger(__name__)

_FETCHERS = {
    "lme": LmeFetcher,
    "cme": CmeFetcher,
    "ice": IceFetcher,
}

# Supported categories (for --list-exchanges and validation)
SUPPORTED_EXCHANGES = tuple(_FETCHERS.keys())

SUPPORTED_CATEGORIES = (
    "energy", "metals", "agriculture", "softs",
    "livestock", "financials", "emissions",
)


class FuturesPipeline:
    """Discover, fetch, and store futures contract specifications."""

    def __init__(self, config: FuturesConfig):
        self._config = config
        self._checkpoint = Checkpoint(config.checkpoint_file)
        self._parquet = ParquetWriter(config.output_path)
        self._jsonl = JSONLWriter(config.jsonl_path) if config.jsonl_path else None
        self._session = requests.Session()
        self._throttler = SyncDomainThrottler(
            base_delay=config.delay,
            max_delay=config.max_delay,
            max_per_domain=1,
        )
        self._shutdown_requested = False

    def run(self):
        """Execute the full futures pipeline (synchronous)."""
        self._shutdown_requested = False
        original_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_sigint)

        try:
            self._run_inner()
        finally:
            signal.signal(signal.SIGINT, original_handler)
            self._session.close()

    def _run_inner(self):
        config = self._config

        # 1. Resume checkpoint
        if config.resume:
            self._checkpoint.load()
            logger.info(
                "Resumed: %d URLs already fetched", len(self._checkpoint.fetched_urls)
            )

        logger.info("=" * 60)
        logger.info("Futures contract pipeline")
        logger.info("Exchanges: %s", ", ".join(config.exchanges))
        if config.categories:
            logger.info("Categories: %s", ", ".join(config.categories))
        logger.info("=" * 60)

        all_contracts: list[FuturesContract] = []
        stats: Counter = Counter()

        # Local-HTML mode: parse pre-crawled files (supplements live fetching)
        if config.local_html_dir:
            local_contracts = self._process_local_html(config.local_html_dir, stats)
            all_contracts.extend(local_contracts)

        # Live exchange fetching
        for exchange_key in config.exchanges:
            if self._shutdown_requested:
                break

            fetcher_cls = _FETCHERS.get(exchange_key.lower())
            if not fetcher_cls:
                logger.warning("Unknown exchange: %s (supported: %s)",
                               exchange_key, ", ".join(SUPPORTED_EXCHANGES))
                continue

            fetcher = fetcher_cls()
            contracts = self._process_exchange(fetcher, stats)
            all_contracts.extend(contracts)

        # 2. Category filter
        if config.categories and all_contracts:
            cat_set = set(config.categories)
            pre_filter = len(all_contracts)
            all_contracts = [c for c in all_contracts if c.asset_class in cat_set]
            if len(all_contracts) < pre_filter:
                logger.info(
                    "Category filter: %d/%d contracts matched",
                    len(all_contracts), pre_filter,
                )

        if not all_contracts:
            logger.info("No contracts found")
            return

        # 3. Store records
        logger.info("")
        logger.info("=" * 60)
        logger.info("Saving %d contracts", len(all_contracts))
        logger.info("=" * 60)

        self._store_contracts(all_contracts)

        # 4. Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("FUTURES PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info("  Contracts discovered: %d", stats["discovered"])
        logger.info("  Contracts fetched:    %d", stats["fetched"])
        logger.info("  Contracts failed:     %d", stats["failed"])
        logger.info("  Contracts stored:     %d", len(all_contracts))
        logger.info("  Output: %s", config.output_dir)
        logger.info("=" * 60)

    def _process_exchange(self, fetcher, stats: Counter) -> list[FuturesContract]:
        """Discover and fetch contracts for a single exchange."""
        config = self._config
        exchange = fetcher.exchange_name

        logger.info("")
        logger.info("-" * 40)
        logger.info("Exchange: %s", exchange)
        logger.info("-" * 40)

        # ICE special path: CSV catalog returns pre-parsed contracts
        if hasattr(fetcher, "discover_contracts_csv"):
            try:
                urls, csv_contracts = fetcher.discover_contracts_csv(
                    self._session, self._throttler,
                    categories=list(config.categories) if config.categories else None,
                )
                if csv_contracts:
                    stats["discovered"] += len(csv_contracts)
                    stats["fetched"] += len(csv_contracts)
                    for c in csv_contracts:
                        c.scraped_at = datetime.now(timezone.utc).isoformat()
                        self._checkpoint.mark_url_fetched(c.source_url)
                    self._checkpoint.save()
                    logger.info("%s: %d contracts from catalog CSV", exchange, len(csv_contracts))
                    return csv_contracts
            except Exception as exc:
                logger.warning("%s CSV catalog failed, falling back to per-page fetch: %s", exchange, exc)

        # Discovery
        try:
            spec_urls = fetcher.discover_contracts(
                self._session, self._throttler,
                categories=list(config.categories) if config.categories else None,
            )
        except Exception as exc:
            logger.error("%s discovery failed: %s", exchange, exc)
            return []

        stats["discovered"] += len(spec_urls)

        # Filter already-fetched
        to_fetch = [
            url for url in spec_urls
            if not self._checkpoint.is_url_fetched(url)
        ]
        skipped = len(spec_urls) - len(to_fetch)
        if skipped:
            logger.info("Skipping %d already-fetched URL(s)", skipped)

        if not to_fetch:
            logger.info("%s: all contracts already fetched", exchange)
            return []

        logger.info("Fetching %d contract spec(s) from %s", len(to_fetch), exchange)

        contracts: list[FuturesContract] = []

        for i, url in enumerate(to_fetch):
            if self._shutdown_requested:
                logger.warning("Shutdown requested — stopping fetch")
                break

            domain = fetcher.domain
            self._throttler.acquire(domain)
            try:
                resp = self._session.get(url, timeout=config.timeout)
                self._throttler.release(domain)

                if resp.status_code != 200:
                    self._throttler.report_failure(domain, resp.status_code)
                    self._checkpoint.mark_url_failed(url)
                    stats["failed"] += 1
                    logger.debug("%s returned %d", url, resp.status_code)
                    self._checkpoint.save_if_due(120)
                    continue

                self._throttler.report_success(domain)
            except Exception as exc:
                self._throttler.release(domain)
                self._checkpoint.mark_url_failed(url)
                stats["failed"] += 1
                logger.debug("Fetch failed %s: %s", url, exc)
                self._checkpoint.save_if_due(120)
                continue

            # Parse
            try:
                contract = fetcher.parse_contract(resp.text, url)
                contract.scraped_at = datetime.now(timezone.utc).isoformat()
            except Exception as exc:
                logger.warning("Parse failed %s: %s", url, exc)
                contract = FuturesContract(
                    exchange=exchange, source_url=url,
                    error=str(exc),
                    scraped_at=datetime.now(timezone.utc).isoformat(),
                )

            if contract.error:
                self._checkpoint.mark_url_failed(url)
                stats["failed"] += 1
            else:
                self._checkpoint.mark_url_fetched(url)
                stats["fetched"] += 1
                contracts.append(contract)

            self._checkpoint.save_if_due(120)

            if (i + 1) % 25 == 0:
                logger.info(
                    "  %s progress: %d/%d", exchange, i + 1, len(to_fetch)
                )

        self._checkpoint.save()
        logger.info(
            "%s: %d fetched, %d failed",
            exchange, stats["fetched"], stats["failed"],
        )
        return contracts

    def _process_local_html(self, html_dir: "Path", stats: Counter) -> list[FuturesContract]:
        """Parse pre-crawled HTML files from a local directory.

        Auto-detects the exchange from the canonical URL in each file.
        Filters to contract-specification pages only.
        """
        import re
        from pathlib import Path

        html_dir = Path(html_dir)
        html_files = sorted(html_dir.glob("*.html"))
        logger.info("Local HTML mode: scanning %d files in %s", len(html_files), html_dir)

        # Only process files that look like contract specs
        spec_files = [f for f in html_files if "contract-specifications" in f.name or "contract-spec" in f.name or "contractSpec" in f.name]
        if not spec_files:
            # Fallback: check all files for canonical URLs containing "contract-spec"
            for f in html_files:
                try:
                    head = f.read_text(encoding="utf-8", errors="ignore")[:2000]
                    if "contract-specifications" in head or "contractSpec" in head:
                        spec_files.append(f)
                except Exception:
                    pass

        logger.info("Found %d contract-specification files", len(spec_files))
        stats["discovered"] += len(spec_files)

        # Map domain patterns → fetcher
        domain_fetcher_map = {
            "lme.com": LmeFetcher(),
            "cmegroup.com": CmeFetcher(),
            "ice.com": IceFetcher(),
        }

        contracts: list[FuturesContract] = []
        for f in spec_files:
            if self._shutdown_requested:
                break

            html = f.read_text(encoding="utf-8", errors="ignore")

            # Extract canonical URL
            match = re.search(r'<link rel="canonical" href="([^"]+)"', html)
            url = match.group(1) if match else f"file://{f}"

            # Pick fetcher by domain
            fetcher = None
            for domain_key, fobj in domain_fetcher_map.items():
                if domain_key in url:
                    fetcher = fobj
                    break

            if not fetcher:
                # Default to LME for lme.com URLs
                if "lme.com" in url:
                    fetcher = LmeFetcher()
                else:
                    logger.debug("Skipping %s — unknown exchange domain", f.name)
                    continue

            try:
                contract = fetcher.parse_contract(html, url)
                contract.scraped_at = datetime.now(timezone.utc).isoformat()
                stats["fetched"] += 1
                contracts.append(contract)
            except Exception as exc:
                logger.warning("Parse failed %s: %s", f.name, exc)
                stats["failed"] += 1

        logger.info("Local HTML: %d parsed, %d failed", stats["fetched"], stats["failed"])
        return contracts

    def _store_contracts(self, contracts: list[FuturesContract]):
        """Write contracts to Parquet (8-col) + optional JSONL."""
        config = self._config
        now_quarter = _current_quarter_tag()

        # 8-col Parquet records
        records = []
        for c in contracts:
            exchange_display = {
                "LME": "LME",
                "CME": "CME Group",
                "ICE": "ICE",
            }.get(c.exchange, c.exchange)

            snippet = f"{c.asset_class} | {c.contract_size} | {c.settlement_type} | {c.quote_currency}"
            title = f"{c.product_name} ({c.ticker})" if c.ticker else c.product_name

            records.append({
                "company": exchange_display,
                "title": title,
                "link": c.source_url,
                "snippet": snippet,
                "date": c.scraped_at,
                "source": _domain_from_url(c.source_url),
                "full_text": json.dumps(c.__dict__, ensure_ascii=False, default=str),
                "source_file": f"futures_{c.exchange.lower()}_{now_quarter}.parquet",
            })

        if records:
            self._parquet.append(records)
            if self._jsonl:
                self._jsonl.append(records)

        # Per-exchange detail JSONL
        by_exchange: dict[str, list[FuturesContract]] = {}
        for c in contracts:
            by_exchange.setdefault(c.exchange.lower(), []).append(c)

        for ex_slug, ex_contracts in by_exchange.items():
            detail_file = config.output_dir / f"{ex_slug}_futures.jsonl"
            with open(detail_file, "w", encoding="utf-8") as f:
                for c in ex_contracts:
                    f.write(json.dumps(c.__dict__, ensure_ascii=False, default=str) + "\n")
            logger.info("Detail JSONL: %s (%d contracts)", detail_file, len(ex_contracts))

    def _handle_sigint(self, sig, frame):
        """Handle Ctrl+C gracefully."""
        if self._shutdown_requested:
            raise KeyboardInterrupt
        logger.warning(
            "Shutdown requested — finishing current contract and saving checkpoint..."
        )
        self._shutdown_requested = True
        self._checkpoint.save()


def _current_quarter_tag() -> str:
    now = datetime.now()
    q = (now.month - 1) // 3 + 1
    return f"{now.year}Q{q}"


def _domain_from_url(url: str) -> str:
    """Extract domain from URL."""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc or url
    except Exception:
        return url
