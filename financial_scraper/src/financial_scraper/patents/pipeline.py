"""Patent pipeline: discover -> fetch -> filter -> store."""

import json
import logging
import signal
import time
from collections import Counter
from pathlib import Path

import requests

from .config import PatentConfig
from .discovery import discover_patent_ids
from .google_patents import PatentDetail, fetch_patent
from .normalize import normalize_assignee, are_same_assignee
from .uspto_fetcher import fetch_patent_from_uspto
from .wipo import resolve_wipo_to_cpc
from ..checkpoint import Checkpoint
from ..fetch.throttle import SyncDomainThrottler
from ..store.output import ParquetWriter, JSONLWriter

logger = logging.getLogger(__name__)


class PatentPipeline:
    """Discover, fetch, filter, and store patent data."""

    def __init__(self, config: PatentConfig):
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
        """Execute the full patent pipeline (synchronous)."""
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
                f"Resumed: {len(self._checkpoint.fetched_urls)} patents already fetched"
            )

        # 2. Discover patent IDs
        logger.info("=" * 60)
        logger.info(f"Patent pipeline for: {config.company}")
        logger.info("=" * 60)

        patent_ids = discover_patent_ids(config)
        if not patent_ids:
            logger.error(
                "No patent IDs found. Use --ids-file, --ids, "
                "--discover-search, or --discover-justia."
            )
            return

        # 3. Filter already-fetched
        to_fetch = [
            pid for pid in patent_ids
            if not self._checkpoint.is_url_fetched(
                f"https://patents.google.com/patent/{pid}/en"
            )
        ]
        skipped = len(patent_ids) - len(to_fetch)
        if skipped:
            logger.info(f"Skipping {skipped} already-fetched patent(s)")

        if not to_fetch:
            logger.info("All patents already fetched. Use --reset to re-fetch.")
            return

        # 4. Resolve classification filters
        cpc_prefixes = list(config.cpc_filter)
        ipc_prefixes = list(config.ipc_filter)

        if config.wipo_categories:
            try:
                wipo_cpc = resolve_wipo_to_cpc(list(config.wipo_categories))
                cpc_prefixes.extend(wipo_cpc)
                logger.info(
                    f"WIPO categories resolved to {len(wipo_cpc)} CPC prefix(es): "
                    f"{', '.join(wipo_cpc[:10])}"
                )
            except ValueError as e:
                logger.error(str(e))
                return

        has_class_filter = bool(cpc_prefixes or ipc_prefixes)

        # 5. Fetch patent pages
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"Fetching {len(to_fetch)} patent(s)")
        logger.info("=" * 60)

        fetched: list[PatentDetail] = []
        stats: Counter = Counter()

        for i, pid in enumerate(to_fetch):
            if self._shutdown_requested:
                logger.warning("Shutdown requested — stopping fetch")
                break

            detail = fetch_patent(
                pid, self._session, self._throttler, timeout=config.timeout
            )

            url = detail.url
            if detail.error:
                self._checkpoint.mark_url_failed(url)
                stats["failed"] += 1
            else:
                self._checkpoint.mark_url_fetched(url)
                stats["fetched"] += 1
                fetched.append(detail)

            self._checkpoint.save_if_due(120)

        self._checkpoint.save()
        logger.info(
            f"Google Patents: {stats['fetched']}/{len(to_fetch)} successful, "
            f"{stats['failed']} failed"
        )

        # 5b. Retry failed US patents via USPTO PatFT/AppFT fallback
        failed_ids = [
            pid for pid in to_fetch
            if pid.startswith("US") and pid not in {p.patent_id for p in fetched}
        ]
        if failed_ids and not self._shutdown_requested:
            logger.info("")
            logger.info("=" * 60)
            logger.info(
                f"Retrying {len(failed_ids)} failed US patent(s) via USPTO"
            )
            logger.info("=" * 60)

            for pid in failed_ids:
                if self._shutdown_requested:
                    break

                detail = fetch_patent_from_uspto(
                    pid, self._session, self._throttler, timeout=config.timeout
                )
                if detail is None:
                    continue

                url = f"https://patents.google.com/patent/{pid}/en"
                if detail.error:
                    stats["uspto_failed"] += 1
                else:
                    self._checkpoint.mark_url_fetched(url)
                    stats["fetched"] += 1
                    stats["failed"] -= 1
                    stats["uspto_recovered"] += 1
                    fetched.append(detail)

                self._checkpoint.save_if_due(120)

            self._checkpoint.save()
            if stats["uspto_recovered"]:
                logger.info(
                    f"USPTO fallback recovered {stats['uspto_recovered']} patent(s)"
                )

        # 6. Post-fetch classification filter
        if has_class_filter and fetched:
            pre_filter = len(fetched)
            fetched = [
                p for p in fetched
                if self._matches_classification(p, cpc_prefixes, ipc_prefixes)
            ]
            logger.info(
                f"Classification filter: {len(fetched)}/{pre_filter} patents matched"
            )

        # 6b. Granted-only post-fetch filter
        if config.granted_only and fetched:
            pre_granted = len(fetched)
            fetched = [p for p in fetched if p.date_granted]
            if len(fetched) < pre_granted:
                logger.info(
                    f"Granted-only filter: {len(fetched)}/{pre_granted} patents "
                    f"have a grant date"
                )

        # 6c. Sort by newest + limit
        if config.limit > 0 and fetched:
            fetched.sort(
                key=lambda p: p.date_granted or p.date_filed or "",
                reverse=True,
            )
            if len(fetched) > config.limit:
                logger.info(
                    f"Limit: keeping top {config.limit} of {len(fetched)} patents"
                )
                fetched = fetched[:config.limit]

        if not fetched:
            logger.info("No patents remaining after filtering")
            return

        # 7. Normalize assignees
        assignee_map: dict[str, str] = {}
        for patent in fetched:
            raw = patent.assignee
            if raw:
                norm = normalize_assignee(raw)
                if norm not in assignee_map:
                    assignee_map[norm] = raw
                elif not are_same_assignee(assignee_map[norm], raw):
                    assignee_map[norm] = raw

        if len(assignee_map) > 1:
            logger.info(f"Assignees found: {list(assignee_map.values())}")

        # 8. Store records
        logger.info("")
        logger.info("=" * 60)
        logger.info("Saving results")
        logger.info("=" * 60)

        company_slug = config.company.lower().replace(" ", "_")

        # 8a. Parquet + JSONL (standard schema)
        records = []
        for patent in fetched:
            snippet = (
                (patent.abstract[:300] + "...")
                if len(patent.abstract) > 300
                else patent.abstract
            )
            records.append({
                "company": config.company,
                "title": patent.title,
                "link": patent.url,
                "snippet": snippet,
                "date": patent.date_granted or patent.date_filed,
                "source": "patents.google.com",
                "full_text": patent.full_text or patent.abstract,
                "source_file": f"{company_slug}_patent_{patent.patent_id}.parquet",
            })

        if records:
            self._parquet.append(records)
            if self._jsonl:
                self._jsonl.append(records)

        # 8b. Patent details JSONL (full metadata without full_text)
        details_file = config.output_dir / f"{company_slug}_patents.jsonl"
        with open(details_file, "w", encoding="utf-8") as f:
            for patent in fetched:
                save = {
                    "patent_id": patent.patent_id,
                    "url": patent.url,
                    "title": patent.title,
                    "abstract": patent.abstract,
                    "patent_number": patent.patent_number,
                    "application_number": patent.application_number,
                    "date_filed": patent.date_filed,
                    "date_granted": patent.date_granted,
                    "assignee": patent.assignee,
                    "inventors": patent.inventors,
                    "classifications_cpc": patent.classifications_cpc,
                    "classifications_ipc": patent.classifications_ipc,
                    "citations_backward": patent.citations_backward,
                    "citations_forward": patent.citations_forward,
                    "non_patent_citations": patent.non_patent_citations,
                    "pdf_url": patent.pdf_url,
                    "expiration_date": patent.expiration_date,
                }
                f.write(json.dumps(save, ensure_ascii=False) + "\n")
        logger.info(f"Patent details: {details_file}")

        # 9. Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("PATENT PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  Patents discovered:     {len(patent_ids)}")
        logger.info(f"  Patents fetched:        {stats['fetched']}")
        logger.info(f"  Patents failed:         {stats['failed']}")
        logger.info(f"  Patents after filter:   {len(fetched)}")
        logger.info(f"  Output: {config.output_dir}")
        logger.info("=" * 60)

    @staticmethod
    def _matches_classification(
        patent: PatentDetail,
        cpc_prefixes: list[str],
        ipc_prefixes: list[str],
    ) -> bool:
        """Check if a patent matches any of the given classification prefixes."""
        if cpc_prefixes:
            for code in patent.classifications_cpc:
                for prefix in cpc_prefixes:
                    if code.startswith(prefix):
                        return True

        if ipc_prefixes:
            for code in patent.classifications_ipc:
                for prefix in ipc_prefixes:
                    if code.startswith(prefix):
                        return True

        # If only CPC filter was given and patent has no CPC codes, don't match
        # If only IPC filter was given and patent has no IPC codes, don't match
        # If both are empty (shouldn't happen — has_class_filter is False), match all
        return False

    def _handle_sigint(self, sig, frame):
        """Handle Ctrl+C gracefully."""
        if self._shutdown_requested:
            raise KeyboardInterrupt
        logger.warning(
            "Shutdown requested — finishing current patent and saving checkpoint..."
        )
        self._shutdown_requested = True
        self._checkpoint.save()
