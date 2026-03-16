"""BigQuery-based patent pipeline.

Reads company names from a CSV, queries the BigQuery public patents
dataset with a **single query** containing all companies (to avoid
redundant column scans), and stores results in the same Parquet + JSONL
format as the Google Patents scraping pipeline.

Requires ``pip install financial-scraper[bigquery]`` and
``gcloud auth application-default login`` for authentication.
"""

import json
import logging
import re
import signal
from dataclasses import dataclass
from pathlib import Path

from .config import PatentConfig
from .google_patents import PatentDetail
from .normalize import are_same_assignee
from ..checkpoint import Checkpoint
from ..store.output import ParquetWriter, JSONLWriter

logger = logging.getLogger(__name__)

# Characters illegal in Windows filenames
_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*]')


def _safe_slug(name: str) -> str:
    """Convert a company name to a filesystem-safe slug."""
    slug = name.lower().replace(" ", "_")
    return _UNSAFE_CHARS.sub("", slug)


@dataclass(frozen=True)
class BigQueryConfig:
    """Configuration specific to the BigQuery patent pipeline."""

    # CSV input
    csv_path: Path
    company_column: str = "name"

    # Query parameters
    country_filter: str = "US"
    include_description: bool = False
    batch_size: int = 50
    dry_run: bool = False
    project_id: str | None = None

    # Filters (shared with PatentConfig)
    granted_only: bool = True
    cpc_filter: tuple[str, ...] = ()
    ipc_filter: tuple[str, ...] = ()
    limit: int = 0

    # Output (shared with PatentConfig)
    output_dir: Path = Path(".")
    output_path: Path = Path("output.parquet")
    jsonl_path: Path | None = None
    checkpoint_file: Path = Path(".bq_patent_checkpoint.json")
    resume: bool = False


class BigQueryPatentPipeline:
    """Discover patents via BigQuery public dataset and store locally."""

    def __init__(self, bq_config: BigQueryConfig):
        self._config = bq_config
        self._checkpoint = Checkpoint(bq_config.checkpoint_file)
        self._parquet = ParquetWriter(bq_config.output_path)
        self._jsonl = JSONLWriter(bq_config.jsonl_path) if bq_config.jsonl_path else None
        self._shutdown_requested = False

    def run(self):
        """Execute the full BigQuery patent pipeline."""
        self._shutdown_requested = False
        original_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_sigint)

        try:
            self._run_inner()
        finally:
            signal.signal(signal.SIGINT, original_handler)

    def _run_inner(self):
        cfg = self._config

        # 1. Lazy import BigQuery client
        try:
            from google.cloud import bigquery
        except ImportError:
            logger.error(
                "google-cloud-bigquery is not installed. "
                "Install with: pip install financial-scraper[bigquery]"
            )
            return

        # 2. Create client (ADC auth)
        try:
            client_kwargs = {}
            if cfg.project_id:
                client_kwargs["project"] = cfg.project_id
            client = bigquery.Client(**client_kwargs)
        except Exception as e:
            error_msg = str(e)
            if "DefaultCredentialsError" in type(e).__name__ or "credentials" in error_msg.lower():
                logger.error(
                    "Google Cloud credentials not found. Run:\n"
                    "  gcloud auth application-default login\n"
                    "Then retry."
                )
            else:
                logger.error(f"Failed to create BigQuery client: {e}")
            return

        # 3. Resume checkpoint
        if cfg.resume:
            self._checkpoint.load()
            logger.info(
                f"Resumed: {len(self._checkpoint.fetched_urls)} patents already stored"
            )

        # 4. Load companies from CSV
        from .bigquery_fetcher import (
            load_companies_from_csv,
            build_query,
            bq_row_to_patent_detail,
            CompanyMatcher,
        )

        rows = load_companies_from_csv(cfg.csv_path, column=cfg.company_column)
        company_names = [
            r[cfg.company_column] for r in rows
            if r.get(cfg.company_column, "").strip()
        ]

        if not company_names:
            logger.error("No company names found in CSV")
            return

        logger.info("=" * 60)
        logger.info("BigQuery Patent Pipeline")
        logger.info(f"  Companies: {len(company_names)}")
        logger.info(f"  Country:   {cfg.country_filter}")
        logger.info(f"  Granted:   {cfg.granted_only}")
        logger.info("=" * 60)

        # 5. Build a SINGLE query with all companies
        #    BigQuery scans columns once regardless of WHERE clause size,
        #    so one query = ~376 GB instead of N batches × 376 GB.
        sql = build_query(
            company_names,
            granted_only=cfg.granted_only,
            include_description=cfg.include_description,
            country=cfg.country_filter,
        )

        # 6. Dry run: estimate cost for the single query
        if cfg.dry_run:
            self._dry_run(client, bigquery, sql)
            return

        # 7. Build fast matcher index
        matcher = CompanyMatcher.from_names(company_names)

        # 8. Execute query and stream results
        logger.info("Executing BigQuery query (this may take a few minutes)...")

        try:
            query_job = client.query(sql)
            result_iter = query_job.result()
            total_rows = result_iter.total_rows
            logger.info(f"Query complete: {total_rows:,} raw patent rows")
        except Exception as e:
            logger.error(f"BigQuery error: {e}")
            return

        # 9. Process results in pages, matching to companies
        #    We process in memory-friendly pages from the BigQuery iterator
        #    and flush to disk periodically.
        patents_by_company: dict[str, list[PatentDetail]] = {}
        total_raw = 0
        total_matched = 0

        page_num = 0
        for page in result_iter.pages:
            if self._shutdown_requested:
                logger.warning("Shutdown requested — stopping")
                break

            for row in page:
                total_raw += 1
                detail = bq_row_to_patent_detail(
                    row, "", include_description=cfg.include_description
                )

                # Skip already-stored patents (resume support)
                if self._checkpoint.is_url_fetched(detail.patent_id):
                    continue

                # Match to a company (O(1) lookup via pre-built index)
                matched = matcher.match(detail.assignee)
                if not matched:
                    continue

                total_matched += 1
                patents_by_company.setdefault(matched, []).append(detail)
                self._checkpoint.mark_url_fetched(detail.patent_id)

            page_num += 1
            if page_num % 10 == 0:
                logger.info(
                    f"  Processed {total_raw:,} rows, "
                    f"{total_matched:,} matched so far..."
                )
                self._checkpoint.save_if_due(120)

                # Flush to disk periodically to limit memory usage
                if len(patents_by_company) >= cfg.batch_size:
                    self._flush_to_disk(patents_by_company, cfg)
                    patents_by_company.clear()

        # Save checkpoint after streaming
        self._checkpoint.save()

        # 9. Apply classification filters
        if cfg.cpc_filter or cfg.ipc_filter:
            cpc_prefixes = list(cfg.cpc_filter)
            ipc_prefixes = list(cfg.ipc_filter)
            for company in list(patents_by_company):
                patents_by_company[company] = [
                    p for p in patents_by_company[company]
                    if self._matches_classification(p, cpc_prefixes, ipc_prefixes)
                ]

        # 10. Apply limit per company
        if cfg.limit > 0:
            for company in patents_by_company:
                patents = patents_by_company[company]
                patents.sort(
                    key=lambda p: p.date_granted or p.date_filed or "",
                    reverse=True,
                )
                patents_by_company[company] = patents[: cfg.limit]

        # 11. Store remaining results
        total_stored = self._flush_to_disk(patents_by_company, cfg)

        # 12. Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("BIGQUERY PATENT PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  Companies queried:  {len(company_names)}")
        logger.info(f"  Raw patent rows:    {total_raw:,}")
        logger.info(f"  Matched to companies: {total_matched:,}")
        logger.info(f"  Stored this run:    {total_stored:,}")
        logger.info(f"  Output:             {cfg.output_dir}")
        logger.info("=" * 60)

    def _dry_run(self, client, bigquery_module, sql: str):
        """Run the query with ``dry_run=True`` to estimate bytes scanned."""
        job_config = bigquery_module.QueryJobConfig(dry_run=True, use_query_cache=False)
        try:
            query_job = client.query(sql, job_config=job_config)
            total_bytes = query_job.total_bytes_processed or 0
        except Exception as e:
            logger.error(f"Dry run error: {e}")
            return

        gb = total_bytes / 1e9
        tb = total_bytes / 1e12
        cost_if_over = max(0, tb - 1.0) * 6.25  # $6.25/TB after 1 TB free tier

        logger.info("")
        logger.info("=" * 60)
        logger.info("DRY RUN COST ESTIMATE (single query, all companies)")
        logger.info("=" * 60)
        logger.info(f"  Bytes scanned:  {total_bytes:,}")
        logger.info(f"  GB scanned:     {gb:.2f}")
        logger.info(f"  Free tier:      1 TB/month (1,000 GB)")
        if gb <= 1000:
            logger.info(f"  Cost:           FREE ({gb:.0f}/{1000} GB of free tier)")
        else:
            logger.info(f"  Cost:           ${cost_if_over:.2f} (exceeds free tier)")
        logger.info("=" * 60)

    def _flush_to_disk(
        self, patents_by_company: dict[str, list[PatentDetail]], cfg=None
    ) -> int:
        """Store patents to disk. Returns count of patents stored."""
        if cfg is None:
            cfg = self._config

        all_records: list[dict] = []
        total = 0

        for company, patents in patents_by_company.items():
            if not patents:
                continue

            company_slug = _safe_slug(company)

            # Standard Parquet/JSONL records
            for patent in patents:
                snippet = (
                    (patent.abstract[:300] + "...")
                    if len(patent.abstract) > 300
                    else patent.abstract
                )
                all_records.append({
                    "company": company,
                    "title": patent.title,
                    "link": patent.url,
                    "snippet": snippet,
                    "date": patent.date_granted or patent.date_filed,
                    "source": "bigquery.patents-public-data",
                    "full_text": patent.full_text or patent.abstract,
                    "source_file": f"{company_slug}_patent_{patent.patent_id}.parquet",
                })

            # Per-company patent details JSONL
            details_file = self._config.output_dir / f"{company_slug}_patents.jsonl"
            mode = "a" if details_file.exists() else "w"
            with open(details_file, mode, encoding="utf-8") as f:
                for patent in patents:
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

            total += len(patents)
            logger.info(f"  {company}: {len(patents)} patents → {details_file.name}")

        # Append to shared Parquet/JSONL
        if all_records:
            self._parquet.append(all_records)
            if self._jsonl:
                self._jsonl.append(all_records)

        return total

    @staticmethod
    def _matches_classification(
        patent: PatentDetail,
        cpc_prefixes: list[str],
        ipc_prefixes: list[str],
    ) -> bool:
        """Check if a patent matches any classification prefixes."""
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
        return False

    def _handle_sigint(self, sig, frame):
        """Handle Ctrl+C gracefully."""
        if self._shutdown_requested:
            raise KeyboardInterrupt
        logger.warning(
            "Shutdown requested — finishing current page and saving checkpoint..."
        )
        self._shutdown_requested = True
        self._checkpoint.save()
