"""BigQuery-based patent pipeline.

Reads company names from a CSV, queries the BigQuery public patents
dataset in batches, and stores results in the same Parquet + JSONL
format as the Google Patents scraping pipeline.

Requires ``pip install financial-scraper[bigquery]`` and
``gcloud auth application-default login`` for authentication.
"""

import json
import logging
import signal
from dataclasses import dataclass
from pathlib import Path

from .config import PatentConfig
from .google_patents import PatentDetail
from .normalize import are_same_assignee
from ..checkpoint import Checkpoint
from ..store.output import ParquetWriter, JSONLWriter

logger = logging.getLogger(__name__)


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
                f"Resumed: {len(self._checkpoint.completed_queries)} batches done"
            )

        # 4. Load companies from CSV
        from .bigquery_fetcher import (
            load_companies_from_csv,
            build_query,
            bq_row_to_patent_detail,
            match_company,
        )

        rows = load_companies_from_csv(cfg.csv_path, column=cfg.company_column)
        company_names = [
            r[cfg.company_column] for r in rows
            if r.get(cfg.company_column, "").strip()
        ]

        if not company_names:
            logger.error("No company names found in CSV")
            return

        # 5. Split into batches
        batches: list[list[str]] = []
        for i in range(0, len(company_names), cfg.batch_size):
            batches.append(company_names[i : i + cfg.batch_size])

        logger.info("=" * 60)
        logger.info(f"BigQuery Patent Pipeline")
        logger.info(f"  Companies: {len(company_names)}")
        logger.info(f"  Batches:   {len(batches)} (size={cfg.batch_size})")
        logger.info(f"  Country:   {cfg.country_filter}")
        logger.info(f"  Granted:   {cfg.granted_only}")
        logger.info("=" * 60)

        # 6. Dry run: estimate cost per batch
        if cfg.dry_run:
            self._dry_run(client, bigquery, batches, cfg)
            return

        # 7. Process batches
        total_patents = 0
        total_stored = 0

        for batch_idx, batch in enumerate(batches):
            if self._shutdown_requested:
                logger.warning("Shutdown requested — stopping")
                break

            batch_key = f"batch_{batch_idx}_{batch[0]}"

            # Skip completed batches
            if self._checkpoint.is_query_done(batch_key):
                logger.info(
                    f"Batch {batch_idx + 1}/{len(batches)}: "
                    f"already done (checkpoint), skipping"
                )
                continue

            logger.info("")
            logger.info(
                f"Batch {batch_idx + 1}/{len(batches)}: "
                f"{len(batch)} companies ({batch[0]} ... {batch[-1]})"
            )

            # Build and execute query
            sql = build_query(
                batch,
                granted_only=cfg.granted_only,
                include_description=cfg.include_description,
                country=cfg.country_filter,
            )

            try:
                query_job = client.query(sql)
                results = list(query_job.result())
            except Exception as e:
                logger.error(f"  BigQuery error: {e}")
                continue

            logger.info(f"  Raw results: {len(results)} patents")
            total_patents += len(results)

            if not results:
                self._checkpoint.mark_query_done(batch_key)
                continue

            # Map rows to PatentDetail and post-filter with are_same_assignee
            patents_by_company: dict[str, list[PatentDetail]] = {}

            for row in results:
                detail = bq_row_to_patent_detail(
                    row, "", include_description=cfg.include_description
                )

                # Match to a company in the batch
                matched = match_company(detail.assignee, batch)
                if not matched:
                    continue

                detail = PatentDetail(
                    patent_id=detail.patent_id,
                    url=detail.url,
                    title=detail.title,
                    abstract=detail.abstract,
                    patent_number=detail.patent_number,
                    application_number=detail.application_number,
                    date_filed=detail.date_filed,
                    date_granted=detail.date_granted,
                    assignee=detail.assignee,
                    inventors=detail.inventors,
                    classifications_cpc=detail.classifications_cpc,
                    classifications_ipc=detail.classifications_ipc,
                    citations_backward=detail.citations_backward,
                    citations_forward=detail.citations_forward,
                    non_patent_citations=detail.non_patent_citations,
                    pdf_url=detail.pdf_url,
                    full_text=detail.full_text,
                    expiration_date=detail.expiration_date,
                )

                patents_by_company.setdefault(matched, []).append(detail)

            # Apply classification filters
            if cfg.cpc_filter or cfg.ipc_filter:
                cpc_prefixes = list(cfg.cpc_filter)
                ipc_prefixes = list(cfg.ipc_filter)
                for company in patents_by_company:
                    patents_by_company[company] = [
                        p for p in patents_by_company[company]
                        if self._matches_classification(p, cpc_prefixes, ipc_prefixes)
                    ]

            # Apply limit per company
            if cfg.limit > 0:
                for company in patents_by_company:
                    patents = patents_by_company[company]
                    patents.sort(
                        key=lambda p: p.date_granted or p.date_filed or "",
                        reverse=True,
                    )
                    patents_by_company[company] = patents[: cfg.limit]

            # Store results
            batch_stored = self._store_batch(patents_by_company)
            total_stored += batch_stored

            # Mark batch done
            self._checkpoint.mark_query_done(batch_key)

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("BIGQUERY PATENT PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  Companies queried:  {len(company_names)}")
        logger.info(f"  Raw patents found:  {total_patents}")
        logger.info(f"  Patents stored:     {total_stored}")
        logger.info(f"  Output:             {cfg.output_dir}")
        logger.info("=" * 60)

    def _dry_run(self, client, bigquery_module, batches, cfg):
        """Run all queries with ``dry_run=True`` to estimate bytes scanned."""
        total_bytes = 0

        for batch_idx, batch in enumerate(batches):
            from .bigquery_fetcher import build_query

            sql = build_query(
                batch,
                granted_only=cfg.granted_only,
                include_description=cfg.include_description,
                country=cfg.country_filter,
            )

            job_config = bigquery_module.QueryJobConfig(dry_run=True, use_query_cache=False)
            try:
                query_job = client.query(sql, job_config=job_config)
                batch_bytes = query_job.total_bytes_processed or 0
                total_bytes += batch_bytes
                logger.info(
                    f"  Batch {batch_idx + 1}/{len(batches)}: "
                    f"{batch_bytes / 1e9:.2f} GB"
                )
            except Exception as e:
                logger.error(f"  Batch {batch_idx + 1} dry run error: {e}")

        gb = total_bytes / 1e9
        tb = total_bytes / 1e12
        cost = tb * 6.25  # $6.25/TB after free tier

        logger.info("")
        logger.info("=" * 60)
        logger.info("DRY RUN COST ESTIMATE")
        logger.info("=" * 60)
        logger.info(f"  Total bytes:   {total_bytes:,}")
        logger.info(f"  Total GB:      {gb:.2f}")
        logger.info(f"  Free tier:     1 TB/month")
        logger.info(f"  Est. cost:     ${cost:.2f} (after free tier)")
        logger.info("=" * 60)

    def _store_batch(
        self, patents_by_company: dict[str, list[PatentDetail]]
    ) -> int:
        """Store a batch of patents. Returns count of patents stored."""
        all_records: list[dict] = []
        total = 0

        for company, patents in patents_by_company.items():
            if not patents:
                continue

            company_slug = company.lower().replace(" ", "_")

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
            "Shutdown requested — finishing current batch and saving checkpoint..."
        )
        self._shutdown_requested = True
        self._checkpoint.save()
