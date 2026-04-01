"""CLI entry point."""

import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from .config import ScraperConfig

# Default exclude list shipped with the package
_DEFAULT_EXCLUDE_FILE = (
    Path(__file__).resolve().parent.parent.parent / "config" / "exclude_domains.txt"
)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
    datefmt="%H:%M:%S",
)

# Suppress noisy loggers
for noisy in [
    "duckduckgo_search", "ddgs", "ddgs.ddgs", "primp", "urllib3", "asyncio",
    "charset_normalizer", "trafilatura",
]:
    logging.getLogger(noisy).setLevel(logging.ERROR)


def _resolve_output_paths(
    args, *, prefix: str = "scrape"
) -> tuple[Path, Path, Path | None, Path | None]:
    """Build datetime-stamped output directory and file paths.

    If --output is an explicit .parquet file, use that directly.
    Otherwise, create a timestamped folder under --output-dir:
        <output_dir>/YYYYMMDD_HHMMSS/<prefix>_YYYYMMDD_HHMMSS.parquet
    """
    if hasattr(args, "output") and args.output and args.output.endswith(".parquet"):
        # Explicit parquet path given
        out_path = Path(args.output)
        out_dir = out_path.parent
        out_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = Path(args.jsonl) if args.jsonl else None
        markdown_path = None
        return out_dir, out_path, jsonl_path, markdown_path

    # Datetime-stamped folder
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = Path(args.output_dir) if args.output_dir else Path(".")
    out_dir = base / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    use_jsonl = args.jsonl or getattr(args, "all_formats", False)
    use_markdown = args.markdown or getattr(args, "all_formats", False)
    out_path = out_dir / f"{prefix}_{ts}.parquet"
    jsonl_path = out_dir / f"{prefix}_{ts}.jsonl" if use_jsonl else None
    markdown_path = out_dir / f"{prefix}_{ts}.md" if use_markdown else None
    return out_dir, out_path, jsonl_path, markdown_path


def _resolve_exclude_file(args) -> Path | None:
    """Resolve the exclude file from CLI args.

    Returns None if --no-exclude is set, otherwise resolves the path
    (defaulting to the built-in exclude_domains.txt).
    """
    if getattr(args, "no_exclude", False):
        return None
    if args.exclude_file is None:
        # Use built-in default
        if _DEFAULT_EXCLUDE_FILE.exists():
            return _DEFAULT_EXCLUDE_FILE
        return None
    return Path(args.exclude_file)


def build_config(args) -> ScraperConfig:
    """Build ScraperConfig from CLI args."""
    out_dir, out_path, jsonl_path, markdown_path = _resolve_output_paths(args)

    return ScraperConfig(
        queries_file=Path(args.queries_file),
        max_results_per_query=args.max_results,
        search_delay_min=args.search_delay_min,
        search_delay_max=args.search_delay_max,
        ddg_region=args.region,
        ddg_timelimit=args.timelimit,
        ddg_backend=args.backend,
        search_type=args.search_type,
        proxy=args.proxy,
        use_tor=args.use_tor,
        tor_socks_port=args.tor_socks_port,
        tor_control_port=args.tor_control_port,
        tor_password=args.tor_password,
        tor_renew_every=args.tor_renew_every,
        max_concurrent_total=args.concurrent,
        max_concurrent_per_domain=args.per_domain,
        fetch_timeout=args.timeout,
        stealth=args.stealth,
        respect_robots=not args.no_robots,
        crawl=args.crawl,
        crawl_depth=args.crawl_depth,
        max_pages_per_domain=args.max_pages_per_domain,
        min_word_count=args.min_words,
        target_language=args.target_language,
        favor_precision=not args.no_favor_precision,
        date_from=args.date_from,
        date_to=args.date_to,
        output_dir=out_dir,
        output_path=out_path,
        jsonl_path=jsonl_path,
        markdown_path=markdown_path,
        exclude_file=_resolve_exclude_file(args),
        save_raw=args.save_raw,
        pdf_dir=out_dir / "pdfs" if args.save_raw else None,
        html_dir=out_dir / "html" if args.save_raw else None,
        checkpoint_file=Path(args.checkpoint),
        resume=args.resume,
        reset_queries=args.reset_queries,
    )


def build_crawl_config(args):
    """Build CrawlConfig from CLI args."""
    from .crawl.config import CrawlConfig

    out_dir, out_path, jsonl_path, markdown_path = _resolve_output_paths(
        args, prefix="crawl"
    )

    return CrawlConfig(
        urls_file=Path(args.urls_file),
        exclude_file=_resolve_exclude_file(args),
        max_depth=args.max_depth,
        max_pages=args.max_pages,
        semaphore_count=args.semaphore_count,
        min_word_count=args.min_words,
        target_language=args.target_language,
        include_tables=True,
        favor_precision=not args.no_favor_precision,
        date_from=args.date_from,
        date_to=args.date_to,
        output_dir=out_dir,
        output_path=out_path,
        jsonl_path=jsonl_path,
        markdown_path=markdown_path,
        checkpoint_file=Path(args.checkpoint),
        resume=args.resume,
        pdf_extractor=args.pdf_extractor,
        save_raw=args.save_raw,
        pdf_dir=out_dir / "pdfs" if args.save_raw else None,
        html_dir=out_dir / "html" if args.save_raw else None,
        check_robots_txt=not args.no_robots,
        stealth=args.stealth,
    )


def _add_search_args(p: argparse.ArgumentParser):
    """Add arguments for the search subcommand."""
    # Required
    p.add_argument("--queries-file", required=True, help="File with queries (one per line)")
    p.add_argument("--output", default=None, help="Explicit .parquet output path (overrides --output-dir)")
    p.add_argument("--output-dir", default=None, help="Base dir for timestamped output folders (default: cwd)")

    # Search
    p.add_argument("--max-results", type=int, default=20)
    p.add_argument("--search-type", choices=["text", "news"], default="text")
    p.add_argument("--timelimit", choices=["d", "w", "m", "y"], default=None)
    p.add_argument("--region", default="wt-wt")
    p.add_argument("--backend", default="auto")
    p.add_argument("--proxy", default=None)

    # Tor
    p.add_argument("--use-tor", action="store_true")
    p.add_argument("--tor-socks-port", type=int, default=9150)
    p.add_argument("--tor-control-port", type=int, default=9051)
    p.add_argument("--tor-password", default="")
    p.add_argument("--tor-renew-every", type=int, default=20)

    # Search delays
    p.add_argument("--search-delay-min", type=float, default=3.0,
                   help="Minimum delay between DDG searches in seconds (default: 3.0)")
    p.add_argument("--search-delay-max", type=float, default=6.0,
                   help="Maximum delay between DDG searches in seconds (default: 6.0)")

    # Fetch
    p.add_argument("--concurrent", type=int, default=10)
    p.add_argument("--per-domain", type=int, default=3)
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--stealth", action="store_true")
    p.add_argument("--no-robots", action="store_true")

    # Crawl
    p.add_argument("--crawl", action="store_true", help="Follow links from fetched pages (BFS)")
    p.add_argument("--crawl-depth", type=int, default=2, help="Max link-following depth (default: 2)")
    p.add_argument("--max-pages-per-domain", type=int, default=50, help="Cap pages fetched per domain")

    # Extract
    p.add_argument("--min-words", type=int, default=100)
    p.add_argument("--target-language", default=None)
    p.add_argument("--no-favor-precision", action="store_true")
    p.add_argument("--date-from", default=None, help="YYYY-MM-DD")
    p.add_argument("--date-to", default=None, help="YYYY-MM-DD")

    # Store
    p.add_argument("--jsonl", action="store_true", help="Also write JSONL output")
    p.add_argument("--markdown", action="store_true", help="Also write Markdown output")
    p.add_argument("--all-formats", action="store_true",
                   help="Write all output formats: Parquet + JSONL + Markdown (shorthand for --jsonl --markdown)")
    p.add_argument("--exclude-file", default=None,
                   help="Domain exclusion list (default: built-in exclude_domains.txt)")
    p.add_argument("--no-exclude", action="store_true",
                   help="Disable domain exclusion filtering entirely")
    p.add_argument("--save-raw", action="store_true",
                   help="Save raw documents (PDFs + HTML) to disk alongside text extraction")
    p.add_argument("--checkpoint", default=".scraper_checkpoint.json")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--reset", action="store_true", help="Delete checkpoint before running (fresh start)")
    p.add_argument("--reset-queries", action="store_true",
                   help="Clear completed queries from checkpoint but keep URL history")


def _add_crawl_args(p: argparse.ArgumentParser):
    """Add arguments for the crawl subcommand."""
    # Required
    p.add_argument("--urls-file", required=True, help="File with seed URLs (one per line)")
    p.add_argument("--output-dir", default=None, help="Base dir for timestamped output folders (default: cwd)")

    # Crawl
    p.add_argument("--max-depth", type=int, default=2, help="Max crawl depth (default: 2)")
    p.add_argument("--max-pages", type=int, default=50, help="Max pages per seed URL (default: 50)")
    p.add_argument("--semaphore-count", type=int, default=2, help="Crawl4ai concurrency (default: 2)")

    # Extract
    p.add_argument("--min-words", type=int, default=100)
    p.add_argument("--target-language", default=None)
    p.add_argument("--no-favor-precision", action="store_true")
    p.add_argument("--date-from", default=None, help="YYYY-MM-DD")
    p.add_argument("--date-to", default=None, help="YYYY-MM-DD")

    # Store
    p.add_argument("--jsonl", action="store_true", help="Also write JSONL output")
    p.add_argument("--markdown", action="store_true", help="Also write Markdown output")
    p.add_argument("--all-formats", action="store_true",
                   help="Write all output formats: Parquet + JSONL + Markdown (shorthand for --jsonl --markdown)")
    p.add_argument("--exclude-file", default=None,
                   help="Domain exclusion list (default: built-in exclude_domains.txt)")
    p.add_argument("--no-exclude", action="store_true",
                   help="Disable domain exclusion filtering entirely")
    p.add_argument("--save-raw", action="store_true",
                   help="Save raw documents (PDFs + HTML) to disk alongside text extraction")
    p.add_argument("--checkpoint", default=".crawl_checkpoint.json")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--reset", action="store_true", help="Delete checkpoint before running (fresh start)")

    # PDF
    p.add_argument("--pdf-extractor", choices=["auto", "docling", "pdfplumber"],
                   default="auto", help="PDF extraction backend (default: auto)")

    # Behavior
    p.add_argument("--no-robots", action="store_true")
    p.add_argument("--stealth", action="store_true")


def _run_search(args):
    """Run the search pipeline."""
    # Lazy import: pipeline imports duckduckgo which sets
    # WindowsSelectorEventLoopPolicy (needed by aiohttp/curl-cffi,
    # but incompatible with Playwright used by crawl subcommand).
    from .pipeline import ScraperPipeline

    # Handle --reset
    if args.reset:
        cp = Path(args.checkpoint)
        if cp.exists():
            cp.unlink()
            logging.getLogger(__name__).info(f"Checkpoint reset: deleted {cp}")

    config = build_config(args)
    pipeline = ScraperPipeline(config)
    asyncio.run(pipeline.run())


def _run_crawl(args):
    """Run the crawl pipeline."""
    from .crawl.pipeline import CrawlPipeline

    # Handle --reset
    if args.reset:
        cp = Path(args.checkpoint)
        if cp.exists():
            cp.unlink()
            logging.getLogger(__name__).info(f"Checkpoint reset: deleted {cp}")

    config = build_crawl_config(args)
    pipeline = CrawlPipeline(config)
    asyncio.run(pipeline.run())


def build_transcript_config(args):
    """Build TranscriptConfig from CLI args."""
    from .transcripts.config import TranscriptConfig

    explicit_output = getattr(args, "output", None)
    if explicit_output:
        # Stable path mode: always write to the same file (safe for --resume)
        out_path = Path(explicit_output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_dir = out_path.parent
        jsonl_path = out_path.with_suffix(".jsonl") if args.jsonl else None
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = Path(args.output_dir) if args.output_dir else Path(".")
        out_dir = base / ts
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"transcripts_{ts}.parquet"
        jsonl_path = out_dir / f"transcripts_{ts}.jsonl" if args.jsonl else None

    tickers = tuple(t.upper() for t in args.tickers) if args.tickers else ()
    quarters = tuple(args.quarters) if args.quarters else ()

    # Resolve from_year / to_year for range mode
    from_year = getattr(args, "from_year", None)
    to_year = getattr(args, "to_year", None)
    if from_year is not None and to_year is None:
        to_year = datetime.now().year

    return TranscriptConfig(
        tickers=tickers,
        tickers_file=Path(args.tickers_file) if args.tickers_file else None,
        year=args.year,
        quarters=quarters,
        from_year=from_year,
        to_year=to_year,
        concurrent=args.concurrent,
        output_dir=out_dir,
        output_path=out_path,
        jsonl_path=jsonl_path,
        checkpoint_file=Path(args.checkpoint),
        resume=args.resume,
        fmp_api_key=getattr(args, "fmp_api_key", ""),
        proxies_file=Path(args.proxies_file) if getattr(args, "proxies_file", None) else None,
        browser_fallback=getattr(args, "browser_fallback", False),
    )


def _add_transcript_args(p: argparse.ArgumentParser):
    """Add arguments for the transcripts subcommand."""
    # Input (at least one required)
    p.add_argument("--tickers", nargs="+", help="Ticker symbols (e.g. AAPL MSFT NVDA)")
    p.add_argument("--tickers-file", default=None, help="File with tickers (one per line)")

    # Filters — single year
    p.add_argument("--year", type=int, default=None, help="Fiscal year (default: current year)")
    p.add_argument("--quarters", nargs="+", choices=["Q1", "Q2", "Q3", "Q4"],
                   help="Filter to specific quarters")

    # Filters — year range (mutually exclusive with --year)
    p.add_argument("--from-year", type=int, default=None,
                   help="First fiscal year for bulk history download (e.g. 2018)")
    p.add_argument("--to-year", type=int, default=None,
                   help="Last fiscal year for bulk history download (default: current year)")

    # Fetch
    p.add_argument("--concurrent", type=int, default=5)

    # Store
    p.add_argument("--output", default=None,
                   help="Explicit .parquet output path — stable across --resume sessions")
    p.add_argument("--output-dir", default=None, help="Base dir for output (default: cwd)")
    p.add_argument("--jsonl", action="store_true", help="Also write JSONL output")
    p.add_argument("--checkpoint", default=".transcript_checkpoint.json")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--reset", action="store_true", help="Delete checkpoint before running")

    # Fallback source
    p.add_argument(
        "--fmp-api-key", default="",
        help="Financial Modeling Prep API key for fallback transcripts (or set FMP_API_KEY env var)",
    )

    # Anti-detection / proxy
    p.add_argument(
        "--proxies-file", default=None,
        help="File with proxy URLs (one per line: http://user:pass@host:port)",
    )
    p.add_argument(
        "--browser-fallback", action="store_true",
        help="Use Playwright browser as final fallback for blocked pages (requires playwright)",
    )


def _run_transcripts(args):
    """Run the transcripts pipeline."""
    from .transcripts.pipeline import TranscriptPipeline

    logger = logging.getLogger(__name__)

    if not args.tickers and not args.tickers_file:
        logger.error("Must provide --tickers or --tickers-file")
        sys.exit(1)

    from_year = getattr(args, "from_year", None)
    if from_year is not None and args.year is not None:
        logger.error("--from-year and --year are mutually exclusive")
        sys.exit(1)

    to_year = getattr(args, "to_year", None)
    if to_year is not None and from_year is None:
        logger.error("--to-year requires --from-year")
        sys.exit(1)

    # Handle --reset
    if args.reset:
        cp = Path(args.checkpoint)
        if cp.exists():
            cp.unlink()
            logging.getLogger(__name__).info(f"Checkpoint reset: deleted {cp}")

    config = build_transcript_config(args)
    pipeline = TranscriptPipeline(config)
    pipeline.run()


def _add_patent_args(p: argparse.ArgumentParser):
    """Add arguments for the patents subcommand."""
    # Data source
    p.add_argument("--source", choices=["google", "bigquery"], default="google",
                   help="Patent data source (default: google)")

    # BigQuery options
    p.add_argument("--bq-csv", default=None,
                   help="CSV file with company names (required for --source bigquery)")
    p.add_argument("--bq-company-column", default="name",
                   help="CSV column containing company names (default: name)")
    p.add_argument("--bq-country", default="US",
                   help="Country code filter for BigQuery (default: US)")
    p.add_argument("--bq-include-description", action="store_true",
                   help="Include full patent description text (large)")
    p.add_argument("--bq-batch-size", type=int, default=50,
                   help="Companies per BigQuery batch (default: 50)")
    p.add_argument("--bq-dry-run", action="store_true",
                   help="Estimate BigQuery cost without executing")
    p.add_argument("--bq-project", default=None,
                   help="GCP project ID for BigQuery billing")

    # Batch mode — targets file
    p.add_argument("--targets-file", default=None,
                   help="JSON config with multiple companies and/or themes "
                        "(see config/patent_targets.json for format)")

    # Single-target mode — inline args
    p.add_argument("--company", default=None,
                   help="Company name (for labeling output)")
    p.add_argument("--ids-file", default=None,
                   help="File with patent IDs (one per line)")
    p.add_argument("--ids", nargs="*",
                   help="Patent IDs to fetch (inline)")
    p.add_argument("--assignee", default=None,
                   help="Assignee name for patent discovery (e.g. 'Droneshield LLC')")
    p.add_argument("--search-queries", nargs="*",
                   help="Search keywords to discover patents by topic "
                        "(e.g. 'drone acoustic detection patent')")

    # Discovery
    p.add_argument("--discover-google", action="store_true", default=True,
                   help="Discover patent IDs via Google Patents (default: on)")
    p.add_argument("--no-discover-google", action="store_true",
                   help="Disable Google Patents discovery")
    p.add_argument("--discover-search", action="store_true",
                   help="Also discover via DuckDuckGo (requires --assignee)")
    p.add_argument("--discover-justia", action="store_true",
                   help="Also discover via Justia (requires --assignee)")
    p.add_argument("--max-discovery", type=int, default=50,
                   help="Max patent IDs to discover per source (default: 50)")

    # Classification filter
    p.add_argument("--cpc-filter", nargs="*",
                   help="Keep only patents matching CPC prefix(es) (e.g. G01S H04)")
    p.add_argument("--ipc-filter", nargs="*",
                   help="Keep only patents matching IPC prefix(es)")
    p.add_argument("--wipo-categories", nargs="*",
                   help="Keep only patents in WIPO technology category (resolved to CPC)")
    p.add_argument("--list-wipo-categories", action="store_true",
                   help="Print all WIPO technology categories and exit")

    # Result filtering
    p.add_argument("--granted-only", action="store_true",
                   help="Only include granted patents (exclude applications)")
    p.add_argument("--limit", type=int, default=0,
                   help="Return only the top N most recent patents (0 = unlimited)")

    # Fetch
    p.add_argument("--delay", type=float, default=4.0,
                   help="Base delay between requests in seconds (default: 4.0)")
    p.add_argument("--timeout", type=int, default=30,
                   help="HTTP timeout in seconds (default: 30)")

    # Store
    p.add_argument("--output-dir", default=None,
                   help="Base dir for output (default: cwd)")
    p.add_argument("--jsonl", action="store_true", help="Also write JSONL output")
    p.add_argument("--checkpoint", default=".patent_checkpoint.json")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--reset", action="store_true",
                   help="Delete checkpoint before running (fresh start)")


def _build_patent_output_paths(args, company_slug: str):
    """Build output paths for a single patent target."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = Path(args.output_dir) if args.output_dir else Path(".")
    out_dir = base / f"{company_slug}_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"patents_{ts}.parquet"
    jsonl_path = out_dir / f"patents_{ts}.jsonl" if args.jsonl else None
    return out_dir, out_path, jsonl_path


def build_patent_config(args):
    """Build a single PatentConfig from CLI args (single-target mode)."""
    from .patents.config import PatentConfig

    company = args.company or "patents"
    slug = company.lower().replace(" ", "_")
    out_dir, out_path, jsonl_path = _build_patent_output_paths(args, slug)

    discover_google = not getattr(args, "no_discover_google", False)

    return PatentConfig(
        company=company,
        ids_file=Path(args.ids_file) if args.ids_file else None,
        ids=tuple(args.ids) if args.ids else (),
        assignee=args.assignee or "",
        search_queries=tuple(args.search_queries) if args.search_queries else (),
        cpc_filter=tuple(args.cpc_filter) if args.cpc_filter else (),
        ipc_filter=tuple(args.ipc_filter) if args.ipc_filter else (),
        wipo_categories=tuple(args.wipo_categories) if args.wipo_categories else (),
        discover_via_google_patents=discover_google,
        discover_via_search=args.discover_search,
        discover_via_justia=args.discover_justia,
        max_discovery_results=args.max_discovery,
        granted_only=args.granted_only,
        limit=args.limit,
        delay=args.delay,
        timeout=args.timeout,
        output_dir=out_dir,
        output_path=out_path,
        jsonl_path=jsonl_path,
        checkpoint_file=Path(args.checkpoint),
        resume=args.resume,
    )


def _run_patents(args):
    """Run the patent pipeline."""
    logger = logging.getLogger(__name__)

    # Handle --list-wipo-categories (print and exit)
    if args.list_wipo_categories:
        from .patents.wipo import list_wipo_categories
        print("\nWIPO Technology Categories:")
        print("=" * 50)
        for cat in list_wipo_categories():
            print(f"  {cat}")
        print(f"\nTotal: {len(list_wipo_categories())} categories")
        return

    # Handle --reset
    if args.reset:
        cp = Path(args.checkpoint)
        if cp.exists():
            cp.unlink()
            logger.info(f"Checkpoint reset: deleted {cp}")

    # --- BigQuery source ---
    if args.source == "bigquery":
        if not args.bq_csv:
            logger.error("--bq-csv is required when using --source bigquery")
            sys.exit(1)

        from .patents.bigquery_pipeline import BigQueryPatentPipeline, BigQueryConfig

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = Path(args.output_dir) if args.output_dir else Path(".")
        out_dir = base / f"bq_patents_{ts}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"patents_{ts}.parquet"
        jsonl_path = out_dir / f"patents_{ts}.jsonl" if args.jsonl else None

        bq_config = BigQueryConfig(
            csv_path=Path(args.bq_csv),
            company_column=args.bq_company_column,
            country_filter=args.bq_country,
            include_description=args.bq_include_description,
            batch_size=args.bq_batch_size,
            dry_run=args.bq_dry_run,
            project_id=args.bq_project,
            granted_only=args.granted_only,
            cpc_filter=tuple(args.cpc_filter) if args.cpc_filter else (),
            ipc_filter=tuple(args.ipc_filter) if args.ipc_filter else (),
            limit=args.limit,
            output_dir=out_dir,
            output_path=out_path,
            jsonl_path=jsonl_path,
            checkpoint_file=Path(args.checkpoint),
            resume=args.resume,
        )

        pipeline = BigQueryPatentPipeline(bq_config)
        pipeline.run()
        return

    from .patents.pipeline import PatentPipeline
    from .patents.config import load_targets_file, PatentConfig

    # --- Batch mode: targets file ---
    if args.targets_file:
        targets = load_targets_file(Path(args.targets_file))
        if not targets:
            logger.error("No targets found in targets file")
            sys.exit(1)

        for i, target in enumerate(targets):
            logger.info(f"\n{'#' * 60}")
            logger.info(f"TARGET {i+1}/{len(targets)}: {target.company}")
            logger.info(f"{'#' * 60}")

            # Build output paths per target
            slug = target.company.lower().replace(" ", "_")
            out_dir, out_path, jsonl_path = _build_patent_output_paths(args, slug)

            # Merge target config with CLI overrides (delay, timeout, resume)
            # CLI --granted-only / --limit override target file values
            granted_only = args.granted_only or target.granted_only
            limit = args.limit if args.limit > 0 else target.limit

            config = PatentConfig(
                company=target.company,
                ids_file=target.ids_file,
                ids=target.ids,
                assignee=target.assignee,
                search_queries=target.search_queries,
                cpc_filter=target.cpc_filter,
                ipc_filter=target.ipc_filter,
                wipo_categories=target.wipo_categories,
                discover_via_google_patents=target.discover_via_google_patents,
                discover_via_search=target.discover_via_search,
                discover_via_justia=target.discover_via_justia,
                max_discovery_results=target.max_discovery_results,
                granted_only=granted_only,
                limit=limit,
                delay=args.delay,
                timeout=args.timeout,
                output_dir=out_dir,
                output_path=out_path,
                jsonl_path=jsonl_path,
                checkpoint_file=Path(f".patent_checkpoint_{slug}.json"),
                resume=args.resume,
            )

            pipeline = PatentPipeline(config)
            pipeline.run()

        return

    # --- Single-target mode: inline args ---
    has_source = (
        args.ids_file
        or args.ids
        or (args.assignee and not getattr(args, "no_discover_google", False))
        or args.discover_search
        or args.discover_justia
        or args.search_queries
    )
    if not has_source:
        logger.error(
            "No patent source specified. Use --targets-file for batch mode, or "
            "--ids-file, --ids, --search-queries, --discover-search, "
            "or --discover-justia for single-target mode."
        )
        sys.exit(1)

    if not args.company:
        logger.error("--company is required in single-target mode")
        sys.exit(1)

    config = build_patent_config(args)
    pipeline = PatentPipeline(config)
    pipeline.run()


def _add_supply_chain_args(p: argparse.ArgumentParser):
    """Add arguments for the supply-chain subcommand."""
    # CSV input
    p.add_argument("--csv", required=True, help="CSV file with company names")
    p.add_argument("--company-column", default="name",
                   help="CSV column containing company names (default: name)")
    p.add_argument("--ticker-column", default="ticker",
                   help="CSV column containing ticker symbols (default: ticker)")
    p.add_argument("--limit-companies", type=int, default=0,
                   help="Process only first N companies (0 = all)")
    p.add_argument("--skip-companies", type=int, default=0,
                   help="Skip first N companies")

    # Output
    p.add_argument("--output-dir", default=None,
                   help="Base dir for timestamped output folders (default: cwd)")
    p.add_argument("--output", default=None,
                   help="Explicit .parquet output path (overrides --output-dir)")

    # Search
    p.add_argument("--max-results", type=int, default=10)
    p.add_argument("--search-type", choices=["text", "news"], default="text")
    p.add_argument("--timelimit", choices=["d", "w", "m", "y"], default=None)
    p.add_argument("--region", default="wt-wt")
    p.add_argument("--backend", default="auto")
    p.add_argument("--proxy", default=None)

    # Tor
    p.add_argument("--use-tor", action="store_true")
    p.add_argument("--tor-socks-port", type=int, default=9150)
    p.add_argument("--tor-control-port", type=int, default=9051)
    p.add_argument("--tor-password", default="")
    p.add_argument("--tor-renew-every", type=int, default=20)

    # Search delays
    p.add_argument("--search-delay-min", type=float, default=3.0)
    p.add_argument("--search-delay-max", type=float, default=6.0)

    # Fetch
    p.add_argument("--concurrent", type=int, default=10)
    p.add_argument("--per-domain", type=int, default=3)
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--stealth", action="store_true")
    p.add_argument("--no-robots", action="store_true")

    # Crawl
    p.add_argument("--crawl", action="store_true", help="Follow links from fetched pages (BFS)")
    p.add_argument("--crawl-depth", type=int, default=2)
    p.add_argument("--max-pages-per-domain", type=int, default=50)

    # Extract
    p.add_argument("--min-words", type=int, default=100)
    p.add_argument("--target-language", default=None)
    p.add_argument("--no-favor-precision", action="store_true")
    p.add_argument("--date-from", default=None, help="YYYY-MM-DD")
    p.add_argument("--date-to", default=None, help="YYYY-MM-DD")

    # Store
    p.add_argument("--jsonl", action="store_true")
    p.add_argument("--markdown", action="store_true")
    p.add_argument("--all-formats", action="store_true")
    p.add_argument("--exclude-file", default=None)
    p.add_argument("--no-exclude", action="store_true")
    p.add_argument("--save-raw", action="store_true",
                   help="Save raw documents (PDFs + HTML) to disk alongside text extraction")
    p.add_argument("--checkpoint", default=".supply_chain_checkpoint.json")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--reset", action="store_true", help="Delete checkpoint before running")
    p.add_argument("--reset-queries", action="store_true",
                   help="Clear completed queries from checkpoint but keep URL history")


def _run_supply_chain(args):
    """Run the supply-chain pipeline."""
    from .pipeline import ScraperPipeline
    from .supply_chain import generate_supply_chain_queries, write_queries_file

    logger = logging.getLogger(__name__)

    # Handle --reset
    if args.reset:
        cp = Path(args.checkpoint)
        if cp.exists():
            cp.unlink()
            logger.info(f"Checkpoint reset: deleted {cp}")

    # Generate queries
    csv_path = Path(args.csv)
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)

    queries = generate_supply_chain_queries(
        csv_path=csv_path,
        company_col=args.company_column,
        ticker_col=args.ticker_column,
        limit=args.limit_companies,
        skip=args.skip_companies,
    )

    if not queries:
        logger.error("No queries generated from CSV")
        sys.exit(1)

    # Resolve output paths
    out_dir, out_path, jsonl_path, markdown_path = _resolve_output_paths(
        args, prefix="supply_chain"
    )

    # Write queries file into the output directory
    queries_file = write_queries_file(queries, out_dir)

    # Build config and run
    config = ScraperConfig(
        queries_file=queries_file,
        max_results_per_query=args.max_results,
        search_delay_min=args.search_delay_min,
        search_delay_max=args.search_delay_max,
        ddg_region=args.region,
        ddg_timelimit=args.timelimit,
        ddg_backend=args.backend,
        search_type=args.search_type,
        proxy=args.proxy,
        use_tor=args.use_tor,
        tor_socks_port=args.tor_socks_port,
        tor_control_port=args.tor_control_port,
        tor_password=args.tor_password,
        tor_renew_every=args.tor_renew_every,
        max_concurrent_total=args.concurrent,
        max_concurrent_per_domain=args.per_domain,
        fetch_timeout=args.timeout,
        stealth=args.stealth,
        respect_robots=not args.no_robots,
        crawl=args.crawl,
        crawl_depth=args.crawl_depth,
        max_pages_per_domain=args.max_pages_per_domain,
        min_word_count=args.min_words,
        target_language=args.target_language,
        favor_precision=not args.no_favor_precision,
        date_from=args.date_from,
        date_to=args.date_to,
        output_dir=out_dir,
        output_path=out_path,
        jsonl_path=jsonl_path,
        markdown_path=markdown_path,
        exclude_file=_resolve_exclude_file(args),
        save_raw=args.save_raw,
        pdf_dir=out_dir / "pdfs" if args.save_raw else None,
        html_dir=out_dir / "html" if args.save_raw else None,
        checkpoint_file=Path(args.checkpoint),
        resume=args.resume,
        reset_queries=args.reset_queries,
    )

    pipeline = ScraperPipeline(config)
    asyncio.run(pipeline.run())


def main():
    p = argparse.ArgumentParser(
        description="Financial Web Scraper - search or deep-crawl modes",
    )
    subparsers = p.add_subparsers(dest="command")

    # "search" subcommand (default when no subcommand given)
    search_parser = subparsers.add_parser(
        "search", help="Search DDG and extract content (default)"
    )
    _add_search_args(search_parser)

    # "crawl" subcommand
    crawl_parser = subparsers.add_parser(
        "crawl", help="Deep-crawl seed URLs using crawl4ai"
    )
    _add_crawl_args(crawl_parser)

    # "transcripts" subcommand
    transcript_parser = subparsers.add_parser(
        "transcripts", help="Download earnings call transcripts from Motley Fool"
    )
    _add_transcript_args(transcript_parser)

    # "patents" subcommand
    patent_parser = subparsers.add_parser(
        "patents", help="Discover and fetch patent data by assignee or topic"
    )
    _add_patent_args(patent_parser)

    # "supply-chain" subcommand
    sc_parser = subparsers.add_parser(
        "supply-chain", help="Generate supply-chain queries from CSV and run search pipeline"
    )
    _add_supply_chain_args(sc_parser)

    # "sec-filings" subcommand
    sec_parser = subparsers.add_parser(
        "sec-filings", help="Download 10-K/20-F filings from SEC EDGAR"
    )
    sec_parser.add_argument("--csv", required=True, help="CSV file with company tickers")
    sec_parser.add_argument("--company-column", default="name")
    sec_parser.add_argument("--ticker-column", default="ticker")
    sec_parser.add_argument("--limit-companies", type=int, default=0)
    sec_parser.add_argument("--skip-companies", type=int, default=0)
    sec_parser.add_argument("--max-filings", type=int, default=0,
                           help="Max filings per company (0 = all available)")
    sec_parser.add_argument("--output-dir", default="sec_filings_output")
    sec_parser.add_argument("--resume", action="store_true")

    # "uk-filings" subcommand
    uk_parser = subparsers.add_parser(
        "uk-filings", help="Download annual accounts from UK Companies House"
    )
    uk_parser.add_argument("--csv", required=True, help="CSV file with company names")
    uk_parser.add_argument("--ch-api-key", default=None,
                          help="Companies House API key (or set COMPANIES_HOUSE_API_KEY env var)")
    uk_parser.add_argument("--company-column", default="name")
    uk_parser.add_argument("--company-number-column", default="company_number")
    uk_parser.add_argument("--limit-companies", type=int, default=0)
    uk_parser.add_argument("--skip-companies", type=int, default=0)
    uk_parser.add_argument("--max-filings", type=int, default=0)
    uk_parser.add_argument("--output-dir", default="uk_filings_output")
    uk_parser.add_argument("--resume", action="store_true")

    # "edinet-filings" subcommand
    edinet_parser = subparsers.add_parser(
        "edinet-filings", help="Download annual securities reports from EDINET (Japan)"
    )
    edinet_parser.add_argument("--csv", required=True, help="CSV file with company names/tickers")
    edinet_parser.add_argument("--edinet-api-key", default=None,
                              help="EDINET API key (or set EDINET_API_KEY env var)")
    edinet_parser.add_argument("--company-column", default="name")
    edinet_parser.add_argument("--ticker-column", default="ticker")
    edinet_parser.add_argument("--scan-days", type=int, default=730,
                              help="Number of days to scan backwards (default: 730)")
    edinet_parser.add_argument("--limit-companies", type=int, default=0)
    edinet_parser.add_argument("--skip-companies", type=int, default=0)
    edinet_parser.add_argument("--max-filings", type=int, default=0)
    edinet_parser.add_argument("--output-dir", default="edinet_filings_output")
    edinet_parser.add_argument("--resume", action="store_true")

    # For backward compatibility: if no subcommand, treat all args as search
    argv = sys.argv[1:]
    if argv and argv[0] not in (
        "search", "crawl", "transcripts", "patents", "supply-chain",
        "sec-filings", "uk-filings", "edinet-filings", "-h", "--help",
    ):
        argv = ["search"] + argv

    args = p.parse_args(argv)

    if not args.command:
        p.print_help()
        sys.exit(1)

    start = time.time()

    if args.command == "crawl":
        _run_crawl(args)
    elif args.command == "transcripts":
        _run_transcripts(args)
    elif args.command == "patents":
        _run_patents(args)
    elif args.command == "supply-chain":
        _run_supply_chain(args)
    elif args.command == "sec-filings":
        from .sec_filings import download_sec_filings
        download_sec_filings(
            csv_path=Path(args.csv),
            output_dir=Path(args.output_dir),
            company_col=args.company_column,
            ticker_col=args.ticker_column,
            limit=args.limit_companies,
            skip=args.skip_companies,
            max_filings_per_company=args.max_filings,
            resume=args.resume,
        )
    elif args.command == "uk-filings":
        from .uk_filings import download_uk_filings
        api_key = args.ch_api_key or os.environ.get("COMPANIES_HOUSE_API_KEY", "")
        if not api_key:
            logger.error("Companies House API key required: --ch-api-key or COMPANIES_HOUSE_API_KEY env var")
            sys.exit(1)
        download_uk_filings(
            csv_path=Path(args.csv),
            output_dir=Path(args.output_dir),
            api_key=api_key,
            company_col=args.company_column,
            company_number_col=args.company_number_column,
            limit=args.limit_companies,
            skip=args.skip_companies,
            max_filings_per_company=args.max_filings,
            resume=args.resume,
        )
    elif args.command == "edinet-filings":
        from .edinet_filings import download_edinet_filings
        api_key = args.edinet_api_key or os.environ.get("EDINET_API_KEY", "")
        if not api_key:
            logger.error("EDINET API key required: --edinet-api-key or EDINET_API_KEY env var")
            sys.exit(1)
        download_edinet_filings(
            csv_path=Path(args.csv),
            output_dir=Path(args.output_dir),
            api_key=api_key,
            company_col=args.company_column,
            ticker_col=args.ticker_column,
            scan_days=args.scan_days,
            limit=args.limit_companies,
            skip=args.skip_companies,
            max_filings_per_company=args.max_filings,
            resume=args.resume,
        )
    else:
        _run_search(args)

    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)
    logging.getLogger(__name__).info(f"Total time: {mins}m {secs}s")


if __name__ == "__main__":
    main()
