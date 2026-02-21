"""CLI entry point."""

import argparse
import asyncio
import logging
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
    "duckduckgo_search", "urllib3", "asyncio",
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

    out_path = out_dir / f"{prefix}_{ts}.parquet"
    jsonl_path = out_dir / f"{prefix}_{ts}.jsonl" if args.jsonl else None
    markdown_path = out_dir / f"{prefix}_{ts}.md" if args.markdown else None
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
        search_delay_min=args.search_delay_min if hasattr(args, "search_delay_min") else 3.0,
        search_delay_max=args.search_delay_max if hasattr(args, "search_delay_max") else 6.0,
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
    p.add_argument("--exclude-file", default=None,
                   help="Domain exclusion list (default: built-in exclude_domains.txt)")
    p.add_argument("--no-exclude", action="store_true",
                   help="Disable domain exclusion filtering entirely")
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
    p.add_argument("--exclude-file", default=None,
                   help="Domain exclusion list (default: built-in exclude_domains.txt)")
    p.add_argument("--no-exclude", action="store_true",
                   help="Disable domain exclusion filtering entirely")
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

    # For backward compatibility: if no subcommand, treat all args as search
    # We detect this by checking if any known search-only args are present
    argv = sys.argv[1:]
    if argv and argv[0] not in ("search", "crawl", "-h", "--help"):
        # No subcommand given — prepend "search" for backward compat
        argv = ["search"] + argv

    args = p.parse_args(argv)

    if not args.command:
        p.print_help()
        sys.exit(1)

    start = time.time()

    if args.command == "crawl":
        _run_crawl(args)
    else:
        _run_search(args)

    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)
    logging.getLogger(__name__).info(f"Total time: {mins}m {secs}s")


if __name__ == "__main__":
    main()
