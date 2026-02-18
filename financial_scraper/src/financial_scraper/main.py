"""CLI entry point."""

import argparse
import asyncio
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from .config import ScraperConfig
from .pipeline import ScraperPipeline

# Windows asyncio compatibility
if sys.platform.lower().startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

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


def _resolve_output_paths(args) -> tuple[Path, Path, Path | None, Path | None]:
    """Build datetime-stamped output directory and file paths.

    If --output is an explicit .parquet file, use that directly.
    Otherwise, create a timestamped folder under --output-dir:
        <output_dir>/YYYYMMDD_HHMMSS/scrape_YYYYMMDD_HHMMSS.parquet
    """
    if args.output and args.output.endswith(".parquet"):
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

    out_path = out_dir / f"scrape_{ts}.parquet"
    jsonl_path = out_dir / f"scrape_{ts}.jsonl" if args.jsonl else None
    markdown_path = out_dir / f"scrape_{ts}.md" if args.markdown else None
    return out_dir, out_path, jsonl_path, markdown_path


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
        exclude_file=Path(args.exclude_file) if args.exclude_file else None,
        checkpoint_file=Path(args.checkpoint),
        resume=args.resume,
    )


def main():
    p = argparse.ArgumentParser(
        description="Financial Web Scraper - DuckDuckGo + trafilatura",
    )
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
    p.add_argument("--exclude-file", default=None)
    p.add_argument("--checkpoint", default=".scraper_checkpoint.json")
    p.add_argument("--resume", action="store_true")

    args = p.parse_args()
    config = build_config(args)

    start = time.time()
    pipeline = ScraperPipeline(config)
    asyncio.run(pipeline.run())

    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)
    logging.getLogger(__name__).info(f"Total time: {mins}m {secs}s")


if __name__ == "__main__":
    main()
