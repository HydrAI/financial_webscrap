#!/usr/bin/env python3
"""Super-deep BFS crawl of the commodity public-data-source catalog.

Reads config/commodity_data_sources.csv (produced by _parse_data_sources.py),
takes every source with crawl_class == "deep", groups them by catalog category,
and runs the ``crawl`` subcommand in BestFirst deep-crawl mode (NOT simple-fetch)
for each category as an isolated, resumable subprocess.

"Super deep" = high BFS depth + a high per-seed page cap, leaning on the
commodity-tuned KeywordRelevanceScorer in crawl/strategy.py to follow the most
relevant links. Each category gets its own checkpoint + output folder so a stall
on one site never blocks the rest, and ``--resume`` picks up exactly where it
stopped.

Usage (defaults are "super deep"):
    C:\\T\\python.exe financial_scraper/scripts/_run_data_source_crawl.py

    # Smaller smoke test:
    ... _run_data_source_crawl.py --max-depth 2 --max-pages 40 --only energy

Scale warning: 115 seeds x depth 4 x 250 pages is potentially >100k page fetches
and can run for many hours. Start with --only on a single category to validate.
"""

import argparse
import csv
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # financial_webscrap/
CONFIG = ROOT / "financial_scraper" / "config"
CSV_PATH = CONFIG / "commodity_data_sources.csv"
OUTPUT_BASE = ROOT / "data_sources_crawl"
SEED_DIR = OUTPUT_BASE / "seeds"
# Dedicated, consolidated store for raw downloaded documents (shared across all
# categories; filenames are SHA-prefixed so there are no cross-site collisions).
RAW_PDF_DIR = OUTPUT_BASE / "raw" / "pdfs"
RAW_HTML_DIR = OUTPUT_BASE / "raw" / "html"


def slug(text: str) -> str:
    s = re.sub(r"[^\w]+", "_", text.lower()).strip("_")
    return s or "misc"


def load_deep_by_category() -> dict[str, list[str]]:
    """Return {category_slug: [urls]} for crawl_class == 'deep'."""
    groups: dict[str, list[str]] = defaultdict(list)
    seen: set[str] = set()
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["crawl_class"] != "deep":
                continue
            cat = slug(row["category"])
            for url in row["all_urls"].split(" ; "):
                url = url.strip()
                if url and url not in seen:
                    seen.add(url)
                    groups[cat].append(url)
    return groups


def run_category(cat: str, urls: list[str], args) -> bool:
    SEED_DIR.mkdir(parents=True, exist_ok=True)
    seed_file = SEED_DIR / f"{cat}.txt"
    seed_file.write_text("\n".join(urls) + "\n", encoding="utf-8")

    output_dir = OUTPUT_BASE / cat
    checkpoint = OUTPUT_BASE / f".checkpoint_{cat}.json"

    print(f"\n{'='*70}")
    print(f"[{cat}] {len(urls)} seed URLs -> {output_dir}")
    print(f"[{cat}] depth={args.max_depth} max_pages={args.max_pages} "
          f"semaphore={args.semaphore}")
    print(f"[{cat}] raw PDFs -> {RAW_PDF_DIR}")
    print(f"[{cat}] raw HTML -> {RAW_HTML_DIR}")
    print(f"{'='*70}")

    cmd = [
        sys.executable, "-m", "financial_scraper", "crawl",
        "--urls-file", str(seed_file),
        "--max-depth", str(args.max_depth),
        "--max-pages", str(args.max_pages),
        "--semaphore-count", str(args.semaphore),
        "--min-words", str(args.min_words),
        "--pdf-dir", str(RAW_PDF_DIR),
        "--html-dir", str(RAW_HTML_DIR),
        "--all-formats",
        "--no-exclude",
        "--output-dir", str(output_dir),
        "--checkpoint", str(checkpoint),
        "--resume",
    ]
    if args.stealth:
        cmd.append("--stealth")
    if args.date_from:
        cmd += ["--date-from", args.date_from]

    start = time.time()
    result = subprocess.run(cmd, env={**os.environ, "PYTHONUTF8": "1"})
    elapsed = (time.time() - start) / 60
    status = "OK" if result.returncode == 0 else f"FAILED (rc={result.returncode})"
    print(f"\n[{cat}] {status} in {elapsed:.1f} min")
    return result.returncode == 0


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--max-depth", type=int, default=4, help="BFS depth (default 4)")
    ap.add_argument("--max-pages", type=int, default=250,
                    help="Max pages per seed URL (default 250)")
    ap.add_argument("--semaphore", type=int, default=3,
                    help="crawl4ai concurrency per run (default 3)")
    ap.add_argument("--min-words", type=int, default=150)
    ap.add_argument("--stealth", action="store_true",
                    help="Stealth mode (forces semaphore=1, slower but politer)")
    ap.add_argument("--date-from", default=None, help="Keep only pages dated >= YYYY-MM-DD")
    ap.add_argument("--only", default="",
                    help="Comma-separated category slugs to run (default: all)")
    ap.add_argument("--list", action="store_true",
                    help="List categories + seed counts and exit")
    args = ap.parse_args()

    if not CSV_PATH.exists():
        print(f"Missing {CSV_PATH}. Run _parse_data_sources.py first.")
        sys.exit(1)

    groups = load_deep_by_category()
    if args.list:
        print("Deep-crawl categories:")
        total = 0
        for cat in sorted(groups):
            print(f"  {cat:30s} {len(groups[cat]):3d} seeds")
            total += len(groups[cat])
        print(f"  {'TOTAL':30s} {total:3d} seeds across {len(groups)} categories")
        return

    only = {slug(c) for c in args.only.split(",") if c.strip()} if args.only else None

    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
    results: dict[str, bool] = {}
    for cat in sorted(groups):
        if only and cat not in only:
            continue
        results[cat] = run_category(cat, groups[cat], args)

    print("\n" + "=" * 70)
    print("SUPER-DEEP DATA-SOURCE CRAWL COMPLETE")
    print("=" * 70)
    for cat, ok in results.items():
        print(f"  {cat:30s} {'OK' if ok else 'FAILED'}")


if __name__ == "__main__":
    main()
