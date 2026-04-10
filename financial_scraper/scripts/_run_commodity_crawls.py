#!/usr/bin/env python3
"""Run all commodity sector crawls sequentially with simple-fetch mode.

Usage:
    PYTHONUTF8=1 python financial_scraper/scripts/_run_commodity_crawls.py
"""

import subprocess
import sys
import time
from pathlib import Path

SECTORS = ["trading", "exchanges", "shipping", "ag", "mining", "oilgas"]
CONFIG_DIR = Path("financial_scraper/config")
OUTPUT_BASE = Path("commodities_crawl")


def run_sector(sector: str):
    seeds_file = CONFIG_DIR / f"commodities_seeds_{sector}.txt"
    if not seeds_file.exists():
        print(f"[{sector}] Seed file not found: {seeds_file}")
        return False

    n_urls = sum(1 for line in open(seeds_file, encoding="utf-8") if line.strip())
    output_dir = OUTPUT_BASE / sector

    print(f"\n{'='*60}")
    print(f"[{sector}] Starting crawl: {n_urls} URLs -> {output_dir}")
    print(f"{'='*60}")

    cmd = [
        sys.executable, "-m", "financial_scraper", "crawl",
        "--urls-file", str(seeds_file),
        "--simple-fetch",
        "--stealth",
        "--save-raw",
        "--min-words", "50",
        "--output-dir", str(output_dir),
        "--no-exclude",
        "--resume",
    ]

    start = time.time()
    result = subprocess.run(cmd, env={**__import__("os").environ, "PYTHONUTF8": "1"})
    elapsed = time.time() - start

    status = "OK" if result.returncode == 0 else f"FAILED (rc={result.returncode})"
    print(f"\n[{sector}] {status} in {elapsed/60:.1f} min")
    return result.returncode == 0


def main():
    print("Commodity Crawl Runner")
    print(f"Sectors: {SECTORS}")

    results = {}
    for sector in SECTORS:
        ok = run_sector(sector)
        results[sector] = ok

    print("\n" + "=" * 60)
    print("ALL SECTORS COMPLETE")
    print("=" * 60)
    for sector, ok in results.items():
        print(f"  {sector}: {'OK' if ok else 'FAILED'}")


if __name__ == "__main__":
    main()
