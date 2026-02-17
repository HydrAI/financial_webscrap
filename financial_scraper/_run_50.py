"""Run scraper on 50 commodity queries with news search."""
import asyncio
import sys
import os
import time
import logging



if sys.platform.lower().startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
for n in ["duckduckgo_search", "urllib3", "asyncio", "charset_normalizer", "trafilatura"]:
    logging.getLogger(n).setLevel(logging.ERROR)

from datetime import datetime
from pathlib import Path
from financial_scraper.config import ScraperConfig
from financial_scraper.pipeline import ScraperPipeline

async def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(f"runs/{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"commodities_300_{ts}.parquet"
    jsonl_path = out_dir / f"commodities_300_{ts}.jsonl"

    config = ScraperConfig(
        queries_file=Path("config/commodities_300.txt"),
        max_results_per_query=10,
        search_type="news",
        search_delay_min=4.0,
        search_delay_max=7.0,
        output_dir=out_dir,
        output_path=out_path,
        jsonl_path=jsonl_path,
        exclude_file=Path("config/exclude_domains.txt"),
        min_word_count=80,
        max_concurrent_total=8,
        max_concurrent_per_domain=2,
        fetch_timeout=25,
    )

    start = time.time()
    pipeline = ScraperPipeline(config)
    await pipeline.run()
    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)

    logger = logging.getLogger(__name__)
    logger.info(f"Total time: {mins}m {secs}s")

    # Summary stats
    if out_path.exists():
        import pyarrow.parquet as pq
        import pandas as pd
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

        t = pq.read_table(out_path)
        df = t.to_pandas()
        print(f"\n{'='*60}")
        print(f"FINAL RESULTS: {out_path}")
        print(f"{'='*60}")
        print(f"Total rows:        {len(df)}")
        print(f"Unique queries:    {df['company'].nunique()}")
        print(f"Unique domains:    {df['source'].nunique()}")
        print(f"Total words:       {df['full_text'].apply(lambda x: len(str(x).split())).sum():,}")
        print(f"Avg words/page:    {df['full_text'].apply(lambda x: len(str(x).split())).mean():.0f}")
        print(f"Date range:        {df['date'].min()} to {df['date'].max()}")
        print(f"\nTop 15 domains:")
        for domain, count in df['source'].value_counts().head(15).items():
            print(f"  {domain:40s} {count:3d}")
        print(f"\nQueries with 0 results:")
        all_queries = set()
        with open("config/commodities_300.txt") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    all_queries.add(line)
        found_queries = set(df['company'].unique())
        missing = all_queries - found_queries
        if missing:
            for q in sorted(missing):
                print(f"  - {q}")
        else:
            print("  None! All queries produced results.")
        print(f"\nSource file tags (sample):")
        for sf in df['source_file'].unique()[:10]:
                    print(f"  {sf}")

# Top level - outside the function
await main()