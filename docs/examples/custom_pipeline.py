"""Advanced usage: stealth mode with date filtering and Tor."""

import asyncio
from pathlib import Path

from financial_scraper.config import ScraperConfig
from financial_scraper.pipeline import ScraperPipeline


def main():
    config = ScraperConfig(
        queries_file=Path("queries.txt"),
        search_type="news",
        max_results_per_query=20,
        output_dir=Path("./runs"),
        jsonl=True,
        # Stealth settings
        stealth=True,
        # Date filtering
        date_from="2025-01-01",
        date_to="2025-12-31",
        # Tor (requires Tor Browser or daemon running)
        use_tor=True,
        tor_socks_port=9150,
        # Resume interrupted runs
        resume=True,
    )

    pipeline = ScraperPipeline(config)
    asyncio.run(pipeline.run())
    print("Done! Results saved to ./runs/")


if __name__ == "__main__":
    main()
