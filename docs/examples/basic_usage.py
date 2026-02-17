"""Basic usage: run a financial news scrape with default settings."""

import asyncio
from pathlib import Path

from financial_scraper.config import ScraperConfig
from financial_scraper.pipeline import ScraperPipeline


def main():
    config = ScraperConfig(
        queries_file=Path("queries.txt"),
        search_type="news",
        max_results_per_query=10,
    )

    pipeline = ScraperPipeline(config)
    asyncio.run(pipeline.run())
    print("Done! Check output directory for results.")


if __name__ == "__main__":
    main()
