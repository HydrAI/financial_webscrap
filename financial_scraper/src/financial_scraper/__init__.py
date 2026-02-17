"""Financial Scraper - Ethical async web scraper for financial research."""

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "ScraperConfig",
    "ScraperPipeline",
]

from financial_scraper.config import ScraperConfig
from financial_scraper.pipeline import ScraperPipeline
