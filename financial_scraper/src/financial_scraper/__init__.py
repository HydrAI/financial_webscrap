"""Financial Scraper - Ethical async web scraper for financial research."""

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "ScraperConfig",
    "ScraperPipeline",
]

from financial_scraper.config import ScraperConfig


def __getattr__(name):
    """Lazy import for ScraperPipeline to avoid eagerly loading duckduckgo
    (which sets WindowsSelectorEventLoopPolicy, breaking Playwright)."""
    if name == "ScraperPipeline":
        from financial_scraper.pipeline import ScraperPipeline
        return ScraperPipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
