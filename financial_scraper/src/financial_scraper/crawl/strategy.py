"""Crawl4ai strategy builder for the crawl subcommand.

Patterns and scoring tuned for general financial-news crawling rather
than investor-relations-specific deep dives.
"""

import datetime

from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.filters import (
    FilterChain,
    ContentTypeFilter,
    URLPatternFilter,
)
from crawl4ai.deep_crawling.scorers import (
    CompositeScorer,
    KeywordRelevanceScorer,
    PathDepthScorer,
    FreshnessScorer,
)

# URL patterns to *reject* — corporate chrome, auth walls, non-article pages.
# These are standard boilerplate paths found on virtually every news/corporate
# site; the glob syntax is dictated by crawl4ai's URLPatternFilter API.
SKIP_URL_PATTERNS = [
    # Auth / account
    "*/login*", "*/signin*", "*/register*", "*/account*",
    # Corporate boilerplate
    "*/contact*", "*/career*", "*/jobs*", "*/about-us*",
    # Legal
    "*privacy*", "*cookie*", "*/terms*", "*/legal*", "*disclaimer*",
    # Navigation dead-ends
    "*/search", "*/search/*", "*faq*",
    # Non-content
    "*/video/*", "*javascript:*", "*mailto:*",
]

# Positive-signal keywords used by the URL scorer to prioritise financial
# content during BFS expansion.
FINANCIAL_SIGNALS = [
    "earnings", "revenue", "quarterly", "annual", "report",
    "results", "financial", "sec", "filings", "filing",
    "news", "press", "article", "release",
]


def build_browser_config() -> BrowserConfig:
    """Headless browser settings for crawl4ai."""
    return BrowserConfig(
        headers={
            "Accept-Language": "en-US,en;q=0.8",
        },
        verbose=False,
    )


def build_crawl_strategy(
    max_depth: int = 2,
    max_pages: int = 50,
) -> BestFirstCrawlingStrategy:
    """Build a BFS deep-crawl strategy scored for financial content."""
    scorer = CompositeScorer(
        scorers=[
            KeywordRelevanceScorer(keywords=FINANCIAL_SIGNALS, weight=0.8),
            PathDepthScorer(optimal_depth=1, weight=0.3),
            FreshnessScorer(current_year=datetime.date.today().year, weight=0.15),
        ]
    )
    filters = [
        URLPatternFilter(patterns=SKIP_URL_PATTERNS, reverse=True),
        ContentTypeFilter(allowed_types=["text/html"]),
    ]
    return BestFirstCrawlingStrategy(
        filter_chain=FilterChain(filters),
        max_depth=max_depth,
        include_external=False,
        url_scorer=scorer,
        max_pages=max_pages,
    )


def build_crawler_config(
    strategy: BestFirstCrawlingStrategy,
    check_robots_txt: bool = True,
    semaphore_count: int = 2,
) -> CrawlerRunConfig:
    """Build the per-run config that wraps the deep-crawl strategy."""
    return CrawlerRunConfig(
        deep_crawl_strategy=strategy,
        exclude_all_images=True,
        exclude_external_images=True,
        exclude_social_media_links=True,
        semaphore_count=semaphore_count,
        check_robots_txt=check_robots_txt,
        # Polite delays: 4s base ± 2s jitter
        mean_delay=4.0,
        max_range=2.0,
    )
