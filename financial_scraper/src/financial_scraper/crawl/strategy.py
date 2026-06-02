"""Crawl4ai strategy builder for the crawl subcommand.

Patterns and scoring tuned for general financial-news crawling rather
than investor-relations-specific deep dives.
"""

import datetime
from urllib.parse import urlparse

from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.filters import (
    FilterChain,
    ContentTypeFilter,
    URLFilter,
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
    "*/contact*", "*/career*", "*/jobs*",
    # Legal
    "*privacy*", "*cookie*", "*/terms*", "*/legal*", "*disclaimer*",
    # Navigation dead-ends
    "*/search", "*/search/*", "*faq*",
    # Non-content
    "*/video/*", "*javascript:*", "*mailto:*",
    # More career variants
    "*/vacancies*", "*/applicant*", "*/talent*",
    # Asset galleries
    "*/media-library*", "*/image-gallery*",
    # Institutional / governance chrome — high-volume, near-zero data signal
    # (these flooded the validation crawl with org-chart and bio pages).
    "*/governance*", "*/leadership*", "*/secretary-general*",
    "*/our-people*", "*/committee*", "*/visit*", "*/history*",
    "*/awards*", "*/events*", "*/who-we-are*", "*/organization-chart*",
]

# Positive-signal keywords used by the URL scorer to prioritise financial
# content during BFS expansion.
FINANCIAL_SIGNALS = [
    # General financial
    "earnings", "revenue", "quarterly", "annual", "report",
    "results", "financial", "sec", "filings", "filing",
    "news", "press", "article", "release",
    # Commodity-specific
    "commodity", "crude", "lng", "refinery", "terminal",
    "shipping", "grain", "crop", "copper", "nickel",
    "aluminium", "iron ore", "lithium", "cobalt",
    "production", "reserves", "throughput", "sustainability",
    "supply chain", "logistics", "market outlook", "trading",
]


def build_browser_config() -> BrowserConfig:
    """Headless browser settings for crawl4ai."""
    return BrowserConfig(
        headers={
            "Accept-Language": "en-US,en;q=0.8",
        },
        verbose=False,
    )


def _normalize_host(netloc: str) -> str:
    """Lowercase host without port or leading ``www.``."""
    host = netloc.lower().split(":")[0]
    return host[4:] if host.startswith("www.") else host


class SeedHostFilter(URLFilter):
    """Restrict a crawl to the seed's exact host (www-normalized).

    Registrable-domain scoping is too loose for shared government domains: a
    seed on ``dmr.nd.gov`` would pull in unrelated siblings like ``arts.nd.gov``
    and ``ndhousing.nd.gov`` (all share the ``nd.gov`` registrable domain).
    Path scoping is too tight: on these sites the real content lives off the
    seed's path (``aer.ca`` reports link out to ``aer.ca/whats-new``). Exact
    host equality keeps all same-host content (any path) while excluding sibling
    subdomains — verified to drop the drift with no loss of on-target pages.
    """

    __slots__ = ("_host",)

    def __init__(self, seed_url: str):
        super().__init__()
        self._host = _normalize_host(urlparse(seed_url).netloc)

    def apply(self, url: str) -> bool:
        ok = _normalize_host(urlparse(url).netloc) == self._host
        self._update_stats(ok)
        return ok


def build_crawl_strategy(
    seed_url: str,
    max_depth: int = 2,
    max_pages: int = 50,
) -> BestFirstCrawlingStrategy:
    """Build a BFS deep-crawl strategy scored for financial content.

    The crawl is scoped to ``seed_url``'s exact host so it stays on the target
    site, and PDFs are allowed through the frontier so report documents
    (OPEC MOMR, IEA OMR, USGS MCS, …) reach the pipeline's PDF download path.
    """
    scorer = CompositeScorer(
        scorers=[
            KeywordRelevanceScorer(keywords=FINANCIAL_SIGNALS, weight=0.8),
            PathDepthScorer(optimal_depth=1, weight=0.3),
            FreshnessScorer(current_year=datetime.date.today().year, weight=0.15),
        ]
    )
    filters = [
        URLPatternFilter(patterns=SKIP_URL_PATTERNS, reverse=True),
        # Allow HTML pages AND PDFs — PDFs were previously filtered out of the
        # BFS frontier, so report documents were never fetched.
        ContentTypeFilter(allowed_types=["text/html", "application/pdf"]),
        # Stay on the seed's own host, defeating drift into sibling subdomains
        # that share a government registrable domain (nd.gov, dot.gov, …).
        SeedHostFilter(seed_url),
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
