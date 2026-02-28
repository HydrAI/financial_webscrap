"""Discover earnings call transcript URLs from Motley Fool sitemaps."""

import logging
import re
from dataclasses import dataclass
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

SITEMAP_URL = "https://www.fool.com/sitemap/{year}/{month:02d}"
TRANSCRIPT_PATH = "/earnings/call-transcripts/"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# URL slug pattern: {company}-{ticker}-q{N}-{year}-earnings-call-transcript
_SLUG_RE = re.compile(
    r"/earnings/call-transcripts/"
    r"(?P<pub_year>\d{4})/(?P<pub_month>\d{2})/(?P<pub_day>\d{2})/"
    r"(?P<company>.+?)-(?P<ticker>[a-z]+)-"
    r"(?:q(?P<quarter>\d)-)?"
    r"(?P<fiscal_year>\d{4})-earnings"
)


@dataclass(frozen=True)
class TranscriptInfo:
    """Metadata for a discovered transcript URL."""
    url: str
    ticker: str
    quarter: str  # "Q1", "Q2", etc.
    year: int  # fiscal year from URL
    pub_date: str  # YYYY-MM-DD publication date


def _parse_transcript_url(url: str) -> TranscriptInfo | None:
    """Extract metadata from a Motley Fool transcript URL."""
    m = _SLUG_RE.search(url.lower())
    if not m:
        return None
    quarter_num = m.group("quarter")
    if not quarter_num:
        return None
    return TranscriptInfo(
        url=url,
        ticker=m.group("ticker").upper(),
        quarter=f"Q{quarter_num}",
        year=int(m.group("fiscal_year")),
        pub_date=f"{m.group('pub_year')}-{m.group('pub_month')}-{m.group('pub_day')}",
    )


def _fetch_sitemap_urls(year: int, month: int) -> list[str]:
    """Fetch all URLs from a Motley Fool monthly sitemap."""
    url = SITEMAP_URL.format(year=year, month=month)
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.debug(f"Sitemap {url} returned {resp.status_code}")
            return []
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch sitemap {url}: {e}")
        return []

    # Parse XML sitemap — extract <loc> tags
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(resp.text, "xml")
    return [loc.text for loc in soup.find_all("loc")]


def discover_transcripts(
    ticker: str,
    year: int | None = None,
    quarters: tuple[str, ...] = (),
) -> list[TranscriptInfo]:
    """Discover transcript URLs for a ticker by scanning Motley Fool sitemaps.

    Scans monthly sitemaps for the target year (and the following year, since
    transcripts for Q4 of year N are often published in Jan/Feb of year N+1).

    Args:
        ticker: Stock ticker symbol (e.g. "AAPL")
        year: Fiscal year to search for (default: current year)
        quarters: Filter to specific quarters (e.g. ("Q1", "Q4")). Empty = all.

    Returns:
        List of TranscriptInfo with matching URLs, deduplicated.
    """
    if year is None:
        year = datetime.now().year

    ticker_upper = ticker.upper()
    ticker_lower = ticker.lower()

    # Scan sitemaps for the prior year, target year, and next year.
    # Companies with fiscal years offset from the calendar year (e.g. NVIDIA,
    # whose FY2025 Q1-Q3 were published in 2024 sitemaps) require looking back,
    # and Q4 transcripts are often published in Jan/Feb of year+1.
    months_to_scan = []
    for y in [year - 1, year, year + 1]:
        for m in range(1, 13):
            # Don't scan future months
            now = datetime.now()
            if y > now.year or (y == now.year and m > now.month):
                break
            months_to_scan.append((y, m))

    logger.info(
        f"Discovering transcripts for {ticker_upper} "
        f"(year={year}, quarters={quarters or 'all'}, "
        f"scanning {len(months_to_scan)} monthly sitemaps)"
    )

    seen_urls: set[str] = set()
    results: list[TranscriptInfo] = []

    for y, m in months_to_scan:
        all_urls = _fetch_sitemap_urls(y, m)
        # Fast filter: only transcript URLs containing the ticker
        transcript_urls = [
            u for u in all_urls
            if TRANSCRIPT_PATH in u and f"-{ticker_lower}-" in u.lower()
        ]

        for url in transcript_urls:
            if url in seen_urls:
                continue
            seen_urls.add(url)

            info = _parse_transcript_url(url)
            if info is None:
                continue
            if info.ticker != ticker_upper:
                continue
            if info.year != year:
                continue
            if quarters and info.quarter not in quarters:
                continue
            results.append(info)

    results.sort(key=lambda t: (t.quarter, t.pub_date))
    logger.info(f"Found {len(results)} transcript(s) for {ticker_upper} {year}")
    return results
