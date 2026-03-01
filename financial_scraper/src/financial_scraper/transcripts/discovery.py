"""Discover earnings call transcript URLs from Motley Fool sitemaps."""

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    """Fetch all URLs from a Motley Fool monthly sitemap.

    Retries up to 3 times on HTTP 429 with exponential backoff (5s, 10s).
    """
    import time as _time
    url = SITEMAP_URL.format(year=year, month=month)
    for attempt in range(3):
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch sitemap {url}: {e}")
            return []

        if resp.status_code == 429:
            wait = 5 * 2 ** attempt  # 5s, 10s
            logger.warning(
                f"Sitemap 429 for {year}-{month:02d} (attempt {attempt+1}/3), "
                f"backing off {wait}s"
            )
            _time.sleep(wait)
            continue

        if resp.status_code != 200:
            logger.warning(f"Sitemap {url} returned {resp.status_code}")
            return []

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "xml")
        return [loc.text for loc in soup.find_all("loc")]

    logger.warning(f"Sitemap {year}-{month:02d} failed after 3 attempts (persistent 429)")
    return []


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


def discover_transcripts_range(
    tickers: list[str],
    from_year: int,
    to_year: int,
    quarters: tuple[str, ...] = (),
    sitemap_workers: int = 4,
) -> dict[str, list[TranscriptInfo]]:
    """Discover transcripts for multiple tickers across a year range.

    Scans each monthly sitemap exactly once, matching all tickers in a single
    pass. This is O(months) instead of O(tickers × years × months), making it
    the correct function for bulk / all-history downloads.

    Args:
        tickers: Ticker symbols to search for.
        from_year: First fiscal year to include (inclusive).
        to_year: Last fiscal year to include (inclusive).
        quarters: Filter to specific quarters. Empty = all quarters.
        sitemap_workers: Parallel sitemap fetch threads (default: 10).

    Returns:
        Dict mapping ticker → sorted list of TranscriptInfo.
    """
    now = datetime.now()
    ticker_upper_set = {t.upper() for t in tickers}
    ticker_lower_set = {t.lower() for t in tickers}

    # Months to scan: one year before from_year through one year after to_year,
    # capped at the current calendar month (no future sitemaps exist).
    months_to_scan: list[tuple[int, int]] = []
    for y in range(from_year - 1, to_year + 2):
        for m in range(1, 13):
            if y > now.year or (y == now.year and m > now.month):
                break
            months_to_scan.append((y, m))

    logger.info(
        f"Bulk discovery: {len(ticker_upper_set)} ticker(s), "
        f"FY{from_year}–FY{to_year}, "
        f"scanning {len(months_to_scan)} monthly sitemaps "
        f"({sitemap_workers} workers)"
    )

    def _fetch_and_filter(year_month: tuple[int, int]) -> list[TranscriptInfo]:
        """Worker: fetch one sitemap, return matching TranscriptInfo items."""
        y, m = year_month
        all_urls = _fetch_sitemap_urls(y, m)
        matched: list[TranscriptInfo] = []
        for url in all_urls:
            if TRANSCRIPT_PATH not in url:
                continue
            url_lower = url.lower()
            # Fast pre-filter: skip URLs that don't contain any known ticker
            if not any(f"-{t}-" in url_lower for t in ticker_lower_set):
                continue
            info = _parse_transcript_url(url)
            if info is None:
                continue
            if info.ticker not in ticker_upper_set:
                continue
            if not (from_year <= info.year <= to_year):
                continue
            if quarters and info.quarter not in quarters:
                continue
            matched.append(info)
        return matched

    # Collect results in the main thread (no shared-state races)
    per_ticker: dict[str, list[TranscriptInfo]] = {t.upper(): [] for t in tickers}
    seen_urls: set[str] = set()
    completed = 0

    with ThreadPoolExecutor(max_workers=sitemap_workers) as executor:
        futures = {
            executor.submit(_fetch_and_filter, ym): ym
            for ym in months_to_scan
        }
        for future in as_completed(futures):
            completed += 1
            if completed % 24 == 0 or completed == len(months_to_scan):
                logger.info(
                    f"  Sitemaps scanned: {completed}/{len(months_to_scan)}"
                )
            try:
                for info in future.result():
                    if info.url not in seen_urls:
                        seen_urls.add(info.url)
                        per_ticker[info.ticker].append(info)
            except Exception as e:
                ym = futures[future]
                logger.warning(f"  Sitemap {ym} error: {e}")

    # Sort each ticker's list by (fiscal year, quarter) and log summary
    total = 0
    for ticker, infos in per_ticker.items():
        infos.sort(key=lambda t: (t.year, t.quarter))
        total += len(infos)
        if infos:
            logger.info(
                f"  {ticker}: {len(infos)} transcript(s) "
                f"(FY{infos[0].year}–FY{infos[-1].year})"
            )
        else:
            logger.info(f"  {ticker}: 0 transcripts found")

    logger.info(f"Bulk discovery complete: {total} total transcript(s)")
    return per_ticker
