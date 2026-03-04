"""Fill temporal gaps using Wayback Machine + Seeking Alpha transcript archives.

1. Query CDX API for all archived SA earnings-call-transcript URLs
2. Cache CDX discovery results to disk
3. Parse ticker + quarter + year from page content
4. Filter to US10002 tickers and missing (ticker, year, quarter) combos
5. Fetch and extract transcript text from archived pages
6. Append to the parquet file

Usage:
    python _fill_wayback_gaps.py [--dry-run] [--limit 500] [--delay 1.5]
    python _fill_wayback_gaps.py --discover-only   # just run CDX discovery
"""

import argparse
import json
import logging
import os
import re
import time
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from lxml import html

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PARQUET_PATH = Path("runs/us10002/transcripts.parquet")
WAYBACK_PARQUET_PATH = Path("runs/us10002/wayback_transcripts.parquet")
TICKERS_PATH = Path("financial_scraper/config/us10002_active_tickers.txt")
CHECKPOINT_PATH = Path("runs/us10002/wayback_checkpoint.json")
CDX_CACHE_PATH = Path("runs/us10002/wayback_cdx_cache.json")

CDX_URL = "https://web.archive.org/cdx/search/cdx"

SCHEMA = pa.schema([
    ("company", pa.string()),
    ("title", pa.string()),
    ("link", pa.string()),
    ("snippet", pa.string()),
    ("date", pa.timestamp("ns")),
    ("source", pa.string()),
    ("full_text", pa.string()),
    ("source_file", pa.string()),
])

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Quarter patterns in SA URL slugs
_Q_RE = re.compile(r"-(?:f\d)?q([1-4])-(\d{4})-", re.IGNORECASE)
# Alternate: q1-2020 at start or after company name
_Q_RE2 = re.compile(r"-q([1-4])-(\d{4})", re.IGNORECASE)
# Fiscal quarter: f2q13 meaning fiscal Q2 of some year
_FQ_RE = re.compile(r"-f\d?q(\d+)[-_]", re.IGNORECASE)

# Ticker in URL: ...-TICKER-q1-2020-... (ticker = uppercase 1-5 letters before qN)
_TICKER_URL_RE = re.compile(
    r"-([a-z]{1,5})-(?:f\d)?q[1-4]-\d{4}-",
    re.IGNORECASE,
)

# Ticker in SA page content: "Company Name (TICKER)" or "Company Name ( TICKER )"
_TICKER_CONTENT_RE = re.compile(
    r"(?:^|\s)\(\s*([A-Z]{1,5})\s*\)",
)
# Also: "Company Name (EXCHANGE:TICKER)"
_TICKER_CONTENT_RE2 = re.compile(
    r"\(\s*(?:NYSE|NASDAQ|AMEX|OTC|OTCBB|OTCQX|OTCQB):\s*([A-Z]{1,5})\s*\)",
)

# Date from SA earnings call content: "Q3 2012 Earnings Call October 15, 2012"
_DATE_CONTENT_RE = re.compile(
    r"(?:earnings\s+call|conference\s+call)\s+"
    r"(\w+\s+\d{1,2},?\s+\d{4})",
    re.IGNORECASE,
)

# Article published date meta tag
_META_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_tickers() -> set[str]:
    with open(TICKERS_PATH) as f:
        return {line.strip().upper() for line in f if line.strip()}


def load_existing_combos() -> set[tuple[str, int, str]]:
    """Return set of (ticker, year, quarter) already in the parquet."""
    if not PARQUET_PATH.exists():
        return set()
    t = pq.read_table(PARQUET_PATH, columns=["company", "date"])
    df = t.to_pandas()
    df["year"] = df["date"].dt.year.astype("Int64")
    df["quarter"] = "Q" + ((df["date"].dt.month - 1) // 3 + 1).astype(str)
    return set(zip(df["company"], df["year"], df["quarter"]))


def load_existing_links() -> set[str]:
    if not PARQUET_PATH.exists():
        return set()
    t = pq.read_table(PARQUET_PATH, columns=["link"])
    return set(t.column("link").to_pylist())


def load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        return json.loads(CHECKPOINT_PATH.read_bytes())
    return {"fetched": [], "empty": [], "failed": [], "skipped_ticker": []}


def save_checkpoint(cp: dict):
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
    tmp.write_bytes(json.dumps(cp).encode())
    os.replace(tmp, CHECKPOINT_PATH)


def load_cdx_cache() -> list[dict]:
    if CDX_CACHE_PATH.exists():
        return json.loads(CDX_CACHE_PATH.read_bytes())
    return []


def save_cdx_cache(entries: list[dict]):
    CDX_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CDX_CACHE_PATH.with_suffix(".json.tmp")
    tmp.write_bytes(json.dumps(entries).encode())
    os.replace(tmp, CDX_CACHE_PATH)


# ---------------------------------------------------------------------------
# CDX Discovery
# ---------------------------------------------------------------------------

def _is_transcript_url(url: str) -> bool:
    """Check if a SA URL is an earnings call transcript (not an article about transcripts)."""
    slug = url.split("/article/")[-1].lower() if "/article/" in url else url.lower()
    if "earnings-call-transcript" not in slug:
        return False
    # Exclude articles that are ABOUT transcripts (e.g., "housing-improves-according-to-q3-earnings-call-transcripts")
    # Real transcripts have company-specific patterns with quarter+year
    if re.search(r"q[1-4]-\d{4}", slug):
        return True
    if re.search(r"f\d?q\d+-\d{4}", slug):
        return True
    # Also accept: "ceo-discusses-qN-YYYY-results"
    if re.search(r"discusses.*q[1-4].*\d{4}", slug):
        return True
    return False


def discover_transcript_urls(session: requests.Session) -> list[dict]:
    """Query Wayback CDX API for all SA earnings-call-transcript URLs.

    Strategy: query per-year with prefix matching, large limit,
    and filter for transcript URLs locally.
    """
    cached = load_cdx_cache()
    if cached:
        log.info(f"Loaded {len(cached)} URLs from CDX cache")
        return cached

    log.info("Querying Wayback CDX API for SA transcript URLs...")
    all_entries = []
    seen_urls = set()

    # Query per crawl-year with collapse=urlkey (dedup at CDX level).
    # Use limit=5000 without page param — this is the combo that works
    # reliably. For years with >5000 unique URLs, we may miss some, but
    # transcript URLs are ~14% of SA articles so 5000 * 0.14 = ~700/year
    # is plenty for most years.
    for crawl_year in range(2008, 2027):
        log.info(f"  CDX crawl year {crawl_year}...")
        year_count = 0

        for attempt in range(3):
            try:
                resp = session.get(CDX_URL, params={
                    "url": "seekingalpha.com/article/",
                    "matchType": "prefix",
                    "output": "json",
                    "fl": "timestamp,original,statuscode,length",
                    "filter": "statuscode:200",
                    "collapse": "urlkey",
                    "limit": "5000",
                    "from": str(crawl_year),
                    "to": str(crawl_year),
                }, timeout=180)

                if resp.status_code == 503 or resp.status_code == 504:
                    log.warning(f"    CDX {resp.status_code}, retry {attempt+1}/3...")
                    time.sleep(30)
                    continue

                if resp.status_code != 200:
                    log.warning(f"    CDX HTTP {resp.status_code}")
                    break

                text = resp.text.strip()
                if not text or text == "[]":
                    log.info(f"    Empty (no archived articles for this year)")
                    break

                rows = json.loads(text)
                if not rows:
                    break

                data_rows = rows[1:] if rows and rows[0][0] == "timestamp" else rows
                if not data_rows:
                    break

                for row in data_rows:
                    ts, url = row[0], row[1]
                    length = row[3] if len(row) > 3 else "0"
                    clean_url = url.replace(":80", "").split("?")[0].rstrip("/")

                    if clean_url in seen_urls:
                        continue
                    if not _is_transcript_url(clean_url):
                        continue

                    seen_urls.add(clean_url)
                    all_entries.append({
                        "timestamp": ts,
                        "url": clean_url,
                        "original_url": url,
                        "length": int(length) if length else 0,
                    })
                    year_count += 1

                log.info(f"    {len(data_rows)} URLs scanned, {year_count} transcripts found (total: {len(all_entries)})")
                break  # Success, move to next year

            except requests.Timeout:
                log.warning(f"    Timeout, retry {attempt+1}/3...")
                time.sleep(30)
                continue
            except Exception as e:
                log.warning(f"    Error: {e}")
                break

        time.sleep(3)

    log.info(f"CDX discovery complete: {len(all_entries)} unique transcript URLs")

    if all_entries:
        save_cdx_cache(all_entries)
        log.info(f"CDX cache saved to {CDX_CACHE_PATH}")

    return all_entries


# ---------------------------------------------------------------------------
# URL Parsing
# ---------------------------------------------------------------------------

def parse_quarter_year_from_url(url: str) -> tuple[str | None, int | None]:
    """Try to extract quarter and year from SA URL slug."""
    slug = url.split("/article/")[-1] if "/article/" in url else url

    # Pattern: -q1-2020- or -q4-2008-
    m = _Q_RE2.search(slug)
    if m:
        return f"Q{m.group(1)}", int(m.group(2))

    return None, None


def parse_ticker_from_url(url: str, target_tickers: set[str]) -> str | None:
    """Try to extract ticker from SA URL slug by matching against known tickers."""
    slug = url.split("/article/")[-1] if "/article/" in url else url
    slug_lower = slug.lower()

    # Strategy 1: explicit ticker before quarter pattern
    # e.g., "apple-aapl-q3-2017" or "ebay-q3-2008"
    m = _TICKER_URL_RE.search(slug)
    if m:
        candidate = m.group(1).upper()
        if candidate in target_tickers:
            return candidate

    # Strategy 2: check each target ticker (2+ chars) against slug words
    # Skip 1-char tickers here — too many false positives from possessives
    # (e.g., "company-s-ceo" where "s" is possessive, not ticker S)
    slug_words = set(slug_lower.replace("-", " ").split())
    for ticker in target_tickers:
        if len(ticker) >= 2 and ticker.lower() in slug_words:
            return ticker

    return None


def extract_ticker_from_content(text: str, target_tickers: set[str]) -> str | None:
    """Extract ticker from SA page content: 'Company (TICKER)' pattern."""
    # Look in first 2000 chars
    head = text[:2000]

    # Try EXCHANGE:TICKER pattern first (more specific)
    for m in _TICKER_CONTENT_RE2.finditer(head):
        ticker = m.group(1).upper()
        if ticker in target_tickers:
            return ticker

    # Try simple (TICKER) pattern
    for m in _TICKER_CONTENT_RE.finditer(head):
        ticker = m.group(1).upper()
        if ticker in target_tickers:
            return ticker

    return None


def extract_date_from_content(text: str, tree) -> str:
    """Extract earnings call date from SA page content."""
    # Try meta tag first
    date_meta = tree.xpath("//meta[@property='article:published_time']/@content")
    if not date_meta:
        date_meta = tree.xpath("//meta[@name='date']/@content")
    if not date_meta:
        date_meta = tree.xpath("//meta[@name='article:published_time']/@content")
    if not date_meta:
        # Wayback pages might have the date in a time element
        date_meta = tree.xpath("//time/@datetime")

    if date_meta:
        m = _META_DATE_RE.search(date_meta[0])
        if m:
            return m.group(1)

    # Try to find date in content: "Earnings Call October 15, 2012"
    m = _DATE_CONTENT_RE.search(text[:1000])
    if m:
        from datetime import datetime
        date_str = m.group(1).replace(",", "").strip()
        for fmt in ["%B %d %Y", "%b %d %Y", "%B %d, %Y", "%b. %d %Y"]:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

    return ""


# ---------------------------------------------------------------------------
# Transcript Extraction
# ---------------------------------------------------------------------------

CONTENT_SELECTORS = [
    "//div[@id='a-body']",
    "//div[contains(@class,'sa-art')]//div[contains(@class,'a-body')]",
    "//div[contains(@class,'article_body')]",
    "//div[contains(@class,'article-body')]",
    "//article//div[contains(@class,'body')]",
    "//article",
]


def fetch_transcript(entry: dict, session: requests.Session) -> dict | None:
    """Fetch and extract transcript from a Wayback Machine archived SA page."""
    ts = entry["timestamp"]
    url = entry["original_url"]
    archive_url = f"https://web.archive.org/web/{ts}id_/{url}"

    try:
        resp = session.get(archive_url, timeout=(10, 45))
    except requests.RequestException as e:
        log.warning(f"  Request error: {e}")
        return None

    if resp.status_code == 429:
        log.warning("  Rate limited (429)")
        return None

    if resp.status_code != 200:
        log.warning(f"  HTTP {resp.status_code}")
        return None

    try:
        tree = html.fromstring(resp.text)
    except Exception:
        return None

    # Extract article content
    full_text = ""
    for sel in CONTENT_SELECTORS:
        elems = tree.xpath(sel)
        if elems:
            text = " ".join(elems[0].itertext()).strip()
            if len(text) > 3000:
                full_text = text
                break

    if len(full_text) < 3000:
        return None

    # Get title
    title_elems = tree.xpath("//title/text()")
    title = title_elems[0].strip() if title_elems else ""
    # Clean SA suffixes
    title = re.sub(r"\s*[-|]\s*Seeking Alpha\s*$", "", title)

    # Extract date
    date_str = extract_date_from_content(full_text, tree)

    return {
        "title": title,
        "full_text": full_text,
        "date": date_str,
        "archive_url": archive_url,
    }


# ---------------------------------------------------------------------------
# Parquet I/O
# ---------------------------------------------------------------------------

def append_to_parquet(records: list[dict]):
    """Write to separate wayback parquet file (avoids locking main file)."""
    if not records:
        return
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)

    out_path = WAYBACK_PARQUET_PATH
    if out_path.exists():
        for attempt in range(3):
            try:
                existing = pq.read_table(out_path)
                break
            except (OSError, pa.ArrowInvalid) as e:
                if attempt < 2:
                    log.warning(f"Parquet read retry {attempt+1}: {e}")
                    time.sleep(1)
                else:
                    raise
        combined = pa.concat_tables([existing, table], promote_options="permissive")
    else:
        combined = table

    tmp = out_path.with_suffix(".parquet.tmp")
    pq.write_table(combined, tmp, compression="snappy")
    os.replace(tmp, out_path)
    log.info(f"Wayback parquet updated: {len(combined)} rows (+{len(records)} new)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fill gaps via Wayback Machine + SA")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show gap count without fetching")
    parser.add_argument("--discover-only", action="store_true",
                        help="Only run CDX discovery, don't fetch")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max transcripts to fetch (0=unlimited)")
    parser.add_argument("--batch-size", type=int, default=25,
                        help="Write to parquet every N successful fetches")
    parser.add_argument("--delay", type=float, default=1.5,
                        help="Delay between Wayback requests in seconds")
    args = parser.parse_args()

    session = requests.Session()
    session.headers["User-Agent"] = UA

    # 1. Load target tickers
    target_tickers = load_tickers()
    log.info(f"Target tickers: {len(target_tickers)}")

    # 2. CDX Discovery
    cdx_entries = discover_transcript_urls(session)
    if not cdx_entries:
        log.error("No CDX entries found. Try again later (Wayback may be overloaded).")
        return

    if args.discover_only:
        # Show stats and exit
        by_year = Counter()
        for e in cdx_entries:
            _, yr = parse_quarter_year_from_url(e["url"])
            if yr:
                by_year[yr] += 1
        print(f"\nTotal archived SA transcript URLs: {len(cdx_entries)}")
        for yr in sorted(by_year):
            print(f"  {yr}: {by_year[yr]}")
        return

    # 3. Parse quarter/year and try URL-based ticker extraction
    log.info("Parsing URLs...")
    parsed = []
    no_quarter = 0
    for entry in cdx_entries:
        quarter, year = parse_quarter_year_from_url(entry["url"])
        if not quarter or not year:
            no_quarter += 1
            continue
        ticker = parse_ticker_from_url(entry["url"], target_tickers)
        parsed.append({
            **entry,
            "ticker": ticker,  # May be None — will resolve from page content
            "quarter": quarter,
            "year": year,
        })

    log.info(f"Parsed: {len(parsed)} with quarter/year ({no_quarter} couldn't parse)")
    with_ticker = sum(1 for p in parsed if p["ticker"])
    log.info(f"  URL-based ticker match: {with_ticker}, needs content extraction: {len(parsed) - with_ticker}")

    # 4. Filter out already-done
    existing_combos = load_existing_combos()
    existing_links = load_existing_links()
    cp = load_checkpoint()
    already_done = set(
        cp.get("fetched", []) + cp.get("empty", []) +
        cp.get("failed", []) + cp.get("skipped_ticker", [])
    )
    log.info(f"Existing combos: {len(existing_combos)}, links: {len(existing_links)}, previously tried: {len(already_done)}")

    # Split into: known-ticker (can filter now) and unknown-ticker (need to fetch to check)
    known_ticker_gaps = []
    unknown_ticker = []
    skipped_existing = 0
    skipped_tried = 0
    skipped_non_target = 0

    for info in parsed:
        url_key = info["url"]
        if url_key in already_done:
            skipped_tried += 1
            continue

        if info["ticker"]:
            combo = (info["ticker"], info["year"], info["quarter"])
            if combo in existing_combos:
                skipped_existing += 1
                continue
            if url_key in existing_links:
                skipped_existing += 1
                continue
            known_ticker_gaps.append(info)
        else:
            unknown_ticker.append(info)

    log.info(f"Known-ticker gaps: {len(known_ticker_gaps)}")
    log.info(f"Unknown-ticker (need fetch to identify): {len(unknown_ticker)}")
    log.info(f"Skipped: {skipped_existing} existing, {skipped_tried} already tried, {skipped_non_target} non-target")

    # Combine: prioritize known-ticker gaps, then unknown-ticker
    # Within each group, prioritize 2010-2017 (our biggest gap)
    priority_years = set(range(2010, 2018))
    known_ticker_gaps.sort(key=lambda g: (0 if g["year"] in priority_years else 1, g["year"], g["ticker"] or ""))
    unknown_ticker.sort(key=lambda g: (0 if g["year"] in priority_years else 1, g["year"]))

    gaps = known_ticker_gaps + unknown_ticker
    total_gaps = len(gaps)

    if args.dry_run:
        by_year = Counter()
        for g in known_ticker_gaps:
            by_year[g["year"]] += 1
        print(f"\nKnown-ticker gaps to fill: {len(known_ticker_gaps)}")
        for yr in sorted(by_year):
            print(f"  {yr}: {by_year[yr]} gaps")
        print(f"Unknown-ticker URLs to check: {len(unknown_ticker)}")
        print(f"Total to process: {total_gaps}")
        return

    if args.limit > 0:
        gaps = gaps[:args.limit]
        log.info(f"Limited to {args.limit} URLs")

    # 5. Fetch transcripts
    batch: list[dict] = []
    fetched_count = 0
    empty_count = 0
    failed_count = 0
    skipped_ticker_count = 0
    start_time = time.time()

    try:
        for i, info in enumerate(gaps):
            url_key = info["url"]

            if (i + 1) % 25 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed * 60 if elapsed > 0 else 0
                log.info(
                    f"Progress: {i+1}/{len(gaps)}, "
                    f"{fetched_count} found, {empty_count} empty, "
                    f"{failed_count} failed, {skipped_ticker_count} wrong ticker "
                    f"({rate:.0f} req/min)"
                )

            ticker_label = info["ticker"] or "???"
            log.info(f"  [{i+1}/{len(gaps)}] {ticker_label} {info['quarter']} {info['year']}")

            result = fetch_transcript(info, session)

            if result and len(result["full_text"]) >= 3000:
                # If ticker unknown, try to extract from content
                ticker = info["ticker"]
                if not ticker:
                    ticker = extract_ticker_from_content(result["full_text"], target_tickers)

                if not ticker:
                    # Not in our target universe
                    cp["skipped_ticker"].append(url_key)
                    skipped_ticker_count += 1
                    log.info(f"    Skip: ticker not in US10002")
                elif (ticker, info["year"], info["quarter"]) in existing_combos:
                    # Already have this combo
                    cp["skipped_ticker"].append(url_key)
                    skipped_ticker_count += 1
                    log.info(f"    Skip: {ticker} {info['quarter']} {info['year']} already exists")
                else:
                    # Extract date
                    sa_url = info["url"]
                    if not sa_url.startswith("http"):
                        sa_url = "https://" + sa_url

                    record = {
                        "company": ticker,
                        "title": result["title"],
                        "link": sa_url,
                        "snippet": result["full_text"][:300],
                        "date": result["date"],
                        "source": "seekingalpha.com (wayback)",
                        "full_text": result["full_text"],
                        "source_file": f"{ticker.lower()}_wayback_{info['year']}{info['quarter']}.parquet",
                    }
                    batch.append(record)
                    cp["fetched"].append(url_key)
                    fetched_count += 1
                    # Add to existing combos to avoid duplicates within this run
                    existing_combos.add((ticker, info["year"], info["quarter"]))
                    log.info(f"    OK: {ticker} {len(result['full_text'])} chars, date={result['date']}")

            elif result is None:
                cp["failed"].append(url_key)
                failed_count += 1
            else:
                cp["empty"].append(url_key)
                empty_count += 1

            # Flush batch
            if len(batch) >= args.batch_size:
                append_to_parquet(batch)
                batch.clear()

            # Save checkpoint frequently (every 50 items processed)
            total_done = fetched_count + empty_count + failed_count + skipped_ticker_count
            if total_done % 50 == 0 and total_done > 0:
                save_checkpoint(cp)

            time.sleep(args.delay)

    except KeyboardInterrupt:
        log.info("Interrupted by user")

    # Final flush
    if batch:
        append_to_parquet(batch)
    save_checkpoint(cp)

    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info("WAYBACK + SEEKING ALPHA GAP FILL SUMMARY")
    log.info("=" * 60)
    log.info(f"URLs processed: {fetched_count + empty_count + failed_count + skipped_ticker_count}")
    log.info(f"Transcripts found: {fetched_count}")
    log.info(f"Empty/short: {empty_count}")
    log.info(f"Failed: {failed_count}")
    log.info(f"Wrong ticker (not US10002): {skipped_ticker_count}")
    log.info(f"Time: {elapsed/60:.1f} min")
    log.info(f"Remaining: {len(gaps) - (fetched_count + empty_count + failed_count + skipped_ticker_count)}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
