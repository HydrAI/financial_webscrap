"""Fill temporal gaps using AlphaStreet earnings call transcripts.

1. Fetch all transcript URLs from AlphaStreet sitemaps (transcript-sitemap*.xml)
2. Parse ticker + quarter + year from each URL
3. Filter to US10002 tickers and missing (ticker, year, quarter) combos
4. Fetch and extract transcript text
5. Append to the parquet file

Usage:
    python _fill_alphastreet_gaps.py [--dry-run] [--limit 500] [--concurrent 2]
"""

import argparse
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from lxml import etree, html

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PARQUET_PATH = Path("runs/us10002/transcripts.parquet")
TICKERS_PATH = Path("financial_scraper/config/us10002_active_tickers.txt")
CHECKPOINT_PATH = Path("runs/us10002/alphastreet_checkpoint.json")
SITEMAP_INDEX = "https://news.alphastreet.com/sitemap.xml"

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

NS = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Parse: company-name-ticker-qN-year-earnings-call-transcript
_URL_RE = re.compile(
    r"/(?P<slug>.+?)-(?P<ticker>[a-z][a-z0-9]*)-q(?P<quarter>\d)-(?P<year>\d{4})-earnings-call-transcript/?$",
    re.IGNORECASE,
)
# Alternate pattern with exchange prefix: company-exchange-ticker-qN-year
_URL_RE2 = re.compile(
    r"/(?P<slug>.+?)-(?:nasdaq|nyse|amex)-(?P<ticker>[a-z][a-z0-9]*)-q(?P<quarter>\d)-(?P<year>\d{4})-earnings",
    re.IGNORECASE,
)

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
    """Return set of URLs already in the parquet."""
    if not PARQUET_PATH.exists():
        return set()
    t = pq.read_table(PARQUET_PATH, columns=["link"])
    return set(t.column("link").to_pylist())


def load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        return json.loads(CHECKPOINT_PATH.read_bytes())
    return {"fetched": [], "empty": [], "failed": []}


def save_checkpoint(cp: dict):
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
    tmp.write_bytes(json.dumps(cp).encode())
    os.replace(tmp, CHECKPOINT_PATH)


def parse_url(url: str) -> dict | None:
    """Extract ticker, quarter, year from AlphaStreet transcript URL."""
    for regex in [_URL_RE, _URL_RE2]:
        m = regex.search(url)
        if m:
            return {
                "ticker": m.group("ticker").upper(),
                "quarter": f"Q{m.group('quarter')}",
                "year": int(m.group("year")),
                "url": url,
            }
    return None


# ---------------------------------------------------------------------------
# Sitemap fetching
# ---------------------------------------------------------------------------

def fetch_transcript_sitemap_urls(session: requests.Session) -> list[str]:
    """Fetch all transcript URLs from AlphaStreet transcript-sitemap*.xml files."""
    log.info("Fetching AlphaStreet sitemap index...")
    resp = session.get(SITEMAP_INDEX, timeout=30)
    resp.raise_for_status()
    root = etree.fromstring(resp.content)
    sub_sitemaps = [loc.text for loc in root.findall(".//s:loc", NS)]

    # Filter to transcript sitemaps only
    transcript_sitemaps = [s for s in sub_sitemaps if "transcript-sitemap" in s]
    log.info(f"Found {len(transcript_sitemaps)} transcript sitemaps")

    all_urls = []
    for sm_url in transcript_sitemaps:
        log.info(f"  Fetching {sm_url}...")
        try:
            resp = session.get(sm_url, timeout=30)
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code} for {sm_url}")
                continue
            root = etree.fromstring(resp.content)
            locs = [loc.text for loc in root.findall(".//s:loc", NS)]
            # Filter to actual transcript URLs (not announcement posts)
            transcripts = [u for u in locs if "earnings-call-transcript" in u.lower()]
            all_urls.extend(transcripts)
            log.info(f"    {len(transcripts)} transcript URLs")
        except Exception as e:
            log.warning(f"  Error fetching {sm_url}: {e}")
        time.sleep(0.5)

    log.info(f"Total transcript URLs from sitemaps: {len(all_urls)}")
    return all_urls


# ---------------------------------------------------------------------------
# Transcript extraction
# ---------------------------------------------------------------------------

def fetch_transcript(url: str, session: requests.Session) -> dict | None:
    """Fetch and extract transcript text from an AlphaStreet page."""
    try:
        resp = session.get(url, timeout=30)
    except requests.RequestException as e:
        log.warning(f"  Request error: {e}")
        return None

    if resp.status_code == 429:
        log.warning(f"  Rate limited (429)")
        return None

    if resp.status_code != 200:
        log.warning(f"  HTTP {resp.status_code}")
        return None

    tree = html.fromstring(resp.text)

    # Extract transcript from dedicated div
    transcript_divs = tree.xpath("//div[contains(@class,'transcript')]")
    if not transcript_divs:
        # Fallback: try article
        transcript_divs = tree.xpath("//article")

    if not transcript_divs:
        return None

    full_text = " ".join(transcript_divs[0].itertext()).strip()
    if len(full_text) < 1000:
        return None  # Too short to be a real transcript

    # Extract title
    title_elems = tree.xpath("//title/text()")
    title = title_elems[0].strip() if title_elems else ""
    # Clean title: remove " - AlphaStreet News" suffix
    title = re.sub(r"\s*-\s*AlphaStreet\s+News\s*$", "", title)

    # Extract date from the page (look for common date patterns)
    date_str = ""
    # Try meta tag first
    date_meta = tree.xpath("//meta[@property='article:published_time']/@content")
    if date_meta:
        date_str = date_meta[0][:10]
    else:
        # Try to find date in transcript header (e.g., "Q4 2025 Earnings Call dated Feb. 26, 2025")
        dated_match = re.search(r"dated\s+(\w+\.?\s+\d{1,2},?\s+\d{4})", full_text[:500])
        if dated_match:
            try:
                from datetime import datetime
                for fmt in ["%b. %d, %Y", "%b %d, %Y", "%B %d, %Y", "%b. %d %Y"]:
                    try:
                        dt = datetime.strptime(dated_match.group(1), fmt)
                        date_str = dt.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
            except Exception:
                pass

    return {
        "title": title,
        "full_text": full_text,
        "date": date_str,
    }


def append_to_parquet(records: list[dict]):
    """Append records to the parquet file (atomic write)."""
    if not records:
        return

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)

    if PARQUET_PATH.exists():
        for attempt in range(3):
            try:
                existing = pq.read_table(PARQUET_PATH)
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

    tmp = PARQUET_PATH.with_suffix(".parquet.tmp")
    pq.write_table(combined, tmp, compression="snappy")
    os.replace(tmp, PARQUET_PATH)
    log.info(f"Parquet updated: {len(combined)} total rows (+{len(records)} new)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fill transcript gaps via AlphaStreet")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show gap count without fetching")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max transcripts to fetch (0=unlimited)")
    parser.add_argument("--concurrent", type=int, default=2,
                        help="Concurrent fetch threads")
    parser.add_argument("--batch-size", type=int, default=25,
                        help="Write to parquet every N successful fetches")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Delay between requests in seconds")
    args = parser.parse_args()

    session = requests.Session()
    session.headers["User-Agent"] = UA

    # 1. Load target tickers
    target_tickers = load_tickers()
    log.info(f"Target tickers: {len(target_tickers)}")

    # 2. Fetch AlphaStreet sitemap URLs
    all_urls = fetch_transcript_sitemap_urls(session)

    # 3. Parse and filter to our tickers
    parsed = []
    unmatched = 0
    for url in all_urls:
        info = parse_url(url)
        if info and info["ticker"] in target_tickers:
            parsed.append(info)
        elif info is None:
            unmatched += 1

    log.info(f"Parsed: {len(parsed)} matching our tickers ({unmatched} URLs didn't parse)")

    # 4. Filter out already-existing combos and links
    existing_combos = load_existing_combos()
    existing_links = load_existing_links()
    log.info(f"Existing combos: {len(existing_combos)}, existing links: {len(existing_links)}")

    cp = load_checkpoint()
    already_done = set(cp.get("fetched", []) + cp.get("empty", []) + cp.get("failed", []))
    log.info(f"Previously tried: {len(already_done)}")

    gaps = []
    skipped_existing = 0
    skipped_tried = 0
    for info in parsed:
        combo = (info["ticker"], info["year"], info["quarter"])
        if combo in existing_combos:
            skipped_existing += 1
            continue
        if info["url"] in already_done or info["url"] in existing_links:
            skipped_tried += 1
            continue
        gaps.append(info)

    log.info(f"Gaps to fill: {len(gaps)} (skipped {skipped_existing} existing, {skipped_tried} already tried)")

    # Prioritise 2022-2024
    priority_years = {2022, 2023, 2024}
    gaps.sort(key=lambda g: (0 if g["year"] in priority_years else 1, g["year"], g["ticker"]))

    if args.dry_run:
        from collections import Counter
        by_year = Counter(g["year"] for g in gaps)
        for yr in sorted(by_year):
            print(f"  {yr}: {by_year[yr]} gaps")
        print(f"  Total: {len(gaps)} gaps to fill")
        return

    if args.limit > 0:
        gaps = gaps[:args.limit]
        log.info(f"Limited to {args.limit} transcripts")

    # 5. Fetch transcripts
    batch: list[dict] = []
    fetched_count = 0
    empty_count = 0
    failed_count = 0
    start_time = time.time()

    try:
        for i, info in enumerate(gaps):
            url = info["url"]

            if (i + 1) % 25 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed * 60 if elapsed > 0 else 0
                log.info(
                    f"Progress: {i+1}/{len(gaps)}, "
                    f"{fetched_count} found, {empty_count} empty, {failed_count} failed "
                    f"({rate:.0f} req/min)"
                )

            log.info(f"  [{i+1}/{len(gaps)}] {info['ticker']} {info['quarter']} {info['year']}")

            result = fetch_transcript(url, session)

            if result and len(result["full_text"]) >= 1000:
                record = {
                    "company": info["ticker"],
                    "title": result["title"],
                    "link": url,
                    "snippet": result["full_text"][:300],
                    "date": result["date"],
                    "source": "alphastreet.com",
                    "full_text": result["full_text"],
                    "source_file": f"{info['ticker'].lower()}_alphastreet_{info['year']}{info['quarter']}.parquet",
                }
                batch.append(record)
                cp["fetched"].append(url)
                fetched_count += 1
                log.info(f"    OK: {len(result['full_text'])} chars, date={result['date']}")
            elif result is None:
                cp["failed"].append(url)
                failed_count += 1
            else:
                cp["empty"].append(url)
                empty_count += 1

            # Flush batch
            if len(batch) >= args.batch_size:
                append_to_parquet(batch)
                batch.clear()
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
    log.info("ALPHASTREET GAP FILL SUMMARY")
    log.info("=" * 60)
    log.info(f"URLs processed: {fetched_count + empty_count + failed_count}")
    log.info(f"Transcripts found: {fetched_count}")
    log.info(f"Empty/short: {empty_count}")
    log.info(f"Failed: {failed_count}")
    log.info(f"Time: {elapsed/60:.1f} min")
    log.info(f"Remaining gaps: {len(gaps) - (fetched_count + empty_count + failed_count)}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
