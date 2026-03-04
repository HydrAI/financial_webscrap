"""Convert research_4/Earnings-Calls-NLP transcripts to standard parquet format.

Source: S&P 500 earnings call transcripts (2015-2020) from Seeking Alpha.
Text files in transcripts/sandp500/ with metadata in transcript_list_500.csv.
"""
import csv
import re
import os
import logging
from collections import Counter
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# Paths
RESEARCH_DIR = Path("research_4/Earnings-Calls-NLP")
TRANSCRIPTS_DIR = RESEARCH_DIR / "transcripts" / "sandp500"
CSV_PATH = RESEARCH_DIR / "transcript_list_500.csv"
TICKERS_PATH = Path("financial_scraper/config/us10002_active_tickers.txt")
EXISTING_PARQUET = Path("runs/us10002/transcripts.parquet")
WAYBACK_PARQUET = Path("runs/us10002/wayback_transcripts.parquet")
OUTPUT_PARQUET = Path("runs/us10002/research4_transcripts.parquet")

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


def load_tickers():
    return set(TICKERS_PATH.read_text().strip().splitlines())


def load_existing_combos():
    """Load (ticker, year, quarter) combos from existing parquets."""
    combos = set()
    for p in [EXISTING_PARQUET, WAYBACK_PARQUET]:
        if not p.exists():
            continue
        t = pq.read_table(p, columns=["company", "date"])
        for company, date in zip(t.column("company").to_pylist(), t.column("date").to_pylist()):
            if company and date:
                ds = str(date)
                yr = ds[:4]
                mo = int(ds[5:7]) if len(ds) >= 7 else 0
                q = f"Q{(mo - 1) // 3 + 1}" if mo else None
                if q:
                    combos.add((company, yr, q))
    return combos


def extract_ticker_from_title(title):
    """Extract ticker from title like 'Apple (AAPL) CEO ...' or 'Apple's (AAPL) ...'."""
    m = re.search(r"\(([A-Z]{1,5})\)", title)
    if m:
        return m.group(1)
    return None


def extract_quarter_year_from_title(title):
    """Extract quarter and year from title like '... Q3 2020 Results ...'."""
    m = re.search(r"Q([1-4])\s+(\d{4})", title)
    if m:
        return f"Q{m.group(1)}", m.group(2)
    # Try FQ pattern
    m = re.search(r"F(?:\d)?Q([1-4])\s+(\d{4})", title)
    if m:
        return f"Q{m.group(1)}", m.group(2)
    return None, None


def parse_date(date_str):
    """Parse date string from CSV."""
    for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%b %d, %Y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


def main():
    target_tickers = load_tickers()
    log.info(f"Target tickers: {len(target_tickers)}")

    existing_combos = load_existing_combos()
    log.info(f"Existing combos: {len(existing_combos)}")

    # Load CSV metadata
    csv_rows = []
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_rows.append(row)
    log.info(f"CSV rows: {len(csv_rows)}")

    # Build title -> metadata lookup
    csv_by_title = {}
    for row in csv_rows:
        title = row["title"].strip()
        csv_by_title[title] = row

    # Enumerate all text files
    txt_files = sorted(TRANSCRIPTS_DIR.glob("*.txt"))
    log.info(f"Text files: {len(txt_files)}")

    # Also check sandp100
    txt_files_100 = sorted((RESEARCH_DIR / "transcripts" / "sandp100").glob("*.txt"))
    log.info(f"Text files (sandp100): {len(txt_files_100)}")

    # Combine, deduplicate by filename stem
    seen_stems = set()
    all_files = []
    for f in txt_files + txt_files_100:
        if f.stem not in seen_stems:
            seen_stems.add(f.stem)
            all_files.append(f)
    log.info(f"Total unique text files: {len(all_files)}")

    # Process
    records = []
    skipped_no_ticker = 0
    skipped_non_target = 0
    skipped_existing = 0
    skipped_short = 0
    skipped_no_date = 0

    for txt_path in all_files:
        title = txt_path.stem  # filename without .txt

        # Extract ticker from title
        ticker = extract_ticker_from_title(title)

        # Check CSV metadata
        csv_row = csv_by_title.get(title)
        if not ticker and csv_row:
            ticker = csv_row.get("Symbol", "").strip().upper()

        if not ticker:
            skipped_no_ticker += 1
            continue

        if ticker not in target_tickers:
            skipped_non_target += 1
            continue

        # Extract quarter/year
        quarter, year = extract_quarter_year_from_title(title)
        if not quarter or not year:
            # Try from text content first line
            pass

        # Check if already exists
        if quarter and year:
            combo = (ticker, year, quarter)
            if combo in existing_combos:
                skipped_existing += 1
                continue

        # Read text
        try:
            text = txt_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            try:
                text = txt_path.read_text(encoding="latin-1")
            except Exception:
                continue

        if len(text) < 3000:
            skipped_short += 1
            continue

        # Parse date
        date = None
        if csv_row and csv_row.get("date"):
            date = parse_date(csv_row["date"])

        if not date:
            # Try to extract date from first line of transcript
            first_line = text.split("\n")[0]
            m = re.search(r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})", first_line)
            if m:
                date = parse_date(m.group(1))

        if not date:
            skipped_no_date += 1
            continue

        # Build URL
        link = ""
        if csv_row and csv_row.get("url"):
            url = csv_row["url"].split("?")[0]  # strip query params
            if not url.startswith("http"):
                link = "https://seekingalpha.com" + url
            else:
                link = url
        else:
            link = f"research4://{txt_path.name}"

        record = {
            "company": ticker,
            "title": title,
            "link": link,
            "snippet": text[:300],
            "date": pd.Timestamp(date),
            "source": "seekingalpha.com (research4)",
            "full_text": text,
            "source_file": txt_path.name,
        }
        records.append(record)

        # Add to existing combos to avoid duplicates within this run
        if quarter and year:
            existing_combos.add((ticker, year, quarter))

    log.info("=" * 60)
    log.info("CONVERSION SUMMARY")
    log.info("=" * 60)
    log.info(f"Total text files: {len(all_files)}")
    log.info(f"Records to save: {len(records)}")
    log.info(f"Skipped - no ticker: {skipped_no_ticker}")
    log.info(f"Skipped - non-US10002: {skipped_non_target}")
    log.info(f"Skipped - already exists: {skipped_existing}")
    log.info(f"Skipped - too short: {skipped_short}")
    log.info(f"Skipped - no date: {skipped_no_date}")

    if records:
        # Year breakdown
        years = Counter()
        for r in records:
            yr = str(r["date"].year)
            years[yr] += 1
        log.info("\nYear breakdown:")
        for yr in sorted(years):
            log.info(f"  {yr}: {years[yr]}")

        companies = set(r["company"] for r in records)
        log.info(f"\nUnique companies: {len(companies)}")

        # Write parquet
        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df, schema=SCHEMA)
        pq.write_table(table, OUTPUT_PARQUET)
        log.info(f"\nWritten: {OUTPUT_PARQUET} ({len(records)} rows, {OUTPUT_PARQUET.stat().st_size/1024/1024:.1f} MB)")
    else:
        log.info("No records to save")


if __name__ == "__main__":
    main()
