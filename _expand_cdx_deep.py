"""Deep CDX expansion — tasks 1+2 combined.

1. 3-digit sub-prefixes for high-yield ranges that hit 10K limit:
   article/340-349, article/360-369, article/430-439, article/460-469, article/480-489
2. Retry article/38 (previously timed out)
3. Year-filtered CDX queries for gap years 2010-2014, 2017 with limit=10000
4. Re-query 2015-2017 with sub-prefix + year filter approach
"""
import json
import os
import re
import time
import requests
from collections import Counter
from pathlib import Path

CDX_URL = "https://web.archive.org/cdx/search/cdx"
CDX_CACHE_PATH = Path("runs/us10002/wayback_cdx_cache.json")

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def _is_transcript_url(url):
    slug = url.split("/article/")[-1].lower() if "/article/" in url else url.lower()
    if "earnings-call-transcript" not in slug:
        return False
    if re.search(r"q[1-4]-\d{4}", slug):
        return True
    if re.search(r"f\d?q\d+-\d{4}", slug):
        return True
    if re.search(r"discusses.*q[1-4].*\d{4}", slug):
        return True
    return False


def query_cdx(session, prefix, extra_params=None, label=""):
    """Query CDX and return new transcript entries."""
    params = {
        "url": prefix,
        "matchType": "prefix",
        "output": "json",
        "fl": "timestamp,original,statuscode,length",
        "filter": "statuscode:200",
        "collapse": "urlkey",
        "limit": "10000",
    }
    if extra_params:
        params.update(extra_params)

    for attempt in range(3):
        try:
            resp = session.get(CDX_URL, params=params, timeout=(10, 300))
            if resp.status_code in (503, 504):
                print(f"  {label}: CDX {resp.status_code}, retry {attempt+1}/3...")
                time.sleep(30)
                continue
            if resp.status_code == 429:
                print(f"  {label}: Rate limited, waiting 60s...")
                time.sleep(60)
                continue
            if resp.status_code != 200:
                print(f"  {label}: HTTP {resp.status_code}")
                return []
            text = resp.text.strip()
            if not text or text == "[]":
                print(f"  {label}: empty")
                return []
            rows = json.loads(text)
            data = rows[1:] if rows and rows[0][0] == "timestamp" else rows
            return data
        except requests.Timeout:
            print(f"  {label}: timeout, retry {attempt+1}/3...")
            time.sleep(30)
        except json.JSONDecodeError:
            print(f"  {label}: JSON parse error")
            return []
        except Exception as e:
            print(f"  {label}: error: {e}")
            return []
    return []


def process_rows(data, seen_urls):
    """Filter CDX rows to new transcript entries."""
    new = []
    for row in data:
        ts, url = row[0], row[1]
        length = row[3] if len(row) > 3 else "0"
        clean_url = url.replace(":80", "").split("?")[0].rstrip("/")
        if clean_url in seen_urls:
            continue
        if not _is_transcript_url(clean_url):
            continue
        seen_urls.add(clean_url)
        new.append({
            "timestamp": ts,
            "url": clean_url,
            "original_url": url,
            "length": int(length) if length else 0,
        })
    return new


# Load existing cache
existing = json.loads(CDX_CACHE_PATH.read_bytes()) if CDX_CACHE_PATH.exists() else []
seen_urls = {e["url"] for e in existing}
print(f"Existing cache: {len(existing)} entries, {len(seen_urls)} unique URLs")

s = requests.Session()
s.headers["User-Agent"] = UA

all_new = []

# ============================================================
# PHASE 1: 3-digit sub-prefixes for high-yield ranges
# ============================================================
print("\n" + "=" * 60)
print("PHASE 1: 3-digit sub-prefixes for high-yield article ranges")
print("=" * 60)

# Ranges that hit/neared the 10K limit as 2-digit prefixes
three_digit_ranges = []
# article/34 had 1,063 results — break into 340-349
for d in range(10):
    three_digit_ranges.append(f"seekingalpha.com/article/34{d}")
# article/36 had 1,017 results
for d in range(10):
    three_digit_ranges.append(f"seekingalpha.com/article/36{d}")
# article/38 timed out — retry with 3-digit
for d in range(10):
    three_digit_ranges.append(f"seekingalpha.com/article/38{d}")
# article/43 had 1,828 results (highest)
for d in range(10):
    three_digit_ranges.append(f"seekingalpha.com/article/43{d}")
# article/46 had 974 results
for d in range(10):
    three_digit_ranges.append(f"seekingalpha.com/article/46{d}")
# article/48 had 1,513 results
for d in range(10):
    three_digit_ranges.append(f"seekingalpha.com/article/48{d}")

print(f"Querying {len(three_digit_ranges)} 3-digit sub-prefixes...")

for prefix in three_digit_ranges:
    short = prefix.split("article/")[-1]
    data = query_cdx(s, prefix, label=short)
    if data:
        new = process_rows(data, seen_urls)
        print(f"  {short}: {len(data)} URLs, {len(new)} new transcripts")
        all_new.extend(new)
    time.sleep(2)

print(f"\nPhase 1 total new: {len(all_new)}")

# ============================================================
# PHASE 2: Year-filtered queries for gap years
# ============================================================
print("\n" + "=" * 60)
print("PHASE 2: Year-filtered CDX for gap years (2010-2014, 2017)")
print("=" * 60)

# Query full SA article prefix but filtered to specific crawl years
# with higher limit. This catches articles archived in those years.
gap_years = [2010, 2011, 2012, 2014, 2017]
for year in gap_years:
    label = f"year={year}"
    data = query_cdx(s, "seekingalpha.com/article/", extra_params={
        "from": str(year),
        "to": str(year),
        "limit": "10000",
    }, label=label)
    if data:
        new = process_rows(data, seen_urls)
        print(f"  {label}: {len(data)} URLs, {len(new)} new transcripts")
        all_new.extend(new)
    time.sleep(5)

# ============================================================
# PHASE 3: Re-query 2015-2017 with sub-prefix + year filter
# ============================================================
print("\n" + "=" * 60)
print("PHASE 3: Sub-prefix + year filter for 2015-2017")
print("=" * 60)

# Use 1-digit article prefixes (2-5) combined with year filters
# This gives us more targeted results than either approach alone
for year in [2015, 2016, 2017]:
    for digit in [2, 3, 4, 5]:
        prefix = f"seekingalpha.com/article/{digit}"
        label = f"art/{digit} yr={year}"
        data = query_cdx(s, prefix, extra_params={
            "from": str(year),
            "to": str(year),
            "limit": "10000",
        }, label=label)
        if data:
            new = process_rows(data, seen_urls)
            if new:
                print(f"  {label}: {len(data)} URLs, {len(new)} new transcripts")
            else:
                print(f"  {label}: {len(data)} URLs, 0 new")
            all_new.extend(new)
        time.sleep(3)

# ============================================================
# PHASE 4: Additional ranges for 2007-2009, 2019+ fill
# ============================================================
print("\n" + "=" * 60)
print("PHASE 4: Additional under-explored ranges")
print("=" * 60)

# Article IDs 1-2M (early SA articles, 2007-2013)
for digit in [1]:  # article/1xxxxx
    for sub in range(10):
        prefix = f"seekingalpha.com/article/{digit}{sub}"
        label = f"art/{digit}{sub}"
        data = query_cdx(s, prefix, label=label)
        if data:
            new = process_rows(data, seen_urls)
            if new:
                print(f"  {label}: {len(data)} URLs, {len(new)} new transcripts")
                all_new.extend(new)
            else:
                print(f"  {label}: {len(data)} URLs, 0 new")
        time.sleep(2)

# Article IDs 5M+ (2019+) — finer granularity
for sub in range(10):
    prefix = f"seekingalpha.com/article/5{sub}"
    label = f"art/5{sub}"
    data = query_cdx(s, prefix, label=label)
    if data:
        new = process_rows(data, seen_urls)
        if new:
            print(f"  {label}: {len(data)} URLs, {len(new)} new transcripts")
            all_new.extend(new)
        else:
            print(f"  {label}: {len(data)} URLs, 0 new")
    time.sleep(2)

# ============================================================
# Summary and save
# ============================================================
print(f"\n{'='*60}")
print(f"TOTAL NEW ENTRIES: {len(all_new)}")
print(f"TOTAL CACHE WILL BE: {len(existing) + len(all_new)}")

# Year breakdown of new entries
years = Counter()
for e in all_new:
    slug = e["url"].split("/article/")[-1].lower()
    for pat in [r"q[1-4]-(\d{4})", r"f\d?q\d+-(\d{4})", r"discusses.*q[1-4].*(\d{4})"]:
        m = re.search(pat, slug)
        if m:
            years[int(m.group(1))] += 1
            break

print("\nNew entries by article year:")
for yr in sorted(years):
    print(f"  {yr}: {years[yr]}")

# Merge and save
if all_new:
    combined = existing + all_new
    tmp = CDX_CACHE_PATH.with_suffix(".json.tmp")
    tmp.write_bytes(json.dumps(combined).encode())
    os.replace(tmp, CDX_CACHE_PATH)
    print(f"\nCache updated: {len(combined)} entries")
else:
    print("\nNo new entries found")
