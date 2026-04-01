"""EDINET (Japan) annual securities report downloader."""

import csv
import io
import json
import logging
import os
import re
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
import trafilatura

logger = logging.getLogger(__name__)

EDINET_API = "https://api.edinet-fsa.go.jp/api/v2"
EDINET_RATE_LIMIT = 1.0  # conservative
DOC_TYPE_YUHO = "120"  # Annual securities report (有価証券報告書)


def _scan_filings(
    api_key: str,
    scan_days: int = 730,
    cache_path: Path | None = None,
) -> list[dict]:
    """Scan EDINET date-by-date to build a filing index.

    Returns list of filing metadata dicts for annual reports.
    """
    # Try loading cache
    if cache_path and cache_path.exists():
        with open(cache_path, "r", encoding="utf-8") as f:
            cached = json.load(f)
        logger.info(f"Loaded {len(cached['filings'])} cached filings (scanned to {cached['last_date']})")
        return cached["filings"]

    filings = []
    today = date.today()
    start_date = today - timedelta(days=scan_days)
    current = start_date

    logger.info(f"Scanning EDINET filings from {start_date} to {today} ({scan_days} days)...")

    while current <= today:
        date_str = current.strftime("%Y-%m-%d")
        try:
            r = requests.get(
                f"{EDINET_API}/documents.json",
                params={
                    "date": date_str,
                    "type": 2,  # metadata only
                    "Subscription-Key": api_key,
                },
                timeout=15,
            )
            time.sleep(EDINET_RATE_LIMIT)

            if r.status_code != 200:
                current += timedelta(days=1)
                continue

            data = r.json()
            results = data.get("results", [])
            for doc in results:
                if doc.get("docTypeCode") == DOC_TYPE_YUHO:
                    filings.append({
                        "doc_id": doc.get("docID", ""),
                        "edinet_code": doc.get("edinetCode", ""),
                        "sec_code": doc.get("secCode", ""),
                        "filer_name": doc.get("filerName", ""),
                        "doc_description": doc.get("docDescription", ""),
                        "filing_date": doc.get("submitDateTime", "")[:10],
                        "period_start": doc.get("periodStart", ""),
                        "period_end": doc.get("periodEnd", ""),
                    })
        except Exception as e:
            logger.warning(f"Error scanning {date_str}: {e}")

        current += timedelta(days=1)

        # Progress every 100 days
        days_done = (current - start_date).days
        if days_done % 100 == 0:
            logger.info(f"  Scanned {days_done}/{scan_days} days, found {len(filings)} annual reports")

    logger.info(f"Scan complete: {len(filings)} annual reports found")

    # Cache results
    if cache_path:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump({"last_date": str(today), "filings": filings}, f, ensure_ascii=False)

    return filings


def _download_filing(doc_id: str, api_key: str) -> str:
    """Download a filing ZIP and extract text from HTML inside."""
    r = requests.get(
        f"{EDINET_API}/documents/{doc_id}",
        params={"type": 2, "Subscription-Key": api_key},  # type=2: PDF
        timeout=60,
    )
    time.sleep(EDINET_RATE_LIMIT)

    if r.status_code != 200 or len(r.content) < 100:
        # Fallback to type=1 (ZIP with XBRL/HTML)
        r = requests.get(
            f"{EDINET_API}/documents/{doc_id}",
            params={"type": 1, "Subscription-Key": api_key},
            timeout=60,
        )
        time.sleep(EDINET_RATE_LIMIT)

    if r.status_code != 200:
        return ""

    content_type = r.headers.get("content-type", "")

    # If PDF, extract with pdfplumber
    if "pdf" in content_type or r.content[:5] == b"%PDF-":
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                return "\n".join(pages)
        except Exception:
            return ""

    # If ZIP, find HTML inside and extract text
    if r.content[:2] == b"PK":
        try:
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                # Look for HTML files in the ZIP
                html_files = [
                    n for n in zf.namelist()
                    if n.endswith((".htm", ".html")) and "XBRL" not in n.upper()
                ]
                if not html_files:
                    html_files = [n for n in zf.namelist() if n.endswith((".htm", ".html"))]

                if html_files:
                    # Take the largest HTML file (usually the main document)
                    html_files.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
                    html_bytes = zf.read(html_files[0])
                    # Try common Japanese encodings
                    for enc in ("utf-8", "shift_jis", "euc-jp", "cp932"):
                        try:
                            html_str = html_bytes.decode(enc)
                            break
                        except (UnicodeDecodeError, LookupError):
                            continue
                    else:
                        html_str = html_bytes.decode("utf-8", errors="replace")

                    text = trafilatura.extract(html_str, favor_precision=False)
                    return text or ""
        except Exception as e:
            logger.warning(f"ZIP extraction failed for {doc_id}: {e}")
            return ""

    return ""


def download_edinet_filings(
    csv_path: Path,
    output_dir: Path,
    api_key: str,
    company_col: str = "name",
    ticker_col: str = "ticker",
    scan_days: int = 730,
    limit: int = 0,
    skip: int = 0,
    max_filings_per_company: int = 0,
    resume: bool = False,
):
    """Download annual securities reports from EDINET for Japanese companies."""
    # Load companies from CSV
    companies: list[tuple[str, str]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get(company_col, "").strip()
            ticker = row.get(ticker_col, "").strip()
            if name:
                companies.append((name, ticker))
    if skip > 0:
        companies = companies[skip:]
    if limit > 0:
        companies = companies[:limit]

    logger.info(f"Processing {len(companies)} companies")

    # Setup output
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "edinet_filings.parquet"
    cache_path = output_dir / ".edinet_scan_cache.json"

    # Resume
    checkpoint_path = output_dir / ".edinet_checkpoint.json"
    done_companies: set[str] = set()
    existing_records = []
    if resume and checkpoint_path.exists():
        with open(checkpoint_path, "r") as f:
            done_companies = set(json.load(f).get("done", []))
        if parquet_path.exists():
            existing_records = pd.read_parquet(parquet_path).to_dict("records")
        logger.info(f"Resumed: {len(done_companies)} companies already done")

    # Scan EDINET for all annual reports
    all_filings = _scan_filings(api_key, scan_days, cache_path)

    # Build index: sec_code -> filings, filer_name -> filings
    by_sec_code: dict[str, list[dict]] = {}
    by_name: dict[str, list[dict]] = {}
    for f in all_filings:
        sc = (f.get("sec_code") or "").strip()
        if sc:
            # SEC code is 5 digits; stock ticker is first 4
            by_sec_code.setdefault(sc[:4], []).append(f)
            by_sec_code.setdefault(sc, []).append(f)
        fn = (f.get("filer_name") or "").strip()
        if fn:
            by_name.setdefault(fn, []).append(f)

    records = list(existing_records)
    total_new = 0

    for idx, (name, ticker) in enumerate(companies):
        company_key = ticker or name
        if company_key in done_companies:
            logger.info(f"[{idx+1}/{len(companies)}] Skipping {name} (already done)")
            continue

        logger.info(f"[{idx+1}/{len(companies)}] {name} ({ticker})")

        # Match filings
        matched = by_sec_code.get(ticker, []) or by_name.get(name, [])
        if not matched:
            logger.info(f"  No filings found")
            done_companies.add(company_key)
            continue

        # Sort by filing date descending
        matched.sort(key=lambda x: x.get("filing_date", ""), reverse=True)
        if max_filings_per_company > 0:
            matched = matched[:max_filings_per_company]

        company_count = 0
        for filing in matched:
            doc_id = filing["doc_id"]
            logger.info(f"  Downloading {filing['filing_date']}: {filing['doc_description']}")

            text = _download_filing(doc_id, api_key)
            words = len(text.split()) if text else 0

            records.append({
                "company": name,
                "ticker": ticker,
                "edinet_code": filing.get("edinet_code", ""),
                "filer_name": filing.get("filer_name", ""),
                "doc_description": filing.get("doc_description", ""),
                "filing_date": filing.get("filing_date", ""),
                "period_start": filing.get("period_start", ""),
                "period_end": filing.get("period_end", ""),
                "doc_id": doc_id,
                "full_text": text,
                "words": words,
            })
            company_count += 1
            total_new += 1
            logger.info(f"    {words:,} words")

        logger.info(f"  Downloaded {company_count} filings for {name}")

        # Checkpoint
        done_companies.add(company_key)
        with open(checkpoint_path, "w") as f:
            json.dump({"done": sorted(done_companies)}, f)
        pd.DataFrame(records).to_parquet(parquet_path)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("EDINET FILINGS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Companies processed: {len(done_companies)}")
    logger.info(f"Filings downloaded: {len(records)} ({total_new} new)")
    logger.info(f"Total words: {sum(r['words'] for r in records):,}")
    logger.info(f"Output: {parquet_path}")
    logger.info("=" * 60)
