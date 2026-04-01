"""SEC EDGAR 10-K / 20-F filing downloader."""

import csv
import json
import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests
import trafilatura

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "FinancialScraper/1.0 (research@example.com)",
    "Accept-Encoding": "gzip, deflate",
}

FORMS_WANTED = {"10-K", "20-F"}
SEC_RATE_LIMIT = 0.15  # 10 req/sec max


def _load_ticker_cik_map() -> dict[str, str]:
    """Fetch SEC ticker -> zero-padded CIK mapping."""
    logger.info("Fetching SEC ticker-CIK mapping...")
    r = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=HEADERS,
        timeout=15,
    )
    r.raise_for_status()
    mapping = {}
    for entry in r.json().values():
        mapping[entry["ticker"].upper()] = str(entry["cik_str"]).zfill(10)
    logger.info(f"Loaded {len(mapping)} ticker-CIK mappings")
    return mapping


def _get_filings(cik: str) -> list[dict]:
    """Get filing metadata from EDGAR submissions API."""
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    r = requests.get(url, headers=HEADERS, timeout=15)
    time.sleep(SEC_RATE_LIMIT)
    r.raise_for_status()
    data = r.json()

    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    filings = []
    for i, form in enumerate(forms):
        if form in FORMS_WANTED:
            acc_clean = accessions[i].replace("-", "")
            doc_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/{acc_clean}/{primary_docs[i]}"
            )
            filings.append({
                "form": form,
                "filing_date": dates[i],
                "accession": accessions[i],
                "primary_doc": primary_docs[i],
                "url": doc_url,
            })
    return filings


def download_sec_filings(
    csv_path: Path,
    output_dir: Path,
    company_col: str = "name",
    ticker_col: str = "ticker",
    limit: int = 0,
    skip: int = 0,
    max_filings_per_company: int = 0,
    resume: bool = False,
):
    """Download 10-K/20-F filings for companies in a CSV."""
    # Load companies
    companies: list[tuple[str, str]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get(company_col, "").strip()
            ticker = row.get(ticker_col, "").strip()
            if name and ticker:
                companies.append((name, ticker))
    if skip > 0:
        companies = companies[skip:]
    if limit > 0:
        companies = companies[:limit]

    logger.info(f"Processing {len(companies)} companies")

    # Setup output
    output_dir.mkdir(parents=True, exist_ok=True)
    html_dir = output_dir / "html"
    html_dir.mkdir(exist_ok=True)
    parquet_path = output_dir / "sec_filings.parquet"

    # Resume support
    checkpoint_path = output_dir / ".sec_filings_checkpoint.json"
    done_tickers: set[str] = set()
    existing_records = []
    if resume and checkpoint_path.exists():
        with open(checkpoint_path, "r") as f:
            done_tickers = set(json.load(f).get("done_tickers", []))
        if parquet_path.exists():
            existing_records = pd.read_parquet(parquet_path).to_dict("records")
        logger.info(f"Resumed: {len(done_tickers)} companies already done")

    # Load ticker -> CIK
    ticker_cik = _load_ticker_cik_map()

    records = list(existing_records)
    total_new = 0

    for idx, (name, ticker) in enumerate(companies):
        if ticker.upper() in done_tickers:
            logger.info(f"[{idx+1}/{len(companies)}] Skipping {ticker} (already done)")
            continue

        cik = ticker_cik.get(ticker.upper())
        if not cik:
            logger.warning(f"[{idx+1}/{len(companies)}] {ticker} ({name}): CIK not found")
            done_tickers.add(ticker.upper())
            continue

        logger.info(f"[{idx+1}/{len(companies)}] {ticker} - {name} (CIK: {cik})")

        try:
            filings = _get_filings(cik)
        except Exception as e:
            logger.warning(f"  Failed to get filings: {e}")
            continue

        if max_filings_per_company > 0:
            filings = filings[:max_filings_per_company]

        company_count = 0
        for filing in filings:
            try:
                r = requests.get(filing["url"], headers=HEADERS, timeout=30)
                time.sleep(SEC_RATE_LIMIT)
                if r.status_code != 200:
                    logger.warning(f"  {filing['form']} {filing['filing_date']}: HTTP {r.status_code}")
                    continue

                # Save raw HTML
                fname = f"{ticker}_{filing['form']}_{filing['filing_date']}.html"
                fname = re.sub(r"[^\w\-.]", "_", fname)
                (html_dir / fname).write_text(r.text, encoding="utf-8")

                # Extract text
                text = trafilatura.extract(
                    r.text, url=filing["url"],
                    include_comments=False, favor_precision=False,
                )
                words = len(text.split()) if text else 0

                records.append({
                    "company": name,
                    "ticker": ticker,
                    "form": filing["form"],
                    "filing_date": filing["filing_date"],
                    "url": filing["url"],
                    "full_text": text or "",
                    "words": words,
                })
                company_count += 1
                total_new += 1
                logger.info(f"  {filing['form']} {filing['filing_date']}: {words:,} words")

            except Exception as e:
                logger.warning(f"  {filing['form']} {filing['filing_date']}: {e}")

        logger.info(f"  Downloaded {company_count} filings for {ticker}")

        # Checkpoint after each company
        done_tickers.add(ticker.upper())
        with open(checkpoint_path, "w") as f:
            json.dump({"done_tickers": sorted(done_tickers)}, f)
        # Save parquet incrementally
        pd.DataFrame(records).to_parquet(parquet_path)

    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("SEC FILINGS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Companies processed: {len(done_tickers)}")
    logger.info(f"Filings downloaded: {len(records)} ({total_new} new)")
    logger.info(f"Total words: {sum(r['words'] for r in records):,}")
    logger.info(f"Output: {parquet_path}")
    logger.info("=" * 60)
