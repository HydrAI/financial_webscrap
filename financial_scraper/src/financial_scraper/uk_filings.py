"""UK Companies House annual accounts downloader."""

import csv
import json
import logging
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
import trafilatura

logger = logging.getLogger(__name__)

BASE_URL = "https://api.company-information.service.gov.uk"
DOC_API = "https://frontend-doc-api.company-information.service.gov.uk"
CH_RATE_LIMIT = 0.5  # 600 req / 5 min


def _get_auth(api_key: str) -> tuple[str, str]:
    """Companies House uses HTTP Basic Auth: key as username, empty password."""
    return (api_key, "")


def _search_company(name: str, api_key: str) -> str | None:
    """Search for a company by name and return the company number."""
    r = requests.get(
        f"{BASE_URL}/search/companies",
        params={"q": name, "items_per_page": 5},
        auth=_get_auth(api_key),
        timeout=15,
    )
    time.sleep(CH_RATE_LIMIT)
    if r.status_code != 200:
        logger.warning(f"Company search failed for '{name}': HTTP {r.status_code}")
        return None

    items = r.json().get("items", [])
    if not items:
        return None

    # Try exact match first, then take first result
    name_lower = name.lower().strip()
    for item in items:
        if item.get("title", "").lower().strip() == name_lower:
            return item["company_number"]
    return items[0]["company_number"]


def _get_filing_history(company_number: str, api_key: str) -> list[dict]:
    """Get annual accounts filings for a company."""
    filings = []
    start_index = 0

    while True:
        r = requests.get(
            f"{BASE_URL}/company/{company_number}/filing-history",
            params={
                "category": "accounts",
                "items_per_page": 100,
                "start_index": start_index,
            },
            auth=_get_auth(api_key),
            timeout=15,
        )
        time.sleep(CH_RATE_LIMIT)
        if r.status_code != 200:
            break

        data = r.json()
        items = data.get("items", [])
        if not items:
            break

        for item in items:
            doc_links = item.get("links", {})
            if "document_metadata" not in doc_links:
                continue
            filings.append({
                "date": item.get("date", ""),
                "type": item.get("type", ""),
                "description": item.get("description", ""),
                "metadata_url": doc_links["document_metadata"],
                "paper_filed": item.get("paper_filed", False),
                "pages": item.get("pages", 0),
            })

        start_index += len(items)
        if start_index >= data.get("total_count", 0):
            break

    return filings


def _download_document(metadata_url: str, api_key: str) -> bytes | None:
    """Download a filing document (PDF) from Companies House."""
    url = metadata_url
    if not url.startswith("http"):
        url = f"{DOC_API}{url}"

    r = requests.get(
        url + "/content",
        auth=_get_auth(api_key),
        headers={"Accept": "application/pdf"},
        timeout=60,
        allow_redirects=True,
    )
    time.sleep(CH_RATE_LIMIT)
    if r.status_code == 200 and len(r.content) > 100:
        return r.content
    return None


def download_uk_filings(
    csv_path: Path,
    output_dir: Path,
    api_key: str,
    company_col: str = "name",
    company_number_col: str = "company_number",
    country_col: str = "",
    country_filter: str = "",
    limit: int = 0,
    skip: int = 0,
    max_filings_per_company: int = 0,
    resume: bool = False,
):
    """Download annual accounts from UK Companies House."""
    # Load companies
    companies: list[tuple[str, str]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        has_number = company_number_col in fieldnames
        for row in reader:
            # Country filter
            if country_col and country_filter:
                rc = row.get(country_col, "").strip().upper()
                if rc != country_filter.upper():
                    continue
            name = row.get(company_col, "").strip()
            number = row.get(company_number_col, "").strip() if has_number else ""
            if name:
                companies.append((name, number))
    if skip > 0:
        companies = companies[skip:]
    if limit > 0:
        companies = companies[:limit]

    logger.info(f"Processing {len(companies)} companies")

    # Setup output
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(exist_ok=True)
    parquet_path = output_dir / "uk_filings.parquet"

    # Resume
    checkpoint_path = output_dir / ".uk_filings_checkpoint.json"
    done_companies: set[str] = set()
    existing_records = []
    if resume and checkpoint_path.exists():
        with open(checkpoint_path, "r") as f:
            done_companies = set(json.load(f).get("done", []))
        if parquet_path.exists():
            existing_records = pd.read_parquet(parquet_path).to_dict("records")
        logger.info(f"Resumed: {len(done_companies)} companies already done")

    records = list(existing_records)
    total_new = 0

    for idx, (name, number) in enumerate(companies):
        company_key = number or name
        if company_key in done_companies:
            logger.info(f"[{idx+1}/{len(companies)}] Skipping {name} (already done)")
            continue

        logger.info(f"[{idx+1}/{len(companies)}] {name}")

        # Resolve company number if not provided
        if not number:
            number = _search_company(name, api_key)
            if not number:
                logger.warning(f"  Company not found: {name}")
                done_companies.add(company_key)
                continue
            logger.info(f"  Found company number: {number}")

        # Get filing history
        filings = _get_filing_history(number, api_key)
        if not filings:
            logger.info(f"  No annual accounts found")
            done_companies.add(company_key)
            continue

        if max_filings_per_company > 0:
            filings = filings[:max_filings_per_company]

        company_count = 0
        for filing in filings:
            pdf_bytes = _download_document(filing["metadata_url"], api_key)
            if not pdf_bytes:
                logger.warning(f"  {filing['date']} {filing['type']}: download failed")
                continue

            # Save PDF
            fname = f"{number}_{filing['date']}_{filing['type']}.pdf"
            fname = re.sub(r"[^\w\-.]", "_", fname)
            (raw_dir / fname).write_bytes(pdf_bytes)

            # Extract text (skip for paper-filed image PDFs — needs OCR)
            text = ""
            paper = filing.get("paper_filed", False)
            if not paper:
                try:
                    import pdfplumber
                    import io
                    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                        pages = [p.extract_text() or "" for p in pdf.pages]
                        text = "\n".join(pages)
                except Exception as e:
                    logger.warning(f"  PDF extraction failed: {e}")

            words = len(text.split()) if text else 0
            size_mb = len(pdf_bytes) / 1_048_576
            records.append({
                "company": name,
                "company_number": number,
                "filing_type": filing["type"],
                "filing_date": filing["date"],
                "description": filing["description"],
                "pages": filing.get("pages", 0),
                "paper_filed": paper,
                "pdf_size_mb": round(size_mb, 1),
                "full_text": text,
                "words": words,
            })
            company_count += 1
            total_new += 1
            note = " (paper-filed, needs OCR)" if paper and words == 0 else ""
            logger.info(f"  {filing['type']} {filing['date']}: {size_mb:.1f}MB, {words:,} words{note}")

        logger.info(f"  Downloaded {company_count} filings for {name}")

        # Checkpoint
        done_companies.add(company_key)
        with open(checkpoint_path, "w") as f:
            json.dump({"done": sorted(done_companies)}, f)
        pd.DataFrame(records).to_parquet(parquet_path)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("UK FILINGS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Companies processed: {len(done_companies)}")
    logger.info(f"Filings downloaded: {len(records)} ({total_new} new)")
    logger.info(f"Total words: {sum(r['words'] for r in records):,}")
    logger.info(f"Output: {parquet_path}")
    logger.info("=" * 60)
