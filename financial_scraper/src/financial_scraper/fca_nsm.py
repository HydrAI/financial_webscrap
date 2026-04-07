"""FCA National Storage Mechanism (NSM) annual report downloader.

The NSM is the UK's official repository for regulated disclosures from
listed issuers under DTR 6.4 — Annual Financial Reports land here by law.
Unlike Companies House (which stores Companies Act statutory accounts,
often as scanned image PDFs), NSM carries the publisher-produced, text-
extractable annual reports.

The portal at https://data.fca.org.uk/#/nsm/nationalstoragemechanism is
a SPA backed by a public Elasticsearch-style endpoint that the portal's
JS bundle calls directly with no auth. We reuse that same endpoint.
"""

import csv
import io
import json
import logging
import re
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
import trafilatura

logger = logging.getLogger(__name__)

SEARCH_API = "https://api.data.fca.org.uk/search"
NSM_INDEX = "fca-nsm-searchdata"
ARTEFACT_BASE = "https://data.fca.org.uk/artefacts"

# Default headline filter value — FCA category covering annual reports
# (type_code FCA04). Matching on "headline" is a fuzzy ES text match, so
# this catches "Annual Financial Report", "Annual Report and Accounts",
# "Annual report - Guarantor", etc.
ANNUAL_REPORT_HEADLINE = "Annual Financial Report"

NSM_RATE_LIMIT = 0.3  # be polite to the FCA backend
PAGE_SIZE = 100
HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://data.fca.org.uk",
    "Referer": "https://data.fca.org.uk/",
    "User-Agent": "FinancialScraper/1.0 (research)",
}


def _search_nsm(
    company: str,
    headline: str = ANNUAL_REPORT_HEADLINE,
    lei: str = "",
    from_date: str = "",
    to_date: str = "",
    page_size: int = PAGE_SIZE,
    max_hits: int = 0,
) -> list[dict]:
    """Query the NSM search API, pagination-safe. Returns list of _source dicts."""
    criteria = []
    if company:
        criteria.append({"name": "company", "value": company})
    if lei:
        criteria.append({"name": "lei", "value": lei})
    if headline:
        criteria.append({"name": "headline", "value": headline})

    date_criteria = []
    if from_date or to_date:
        date_criteria.append({
            "name": "publication_date",
            "value": {"from": from_date or "01/01/2000",
                      "to": to_date or "31/12/2099"},
        })

    out: list[dict] = []
    frm = 0
    while True:
        body = {
            "from": frm,
            "size": page_size,
            "sort": "submitted_date",
            "sortorder": "desc",
            "criteriaObj": {"criteria": criteria, "dateCriteria": date_criteria},
        }
        try:
            r = requests.post(
                SEARCH_API,
                params={"index": NSM_INDEX},
                headers=HEADERS,
                data=json.dumps(body),
                timeout=30,
            )
        except Exception as e:
            logger.warning(f"  NSM search failed: {e}")
            return out
        time.sleep(NSM_RATE_LIMIT)
        if r.status_code != 200:
            logger.warning(f"  NSM search HTTP {r.status_code}: {r.text[:200]}")
            return out
        try:
            data = r.json()
        except Exception:
            logger.warning("  NSM search: non-JSON response")
            return out
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        for h in hits:
            src = h.get("_source", {})
            src["_id"] = h.get("_id", "")
            out.append(src)
        if max_hits and len(out) >= max_hits:
            return out[:max_hits]
        if len(hits) < page_size:
            break
        frm += page_size
    return out


def _name_matches(csv_name: str, hit_company: str) -> bool:
    """Loose post-hoc sanity check that a hit's `company` field plausibly
    matches the CSV row. The ES `company` criterion is a fuzzy text match,
    so "BP" returns unrelated ETFs — we defend against that here.
    """
    def norm(s: str) -> str:
        s = s.lower()
        s = re.sub(r"\b(plc|p\.l\.c\.?|ltd|limited|holdings|group|the)\b", "", s)
        s = re.sub(r"[^a-z0-9]+", " ", s).strip()
        return s

    a = norm(csv_name)
    b = norm(hit_company)
    if not a or not b:
        return False
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if not a_tokens:
        return False
    # Require the first (usually distinctive) token of the CSV name to
    # appear in the hit, plus at least 50% token overlap.
    if next(iter(a.split()), "") not in b_tokens:
        return False
    overlap = len(a_tokens & b_tokens) / len(a_tokens)
    return overlap >= 0.5


def _detect_kind(data: bytes) -> str:
    """Detect payload kind by magic bytes. Returns 'pdf', 'zip', 'html', 'bin'."""
    if data[:4] == b"%PDF":
        return "pdf"
    if data[:2] == b"PK":
        return "zip"
    head = data[:500].lstrip().lower()
    if head.startswith(b"<!doctype") or head.startswith(b"<html") or b"<html" in head:
        return "html"
    return "bin"


def _extract_zip_text(data: bytes) -> str:
    """Extract text from an ESEF iXBRL zip package.

    ESEF reports are XHTML files wrapped in a zip. We grab the largest
    .xhtml/.html member and run trafilatura on it.
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except Exception as e:
        logger.warning(f"  zip open failed: {e}")
        return ""
    candidates = [
        (zi.file_size, zi.filename) for zi in zf.infolist()
        if zi.filename.lower().endswith((".xhtml", ".html", ".htm"))
    ]
    if not candidates:
        return ""
    candidates.sort(reverse=True)
    try:
        with zf.open(candidates[0][1]) as fh:
            html = fh.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"  zip read failed: {e}")
        return ""
    # ESEF iXBRL reports are often 50-150 MB single xhtml files with ix:*
    # namespaces that confuse lxml.html and trafilatura. Use lxml.etree
    # in recovery mode, which handles XHTML with namespaces correctly.
    try:
        from lxml import etree
        parser = etree.XMLParser(recover=True, huge_tree=True)
        root = etree.fromstring(html.encode("utf-8"), parser=parser)
        if root is None:
            raise ValueError("empty root")
        # itertext() walks all text nodes regardless of namespace
        text = " ".join(t for t in root.itertext() if t and t.strip())
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            return text
    except Exception as e:
        logger.warning(f"  lxml.etree parse failed: {e}")
    # Final fallback: regex strip
    stripped = re.sub(r"<[^>]+>", " ", html)
    stripped = re.sub(r"\s+", " ", stripped).strip()
    return stripped


def _extract_pdf_text(data: bytes) -> str:
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages)
    except Exception as e:
        logger.warning(f"  PDF extraction failed: {e}")
        return ""


def _extract_html_text(data: bytes) -> str:
    try:
        html = data.decode("utf-8", errors="replace")
    except Exception:
        return ""
    return trafilatura.extract(
        html, include_comments=False, favor_precision=False,
    ) or ""


def _download(url: str, timeout: int = 60) -> bytes | None:
    try:
        r = requests.get(url, headers={"User-Agent": HEADERS["User-Agent"]},
                         timeout=timeout, allow_redirects=True)
    except Exception as e:
        logger.warning(f"  download failed: {e}")
        return None
    if r.status_code == 200 and len(r.content) > 100:
        return r.content
    logger.warning(f"  download HTTP {r.status_code}")
    return None


def download_fca_nsm(
    csv_path: Path,
    output_dir: Path,
    company_col: str = "company_name",
    lei_col: str = "",
    country_col: str = "",
    country_filter: str = "",
    headline: str = ANNUAL_REPORT_HEADLINE,
    from_date: str = "",
    to_date: str = "",
    limit: int = 0,
    skip: int = 0,
    max_filings_per_company: int = 0,
    resume: bool = False,
):
    """Download annual reports from the FCA NSM for companies in a CSV."""
    companies: list[tuple[str, str]] = []  # (name, lei)
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if country_col and country_filter:
                cc = row.get(country_col, "").strip().upper()
                if cc != country_filter.upper():
                    continue
            name = row.get(company_col, "").strip()
            lei = row.get(lei_col, "").strip() if lei_col else ""
            if name or lei:
                companies.append((name, lei))
    if skip > 0:
        companies = companies[skip:]
    if limit > 0:
        companies = companies[:limit]

    logger.info(f"Processing {len(companies)} companies")

    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(exist_ok=True)
    parquet_path = output_dir / "fca_nsm.parquet"

    checkpoint_path = output_dir / ".fca_nsm_checkpoint.json"
    done_keys: set[str] = set()
    existing_records: list[dict] = []
    if resume and checkpoint_path.exists():
        with open(checkpoint_path, "r") as f:
            done_keys = set(json.load(f).get("done", []))
        if parquet_path.exists():
            existing_records = pd.read_parquet(parquet_path).to_dict("records")
        logger.info(f"Resumed: {len(done_keys)} companies already done")

    records = list(existing_records)
    total_new = 0

    for idx, (name, lei) in enumerate(companies):
        key = (lei or name).upper()
        if key in done_keys:
            logger.info(f"[{idx+1}/{len(companies)}] Skipping {name or lei} (done)")
            continue

        logger.info(f"[{idx+1}/{len(companies)}] {name} {('('+lei+')') if lei else ''}")
        hits = _search_nsm(
            company=name if not lei else "",
            lei=lei,
            headline=headline,
            from_date=from_date,
            to_date=to_date,
            max_hits=max_filings_per_company or 0,
        )
        logger.info(f"  {len(hits)} hits")

        # Post-filter by name match when we didn't query by LEI
        if not lei and name:
            hits = [h for h in hits if _name_matches(name, h.get("company", ""))]
            logger.info(f"  {len(hits)} after name filter")

        if max_filings_per_company:
            hits = hits[:max_filings_per_company]

        company_count = 0
        for hit in hits:
            dl = hit.get("download_link", "")
            if not dl:
                continue
            url = f"{ARTEFACT_BASE}/{dl.lstrip('/')}"
            data = _download(url)
            time.sleep(NSM_RATE_LIMIT)
            if not data:
                continue

            kind = _detect_kind(data)
            ext = {"pdf": ".pdf", "zip": ".zip", "html": ".html"}.get(kind, ".bin")
            fname = f"{hit.get('_id','doc')}_{hit.get('document_date','')[:10]}{ext}"
            fname = re.sub(r"[^\w\-.]", "_", fname)
            (raw_dir / fname).write_bytes(data)

            if kind == "pdf":
                text = _extract_pdf_text(data)
            elif kind == "zip":
                text = _extract_zip_text(data)
            elif kind == "html":
                text = _extract_html_text(data)
            else:
                text = ""

            words = len(text.split()) if text else 0
            size_mb = len(data) / 1_048_576
            records.append({
                "csv_company": name,
                "csv_lei": lei,
                "hit_company": hit.get("company", ""),
                "hit_lei": hit.get("lei", ""),
                "disclosure_id": hit.get("_id", ""),
                "headline": hit.get("headline", ""),
                "type": hit.get("type", ""),
                "type_code": hit.get("type_code", ""),
                "document_date": hit.get("document_date", ""),
                "publication_date": hit.get("publication_date", ""),
                "source": hit.get("source", ""),
                "download_url": url,
                "file_kind": kind,
                "file_size_mb": round(size_mb, 2),
                "full_text": text,
                "words": words,
            })
            company_count += 1
            total_new += 1
            logger.info(
                f"  {hit.get('document_date','')[:10]} "
                f"{hit.get('type','')}: {size_mb:.1f}MB, {words:,} words"
            )

        logger.info(f"  Downloaded {company_count} reports for {name}")

        done_keys.add(key)
        with open(checkpoint_path, "w") as f:
            json.dump({"done": sorted(done_keys)}, f)
        if records:
            pd.DataFrame(records).to_parquet(parquet_path)

    logger.info("\n" + "=" * 60)
    logger.info("FCA NSM SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Companies processed: {len(done_keys)}")
    logger.info(f"Reports downloaded: {len(records)} ({total_new} new)")
    logger.info(f"Total words: {sum(r['words'] for r in records):,}")
    logger.info(f"Output: {parquet_path}")
    logger.info("=" * 60)
