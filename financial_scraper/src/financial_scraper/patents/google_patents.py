"""Parse Google Patents pages using DC meta tags and itemprop attributes."""

import logging
import re
import time
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup

from ..fetch.throttle import SyncDomainThrottler

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

GOOGLE_PATENTS_DOMAIN = "patents.google.com"

# Regex to extract patent IDs from URLs
_PATENT_ID_RE = re.compile(r"/patent/([A-Z]{2}\d+[A-Z]*\d*)/")


@dataclass
class PatentDetail:
    """Structured patent data extracted from a Google Patents page."""

    patent_id: str
    url: str = ""
    title: str = ""
    abstract: str = ""
    patent_number: str = ""
    application_number: str = ""
    date_filed: str = ""
    date_granted: str = ""
    assignee: str = ""
    inventors: list[str] = field(default_factory=list)
    classifications_cpc: list[str] = field(default_factory=list)
    classifications_ipc: list[str] = field(default_factory=list)
    citations_backward: list[str] = field(default_factory=list)
    citations_forward: list[str] = field(default_factory=list)
    non_patent_citations: list[str] = field(default_factory=list)
    pdf_url: str = ""
    full_text: str = ""
    expiration_date: str = ""
    error: str = ""

    @property
    def has_data(self) -> bool:
        return bool(self.title and self.abstract)


def parse_patent_page(html: str, patent_id: str) -> PatentDetail:
    """Parse a Google Patents HTML page using DC meta tags and itemprop attributes.

    Layer 1 — DC meta tags (reliable, always server-rendered):
      DC.title, DC.description, DC.date, DC.contributor,
      citation_patent_number, citation_pdf_url, citation_patent_application_number,
      scheme="assignee", scheme="inventor", scheme="cpci", scheme="ipc",
      DC.relation, citation_reference

    Layer 2 — itemprop attributes (may be present in some pages):
      itemprop="title", itemprop="forward-citations",
      itemprop="expiration"
    """
    url = f"https://patents.google.com/patent/{patent_id}/en"
    result = PatentDetail(patent_id=patent_id, url=url)
    soup = BeautifulSoup(html, "html.parser")

    # --- Layer 1: DC meta tags ---

    # Title
    meta = soup.find("meta", {"name": "DC.title"})
    result.title = meta["content"].strip() if meta and meta.get("content") else ""

    # Abstract
    meta = soup.find("meta", {"name": "DC.description"})
    result.abstract = meta["content"].strip() if meta and meta.get("content") else ""

    # Patent number
    meta = soup.find("meta", {"name": "citation_patent_number"})
    result.patent_number = (
        meta["content"].strip() if meta and meta.get("content") else patent_id
    )

    # Application number
    meta = soup.find("meta", {"name": "citation_patent_application_number"})
    result.application_number = (
        meta["content"].strip() if meta and meta.get("content") else ""
    )

    # PDF URL
    meta = soup.find("meta", {"name": "citation_pdf_url"})
    result.pdf_url = meta["content"].strip() if meta and meta.get("content") else ""

    # Dates (DC.date appears twice: filing date then grant date)
    dates = [
        m["content"]
        for m in soup.find_all("meta", {"name": "DC.date"})
        if m.get("content")
    ]
    result.date_filed = dates[0] if len(dates) > 0 else ""
    result.date_granted = dates[1] if len(dates) > 1 else ""

    # Assignee (from scheme="assignee")
    meta = soup.find("meta", {"scheme": "assignee"})
    result.assignee = meta["content"].strip() if meta and meta.get("content") else ""

    # Inventors (from scheme="inventor")
    result.inventors = [
        m["content"]
        for m in soup.find_all("meta", {"scheme": "inventor"})
        if m.get("content")
    ]

    # If no inventors from scheme, extract from DC.contributor minus assignee
    if not result.inventors:
        contributors = [
            m["content"]
            for m in soup.find_all("meta", {"name": "DC.contributor"})
            if m.get("content")
        ]
        result.inventors = [c for c in contributors if c != result.assignee]

    # Classifications (CPC + IPC)
    result.classifications_cpc = [
        m["content"]
        for m in soup.find_all("meta", {"scheme": "cpci"})
        if m.get("content")
    ]
    result.classifications_ipc = [
        m["content"]
        for m in soup.find_all("meta", {"scheme": "ipc"})
        if m.get("content")
    ]

    # Backward citations (DC.relation = patents cited by this patent)
    result.citations_backward = [
        m["content"]
        for m in soup.find_all("meta", {"name": "DC.relation"})
        if m.get("content")
    ]

    # Non-patent citations
    result.non_patent_citations = [
        m["content"][:200]
        for m in soup.find_all("meta", {"name": "citation_reference"})
        if m.get("content")
    ]

    # --- Layer 2: itemprop attributes ---

    # Forward citations (patents that cite this patent)
    fwd_section = soup.find(attrs={"itemprop": "forward-citations"})
    if fwd_section:
        for link in fwd_section.find_all("a", href=True):
            m = _PATENT_ID_RE.search(link["href"])
            if m:
                result.citations_forward.append(m.group(1))

    # Expiration date
    exp_elem = soup.find(attrs={"itemprop": "expiration"})
    if exp_elem:
        result.expiration_date = exp_elem.get_text(strip=True)

    # Backup title from itemprop (if DC.title was empty)
    if not result.title:
        title_elem = soup.find(attrs={"itemprop": "title"})
        if title_elem:
            result.title = title_elem.get_text(strip=True)

    # Full page text for downstream analysis
    result.full_text = soup.get_text(separator="\n", strip=True)

    return result


def fetch_patent(
    patent_id: str,
    session: requests.Session,
    throttler: SyncDomainThrottler,
    timeout: int = 30,
) -> PatentDetail:
    """Fetch and parse a single patent page from Google Patents.

    Uses SyncDomainThrottler for rate limiting. Returns PatentDetail
    with error field set on failure.
    """
    url = f"https://patents.google.com/patent/{patent_id}/en"
    logger.info(f"  Fetching: {patent_id}")

    max_retries = 3
    resp = None
    for attempt in range(max_retries):
        throttler.acquire(GOOGLE_PATENTS_DOMAIN)
        try:
            resp = session.get(url, headers=HEADERS, timeout=timeout)
        except Exception as e:
            logger.warning(f"  Request failed for {patent_id}: {e}")
            return PatentDetail(patent_id=patent_id, url=url, error=str(e))
        finally:
            throttler.release(GOOGLE_PATENTS_DOMAIN)

        if resp.status_code == 200:
            throttler.report_success(GOOGLE_PATENTS_DOMAIN)
            break
        elif resp.status_code == 503 and attempt < max_retries - 1:
            wait = 10 * (2 ** attempt)  # 10s, 20s
            logger.info(
                f"  503 for {patent_id} — retry {attempt + 1}/{max_retries} in {wait}s"
            )
            throttler.report_failure(GOOGLE_PATENTS_DOMAIN, resp.status_code)
            time.sleep(wait)
        else:
            throttler.report_failure(GOOGLE_PATENTS_DOMAIN, resp.status_code)
            logger.warning(f"  HTTP {resp.status_code} for {patent_id}")
            return PatentDetail(
                patent_id=patent_id, url=url,
                error=f"HTTP {resp.status_code}",
            )

    detail = parse_patent_page(resp.text, patent_id)

    if detail.has_data:
        logger.info(f"  OK - {detail.title[:60]}")
    else:
        logger.warning(f"  SPARSE DATA for {patent_id}")

    return detail
