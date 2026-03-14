"""Fallback patent lookup using a local PatentsView cache.

When Google Patents returns 503 (rate limit), this module looks up patent
metadata from a local cache built from PatentsView bulk TSV files hosted
on S3 (free, no auth required).

The cache is a parquet file containing patent_id, title, abstract, date,
assignee, and CPC codes — everything except claims and full description.
Build it once with:
    python scripts/bulk_patents.py --build-cache --cache-dir .patent_cache

Source: https://s3.amazonaws.com/data.patentsview.org/download/
  - g_patent.tsv.zip       (~219 MB) — title, abstract, date, type
  - g_assignee_disambiguated.tsv.zip (~342 MB) — assignees
  - g_cpc_current.tsv.zip  (~472 MB) — CPC codes
  - g_application.tsv.zip  (~68 MB)  — filing dates
"""

import logging
import re
from pathlib import Path

from .google_patents import PatentDetail

logger = logging.getLogger(__name__)

# Default cache location
DEFAULT_CACHE_DIR = Path(".patent_cache")

# Patent number extraction: US8776030B2 -> 8776030
_US_NUM_RE = re.compile(r"^US0*(\d+)")


def _extract_patent_number(patent_id: str) -> str:
    """US8776030B2 -> 8776030"""
    m = _US_NUM_RE.match(patent_id)
    return m.group(1) if m else ""


class PatentsViewCache:
    """Local cache of PatentsView bulk data for fast patent lookups."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR):
        self._cache_dir = cache_dir
        self._patents = None  # lazy-loaded DataFrame
        self._loaded = False

    @property
    def cache_file(self) -> Path:
        return self._cache_dir / "patentsview_cache.parquet"

    @property
    def available(self) -> bool:
        return self.cache_file.exists()

    def _load(self):
        """Lazy-load the cache into memory."""
        if self._loaded:
            return

        if not self.available:
            self._loaded = True
            return

        try:
            import pandas as pd
            self._patents = pd.read_parquet(
                self.cache_file,
                columns=[
                    "patent_id", "patent_title", "patent_abstract",
                    "patent_date", "patent_type",
                    "assignee", "filing_date", "cpc_codes",
                ],
            )
            # Index by patent_id for O(1) lookups
            self._patents = self._patents.set_index("patent_id")
            logger.info(
                f"PatentsView cache loaded: {len(self._patents)} patents "
                f"from {self.cache_file}"
            )
        except Exception as e:
            logger.warning(f"Failed to load PatentsView cache: {e}")
            self._patents = None

        self._loaded = True

    def lookup(self, patent_id: str) -> PatentDetail | None:
        """Look up a patent by ID from the local cache.

        Returns PatentDetail with title, abstract, dates, assignee, CPC.
        Returns None if patent not found or cache not available.
        """
        self._load()

        if self._patents is None:
            return None

        # Try with the numeric patent number (PatentsView uses numeric IDs)
        patent_number = _extract_patent_number(patent_id)
        if not patent_number:
            return None

        if patent_number not in self._patents.index:
            return None

        row = self._patents.loc[patent_number]

        # Handle potential duplicate rows (take first)
        if hasattr(row, "iloc"):
            row = row.iloc[0]

        title = str(row.get("patent_title", "") or "")
        abstract = str(row.get("patent_abstract", "") or "")
        grant_date = str(row.get("patent_date", "") or "")
        filing_date = str(row.get("filing_date", "") or "")
        assignee = str(row.get("assignee", "") or "")
        cpc_raw = str(row.get("cpc_codes", "") or "")

        detail = PatentDetail(patent_id=patent_id)
        detail.url = f"https://patents.google.com/patent/{patent_id}/en"
        detail.title = title
        detail.abstract = abstract
        detail.full_text = abstract  # cache only has abstract, not full text
        detail.date_granted = grant_date
        detail.date_filed = filing_date
        detail.assignee = assignee
        detail.classifications_cpc = [
            c.strip() for c in cpc_raw.split(";") if c.strip()
        ]

        return detail


# Module-level singleton (lazy-initialized)
_cache: PatentsViewCache | None = None


def fetch_patent_from_uspto(
    patent_id: str,
    session=None,
    throttler=None,
    timeout: int = 30,
) -> PatentDetail | None:
    """Look up a US patent from the local PatentsView cache.

    This is the fallback when Google Patents returns 503. It provides
    metadata (title, abstract, dates, assignee, CPC) but not full text.

    The cache must be built first by running:
        python scripts/bulk_patents.py --build-cache

    Args:
        patent_id: Patent ID (e.g. US8776030B2)
        session: Unused (kept for interface compatibility)
        throttler: Unused (kept for interface compatibility)
        timeout: Unused (kept for interface compatibility)

    Returns:
        PatentDetail on cache hit, None on miss or if cache not available.
    """
    global _cache

    if not patent_id.startswith("US"):
        return None

    if _cache is None:
        _cache = PatentsViewCache()

    if not _cache.available:
        # Only log this once
        if not getattr(fetch_patent_from_uspto, "_warned", False):
            logger.info(
                "No PatentsView cache found. Build one for fallback lookups:\n"
                "  python scripts/bulk_patents.py --build-cache --cache-dir .patent_cache"
            )
            fetch_patent_from_uspto._warned = True
        return None

    detail = _cache.lookup(patent_id)
    if detail and detail.has_data:
        logger.info(f"  Cache hit: {patent_id} - {detail.title[:60]}")
    elif detail:
        logger.info(f"  Cache hit (sparse): {patent_id}")
    else:
        logger.debug(f"  Cache miss: {patent_id}")

    return detail
