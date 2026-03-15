"""BigQuery patent data fetching utilities.

Queries the ``patents-public-data.patents.publications`` public dataset
by assignee name. Requires ``google-cloud-bigquery`` (install with
``pip install financial-scraper[bigquery]``).
"""

import csv
import logging
import re
from pathlib import Path

from .google_patents import PatentDetail
from .normalize import normalize_assignee, are_same_assignee

logger = logging.getLogger(__name__)

# ── CSV loading ──────────────────────────────────────────────────────────

def load_companies_from_csv(csv_path: Path, column: str = "name") -> list[dict]:
    """Read a CSV and return rows as dicts. *column* must exist."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        if column not in (reader.fieldnames or []):
            raise ValueError(
                f"Column '{column}' not found in {path}. "
                f"Available: {reader.fieldnames}"
            )
        rows = list(reader)

    logger.info(f"Loaded {len(rows)} companies from {path} (column='{column}')")
    return rows


# ── Assignee pattern helpers ─────────────────────────────────────────────

def build_assignee_patterns(company_name: str) -> list[str]:
    """Generate SQL ``UPPER(a.name) LIKE ...`` patterns for a company.

    Strips corporate suffixes via :func:`normalize_assignee`, uppercases,
    and wraps in ``%...%`` wildcards.
    """
    norm = normalize_assignee(company_name).upper()
    if not norm:
        return []

    # Remove punctuation that would confuse LIKE
    norm = re.sub(r"[^A-Z0-9 ]", "", norm).strip()
    if not norm:
        return []

    # Single pattern: %NORMALIZED_NAME%
    return [f"%{norm}%"]


# ── SQL query builder ────────────────────────────────────────────────────

_BASE_COLUMNS = """\
  publication_number,
  filing_date,
  grant_date,
  (SELECT text FROM UNNEST(title_localized) WHERE language = 'en' LIMIT 1) AS title_en,
  (SELECT text FROM UNNEST(abstract_localized) WHERE language = 'en' LIMIT 1) AS abstract_en,
  (SELECT text FROM UNNEST(claims_localized) WHERE language = 'en' LIMIT 1) AS claims_en,
  assignee_harmonized,
  inventor_harmonized,
  cpc,
  ipc"""

_DESCRIPTION_COLUMN = """,
  (SELECT text FROM UNNEST(description_localized) WHERE language = 'en' LIMIT 1) AS description_en"""


def build_query(
    companies: list[str],
    *,
    granted_only: bool = True,
    include_description: bool = False,
    country: str = "US",
) -> str:
    """Build a BigQuery SQL string that finds patents for *companies*.

    Parameters
    ----------
    companies:
        Company names (raw — will be normalized internally).
    granted_only:
        If True, restrict to ``grant_date > 0``.
    include_description:
        If True, also SELECT the full description text (large!).
    country:
        ISO-2 country code filter (default ``US``).
    """
    # Collect all LIKE clauses
    like_clauses: list[str] = []
    for name in companies:
        patterns = build_assignee_patterns(name)
        for pat in patterns:
            escaped = pat.replace("'", "\\'")
            like_clauses.append(f"UPPER(a.name) LIKE '{escaped}'")

    if not like_clauses:
        raise ValueError("No valid company names to query")

    assignee_condition = " OR ".join(like_clauses)

    columns = _BASE_COLUMNS
    if include_description:
        columns += _DESCRIPTION_COLUMN

    where_parts = [f"country_code = '{country}'"]
    if granted_only:
        where_parts.append("grant_date > 0")

    where_clause = " AND ".join(where_parts)

    return f"""\
SELECT
{columns}
FROM `patents-public-data.patents.publications`
WHERE {where_clause}
  AND EXISTS (
    SELECT 1 FROM UNNEST(assignee_harmonized) AS a
    WHERE {assignee_condition}
  )
"""


# ── Row → PatentDetail mapping ───────────────────────────────────────────

def _int_date_to_str(d: int | None) -> str:
    """Convert BigQuery INT64 date ``YYYYMMDD`` to ``YYYY-MM-DD``."""
    if not d or d <= 0:
        return ""
    s = str(d)
    if len(s) != 8:
        return ""
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def _format_publication_number(pub_num: str) -> str:
    """Reformat ``US-9275645-B2`` → ``US9275645B2``."""
    return pub_num.replace("-", "")


def _extract_cpc_codes(cpc_list) -> list[str]:
    """Extract CPC codes from BigQuery REPEATED RECORD."""
    if not cpc_list:
        return []
    codes = []
    for entry in cpc_list:
        code = entry.get("code", "") if isinstance(entry, dict) else ""
        if code:
            codes.append(code)
    return codes


def _extract_ipc_codes(ipc_list) -> list[str]:
    """Extract IPC codes from BigQuery REPEATED RECORD."""
    if not ipc_list:
        return []
    codes = []
    for entry in ipc_list:
        code = entry.get("code", "") if isinstance(entry, dict) else ""
        if code:
            codes.append(code)
    return codes


def _extract_inventor_names(inventor_list) -> list[str]:
    """Extract inventor names from BigQuery REPEATED RECORD."""
    if not inventor_list:
        return []
    names = []
    for entry in inventor_list:
        name = entry.get("name", "") if isinstance(entry, dict) else ""
        if name:
            names.append(name)
    return names


def _extract_first_assignee(assignee_list) -> str:
    """Extract the first assignee name from BigQuery REPEATED RECORD."""
    if not assignee_list:
        return ""
    first = assignee_list[0]
    return first.get("name", "") if isinstance(first, dict) else ""


def bq_row_to_patent_detail(
    row,
    matched_company: str,
    include_description: bool = False,
) -> PatentDetail:
    """Map a BigQuery result row to a :class:`PatentDetail`.

    Parameters
    ----------
    row:
        A BigQuery Row object (dict-like).
    matched_company:
        The company name this patent was matched to.
    include_description:
        Whether the query included the description column.
    """
    pub_num = row.get("publication_number", "") or ""
    patent_id = _format_publication_number(pub_num)

    title = row.get("title_en", "") or ""
    abstract = row.get("abstract_en", "") or ""
    claims = row.get("claims_en", "") or ""
    description = row.get("description_en", "") or "" if include_description else ""

    # Build full_text: claims (+ description if requested), fallback to abstract
    text_parts = [p for p in [claims, description] if p]
    full_text = "\n\n".join(text_parts) if text_parts else abstract

    filing_date = _int_date_to_str(row.get("filing_date"))
    grant_date = _int_date_to_str(row.get("grant_date"))

    assignee = _extract_first_assignee(row.get("assignee_harmonized"))

    return PatentDetail(
        patent_id=patent_id,
        url=f"https://patents.google.com/patent/{patent_id}/en",
        title=title,
        abstract=abstract,
        patent_number=patent_id,
        application_number="",
        date_filed=filing_date,
        date_granted=grant_date,
        assignee=assignee or matched_company,
        inventors=_extract_inventor_names(row.get("inventor_harmonized")),
        classifications_cpc=_extract_cpc_codes(row.get("cpc")),
        classifications_ipc=_extract_ipc_codes(row.get("ipc")),
        citations_backward=[],
        citations_forward=[],
        non_patent_citations=[],
        pdf_url="",
        full_text=full_text,
        expiration_date="",
    )


# ── Company matching ─────────────────────────────────────────────────────

def match_company(assignee_name: str, company_names: list[str]) -> str:
    """Find the best matching company for an assignee name.

    Uses :func:`are_same_assignee` from ``normalize.py``.
    Returns the matched company name, or empty string if no match.
    """
    if not assignee_name:
        return ""
    for name in company_names:
        if are_same_assignee(assignee_name, name):
            return name
    # Fallback: substring match on normalized names
    norm_assignee = normalize_assignee(assignee_name).upper()
    for name in company_names:
        norm_company = normalize_assignee(name).upper()
        if norm_company and norm_company in norm_assignee:
            return name
    return ""
