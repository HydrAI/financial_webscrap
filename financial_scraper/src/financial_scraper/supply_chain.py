"""Supply-chain query generation from company CSV."""

import csv
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Suffixes to strip for cleaner search queries
_SUFFIX_RE = re.compile(
    r"\s*\b(Inc|Corp|Co|Ltd|Plc|LLC|LP|NV|SA|SE|AG|Group|Holdings?|International|Brands?|Enterprises?)\.?\s*$",
    re.IGNORECASE,
)

# Ticker-based queries — hit financial databases (CSIMarket, SEC, SeekingAlpha)
_TICKER_TEMPLATES = [
    '{ticker} supply chain',
    '{ticker} suppliers',
    '{ticker} customers',
    '{ticker} 10-K',
]

# Company-name queries — broader web coverage
_NAME_TEMPLATES = [
    '{company} supply chain',
    '{company} suppliers',
    '{company} customers revenue',
    '{company} annual report',
]


def _clean_company_name(name: str) -> str:
    """Strip legal suffixes for better search recall."""
    cleaned = _SUFFIX_RE.sub("", name).strip()
    # If stripping removed everything or left a single char, keep original
    return cleaned if len(cleaned) > 1 else name


def generate_supply_chain_queries(
    csv_path: Path,
    company_col: str = "name",
    ticker_col: str = "ticker",
    limit: int = 0,
    skip: int = 0,
) -> list[tuple[str, str, str]]:
    """Read CSV and generate supply-chain queries.

    Returns list of (company_name, ticker, query) tuples.
    Uses ticker-based queries for financial DB hits and
    company-name queries for broader web coverage.
    """
    companies: list[tuple[str, str]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if company_col not in reader.fieldnames:
            raise ValueError(
                f"Column '{company_col}' not found in CSV. "
                f"Available: {reader.fieldnames}"
            )
        for row in reader:
            name = row.get(company_col, "").strip()
            ticker = row.get(ticker_col, "").strip()
            if name:
                companies.append((name, ticker))

    # Apply skip/limit
    if skip > 0:
        companies = companies[skip:]
    if limit > 0:
        companies = companies[:limit]

    logger.info(f"Generating queries for {len(companies)} companies")

    results: list[tuple[str, str, str]] = []
    for name, ticker in companies:
        clean = _clean_company_name(name)
        # Ticker queries (skip if no ticker)
        if ticker:
            for template in _TICKER_TEMPLATES:
                query = template.format(ticker=ticker)
                results.append((name, ticker, query))
        # Name queries
        for template in _NAME_TEMPLATES:
            query = template.format(company=clean)
            results.append((name, ticker, query))

    per_company = len(_TICKER_TEMPLATES) + len(_NAME_TEMPLATES)
    logger.info(f"Generated {len(results)} queries (up to {per_company} per company)")
    return results


def write_queries_file(
    queries: list[tuple[str, str, str]],
    output_dir: Path,
) -> Path:
    """Write queries to a text file and return the path."""
    queries_path = output_dir / "queries_supply_chain.txt"
    with open(queries_path, "w", encoding="utf-8") as f:
        for _company, _ticker, query in queries:
            f.write(query + "\n")
    logger.info(f"Wrote {len(queries)} queries to {queries_path}")
    return queries_path
