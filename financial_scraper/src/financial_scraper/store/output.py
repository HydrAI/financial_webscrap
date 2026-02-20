"""Parquet and JSON Lines writers with append mode.

Output schema matches C:\\_DATA\\doc_parquet\\merged_by_year format:
  company, title, link, snippet, date (timestamp), source, full_text, source_file
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Schema matching the merged_by_year parquet format
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


def _parse_date(date_str: str | None) -> pd.Timestamp | None:
    """Parse date string to pandas Timestamp for parquet timestamp[ns] column."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return pd.Timestamp(datetime.strptime(date_str, fmt))
        except ValueError:
            continue
    try:
        return pd.Timestamp(date_str)
    except Exception:
        return None


def make_source_file_tag(query: str, date_str: str | None, search_type: str) -> str:
    """Generate a source_file tag like 'queryslug_ddg_2025Q1.parquet'.

    Mirrors the naming convention in merged_by_year (e.g. themename0001_gnews_2025Q4).
    """
    # Slugify the query: lowercase, replace spaces/special with _
    slug = query.lower().strip()
    for ch in " /-,.;:()[]{}":
        slug = slug.replace(ch, "_")
    slug = "_".join(part for part in slug.split("_") if part)[:40]

    # Quarter from the article date or current date
    if date_str:
        ts = _parse_date(date_str)
        if ts:
            q = (ts.month - 1) // 3 + 1
            quarter_tag = f"{ts.year}Q{q}"
        else:
            quarter_tag = _current_quarter_tag()
    else:
        quarter_tag = _current_quarter_tag()

    if search_type == "crawl":
        mode = "crawl"
    elif search_type == "news":
        mode = "ddgnews"
    else:
        mode = "ddgtext"
    return f"{slug}_{mode}_{quarter_tag}.parquet"


def _current_quarter_tag() -> str:
    now = datetime.now()
    q = (now.month - 1) // 3 + 1
    return f"{now.year}Q{q}"


class ParquetWriter:
    """Append-mode Parquet writer (merged_by_year compatible)."""

    def __init__(self, path: Path):
        self._path = Path(path)

    def append(self, records: list[dict]):
        if not records:
            return
        # Ensure all fields present with defaults
        for r in records:
            for field in SCHEMA:
                if field.name not in r:
                    if field.type == pa.timestamp("ns"):
                        r[field.name] = None
                    else:
                        r[field.name] = ""

        # Build DataFrame so we can use pandas Timestamp for the date column
        df = pd.DataFrame(records)
        # Coerce date column to datetime
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)

        if self._path.exists():
            existing = pq.read_table(self._path)
            combined = pa.concat_tables([existing, table], promote_options="permissive")
            pq.write_table(combined, self._path, compression="snappy")
            logger.info(f"Appended {len(records)} rows to {self._path} (total: {len(combined)})")
        else:
            pq.write_table(table, self._path, compression="snappy")
            logger.info(f"Created {self._path} with {len(records)} rows")


class JSONLWriter:
    """Append-mode JSON Lines writer."""

    def __init__(self, path: Path):
        self._path = Path(path)

    def append(self, records: list[dict]):
        if not records:
            return
        with open(self._path, "a", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        logger.info(f"Appended {len(records)} records to {self._path}")
