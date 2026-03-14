"""Bulk patent download from PatentsView S3 data tables.

Downloads free, no-auth TSV files from PatentsView's S3 bucket, joins them
into a unified dataset, filters by assignee / CPC / date, and outputs
parquet + JSONL in the same schema as the live pipeline.

Data source (free, no authentication):
  https://s3.amazonaws.com/data.patentsview.org/download/

Tables used:
  g_patent.tsv.zip       (~219 MB) — patent_id, title, abstract, date, type
  g_assignee_disambiguated.tsv.zip (~342 MB) — assignee organizations
  g_cpc_current.tsv.zip  (~472 MB) — CPC classification codes
  g_application.tsv.zip  (~68 MB)  — application/filing dates

Usage:
  # Download + filter by assignee
  python scripts/bulk_patents.py \\
    --assignee "NVIDIA" \\
    --output-dir runs/bulk_patents \\
    --parquet --jsonl

  # Filter by CPC codes and year range
  python scripts/bulk_patents.py \\
    --cpc-filter G06F H04L \\
    --date-from 2020-01-01 \\
    --assignee "Google" \\
    --output-dir runs/bulk_patents \\
    --parquet

  # Build a local cache for the live pipeline fallback
  python scripts/bulk_patents.py --build-cache --cache-dir .patent_cache

  # Use already-downloaded TSV files (skip download)
  python scripts/bulk_patents.py \\
    --download-dir /path/to/tsvs \\
    --assignee "Tesla" \\
    --output-dir runs/bulk_patents \\
    --parquet
"""

import argparse
import io
import json
import logging
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

S3_BASE = "https://s3.amazonaws.com/data.patentsview.org/download"

TABLES = {
    "g_patent": {
        "url": f"{S3_BASE}/g_patent.tsv.zip",
        "columns": [
            "patent_id", "patent_title", "patent_date",
            "patent_type", "patent_abstract", "num_claims",
        ],
    },
    "g_assignee": {
        "url": f"{S3_BASE}/g_assignee_disambiguated.tsv.zip",
        "columns": [
            "patent_id", "assignee_id", "assignee_organization",
            "assignee_individual_name_first", "assignee_individual_name_last",
        ],
    },
    "g_cpc_current": {
        "url": f"{S3_BASE}/g_cpc_current.tsv.zip",
        "columns": [
            "patent_id", "cpc_group_id", "cpc_category",
        ],
    },
    "g_application": {
        "url": f"{S3_BASE}/g_application.tsv.zip",
        "columns": ["patent_id", "filing_date"],
    },
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_table(name: str, dest_dir: Path) -> Path | None:
    """Download a PatentsView TSV zip from S3, return path to extracted TSV."""
    info = TABLES[name]
    url = info["url"]
    zip_name = url.rsplit("/", 1)[1]
    zip_path = dest_dir / zip_name
    tsv_path = dest_dir / zip_name.replace(".zip", "")

    # Already extracted?
    if tsv_path.exists():
        logger.info(f"  {name}: already extracted ({tsv_path.name})")
        return tsv_path

    # Already downloaded zip?
    if not zip_path.exists():
        logger.info(f"  Downloading {zip_name} ...")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=600, stream=True)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"  Download failed for {name}: {e}")
            return None

        size = 0
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                size += len(chunk)

        logger.info(f"  Downloaded: {zip_name} ({size / (1024*1024):.1f} MB)")

    # Extract
    logger.info(f"  Extracting {zip_name} ...")
    try:
        with zipfile.ZipFile(zip_path) as zf:
            members = [m for m in zf.namelist() if m.endswith(".tsv")]
            if not members:
                logger.error(f"  No TSV file found in {zip_name}")
                return None
            zf.extract(members[0], dest_dir)
            extracted = dest_dir / members[0]
            if extracted != tsv_path:
                extracted.rename(tsv_path)
    except Exception as e:
        logger.error(f"  Extraction failed for {zip_name}: {e}")
        return None

    logger.info(f"  Extracted: {tsv_path.name}")
    return tsv_path


# ---------------------------------------------------------------------------
# Loading + Joining
# ---------------------------------------------------------------------------

def load_patents(
    download_dir: Path,
    assignee_filter: str = "",
    cpc_filter: list[str] | None = None,
    date_from: str = "",
    date_to: str = "",
    limit: int = 0,
) -> pd.DataFrame:
    """Download, load, join, and filter PatentsView tables."""

    # 1. Download all tables
    logger.info("Downloading PatentsView tables from S3 ...")
    paths: dict[str, Path | None] = {}
    for name in TABLES:
        paths[name] = download_table(name, download_dir)

    if paths["g_patent"] is None:
        logger.error("Cannot proceed without g_patent table")
        return pd.DataFrame()

    # 2. Load g_patent (core table)
    logger.info("Loading g_patent ...")
    df = pd.read_csv(
        paths["g_patent"], sep="\t",
        dtype=str, na_values=[""],
        usecols=lambda c: c in TABLES["g_patent"]["columns"],
    )
    logger.info(f"  g_patent: {len(df)} rows")

    # 3. Filter by date range (early filter to reduce memory)
    if date_from:
        df = df[df["patent_date"] >= date_from]
    if date_to:
        df = df[df["patent_date"] <= date_to]

    if date_from or date_to:
        logger.info(f"  After date filter: {len(df)} rows")

    # 4. Join assignee table
    if paths["g_assignee"] is not None:
        logger.info("Loading g_assignee ...")
        df_asn = pd.read_csv(
            paths["g_assignee"], sep="\t",
            dtype=str, na_values=[""],
            usecols=lambda c: c in TABLES["g_assignee"]["columns"],
        )
        # Keep first assignee per patent (primary)
        df_asn = df_asn.drop_duplicates(subset=["patent_id"], keep="first")
        df = df.merge(
            df_asn[["patent_id", "assignee_organization"]],
            on="patent_id", how="left",
        )
        df.rename(columns={"assignee_organization": "assignee"}, inplace=True)
        logger.info(f"  Joined assignees: {df['assignee'].notna().sum()} patents have assignees")

        # Filter by assignee
        if assignee_filter:
            mask = df["assignee"].str.contains(
                assignee_filter, case=False, na=False
            )
            df = df[mask]
            logger.info(f"  After assignee filter '{assignee_filter}': {len(df)} rows")
    elif assignee_filter:
        logger.warning("g_assignee table not available — cannot filter by assignee")

    # 5. Join CPC table
    if paths["g_cpc_current"] is not None:
        logger.info("Loading g_cpc_current ...")
        df_cpc = pd.read_csv(
            paths["g_cpc_current"], sep="\t",
            dtype=str, na_values=[""],
            usecols=lambda c: c in TABLES["g_cpc_current"]["columns"],
        )
        # Aggregate CPC codes per patent into semicolon-separated string
        cpc_agg = (
            df_cpc.groupby("patent_id")["cpc_group_id"]
            .apply(lambda x: "; ".join(x.dropna().unique()))
            .reset_index()
            .rename(columns={"cpc_group_id": "cpc_codes"})
        )
        df = df.merge(cpc_agg, on="patent_id", how="left")
        logger.info(f"  Joined CPC codes: {df['cpc_codes'].notna().sum()} patents have CPC")

        # Filter by CPC prefix
        if cpc_filter:
            mask = df["cpc_codes"].apply(
                lambda codes: any(
                    c.startswith(prefix)
                    for prefix in cpc_filter
                    for c in str(codes).split("; ")
                ) if pd.notna(codes) else False
            )
            df = df[mask]
            logger.info(f"  After CPC filter {cpc_filter}: {len(df)} rows")
    elif cpc_filter:
        logger.warning("g_cpc_current table not available — cannot filter by CPC")

    # 6. Join application table (filing dates)
    if paths["g_application"] is not None:
        logger.info("Loading g_application ...")
        df_app = pd.read_csv(
            paths["g_application"], sep="\t",
            dtype=str, na_values=[""],
            usecols=lambda c: c in TABLES["g_application"]["columns"],
        )
        df_app = df_app.drop_duplicates(subset=["patent_id"], keep="first")
        df = df.merge(df_app, on="patent_id", how="left")
        logger.info(f"  Joined filing dates: {df['filing_date'].notna().sum()} patents")

    # 7. Sort by grant date (newest first) + limit
    df = df.sort_values("patent_date", ascending=False, na_position="last")
    if limit > 0:
        df = df.head(limit)
        logger.info(f"  Limit: top {limit} patents")

    logger.info(f"Final result: {len(df)} patents")
    return df


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def save_parquet_kg(df: pd.DataFrame, output_path: Path):
    """Save in the standard KG 8-column schema."""
    records = []
    for _, row in df.iterrows():
        abstract = str(row.get("patent_abstract", "") or "")
        snippet = (abstract[:300] + "...") if len(abstract) > 300 else abstract
        patent_id = str(row["patent_id"])
        records.append({
            "company": str(row.get("assignee", "") or ""),
            "title": str(row.get("patent_title", "") or ""),
            "link": f"https://patents.google.com/patent/US{patent_id}B2/en",
            "snippet": snippet,
            "date": str(row.get("patent_date", "") or ""),
            "source": "PatentsView S3",
            "full_text": abstract,
            "source_file": f"patent_US{patent_id}.parquet",
        })

    pd.DataFrame(records).to_parquet(output_path, index=False)
    logger.info(f"Parquet (KG schema): {output_path} ({len(records)} rows)")


def save_jsonl(df: pd.DataFrame, output_path: Path):
    """Save full metadata to JSONL."""
    with open(output_path, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            record = {
                "patent_id": f"US{row['patent_id']}",
                "title": str(row.get("patent_title", "") or ""),
                "abstract": str(row.get("patent_abstract", "") or ""),
                "url": f"https://patents.google.com/patent/US{row['patent_id']}B2/en",
                "assignee": str(row.get("assignee", "") or ""),
                "date_granted": str(row.get("patent_date", "") or ""),
                "date_filed": str(row.get("filing_date", "") or ""),
                "patent_type": str(row.get("patent_type", "") or ""),
                "num_claims": str(row.get("num_claims", "") or ""),
                "cpc_codes": str(row.get("cpc_codes", "") or ""),
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.info(f"JSONL: {output_path} ({len(df)} records)")


def save_cache(df: pd.DataFrame, cache_dir: Path):
    """Save a lookup-optimized parquet for the live pipeline fallback."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / "patentsview_cache.parquet"

    # Keep columns the fallback needs
    cols = [
        "patent_id", "patent_title", "patent_abstract", "patent_date",
        "patent_type", "assignee", "filing_date", "cpc_codes",
    ]
    existing = [c for c in cols if c in df.columns]
    cache_df = df[existing].copy()

    cache_df.to_parquet(cache_path, index=False)
    logger.info(
        f"Cache saved: {cache_path} ({len(cache_df)} patents, "
        f"{cache_path.stat().st_size / (1024*1024):.1f} MB)"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Bulk patent download from PatentsView S3 data tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Filters
    parser.add_argument(
        "--assignee",
        help="Filter by assignee (substring match, case-insensitive)",
    )
    parser.add_argument(
        "--cpc-filter", nargs="*",
        help="Filter by CPC prefix(es) (e.g. G06F H04L)",
    )
    parser.add_argument(
        "--date-from",
        help="Only patents granted on or after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--date-to",
        help="Only patents granted on or before this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max total patents to return (0 = unlimited)",
    )

    # Output
    parser.add_argument(
        "--output-dir", default=None,
        help="Output directory for parquet/JSONL results",
    )
    parser.add_argument("--parquet", action="store_true", help="Write parquet output")
    parser.add_argument("--jsonl", action="store_true", help="Write JSONL output")

    # Cache
    parser.add_argument(
        "--build-cache", action="store_true",
        help="Build a local cache for the live pipeline fallback",
    )
    parser.add_argument(
        "--cache-dir", default=".patent_cache",
        help="Directory for the fallback cache (default: .patent_cache)",
    )

    # Download
    parser.add_argument(
        "--download-dir", default=None,
        help="Directory to cache downloaded TSV files "
             "(default: <output-dir>/downloads or <cache-dir>/downloads)",
    )

    args = parser.parse_args()

    if not args.parquet and not args.jsonl and not args.build_cache:
        parser.error("Must specify at least one of --parquet, --jsonl, or --build-cache")

    if (args.parquet or args.jsonl) and not args.output_dir:
        parser.error("--output-dir is required when using --parquet or --jsonl")

    # Resolve directories
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    download_dir = Path(args.download_dir) if args.download_dir else (
        Path(args.output_dir) / "downloads" if args.output_dir
        else Path(args.cache_dir) / "downloads"
    )
    download_dir.mkdir(parents=True, exist_ok=True)

    # Load + filter
    df = load_patents(
        download_dir=download_dir,
        assignee_filter=args.assignee or "",
        cpc_filter=args.cpc_filter,
        date_from=args.date_from or "",
        date_to=args.date_to or "",
        limit=args.limit,
    )

    if df.empty:
        logger.warning("No patents matched the filters")
        sys.exit(0)

    # Save outputs
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.parquet:
        save_parquet_kg(df, output_dir / f"bulk_patents_{ts}.parquet")

    if args.jsonl:
        save_jsonl(df, output_dir / f"bulk_patents_{ts}.jsonl")

    if args.build_cache:
        save_cache(df, Path(args.cache_dir))

    # Summary
    assignees = df["assignee"].dropna().unique() if "assignee" in df.columns else []
    dates = df["patent_date"].dropna()

    logger.info("")
    logger.info("=" * 60)
    logger.info("BULK PATENT DOWNLOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"  Patents matched:      {len(df)}")
    logger.info(f"  Unique assignees:     {len(assignees)}")
    if len(dates) > 0:
        logger.info(f"  Date range:           {dates.min()} to {dates.max()}")
    if len(assignees) > 0 and len(assignees) <= 10:
        logger.info(f"  Assignees:            {', '.join(sorted(assignees))}")
    if args.output_dir:
        logger.info(f"  Output:               {args.output_dir}")
    if args.build_cache:
        logger.info(f"  Cache:                {args.cache_dir}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
