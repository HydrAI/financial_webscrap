"""Reshape the commodity data-source crawl into chunked KG 8-col parquets.

Non-destructive companion to _reshape_to_merged.py: it processes ONLY the
data_sources_crawl/ outputs and writes to a DEDICATED directory
(kg_input_data_sources/), so it never touches the main kg_input/ corpus.

Reuses the exact chunking from _reshape_to_merged (explode_chunks → 5000-char
paragraph/sentence-aware chunks), partitioned by year.

Output:
    kg_input_data_sources/merged_{YYYY}.parquet
    kg_input_data_sources/merged_undated.parquet

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_reshape_data_sources.py
"""

import functools
import importlib.util
import sys
from pathlib import Path

import pandas as pd

print = functools.partial(print, flush=True)  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parents[2]
CRAWL_DIR = ROOT / "data_sources_crawl"
OUT = ROOT / "kg_input_data_sources"

# Import the shared reshape helpers (explode_chunks, TARGET_COLS, CHUNK_CHARS)
# from the sibling module without making scripts/ a package.
_spec = importlib.util.spec_from_file_location(
    "_reshape_to_merged", Path(__file__).with_name("_reshape_to_merged.py")
)
_rm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_rm)
explode_chunks = _rm.explode_chunks
TARGET_COLS = _rm.TARGET_COLS
CHUNK_CHARS = _rm.CHUNK_CHARS


def find_category_parquets() -> list[Path]:
    """One crawl parquet per category (skip raw/ and seeds/)."""
    paths = []
    for p in sorted(CRAWL_DIR.glob("*/*/crawl_*.parquet")):
        if "raw" in p.parts or "seeds" in p.parts:
            continue
        paths.append(p)
    return paths


def main():
    if not CRAWL_DIR.exists():
        print(f"Crawl dir not found: {CRAWL_DIR}")
        sys.exit(1)

    paths = find_category_parquets()
    if not paths:
        print(f"No crawl parquets under {CRAWL_DIR}")
        sys.exit(1)

    print(f"Reshaping {len(paths)} category parquets -> {OUT}")
    OUT.mkdir(parents=True, exist_ok=True)

    # Accumulate chunked rows by year across all categories.
    by_year: dict[str, list[pd.DataFrame]] = {}
    total_in = total_chunks = 0

    for path in paths:
        category = path.parents[1].name
        df = pd.read_parquet(path)
        # Ensure the 8 target columns exist (crawl output already has them).
        for c in TARGET_COLS:
            if c not in df.columns:
                df[c] = "" if c != "date" else pd.NaT
        out = explode_chunks(df[TARGET_COLS])
        total_in += len(df)
        total_chunks += len(out)
        years = out["date"].dt.year
        undated = years.isna()
        if undated.any():
            by_year.setdefault("undated", []).append(out[undated])
        for year, part in out[~undated].groupby(years[~undated]):
            by_year.setdefault(str(int(year)), []).append(part)
        print(f"  {category:44s} {len(df):5d} rows -> {len(out):6d} chunks")

    print(f"\nWriting year-partitioned files...")
    rows_out = 0
    for year in sorted(by_year):
        df = pd.concat(by_year[year], ignore_index=True)
        name = "merged_undated.parquet" if year == "undated" else f"merged_{year}.parquet"
        df.to_parquet(OUT / name, index=False)
        rows_out += len(df)
        print(f"  {name:28s} {len(df):6d} rows")

    print(f"\nDone. {total_in:,} source rows -> {total_chunks:,} chunks "
          f"({rows_out:,} written) in {OUT}")


if __name__ == "__main__":
    main()
