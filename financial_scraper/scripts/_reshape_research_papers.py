"""Reshape the research-paper corpora into chunked KG 8-col parquets.

Non-destructive: processes the *_fulltext.parquet corpora (full PDF text where
available, else abstract) and writes to a DEDICATED directory
(kg_input_research_papers/<corpus>/), so kg_input/ is never touched.

Reuses explode_chunks from _reshape_to_merged (5000-char paragraph/sentence-
aware chunks), partitioned by year. The abstract stays in the source
*_fulltext.parquet; here `full_text` (PDF text or abstract fallback) is chunked.

Output:
    kg_input_research_papers/ml_futures/merged_{YYYY}.parquet
    kg_input_research_papers/cta_trend/merged_{YYYY}.parquet

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_reshape_research_papers.py
"""

import functools
import glob
import importlib.util
from pathlib import Path

import pandas as pd

print = functools.partial(print, flush=True)  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"
OUT = ROOT / "kg_input_research_papers"

_spec = importlib.util.spec_from_file_location(
    "_reshape_to_merged", Path(__file__).with_name("_reshape_to_merged.py")
)
_rm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_rm)
explode_chunks = _rm.explode_chunks
TARGET_COLS = _rm.TARGET_COLS


def latest(pattern: str) -> str | None:
    fs = sorted(glob.glob(str(PAPERS / pattern)))
    return fs[-1] if fs else None


def reshape_one(input_path: str, corpus: str):
    df = pd.read_parquet(input_path)
    for c in TARGET_COLS:
        if c not in df.columns:
            df[c] = "" if c != "date" else pd.NaT
    out = explode_chunks(df[TARGET_COLS])

    out_dir = OUT / corpus
    out_dir.mkdir(parents=True, exist_ok=True)
    years = out["date"].dt.year
    undated = years.isna()
    rows = 0
    if undated.any():
        out[undated].to_parquet(out_dir / "merged_undated.parquet", index=False)
        rows += int(undated.sum())
    for year, part in out[~undated].groupby(years[~undated]):
        part.to_parquet(out_dir / f"merged_{int(year)}.parquet", index=False)
    print(f"  {corpus:12s}: {len(df):4d} papers -> {len(out):6d} chunks "
          f"({out['date'].notna().sum()} dated) -> {out_dir}")
    return len(df), len(out)


def main():
    corpora = [
        (PAPERS / "papers_tierB_futures_fulltext.parquet", "ml_futures"),
        (latest("cta_trend_papers_*_fulltext.parquet"), "cta_trend"),
        (latest("equity_sentiment_papers_*_fulltext.parquet"), "equity_sentiment"),
        (latest("inst_ownership_papers_*_fulltext.parquet"), "inst_ownership"),
        (latest("factor_conditioning_papers_*_fulltext.parquet"), "factor_conditioning"),
    ]
    print(f"Reshaping research-paper corpora -> {OUT}")
    tot_p = tot_c = 0
    for path, corpus in corpora:
        if not path or not Path(path).exists():
            print(f"  SKIP (missing): {corpus}")
            continue
        p, c = reshape_one(str(path), corpus)
        tot_p += p
        tot_c += c
    print(f"\nDone. {tot_p} papers -> {tot_c} chunks in {OUT}")


if __name__ == "__main__":
    main()
