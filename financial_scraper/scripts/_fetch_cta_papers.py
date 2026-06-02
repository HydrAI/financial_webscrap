#!/usr/bin/env python3
"""Fetch CTA / Managed-Futures / Trend-Following research papers (arXiv + OpenAlex).

Reuses the query-parameterized fetchers in _fetch_research_papers.py with a
topic-specific query set and a CTA/trend-following relevance filter.

Output:
    research_papers/cta_trend_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/cta_trend_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_cta_papers.py
"""

import argparse
import importlib.util
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "research_papers"

# Reuse the API fetchers from the sibling module (underscore file → load by path).
_spec = importlib.util.spec_from_file_location(
    "_fetch_research_papers", Path(__file__).with_name("_fetch_research_papers.py")
)
_frp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_frp)

# arXiv: (phrase A) AND (phrase B) per query.
ARXIV_PAIRS = [
    ("trend following", "futures"), ("trend following", "commodity"),
    ("trend following", "returns"), ("trend-following", "futures"),
    ("time series momentum", "futures"), ("time series momentum", "returns"),
    ("cross-sectional momentum", "futures"), ("momentum strategy", "futures"),
    ("managed futures", "returns"), ("managed futures", "hedge fund"),
    ("managed futures", "performance"), ("commodity trading advisor", "returns"),
    ("commodity trading advisor", "performance"), ("systematic trading", "futures"),
    ("trend following", "crisis alpha"),
]

OPENALEX_QUERIES = [
    "managed futures returns",
    "commodity trading advisor performance",
    "trend following strategy futures",
    "time series momentum futures",
    "trend following hedge funds",
    "managed futures diversification portfolio",
    "CTA commodity trading advisor replication",
    "trend following commodity futures returns",
    "systematic trend following strategy",
    "cross-sectional momentum futures markets",
    "managed futures crisis alpha",
    "trend following risk premium",
]

# Relevance: a CTA/trend term AND a finance/futures context; drop physics/medical
# senses of 'momentum'/'CTA'.
CTA_RE = (
    r"trend[ -]following|managed futures|commodity trading advisor|"
    r"time[ -]series momentum|cross[ -]sectional momentum|momentum strateg|"
    r"systematic (?:trading|macro|trend)|crisis alpha|trend strateg|\bcta\b"
)
FIN_RE = (
    r"futures|hedge fund|trading|trader|return|portfolio|investment|"
    r"market|price|strateg|commodit|asset|risk premi|sharpe|drawdown|alpha"
)
OFF_RE = (
    r"\bphysic|particle|quantum|angular momentum|computed tomograph|"
    r"angiograph|\bmedical\b|patient|protein|transit authority"
)


def cta_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    keep = (blob.str.contains(CTA_RE, regex=True)
            & blob.str.contains(FIN_RE, regex=True)
            & ~blob.str.contains(OFF_RE, regex=True))
    return df[keep].sort_values("citations", ascending=False).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-arxiv", type=int, default=80)
    ap.add_argument("--max-openalex", type=int, default=600)
    ap.add_argument("--arxiv-delay", type=float, default=3.0)
    ap.add_argument("--openalex-delay", type=float, default=1.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching arXiv (CTA / trend following)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (CTA / managed futures)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "cta_trend")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"cta_trend_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = cta_filter(df)
    cpath = OUT_DIR / f"cta_trend_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  CTA-relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 15 CTA papers (by citations) ===")
    for _, r in clean.head(15).iterrows():
        print(f"  [{int(r.citations):5d}] {str(r.title)[:88]} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
