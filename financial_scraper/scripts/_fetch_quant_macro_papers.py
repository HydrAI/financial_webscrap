#!/usr/bin/env python3
"""Fetch Systematic / Quant Macro model research papers (arXiv + OpenAlex).

Topic: systematic global macro and quantitative cross-asset strategies — macro
factor investing, cross-asset trend/carry/value/momentum, regime-switching and
business-cycle asset allocation, risk parity, currency carry, macro nowcasting.

Reuses the query-parameterized fetchers in _fetch_research_papers.py.

Output:
    research_papers/quant_macro_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/quant_macro_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_quant_macro_papers.py
"""

import argparse
import importlib.util
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "research_papers"

_spec = importlib.util.spec_from_file_location(
    "_fetch_research_papers", Path(__file__).with_name("_fetch_research_papers.py")
)
_frp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_frp)

ARXIV_PAIRS = [
    ("global macro", "strategy"), ("systematic macro", "trading"),
    ("quantitative macro", "investing"), ("macro factor", "asset allocation"),
    ("cross-asset", "momentum"), ("cross-asset", "carry"),
    ("trend following", "asset allocation"), ("regime switching", "asset allocation"),
    ("tactical asset allocation", "returns"), ("macroeconomic", "asset returns"),
    ("nowcasting", "financial markets"), ("risk parity", "multi-asset"),
    ("multi-asset", "factor"), ("business cycle", "asset returns"),
    ("carry", "currency"),
]

OPENALEX_QUERIES = [
    "systematic global macro strategy returns",
    "quantitative macro investing multi-asset",
    "cross-asset momentum carry value",
    "macro factor investing asset allocation",
    "regime switching tactical asset allocation",
    "macroeconomic predictors stock bond returns",
    "currency carry trade strategy returns",
    "risk parity multi-asset portfolio",
    "macroeconomic nowcasting financial markets",
    "business cycle dynamic asset allocation",
    "trend following multi-asset managed futures macro",
    "regime detection portfolio allocation machine learning",
]

# Require a SYSTEMATIC-STRATEGY signal (not the bare word "macroeconomic", which
# pulls in general macro/corporate finance) AND a returns/strategy context, minus
# general macroeconomics.
STRAT_RE = (
    r"global macro|systematic macro|quant(?:itative)? macro|macro factor|"
    r"macro strateg|cross[- ]asset|multi[- ]asset|risk parity|"
    r"tactical asset allocation|trend[- ]following|time[- ]series momentum|"
    r"carry trade|managed futures|factor (?:timing|investing)|"
    r"momentum (?:strateg|portfolio|everywhere)|dynamic asset allocation|"
    r"regime[- ]switch\w*|currency carry"
)
CTX_RE = (
    r"portfolio|return|sharpe|allocation|trading strateg|backtest|long[- ]short|"
    r"hedge fund|excess return|risk[- ]adjusted|investment strateg|predictab"
)
OFF_RE = (
    r"\bdsge\b|monetary policy|forward guidance|liquidity trap|fiscal|"
    r"inflation dynamics|capital structure|small business|market power|"
    r"organizational|household debt|sovereign debt|central bank communicat|"
    r"gdp growth|labor market|housing market spill|corporate investment|"
    r"heterogeneous (?:agent|household)"
)


def macro_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    keep = (blob.str.contains(STRAT_RE, regex=True)
            & blob.str.contains(CTX_RE, regex=True)
            & ~blob.str.contains(OFF_RE, regex=True))
    return df[keep].sort_values("citations", ascending=False).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-arxiv", type=int, default=90)
    ap.add_argument("--max-openalex", type=int, default=600)
    ap.add_argument("--arxiv-delay", type=float, default=5.0)
    ap.add_argument("--openalex-delay", type=float, default=1.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching arXiv (systematic / quant macro)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (systematic / quant macro)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "quant_macro")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"quant_macro_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = macro_filter(df)
    cpath = OUT_DIR / f"quant_macro_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  quant-macro relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 15 papers (by citations) ===")
    for _, r in clean.head(15).iterrows():
        print(f"  [{int(r.citations):5d}] {str(r.title)[:88]} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
