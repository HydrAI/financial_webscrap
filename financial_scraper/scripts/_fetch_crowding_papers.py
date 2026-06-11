#!/usr/bin/env python3
"""Fetch papers on CROWDING in equity factors / strategies.

Topic: strategy & factor crowding in equities - capacity decay of anomalies,
crowded trades and correlated unwinds (the Aug-2007 "quant quake"), measuring
crowding (short interest, institutional/hedge-fund overlap, ownership breadth,
flows, valuation spreads), arbitrage capacity & limits to arbitrage, factor
return decay post-publication, crowding risk premia and tail/unwind risk.

Reuses the query-parameterized fetchers in _fetch_research_papers.py.

Output:
    research_papers/crowding_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/crowding_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_crowding_papers.py
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
    ("crowded trade", "stock"), ("strategy crowding", "returns"),
    ("factor crowding", "equity"), ("crowding", "arbitrage"),
    ("quant crisis", "equity"), ("crowded", "hedge fund"),
    ("arbitrage capacity", "anomaly"), ("limits to arbitrage", "stock returns"),
    ("anomaly decay", "publication"), ("factor", "capacity"),
    ("liquidation", "fire sale"), ("deleveraging", "stock returns"),
    ("crowding", "momentum crash"), ("short interest", "crowding"),
    ("ownership breadth", "stock returns"),
]

OPENALEX_QUERIES = [
    "crowded trades and stock returns",
    "strategy crowding hedge funds equity",
    "factor crowding capacity anomaly decay",
    "limits to arbitrage anomaly returns",
    "arbitrage capacity crowding stock anomalies",
    "quant crisis August 2007 quant meltdown",
    "crowded trade unwind fire sale liquidation stocks",
    "does academic research destroy stock return predictability",
    "anomaly returns post-publication decay arbitrage",
    "hedge fund crowding correlated trades equity",
    "short interest crowding stock returns",
    "breadth of ownership and stock returns",
    "institutional herding crowding price impact",
    "momentum crashes crowding deleveraging",
    "valuation spreads factor timing crowding",
    "common ownership crowding comovement returns",
    "smart money flows capacity constraints anomalies",
]

# A CROWDING / capacity / arbitrage-limit term ...
CROWD_RE = (
    r"crowd\w*|crowded trade|quant (?:crisis|meltdown|quake)|"
    r"capacity (?:constraint|of|decay|limit)|arbitrage capacity|"
    r"limits? to arbitrage|anomaly decay|"
    r"(?:return |alpha )?(?:predictability )?(?:decay|decline|disappear|attenuat)|"
    r"post[- ]publication|out[- ]of[- ]sample decay|"
    r"fire[- ]?sale|deleverag|liquidation spiral|unwind|"
    r"correlated (?:trading|liquidation|unwind)|"
    r"valuation spread|ownership breadth|breadth of ownership|"
    r"short interest|comovement|crash risk"
)
# ... AND an equity / anomaly / strategy-returns context ...
EQUITY_RE = (
    r"stock return|\bequit|cross[- ]section of (?:stock )?returns|expected return|"
    r"\banomal|factor (?:return|premi|investing|model)|momentum|value (?:premium|"
    r"factor|strateg)|hedge fund|arbitrage|portfolio|asset pric|smart beta|"
    r"trading strateg|abnormal return|excess return"
)
# ... minus the off-domain / non-finance "crowd"/"capacity" senses.
OFF_RE = (
    r"crowdsourc|crowdfund|crowd (?:counting|density|simulation|behavior modeling)|"
    r"pedestrian|traffic flow|wireless|spectrum|network capacity|server|data center|"
    r"channel capacity|battery|reservoir|patient|\bdisease\b|biomedical|crop|soil|"
    r"\bimage\b|video|social media crowd|disaster|evacuat|carrying capacity|"
    r"ecolog|species|habitat"
)


def crowd_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    keep = (blob.str.contains(CROWD_RE, regex=True)
            & blob.str.contains(EQUITY_RE, regex=True)
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
    print("Fetching arXiv (factor / strategy crowding)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (factor / strategy crowding)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "crowding")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"crowding_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = crowd_filter(df)
    cpath = OUT_DIR / f"crowding_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  crowding-relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 15 papers (by citations) ===")
    for _, r in clean.head(15).iterrows():
        t = str(r.title).encode("ascii", "replace").decode()[:88]
        print(f"  [{int(r.citations):5d}] {t} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
