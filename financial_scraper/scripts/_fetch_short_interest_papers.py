#!/usr/bin/env python3
"""Fetch papers on SHORT INTEREST / short selling in equities.

Topic: the predictive and informational content of short selling in equities -
short interest ratio / days-to-cover as a return predictor, short sellers as
informed traders, price discovery, securities-lending market (loan fees,
utilization, supply/demand), short-sale constraints & overpricing (Miller
divergence-of-opinion), failures-to-deliver / naked shorting, short squeezes
(meme stocks), and short-selling regulation (Reg SHO, short-sale bans).

Reuses the query-parameterized fetchers in _fetch_research_papers.py.

Output:
    research_papers/short_interest_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/short_interest_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_short_interest_papers.py
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
    ("short interest", "stock returns"), ("short selling", "returns"),
    ("short sellers", "informed"), ("short-sale constraints", "stock"),
    ("securities lending", "returns"), ("days to cover", "stock"),
    ("short squeeze", "stock"), ("failures to deliver", "stock"),
    ("naked short selling", "equity"), ("short selling", "price discovery"),
    ("short sale ban", "stock returns"), ("stock loan", "fee"),
    ("short interest", "predictability"), ("divergence of opinion", "overpricing"),
    ("regulation SHO", "short"),
]

OPENALEX_QUERIES = [
    "short interest and the cross-section of stock returns",
    "short sellers informed trading stock returns",
    "short selling price discovery equity market",
    "securities lending fees loan supply stock returns",
    "short-sale constraints overpricing divergence of opinion",
    "days to cover short interest ratio return predictability",
    "short squeeze meme stocks retail short interest",
    "failures to deliver naked short selling stock returns",
    "short sale ban regulation short selling market quality",
    "regulation SHO pilot short-sale constraints anomalies",
    "stock lending market shorting demand utilization",
    "short interest as a predictor of stock returns",
    "shorting flow short selling around earnings announcements",
    "aggregate short interest market returns predictability",
    "short selling bear raids manipulation equity",
]

# A SHORT-SELLING / short-interest / lending term ...
SHORT_RE = (
    r"short interest|short[- ]sell\w*|short seller|short[- ]sale|short[- ]sold|"
    r"shorting|shortable|short position|securities lending|stock (?:loan|lending)|"
    r"\bstock loan|loan fee|lending fee|days[- ]to[- ]cover|short squeeze|"
    r"failures? to deliver|naked short|short[- ]sale (?:constraint|ban|restriction)|"
    r"divergence of opinion|regulation sho|\breg sho\b|uptick rule|"
    r"shorting demand|utilization rate|rebate rate|short ratio"
)
# ... AND an equity / returns / market context ...
EQUITY_RE = (
    r"stock|\bequit|share price|cross[- ]section of (?:stock )?returns|"
    r"return predictab|abnormal return|expected return|excess return|"
    r"price discovery|market (?:quality|efficien|liquidity)|overpric|mispric|"
    r"\banomal|firm|portfolio|trading"
)
# ... minus the off-domain / wrong-sense "short" usages.
OFF_RE = (
    r"short[- ]term memory|lstm|long short[- ]term|short[- ]rate model|"
    r"short[- ]rate dynamics|shortest path|short circuit|short[- ]channel|"
    r"short[- ]read|short[- ]wavelength|short[- ]pulse|short[- ]range order|"
    r"patient|\bdisease\b|biomedical|crop|soil|protein|gene |antenna|"
    r"short story|short film|short[- ]staffed|musculoskeletal|short stature"
)


def short_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    keep = (blob.str.contains(SHORT_RE, regex=True)
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
    print("Fetching arXiv (short interest / short selling)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (short interest / short selling)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "short_interest")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"short_interest_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = short_filter(df)
    cpath = OUT_DIR / f"short_interest_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  short-interest relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 15 papers (by citations) ===")
    for _, r in clean.head(15).iterrows():
        t = str(r.title).encode("ascii", "replace").decode()[:88]
        print(f"  [{int(r.citations):5d}] {t} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
