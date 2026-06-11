#!/usr/bin/env python3
"""Fetch papers on 13F institutional ownership disclosure & alpha in equities.

Topic: information content of SEC Form 13F institutional holdings disclosures —
institutional ownership and stock returns, copycat / clone strategies, hedge-fund
13F replication, smart-money and informed institutional trading, ownership changes
and the cross-section of returns, disclosure timing/lag and front-running, crowding.

Reuses the query-parameterized fetchers in _fetch_research_papers.py.

Output:
    research_papers/inst_ownership_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/inst_ownership_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_inst_ownership_papers.py
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
    ("13F", "institutional"), ("13F", "holdings"),
    ("institutional ownership", "stock returns"),
    ("institutional holdings", "alpha"),
    ("hedge fund holdings", "returns"),
    ("institutional investors", "informed trading"),
    ("copycat", "hedge fund"), ("13F", "replication"),
    ("ownership", "cross-section of returns"),
    ("institutional trading", "stock"),
    ("smart money", "equity"), ("institutional demand", "stock price"),
    ("blockholder", "stock returns"), ("mutual fund holdings", "performance"),
    ("disclosure", "institutional ownership"),
]

OPENALEX_QUERIES = [
    "13F institutional holdings stock returns",
    "institutional ownership cross-section of stock returns",
    "hedge fund 13F holdings performance alpha",
    "copycat hedge fund 13F replication strategy",
    "smart money informed institutional trading equity",
    "institutional investors stock return predictability",
    "13F disclosure information content front-running",
    "changes in institutional ownership stock returns",
    "institutional demand and stock prices",
    "mutual fund holdings best ideas alpha",
    "blockholder ownership and firm value returns",
    "institutional herding crowding stock returns",
    "13F filing lag disclosure timing portfolio",
    "institutional investor concentration abnormal returns",
    "form 13F holdings duplication mimicking portfolio",
]

# A 13F / institutional-holdings-disclosure term ...
INST_RE = (
    r"\b13[- ]?f\b|institutional (?:ownership|holding|investor|demand|trading|"
    r"trade)|hedge fund (?:holding|portfolio|position|disclos)|"
    r"mutual fund holding|copycat|clone (?:portfolio|strateg)|"
    r"mimicking portfolio|smart money|blockholder|informed (?:institution|trading)|"
    r"best ideas"
)
# ... AND a RETURNS / alpha / price-impact context (not firm-value/governance,
# which is a different literature that the bare term "institutional ownership"
# otherwise drags in).
RET_RE = (
    r"stock return|abnormal return|excess return|"
    r"cross[- ]section of (?:stock )?returns|return predictab|\balpha\b|"
    r"stock price|expected return|risk[- ]adjusted|"
    r"portfolio (?:return|perform|strateg)|outperform|trading profit|"
    r"price (?:impact|pressure)|\banomal"
)
# Corporate-governance / ownership-structure / legal / activism / climate / ESG —
# the adjacent-but-off-topic literature that shares the "ownership" vocabulary.
OFF_RE = (
    r"corporate governance|firm value|shareholder value|"
    r"board (?:of director|independ|divers)|ownership structure|firm performance|"
    r"\bvoting\b|proxy (?:contest|vote|advis)|activis|climate|\besg\b|sustainab|"
    r"blockchain|financializ|dispersed ownership|separation of own|"
    r"corporate control|cross[- ]country|legal (?:origin|protection)|"
    r"ownership concentration and firm|patient|\bdisease\b|biomedical"
)
# Strong, unambiguous on-topic title signal — rescued even if OFF matches.
RESCUE_RE = (
    r"\b13[- ]?f\b|copycat|mimicking portfolio|clone (?:portfolio|strateg)|"
    r"best ideas|institutional (?:ownership|holding|trading).{0,40}return|"
    r"informed (?:institutional )?trading"
)


def inst_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    title = df["title"].fillna("").str.lower()
    keep = (blob.str.contains(INST_RE, regex=True)
            & blob.str.contains(RET_RE, regex=True)
            & (~blob.str.contains(OFF_RE, regex=True)
               | title.str.contains(RESCUE_RE, regex=True)))
    return df[keep].sort_values("citations", ascending=False).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-arxiv", type=int, default=90)
    ap.add_argument("--max-openalex", type=int, default=600)
    ap.add_argument("--arxiv-delay", type=float, default=5.0)
    ap.add_argument("--openalex-delay", type=float, default=1.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching arXiv (13F / institutional ownership)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (13F / institutional ownership)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "inst_ownership")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"inst_ownership_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = inst_filter(df)
    cpath = OUT_DIR / f"inst_ownership_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  institutional-ownership relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 15 papers (by citations) ===")
    for _, r in clean.head(15).iterrows():
        t = str(r.title).encode("ascii", "replace").decode()[:88]
        print(f"  [{int(r.citations):5d}] {t} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
