#!/usr/bin/env python3
"""Fetch papers on STATISTICAL ARBITRAGE methodologies (equities and/or futures).

Goal: HUGE coverage of the methods literature -
  - Pairs trading: distance, cointegration, copula, Kalman/state-space, ML.
  - Mean reversion / Ornstein-Uhlenbeck / mean-reverting & sparse portfolios.
  - Cointegration / error-correction / convergence trading.
  - Statistical / PCA factor stat-arb, eigenportfolios, residual reversal.
  - Index & ETF arbitrage, calendar/spread trading in futures.
  - ML / deep-learning / reinforcement-learning stat-arb.
  - Optimal execution & stochastic control of mean-reverting spreads.
  - Lead-lag, cross-asset & high-frequency stat-arb; limits to arbitrage.

Large query set + raised caps for breadth. Reuses the query-parameterized
fetchers in _fetch_research_papers.py.

Output:
    research_papers/statarb_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/statarb_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_statarb_papers.py
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
    ("statistical arbitrage", "equity"), ("statistical arbitrage", "trading"),
    ("statistical arbitrage", "machine learning"), ("statistical arbitrage", "deep learning"),
    ("pairs trading", "stocks"), ("pairs trading", "cointegration"),
    ("pairs trading", "copula"), ("pairs trading", "reinforcement learning"),
    ("pairs trading", "machine learning"), ("mean reversion", "trading strategy"),
    ("mean reverting", "portfolio"), ("Ornstein-Uhlenbeck", "trading"),
    ("cointegration", "trading strategy"), ("convergence trading", "arbitrage"),
    ("sparse mean reverting", "portfolio"), ("optimal", "pairs trading"),
    ("stochastic control", "pairs trading"), ("Kalman filter", "pairs trading"),
    ("index arbitrage", "futures"), ("ETF arbitrage", "stocks"),
    ("spread trading", "futures"), ("eigenportfolio", "statistical arbitrage"),
    ("residual", "stock return reversal"), ("relative value", "arbitrage"),
    ("lead-lag", "trading"), ("market neutral", "equity strategy"),
    ("deep learning", "pairs trading"), ("optimal execution", "mean reverting"),
    ("cointegrated", "futures"), ("high frequency", "statistical arbitrage"),
]

OPENALEX_QUERIES = [
    "statistical arbitrage equity strategy",
    "statistical arbitrage machine learning stock returns",
    "pairs trading performance of a relative value arbitrage rule",
    "pairs trading cointegration approach stocks",
    "pairs trading copula method",
    "pairs trading deep reinforcement learning",
    "mean reversion trading strategy stock",
    "mean reverting portfolio Ornstein Uhlenbeck optimal trading",
    "sparse mean reverting portfolio selection",
    "cointegration based statistical arbitrage",
    "convergence trading limits of arbitrage",
    "optimal pairs trading stochastic control",
    "Kalman filter dynamic hedge ratio pairs trading",
    "index arbitrage stock index futures",
    "ETF arbitrage authorized participants mispricing",
    "futures spread trading calendar spread cointegration",
    "PCA statistical arbitrage eigenportfolio residual",
    "statistical arbitrage in the US equities market",
    "residual reversal idiosyncratic momentum stock returns",
    "market neutral long short equity strategy",
    "lead lag relationship cross-section returns trading",
    "high frequency statistical arbitrage market making",
    "deep learning statistical arbitrage equity",
    "reinforcement learning pairs trading agent",
    "optimal stopping pairs trading entry exit threshold",
    "mean reverting spread optimal liquidation execution",
    "cryptocurrency statistical arbitrage pairs trading",
    "machine learning mean reversion cross-section stocks",
]

# STRONG, specific stat-arb terms - unambiguous, kept without a context gate.
STRONG_RE = (
    r"statistical arbitrage|stat[- ]arb|pairs?[- ]trading|pair trading|"
    r"convergence trading|index[- ]?futures? arbitrage|index arbitrage|"
    r"etf arbitrage|eigenportfolio|mean[- ]revert\w+ portfolio|"
    r"relative[- ]value arbitrage|ornstein[- ]uhlenbeck|"
    r"cointegrat\w* (?:pair|portfolio|trading|spread|strateg|stock|equit|futures|arbitrage)"
)
# WEAK / broad trading terms - require a clear trading/returns/market context.
WEAK_RE = (
    r"mean[- ]revers\w+|mean[- ]revert\w+|spread trading|relative value|"
    r"market[- ]neutral|lead[- ]lag|residual (?:reversal|momentum)|hedge ratio|"
    r"long[- ]short (?:equity|portfolio|strateg)"
)
# Cointegration is heavily used in MACRO; admit it only with a trading context.
COINT_RE = r"cointegrat\w+"
COINT_CTX_RE = (
    r"pairs?|spread|trading strateg|stat\w* arb|arbitrage|stock pair|"
    r"hedge ratio|long[- ]short|portfolio"
)
# Trading / market context (for WEAK + COINT terms).
CTX_RE = (
    r"\bstock|\bequit|futures|\betf\b|trading strateg|portfolio|return|profit|"
    r"backtest|long[- ]short|market[- ]neutral|sharpe|\bspread\b|hedge|cryptocurrenc"
)
# Off-domain + wrong-sense (esp. macro-econometrics that shares "cointegration"/
# "convergence", and game-theory/growth-economics "arbitrage"/"nucleolus").
OFF_RE = (
    r"patient|\bdisease\b|biomedical|\bgene\b|protein|soil|crop|ecolog|climate|"
    r"rainfall|hydrolog|species|epidemi|neuroscience|galaxy|seismic|traffic flow|"
    r"power grid|wind speed|purchasing power parity|monetary policy|zero bound|"
    r"corporate governance|regional (?:inequality|convergence)|beta convergence|"
    r"economic (?:growth|convergence|integration)|income convergence|"
    r"characteristic function game|nucleolus|real exchange rate|life[- ]?cycle|"
    r"labor market|inflation dynamics|branch[- ]and[- ]bound"
)


def statarb_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    strong = blob.str.contains(STRONG_RE, regex=True)
    weak = blob.str.contains(WEAK_RE, regex=True)
    coint = blob.str.contains(COINT_RE, regex=True) & blob.str.contains(COINT_CTX_RE, regex=True)
    ctx = blob.str.contains(CTX_RE, regex=True)
    off = blob.str.contains(OFF_RE, regex=True)
    keep = (strong | ((weak | coint) & ctx)) & ~off
    return df[keep].sort_values("citations", ascending=False).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    # Raised caps for HUGE coverage.
    ap.add_argument("--max-arxiv", type=int, default=150)
    ap.add_argument("--max-openalex", type=int, default=800)
    ap.add_argument("--arxiv-delay", type=float, default=5.0)
    ap.add_argument("--openalex-delay", type=float, default=1.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching arXiv (statistical arbitrage methodologies)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (statistical arbitrage methodologies)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "statarb")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"statarb_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = statarb_filter(df)
    cpath = OUT_DIR / f"statarb_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  stat-arb relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 18 papers (by citations) ===")
    for _, r in clean.head(18).iterrows():
        t = str(r.title).encode("ascii", "replace").decode()[:88]
        print(f"  [{int(r.citations):5d}] {t} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
