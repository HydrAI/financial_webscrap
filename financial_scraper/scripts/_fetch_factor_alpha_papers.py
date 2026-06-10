#!/usr/bin/env python3
"""Fetch papers on weighting & combining equity factors into a factor alpha.

Topic: multi-factor models, factor/alpha-signal combination and weighting schemes
(IC-weighted, IC-IR / max-ICIR, equal-weight, optimization-based, ML-based),
composite alpha construction, factor selection/timing, combining anomalies.

Reuses the query-parameterized fetchers in _fetch_research_papers.py.

Output:
    research_papers/factor_alpha_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/factor_alpha_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_factor_alpha_papers.py
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
    ("factor combination", "stock"), ("multi-factor model", "equity"),
    ("alpha factor", "stock"), ("information coefficient", "factor"),
    ("factor weighting", "portfolio"), ("signal combination", "stock returns"),
    ("combining factors", "expected returns"), ("factor investing", "equity"),
    ("cross-sectional", "stock return prediction"), ("machine learning", "factor model"),
    ("alpha combination", "trading"), ("composite factor", "stock"),
    ("stock selection", "multi-factor"), ("factor timing", "equity"),
    ("characteristics", "cross-section of returns"),
    # regularization / shrinkage / high-dimensional factor selection
    ("lasso", "stock returns"), ("elastic net", "cross-section of returns"),
    ("regularization", "factor model"), ("shrinkage", "cross-section of returns"),
    ("high-dimensional", "asset pricing"), ("machine learning", "cross-section of returns"),
    ("variable selection", "stock returns"),
]

OPENALEX_QUERIES = [
    "combining factors equity alpha",
    "multi-factor model stock selection weighting",
    "information coefficient factor weighting",
    "IC weighted factor combination stock",
    "alpha signal combination machine learning equity",
    "factor investing combining characteristics returns",
    "cross-sectional stock return prediction factors",
    "optimal factor weighting portfolio construction",
    "combining anomalies expected stock returns",
    "machine learning factor model equity returns",
    "composite alpha factor construction stock",
    "factor timing rotation equity",
    "shrinking the cross-section stock returns",
    "lasso factor selection expected stock returns",
    "elastic net cross-section of returns",
    "taming the factor zoo test of new factors",
    "high-dimensional factor model stock returns",
    "empirical asset pricing machine learning characteristics",
    "sparse stochastic discount factor",
    "dissecting characteristics nonparametrically",
]

# A factor-investing-specific term AND an equity/returns term; drop the
# psychometric/biomedical senses of "factor".
FACTOR_RE = (
    r"factor model|multi[- ]?factor|alpha factor|factor combination|"
    r"factor weighting|factor investing|smart beta|information coefficient|"
    r"\bic[- ]weight|\bicir\b|information ratio|signal combination|"
    r"alpha (?:signal|combination|blend|model)|cross[- ]section(?:al)?|"
    r"characteristic|anomal|factor zoo|composite (?:factor|alpha)|"
    r"factor timing|factor selection|factor portfolio|risk premia|factor premia|"
    r"\blasso\b|ridge regression|\belastic net\b|\bshrinkage\b|regulari[sz]|"
    r"penaliz|variable selection|stochastic discount factor|high[- ]dimensional|"
    r"empirical asset pricing via machine|taming the factor zoo|dissecting characteristics"
)
EQUITY_RE = (
    r"stock|\bequit|expected return|cross[- ]section of (?:stock )?returns|"
    r"portfolio|asset pric|\bfirm|stock return|excess return|securities"
)
OFF_RE = (
    r"confirmatory factor analysis|exploratory factor analysis|psychometr|"
    r"questionnaire|latent factor.{0,20}survey|risk factors for|patient|"
    r"\bdisease\b|biomedical|soil|crop|growth factor|transcription factor|"
    r"impact factor"
)


def factor_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    keep = (blob.str.contains(FACTOR_RE, regex=True)
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
    print("Fetching arXiv (factor combination / alpha weighting)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (factor combination / alpha weighting)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "factor_alpha")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"factor_alpha_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = factor_filter(df)
    cpath = OUT_DIR / f"factor_alpha_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  factor-alpha relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 15 papers (by citations) ===")
    for _, r in clean.head(15).iterrows():
        print(f"  [{int(r.citations):5d}] {str(r.title)[:88]} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
