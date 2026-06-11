#!/usr/bin/env python3
"""Fetch papers on CONDITIONING / INTERACTING equity factors (not just linearly
combining them).

Topic: how to condition one signal on another rather than weight them additively.
Four threads (all requested):
  1. Cross-sectional interactions — characteristic x characteristic, conditional
     double-sorts (e.g. demand growth conditioned on quality, value x profitability).
  2. Nonlinear ML that captures interactions — trees/GBM, neural nets, "deep
     factors", interpretability of interaction effects.
  3. Conditional factor models — instrumented PCA (IPCA), conditional betas/loadings
     as functions of characteristics, characteristic-managed portfolios, conditional
     alphas.
  4. Time-series / regime conditioning — factor timing on macro/state variables,
     regime-switching, business-cycle-conditioned exposures.

Distinct from factor_alpha (which was *linear* IC/optimization weighting).

Reuses the query-parameterized fetchers in _fetch_research_papers.py.

Output:
    research_papers/factor_conditioning_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/factor_conditioning_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_factor_conditioning_papers.py
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
    # 1. cross-sectional interactions / conditional sorts
    ("conditional", "cross-section of returns"),
    ("interaction effect", "stock returns"),
    ("double sort", "stock returns"),
    ("characteristic interaction", "expected returns"),
    ("nonlinear", "cross-section of returns"),
    # 2. nonlinear ML capturing interactions
    ("machine learning", "cross-section of returns"),
    ("deep learning", "asset pricing"),
    ("gradient boosting", "stock returns"),
    ("neural network", "expected returns"),
    ("interactions", "machine learning asset pricing"),
    # 3. conditional factor models
    ("conditional factor model", "returns"),
    ("instrumented principal component", "returns"),
    ("conditional beta", "asset pricing"),
    ("characteristic-managed", "portfolio"),
    ("conditional asset pricing", "characteristics"),
    ("time-varying", "factor loading"),
    # 4. time-series / regime conditioning
    ("factor timing", "macroeconomic"),
    ("regime switching", "factor returns"),
    ("business cycle", "factor premia"),
    ("conditional", "factor premium"),
]

OPENALEX_QUERIES = [
    # cross-sectional interactions
    "conditional double sort cross-section of stock returns",
    "interaction effects characteristics expected stock returns",
    "characteristic interactions cross-section returns",
    "nonlinearity cross-section of expected returns",
    "value momentum interaction conditional portfolio",
    # nonlinear ML
    "machine learning cross-section of expected returns interactions",
    "deep learning empirical asset pricing characteristics",
    "gradient boosting random forest stock return prediction",
    "neural network conditional asset pricing",
    "interpretable machine learning factor interactions returns",
    # conditional factor models
    "instrumented principal components analysis IPCA characteristics",
    "conditional factor model time-varying betas characteristics",
    "characteristic-managed portfolios conditional alpha",
    "conditional asset pricing model stochastic discount factor",
    "characteristics betas covariances expected returns",
    # regime / time-series conditioning
    "factor timing macroeconomic conditioning variables",
    "regime switching factor premia equity",
    "business cycle conditional factor returns",
    "conditional expected returns macroeconomic state variables",
    "time-varying risk premia conditioning information",
]

# STRONG, specific conditioning/interaction terms — kept with a light equity
# context (they are unambiguous enough on their own).
STRONG_RE = (
    r"interaction (?:effect|term)|conditional (?:factor|beta|alpha|asset pric|"
    r"expected return|sort|double[- ]?sort|cross[- ]section|covariance|"
    r"information)|double[- ]?sort|condition(?:ing|ed) on|"
    r"instrumented principal component|\bipca\b|"
    r"time[- ]varying (?:beta|loading|risk premi|exposure)|"
    r"characteristic[- ]managed|factor timing|regime[- ]switch\w*|"
    r"conditioning (?:variable|information)|state[- ]dependent"
)
# WEAK / generic-ML terms — they capture interactions but are too broad alone, so
# they require a STRONG asset-pricing context (not merely the word "stock").
WEAK_RE = (
    r"nonlinear\w*|non[- ]linear|gradient boost|random forest|neural network|"
    r"deep learning|deep factor|machine learning|tree[- ]based"
)
# Light equity context (for STRONG terms).
EQUITY_RE = (
    r"cross[- ]section of (?:stock |expected )?returns|stock return|expected return|"
    r"asset pric|\bequit|factor (?:model|premi|return|zoo|investing)|"
    r"asset pricing anomal|risk premi|stochastic discount factor"
)
# Strong asset-pricing context (for WEAK/ML terms). Deliberately excludes bare
# "anomaly", which would otherwise admit ML *anomaly-detection* papers.
AP_RE = (
    r"cross[- ]section of (?:stock |expected )?returns|asset pric|"
    r"factor (?:model|premi|zoo)|expected (?:stock )?return|"
    r"stochastic discount factor|"
    r"(?:asset pricing|return|market|pricing|cross[- ]sectional) anomal|"
    r"characteristic.{0,18}return|empirical asset pricing|equity (?:premi|return)"
)
# Psychometric / biomedical / ecology + unrelated-ML-application senses (security,
# vision, networking, anomaly-detection) that share the ML / "factor" vocabulary.
OFF_RE = (
    r"confirmatory factor analysis|exploratory factor analysis|psychometr|"
    r"questionnaire|patient|\bdisease\b|biomedical|soil|crop|canopy|remote sensing|"
    r"growth factor|transcription factor|impact factor|gene |protein|ecolog|"
    r"communications network|telecommunic|routing|microblog|topic model|"
    r"climate (?:change|risk)|corporate social|\bcsr\b|carbon|sentiment analysis|"
    r"\bimage\b|traffic|intrusion|cyber|web attack|malware|batter|energy storage|"
    r"consensus algorithm|object class|document recognition|control practical|"
    r"internet|cloud computing|anomaly score|anomaly detection|fault|fraud detection"
)


def cond_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    strong = blob.str.contains(STRONG_RE, regex=True)
    weak = blob.str.contains(WEAK_RE, regex=True)
    eq = blob.str.contains(EQUITY_RE, regex=True)
    ap = blob.str.contains(AP_RE, regex=True)
    off = blob.str.contains(OFF_RE, regex=True)
    keep = ((strong & eq) | (weak & ap)) & ~off
    return df[keep].sort_values("citations", ascending=False).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-arxiv", type=int, default=90)
    ap.add_argument("--max-openalex", type=int, default=600)
    ap.add_argument("--arxiv-delay", type=float, default=5.0)
    ap.add_argument("--openalex-delay", type=float, default=1.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching arXiv (factor conditioning / interactions)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (factor conditioning / interactions)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "factor_conditioning")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"factor_conditioning_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = cond_filter(df)
    cpath = OUT_DIR / f"factor_conditioning_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  conditioning-relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 15 papers (by citations) ===")
    for _, r in clean.head(15).iterrows():
        t = str(r.title).encode("ascii", "replace").decode()[:88]
        print(f"  [{int(r.citations):5d}] {t} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
