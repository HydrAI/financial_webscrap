#!/usr/bin/env python3
"""Fetch Genetic-Algorithm / evolutionary-computation in Machine-Learning papers.

Reuses the query-parameterized fetchers in _fetch_research_papers.py. Covers GAs,
genetic programming, evolutionary algorithms, and neuroevolution applied to ML
(feature selection, hyperparameter optimization, neural architecture search,
GA-optimized classifiers). Filter excludes the biology-genetics sense.

Output:
    research_papers/ga_ml_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/ga_ml_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_ga_ml_papers.py
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

# arXiv: (evolutionary phrase) AND (ML phrase) per query.
ARXIV_PAIRS = [
    ("genetic algorithm", "machine learning"), ("genetic algorithm", "neural network"),
    ("genetic algorithm", "feature selection"), ("genetic algorithm", "hyperparameter"),
    ("genetic algorithm", "deep learning"), ("genetic algorithm", "classification"),
    ("genetic algorithm", "support vector machine"), ("genetic algorithm", "LSTM"),
    ("genetic programming", "machine learning"), ("genetic programming", "feature"),
    ("evolutionary algorithm", "neural network"), ("evolutionary algorithm", "deep learning"),
    ("neuroevolution", "neural network"), ("evolutionary", "neural architecture search"),
    ("differential evolution", "neural network"),
]

OPENALEX_QUERIES = [
    "genetic algorithm machine learning",
    "genetic algorithm feature selection machine learning",
    "genetic algorithm hyperparameter optimization neural network",
    "genetic algorithm optimized neural network",
    "neuroevolution neural network",
    "genetic programming machine learning",
    "evolutionary algorithm deep learning",
    "genetic algorithm support vector machine classification",
    "genetic algorithm neural architecture search",
    "genetic algorithm LSTM forecasting",
    "evolutionary computation machine learning model",
    "genetic algorithm random forest feature selection",
]

# Relevance: an evolutionary/GA term AND an ML term; drop biology-genetics.
GA_RE = (
    r"genetic algorithm|genetic programming|evolutionary algorithm|"
    r"evolutionary computation|neuroevolution|differential evolution|"
    r"memetic algorithm|evolution strateg|\bnsga\b|evolutionary optimi"
)
ML_RE = (
    r"machine learning|deep learning|neural network|\bsvm\b|support vector|"
    r"random forest|\bcnn\b|\blstm\b|\bgru\b|classifier|classification|"
    r"feature selection|hyperparameter|gradient boost|xgboost|model training|"
    r"architecture search|ensemble|regression model|clustering|reinforcement learning"
)
OFF_RE = (
    r"\bgenome\b|dna sequenc|protein structure|protein folding|gene expression profil|"
    r"crop breeding|plant breeding|genetic disorder|gene therapy|"
    r"genetic variant|genetic diversity of|population genetics"
)


def ga_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    keep = (blob.str.contains(GA_RE, regex=True)
            & blob.str.contains(ML_RE, regex=True)
            & ~blob.str.contains(OFF_RE, regex=True))
    return df[keep].sort_values("citations", ascending=False).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-arxiv", type=int, default=80)
    ap.add_argument("--max-openalex", type=int, default=600)
    ap.add_argument("--arxiv-delay", type=float, default=6.0)
    ap.add_argument("--openalex-delay", type=float, default=1.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching arXiv (genetic algorithm / evolutionary ML)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (genetic algorithm / evolutionary ML)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "ga_ml")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"ga_ml_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = ga_filter(df)
    cpath = OUT_DIR / f"ga_ml_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  GA-in-ML relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 15 papers (by citations) ===")
    for _, r in clean.head(15).iterrows():
        print(f"  [{int(r.citations):5d}] {str(r.title)[:88]} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
