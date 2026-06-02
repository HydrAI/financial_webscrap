#!/usr/bin/env python3
"""Fetch NLP-sentiment-for-stocks/equities research papers (arXiv + OpenAlex).

Reuses the query-parameterized fetchers in _fetch_research_papers.py with a
topic-specific query set and an equity-sentiment relevance filter (NLP/sentiment
term AND a stock/equity term, minus non-finance sentiment domains like movie/
product reviews).

Output:
    research_papers/equity_sentiment_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/equity_sentiment_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_sentiment_papers.py
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

# arXiv: (NLP/sentiment phrase) AND (equity/stock phrase) per query.
ARXIV_PAIRS = [
    ("sentiment analysis", "stock"), ("sentiment analysis", "stock market"),
    ("sentiment analysis", "equity"), ("news sentiment", "stock"),
    ("twitter sentiment", "stock"), ("social media sentiment", "stock"),
    ("natural language processing", "stock"), ("text mining", "stock"),
    ("financial news", "stock price"), ("word embedding", "stock"),
    ("FinBERT", "stock"), ("BERT", "stock market"),
    ("large language model", "stock"), ("sentiment", "stock price prediction"),
    ("textual analysis", "stock return"),
]

OPENALEX_QUERIES = [
    "stock market prediction sentiment analysis",
    "news sentiment stock returns natural language processing",
    "twitter sentiment stock price prediction",
    "financial news text mining stock movement",
    "FinBERT financial sentiment stock",
    "social media sentiment equity returns",
    "deep learning sentiment analysis stock market",
    "natural language processing stock price forecasting",
    "investor sentiment textual analysis stock returns",
    "large language models financial sentiment equity",
    "news headlines stock movement prediction NLP",
    "10-K textual analysis stock returns",
]

# Relevance: a genuine NLP / TEXT-method signal (not the bare word 'sentiment',
# which also matches non-NLP behavioral investor-sentiment indices) AND an
# equity/stock term; minus non-finance sentiment domains and 'brand equity'.
SENT_RE = (
    r"sentiment analysis|sentiment scor|sentiment classif|news sentiment|"
    r"social media sentiment|text-based sentiment|lexicon|"
    r"\bnlp\b|natural language process|text mining|textual analysis|"
    r"news analyt|finbert|\bbert\b|word embedding|word2vec|topic model|"
    r"language model|\bllm\b|\bgpt\b|chatgpt|text classification|"
    r"\btweets?\b|twitter|news headlines|financial news|text data"
)
EQUITY_RE = (
    r"stock|\bequity market|\bequities\b|share price|securities market|"
    r"s&p 500|s&amp;p 500|nasdaq|\bdow\b|firm[- ]level return|stock return|"
    r"cross[- ]section of (?:stock )?returns|\b10-?k\b|earnings call|"
    r"listed compan|capital market"
)
OFF_RE = (
    r"movie review|film review|product review|customer review|amazon review|"
    r"\bimdb\b|restaurant review|hotel review|e-commerce review|app review|"
    r"election|political sentiment|arab spring|social movement|protest|"
    r"public health|covid sentiment|\bmedical\b|patient sentiment|"
    r"brand equity|marketing|paleontolog|private equity"
)


def sentiment_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    keep = (blob.str.contains(SENT_RE, regex=True)
            & blob.str.contains(EQUITY_RE, regex=True)
            & ~blob.str.contains(OFF_RE, regex=True))
    return df[keep].sort_values("citations", ascending=False).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-arxiv", type=int, default=100)
    ap.add_argument("--max-openalex", type=int, default=600)
    ap.add_argument("--arxiv-delay", type=float, default=3.0)
    ap.add_argument("--openalex-delay", type=float, default=1.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching arXiv (equity sentiment / NLP)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (equity sentiment / NLP)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "equity_sentiment")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"equity_sentiment_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = sentiment_filter(df)
    cpath = OUT_DIR / f"equity_sentiment_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  equity-sentiment relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 15 papers (by citations) ===")
    for _, r in clean.head(15).iterrows():
        print(f"  [{int(r.citations):5d}] {str(r.title)[:88]} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
