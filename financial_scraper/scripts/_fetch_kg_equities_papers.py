#!/usr/bin/env python3
"""Fetch Knowledge-Graph / GNN-for-equities research papers (arXiv + OpenAlex).

Reuses the query-parameterized fetchers in _fetch_research_papers.py with a
topic-specific query set and a relevance filter for graph-based methods
(knowledge graphs, GNN/GAT/GCN, graph embeddings) applied to stocks/equities.

Output:
    research_papers/kg_equities_papers_<ts>.parquet (+ .jsonl)        [raw]
    research_papers/kg_equities_papers_<ts>_clean.parquet (+ .jsonl)  [filtered]

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_kg_equities_papers.py
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

# arXiv: (graph method phrase) AND (equity/stock phrase) per query.
ARXIV_PAIRS = [
    ("knowledge graph", "stock"), ("knowledge graph", "equity"),
    ("knowledge graph", "financial market"), ("financial knowledge graph", "prediction"),
    ("graph neural network", "stock"), ("graph neural network", "equity"),
    ("graph neural network", "stock movement"), ("graph attention", "stock"),
    ("graph convolutional", "stock"), ("relational", "stock prediction"),
    ("heterogeneous graph", "stock"), ("graph embedding", "stock"),
    ("company graph", "stock"), ("graph-based", "stock prediction"),
    ("temporal graph", "stock"),
]

OPENALEX_QUERIES = [
    "knowledge graph stock prediction",
    "graph neural network stock movement prediction",
    "financial knowledge graph equity",
    "graph attention network stock market",
    "knowledge graph stock recommendation",
    "relational stock prediction graph neural network",
    "company knowledge graph financial market",
    "heterogeneous graph stock prediction",
    "graph embedding stock market forecasting",
    "GNN stock price forecasting",
    "knowledge graph financial news stock",
    "temporal graph network equity returns",
]

# Relevance: a graph/KG method term AND an equity/stock term; drop non-finance
# graph domains (molecular, traffic, generic recsys, biomedical).
KG_RE = (
    r"knowledge graph|graph neural|\bgnn\b|graph attention|\bgat\b|"
    r"graph convolution|\bgcn\b|relational graph|heterogeneous graph|"
    r"graph embedding|node embedding|graph[- ]based|company graph|entity graph|"
    r"temporal graph|link prediction|graph representation"
)
EQUITY_RE = (
    r"stock|\bequity\b|\bequities\b|share price|financial market|stock market|"
    r"stock price|stock movement|firm[- ]level|listed compan|s&p 500|s&amp;p 500|"
    r"return prediction|portfolio|market prediction"
)
OFF_RE = (
    r"molecul|protein|biomedical|drug discovery|chemical|\bgene\b|"
    r"traffic|road network|power grid|sensor network|"
    r"social network analysis of|citation network|e-commerce recommend|"
    r"point[- ]of[- ]interest|knowledge graph completion benchmark"
)


def kg_filter(df: pd.DataFrame) -> pd.DataFrame:
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df["categories"].fillna("")).str.lower()
    keep = (blob.str.contains(KG_RE, regex=True)
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
    print("Fetching arXiv (knowledge graph / GNN for equities)...")
    arxiv = _frp.fetch_arxiv(ARXIV_PAIRS, args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique\n")

    print("Fetching OpenAlex (knowledge graph / GNN for equities)...")
    openalex = _frp.fetch_openalex(OPENALEX_QUERIES, args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique\n")

    papers = _frp.dedupe(arxiv + openalex)
    df = _frp.papers_to_df(papers, "kg_equities")
    print(f"Combined after dedupe: {len(df)} papers")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw = OUT_DIR / f"kg_equities_papers_{ts}.parquet"
    df.to_parquet(raw, index=False)
    df.to_json(raw.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    clean = kg_filter(df)
    cpath = OUT_DIR / f"kg_equities_papers_{ts}_clean.parquet"
    clean.to_parquet(cpath, index=False)
    clean.to_json(cpath.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Raw: {len(df)}  |  KG-equities relevant: {len(clean)}")
    print(f"  arxiv={int((clean.source=='arxiv').sum())} openalex={int((clean.source=='openalex').sum())}")
    print(f"  with PDF link: {int((clean.pdf_url.str.len()>0).sum())}")
    print(f"Output: {cpath}")
    print(f"\n=== 15 papers (by citations) ===")
    for _, r in clean.head(15).iterrows():
        print(f"  [{int(r.citations):5d}] {str(r.title)[:88]} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
