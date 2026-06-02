#!/usr/bin/env python3
"""Fetch ML-in-futures-markets research papers from arXiv + OpenAlex APIs.

Structured academic metadata (title, authors, year, abstract, venue, citations,
PDF link) with no scraping friction. Covers four method families
(deep learning, reinforcement learning, classical ML, neural sequence models)
across commodity / index / rate futures.

Output (KG 8-col compatible + paper metadata columns):
    research_papers/ml_futures_papers_<ts>.parquet
    research_papers/ml_futures_papers_<ts>.jsonl

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_fetch_research_papers.py
    ... --max-arxiv 150 --max-openalex 600
"""

import argparse
import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "research_papers"
MAILTO = "tguida@hotmail.com"  # OpenAlex polite-pool identifier

ARXIV_API = "http://export.arxiv.org/api/query"
OPENALEX_API = "https://api.openalex.org/works"

# Method families x market targets. Kept as phrase pairs so each API query is
# (method) AND (market) — precise enough to stay on-topic.
METHODS = [
    "deep learning", "LSTM", "recurrent neural network",
    "convolutional neural network", "transformer neural network",
    "reinforcement learning", "deep reinforcement learning", "trading agent",
    "machine learning", "random forest", "support vector machine",
    "gradient boosting",
]
MARKETS = [
    "futures", "futures market", "commodity futures",
    "index futures", "interest rate futures", "futures price prediction",
]

# Curated OpenAlex free-text searches (it ranks by relevance internally).
OPENALEX_QUERIES = [
    "machine learning futures price prediction",
    "deep learning commodity futures forecasting",
    "reinforcement learning futures trading",
    "LSTM futures price forecasting",
    "transformer model futures market prediction",
    "random forest commodity futures returns",
    "support vector machine futures trading",
    "gradient boosting futures price",
    "deep reinforcement learning index futures",
    "neural network interest rate futures",
    "machine learning algorithmic trading futures",
    "convolutional neural network commodity price",
]


def _norm_title(t: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (t or "").lower()).strip()


# ---------------- arXiv ----------------

def fetch_arxiv(max_per_query: int, delay: float) -> list[dict]:
    ns = {"a": "http://www.w3.org/2005/Atom"}
    records: dict[str, dict] = {}
    # Pair a few high-signal methods with each market to bound query count.
    pairs = [(m, k) for m in METHODS for k in ("futures", "futures market")]
    pairs += [("reinforcement learning", "trading"),
              ("deep learning", "commodity price"),
              ("machine learning", "index futures")]
    for method, market in pairs:
        q = f'all:"{method}" AND all:"{market}"'
        params = {
            "search_query": q,
            "start": 0,
            "max_results": max_per_query,
            "sortBy": "relevance",
            "sortOrder": "descending",
        }
        try:
            r = requests.get(ARXIV_API, params=params, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"  arXiv query failed ({method}/{market}): {e}")
            time.sleep(delay)
            continue
        root = ET.fromstring(r.text)
        n = 0
        for entry in root.findall("a:entry", ns):
            aid = (entry.findtext("a:id", "", ns) or "").strip()
            arxiv_id = aid.rsplit("/", 1)[-1]
            if not arxiv_id or arxiv_id in records:
                continue
            title = " ".join((entry.findtext("a:title", "", ns) or "").split())
            summary = " ".join((entry.findtext("a:summary", "", ns) or "").split())
            published = (entry.findtext("a:published", "", ns) or "")[:10]
            authors = [a.findtext("a:name", "", ns) for a in entry.findall("a:author", ns)]
            cats = [c.get("term") for c in entry.findall("a:category", ns)]
            pdf_url = ""
            for link in entry.findall("a:link", ns):
                if link.get("title") == "pdf" or link.get("type") == "application/pdf":
                    pdf_url = link.get("href", "")
            records[arxiv_id] = {
                "title": title,
                "authors": ", ".join(filter(None, authors)),
                "year": published[:4],
                "date": published,
                "abstract": summary,
                "venue": "arXiv",
                "source": "arxiv",
                "doi": "",
                "citations": 0,
                "categories": ", ".join(filter(None, cats)),
                "pdf_url": pdf_url,
                "link": aid,
                "arxiv_id": arxiv_id,
                "openalex_id": "",
            }
            n += 1
        print(f"  arXiv [{method} / {market}]: +{n} (total {len(records)})")
        time.sleep(delay)
    return list(records.values())


# ---------------- OpenAlex ----------------

def _reconstruct_abstract(inv: dict | None) -> str:
    if not inv:
        return ""
    pos = {}
    for word, idxs in inv.items():
        for i in idxs:
            pos[i] = word
    return " ".join(pos[i] for i in sorted(pos))


def fetch_openalex(max_total: int, delay: float) -> list[dict]:
    records: dict[str, dict] = {}
    per_page = 200
    for query in OPENALEX_QUERIES:
        cursor = "*"
        got = 0
        while got < max_total // len(OPENALEX_QUERIES) + per_page:
            params = {
                "search": query,
                "per-page": per_page,
                "cursor": cursor,
                "mailto": MAILTO,
                "filter": "type:article",
            }
            try:
                r = requests.get(OPENALEX_API, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
            except (requests.RequestException, ValueError) as e:
                print(f"  OpenAlex query failed ({query}): {e}")
                break
            results = data.get("results", [])
            if not results:
                break
            for w in results:
                oid = (w.get("id") or "").rsplit("/", 1)[-1]
                if not oid or oid in records:
                    continue
                loc = w.get("primary_location") or {}
                best_oa = w.get("best_oa_location") or {}
                pdf_url = best_oa.get("pdf_url") or loc.get("pdf_url") or ""
                landing = loc.get("landing_page_url") or w.get("id")
                venue = (loc.get("source") or {}).get("display_name") or ""
                records[oid] = {
                    "title": w.get("title") or "",
                    "authors": ", ".join(
                        (a.get("author") or {}).get("display_name", "")
                        for a in (w.get("authorships") or [])
                    ),
                    "year": str(w.get("publication_year") or ""),
                    "date": w.get("publication_date") or "",
                    "abstract": _reconstruct_abstract(w.get("abstract_inverted_index")),
                    "venue": venue,
                    "source": "openalex",
                    "doi": (w.get("doi") or "").replace("https://doi.org/", ""),
                    "citations": int(w.get("cited_by_count") or 0),
                    "categories": ", ".join(
                        (c.get("display_name") or "")
                        for c in (w.get("concepts") or [])[:5]
                    ),
                    "pdf_url": pdf_url,
                    "link": landing,
                    "arxiv_id": "",
                    "openalex_id": oid,
                }
                got += 1
            cursor = (data.get("meta") or {}).get("next_cursor")
            if not cursor:
                break
            time.sleep(delay)
        print(f"  OpenAlex [{query}]: total {len(records)}")
    return list(records.values())


_ML_RE = (
    r"machine learning|deep learning|neural network|\blstm\b|\bgru\b|\bcnn\b|"
    r"transformer|reinforcement learning|random forest|support vector|\bsvm\b|"
    r"gradient boost|xgboost|lightgbm|ensemble|autoencoder|attention"
)
_FUT_RE = (
    r"futures|commodit|crude|\bwti\b|brent|natural gas|soybean|corn|wheat|"
    r"copper|nickel|aluminium|stock index|index futures|bond futures|"
    r"interest rate futures|algorithmic trading|trading strateg|price forecast|"
    r"price prediction|volatility forecast|hedging"
)


def relevance_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Keep rows that mention both an ML method and a futures/markets term.

    The APIs rank by loose relevance and pull a lot of off-domain noise
    (medical 'future work', agronomy crop yields, smart cities). Requiring an
    ML term AND a futures/markets term together cuts ~4,900 hits to the few
    hundred that are genuinely ML-in-futures. Recall is favored over precision
    (some agronomy crop-name matches survive), so review the tail.
    """
    blob = (df["title"].fillna("") + " " + df["full_text"].fillna("")).str.lower()
    keep = blob.str.contains(_ML_RE, regex=True) & blob.str.contains(_FUT_RE, regex=True)
    return df[keep].sort_values("citations", ascending=False).reset_index(drop=True)


def dedupe(papers: list[dict]) -> list[dict]:
    """Cross-source dedupe by DOI, then normalized title."""
    by_doi: dict[str, dict] = {}
    by_title: dict[str, dict] = {}
    out: list[dict] = []
    for p in papers:
        doi = p.get("doi", "").lower()
        nt = _norm_title(p.get("title", ""))
        if doi and doi in by_doi:
            continue
        if nt and nt in by_title:
            continue
        if doi:
            by_doi[doi] = p
        if nt:
            by_title[nt] = p
        out.append(p)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-arxiv", type=int, default=100,
                    help="Max results per arXiv query (default 100)")
    ap.add_argument("--max-openalex", type=int, default=600,
                    help="Approx total OpenAlex works to pull (default 600)")
    ap.add_argument("--arxiv-delay", type=float, default=3.0)
    ap.add_argument("--openalex-delay", type=float, default=1.0)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Fetching arXiv...")
    arxiv = fetch_arxiv(args.max_arxiv, args.arxiv_delay)
    print(f"arXiv: {len(arxiv)} unique papers\n")

    print("Fetching OpenAlex...")
    openalex = fetch_openalex(args.max_openalex, args.openalex_delay)
    print(f"OpenAlex: {len(openalex)} unique papers\n")

    papers = dedupe(arxiv + openalex)
    print(f"Combined after cross-source dedupe: {len(papers)} papers")

    # KG 8-col compatible record + paper metadata.
    rows = []
    for p in papers:
        abstract = p["abstract"]
        rows.append({
            "company": p["venue"] or p["source"],
            "title": p["title"],
            "link": p["pdf_url"] or p["link"],
            "snippet": abstract[:300],
            "date": p["date"],
            "source": p["source"],
            "full_text": abstract,        # abstract for now; PDFs are a later pass
            "source_file": f"{p['source']}_ml_futures",
            # --- paper metadata ---
            "authors": p["authors"],
            "year": p["year"],
            "venue": p["venue"],
            "doi": p["doi"],
            "citations": p["citations"],
            "categories": p["categories"],
            "pdf_url": p["pdf_url"],
            "arxiv_id": p["arxiv_id"],
            "openalex_id": p["openalex_id"],
        })
    df = pd.DataFrame(rows)
    df = df[df["title"].str.len() > 0].reset_index(drop=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    pq_path = OUT_DIR / f"ml_futures_papers_{ts}.parquet"
    jl_path = OUT_DIR / f"ml_futures_papers_{ts}.jsonl"
    df.to_parquet(pq_path, index=False)
    with open(jl_path, "w", encoding="utf-8") as f:
        for _, r in df.iterrows():
            f.write(json.dumps(r.to_dict(), ensure_ascii=False) + "\n")

    # Relevance-filtered subset (ML term AND futures/markets term).
    clean = relevance_filter(df)
    clean_pq = OUT_DIR / f"ml_futures_papers_{ts}_clean.parquet"
    clean.to_parquet(clean_pq, index=False)
    clean.to_json(OUT_DIR / f"ml_futures_papers_{ts}_clean.jsonl",
                  orient="records", lines=True, force_ascii=False)

    print(f"\n=== Summary ===")
    print(f"Total papers (raw): {len(df)}  |  relevance-filtered: {len(clean)}")
    print(f"  arXiv:    {(df['source']=='arxiv').sum()}")
    print(f"  OpenAlex: {(df['source']=='openalex').sum()}")
    print(f"  with PDF link: {(df['pdf_url'].str.len()>0).sum()}")
    print(f"  with abstract: {(df['full_text'].str.len()>0).sum()}")
    yr = df[df['year'].str.len()==4]['year'].value_counts().sort_index()
    print(f"  year range: {yr.index.min() if len(yr) else '?'}–{yr.index.max() if len(yr) else '?'}")
    print(f"Output: {pq_path}")


if __name__ == "__main__":
    main()
