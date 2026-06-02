#!/usr/bin/env python3
"""Two-tier relevance filter for the ML research-paper corpus.

Reads research_papers/ml_futures_papers_combined.parquet and narrows it:

  Tier A  (financial)        : ML method AND a finance/markets term, MINUS
                               off-domain noise (agronomy, remote sensing,
                               medical/bio, smart-city/CV/SE).
  Tier B  (futures asset cls): subset of A that explicitly concerns FUTURES
                               contracts (commodity/index/rate/financial
                               futures) — the \\bfutures\\b token excludes the
                               'future work' / 'in the future' false hits.

Output:
  research_papers/papers_tierA_financial.parquet (+ .jsonl)
  research_papers/papers_tierB_futures.parquet   (+ .jsonl)

Usage:
  C:\\T\\python.exe financial_scraper/scripts/_filter_papers.py
"""

import glob
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "research_papers"

ML_RE = (
    r"machine learning|deep learning|neural network|\blstm\b|\bgru\b|\bcnn\b|"
    r"transformer|reinforcement learning|\bdrl\b|random forest|support vector|"
    r"\bsvm\b|gradient boost|xgboost|lightgbm|\bensemble\b|autoencoder|"
    r"attention mechanism|\bgan\b|graph neural"
)

# Genuine finance / markets vocabulary.
FINANCE_RE = (
    r"financial market|stock market|stock price|equity|trading|trader|"
    r"price forecast|price prediction|volatility|portfolio|asset pric|"
    r"derivative|option pric|\bfutures\b|forex|foreign exchange|exchange rate|"
    r"\bbond\b|treasury|yield curve|cryptocurrenc|bitcoin|hedg|return predict|"
    r"algorithmic trading|market microstructure|s&p 500|s&amp;p|nasdaq|"
    r"commodity price|crude oil price|electricity price|carbon price|"
    r"financial time series|quantitative finance|order book|limit order"
)

# Off-domain clusters that survive a naive keyword match — drop them.
OFFDOMAIN_RE = (
    r"crop yield|yield prediction|remote sensing|satellite imag|vegetation|"
    r"\bsoil\b|rainfall|precipitation forecast|evapotranspiration|"
    r"\bmedical\b|clinical|patient|cancer|tumor|disease|protein|genom|"
    r"single-cell|thrombectomy|molecul|drug |healthcare|"
    r"smart city|traffic flow|autonomous driving|object detection|"
    r"image segmentation|software engineering|wireless|antenna|"
    r"intrusion detection|sentiment of tweets about|"
    # 'futures' in the foresight / scenario-planning sense (not the asset class)
    r"speculative futures|futures studies|futures literacy|futures thinking|"
    r"adaptive policy pathway|scenario planning|strategic foresight"
)

# Explicit futures-AS-AN-ASSET-CLASS signal. A bare "futures" token is too
# loose (it matched "futures company", or a passing mention in a generic
# algorithmic-trading abstract), so we require a qualified asset phrase and
# apply it to the TITLE — the paper must be *about* a futures series, not
# merely mention one.
FUTURES_RE = (
    r"futures price|futures market|futures contract|futures return|"
    r"futures volatilit|futures trading|futures curve|futures hedg|"
    r"futures spread|term structure of futures|continuous futures|rolling futures|"
    r"index futures|stock index futures|equity futures|financial futures|"
    r"bond futures|treasury futures|interest[ -]rate futures|currency futures|"
    r"fx futures|commodity futures|agricultural futures|metal futures|"
    r"energy futures|crude[ -]?oil futures|oil futures|natural gas futures|"
    r"gas futures|gold futures|silver futures|copper futures|nickel futures|"
    r"alumin\w+ futures|zinc futures|iron ore futures|coal futures|"
    r"carbon futures|soybean futures|corn futures|wheat futures|cotton futures|"
    r"sugar futures|coffee futures|livestock futures|cattle futures|"
    r"vix futures|vstoxx futures|bitcoin futures|crypto\w* futures|"
    r"csi 300 futures|s&p 500 futures|s&amp;p 500 futures|nikkei futures|"
    r"\w+ futures contract"
)
# Institutional 'futures' senses that are NOT the asset class.
FUTURES_INSTITUTIONAL_RE = (
    r"futures company|futures broker|futures commission|futures association|"
    r"futures industry"
)


def _blob(df: pd.DataFrame) -> pd.Series:
    return (df["title"].fillna("") + " " + df["full_text"].fillna("")
            + " " + df.get("categories", "").fillna("")).str.lower()


def main():
    fs = sorted(glob.glob(str(OUT / "ml_futures_papers_combined.parquet")))
    if not fs:
        print("Combined corpus not found; run the fetch/combine step first.")
        sys.exit(1)
    df = pd.read_parquet(fs[-1])
    blob = _blob(df)

    is_ml = blob.str.contains(ML_RE, regex=True)
    is_fin = blob.str.contains(FINANCE_RE, regex=True)
    is_off = blob.str.contains(OFFDOMAIN_RE, regex=True)
    is_fut = blob.str.contains(FUTURES_RE, regex=True)

    tierA = df[is_ml & is_fin & ~is_off].copy()
    # Tier B is a strict subset of A: an explicit futures-asset phrase must
    # appear in the TITLE (so the paper is *about* futures, not merely
    # mentioning them), and not in the institutional sense.
    t = tierA["title"].fillna("").str.lower()
    tierB = tierA[t.str.contains(FUTURES_RE, regex=True)
                  & ~t.str.contains(FUTURES_INSTITUTIONAL_RE, regex=True)].copy()

    for name, sub in (("papers_tierA_financial", tierA), ("papers_tierB_futures", tierB)):
        sub = sub.sort_values("citations", ascending=False).reset_index(drop=True)
        sub.to_parquet(OUT / f"{name}.parquet", index=False)
        sub.to_json(OUT / f"{name}.jsonl", orient="records", lines=True, force_ascii=False)

    print(f"Combined corpus:        {len(df)}")
    print(f"Tier A (financial):     {len(tierA)}   (dropped {len(df)-len(tierA)} off-topic/agronomy/etc.)")
    print(f"Tier B (futures asset): {len(tierB)}   (subset of A)")
    print()
    print("=== Tier B sample (futures, by citations) ===")
    tb = tierB.sort_values("citations", ascending=False)
    for _, r in tb.head(18).iterrows():
        print(f"  [{int(r.citations):5d}] {str(r.title)[:86]} ({r.year}, {r.source})")


if __name__ == "__main__":
    main()
