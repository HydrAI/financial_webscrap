#!/usr/bin/env python3
"""Thematic taxonomy of the BROAD factor / cross-section asset-pricing corpus.

Classifies the 538-paper broad corpus by factor/anomaly type, asset-pricing model,
methodology, and themes; emits a Markdown literature map + labeled parquet.

Reads research_papers/factor_alpha_papers_*_fulltext.parquet (the broad corpus;
note this glob does NOT match factor_alpha_combine_papers_*).
Output:
    research_papers/factor_alpha_taxonomy.md
    research_papers/factor_alpha_labeled.parquet
"""

import glob
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"

FACTORS = [
    ("Factors / anomalies", "Value", r"\bvalue\b|book[- ]to[- ]market|earnings yield|\bb/m\b|cash flow yield"),
    ("Factors / anomalies", "Momentum", r"momentum"),
    ("Factors / anomalies", "Size", r"\bsize\b|small[- ]cap|market capitali"),
    ("Factors / anomalies", "Profitability / quality", r"profitab|quality|gross profit|\broe\b|earnings quality"),
    ("Factors / anomalies", "Investment / asset growth", r"investment factor|asset growth|capital investment|accrual"),
    ("Factors / anomalies", "Low volatility / beta", r"low[- ]volatilit|idiosyncratic vol|low[- ]beta|betting against beta|\bbab\b"),
    ("Factors / anomalies", "Liquidity / illiquidity", r"liquidit|illiquid|amihud|bid[- ]ask"),
    ("Factors / anomalies", "Default / distress risk", r"default risk|distress|bankruptcy|credit risk"),
    ("Factors / anomalies", "Sentiment / behavioral", r"sentiment|behavioral|overreact|underreact|differences of opinion|disagreement"),
]
MODELS = [
    ("Asset-pricing models", "CAPM", r"\bcapm\b|capital asset pricing"),
    ("Asset-pricing models", "Fama-French 3-factor", r"three[- ]factor|3[- ]factor|fama[- ]?french"),
    ("Asset-pricing models", "Carhart / 4-factor (momentum)", r"carhart|four[- ]factor|4[- ]factor"),
    ("Asset-pricing models", "Fama-French 5-factor", r"five[- ]factor|5[- ]factor"),
    ("Asset-pricing models", "q-factor / investment-based", r"q-?factor|investment[- ]based|hou[- ]xue[- ]zhang"),
    ("Asset-pricing models", "Conditional / ICAPM / consumption", r"conditional (?:capm|factor|model)|icapm|consumption[- ]based|intertemporal"),
    ("Asset-pricing models", "Characteristic / latent-factor", r"characteristic[- ]based|latent factor|principal component|\bipca\b|instrumented"),
]
METHODS = [
    ("Methodology", "Portfolio sorts", r"portfolio sort|sorted portfolio|decile|quintile|long[- ]short portfolio"),
    ("Methodology", "Fama-MacBeth / cross-sectional regression", r"fama[- ]?macbeth|cross[- ]sectional regression"),
    ("Methodology", "Time-series regression / GMM", r"time[- ]series regression|\bgmm\b|spanning test|\bgrs\b"),
    ("Methodology", "Machine learning", r"machine learning|deep learning|neural network|random forest|gradient boost|\blasso\b"),
    ("Methodology", "Factor zoo / multiple testing / replication", r"factor zoo|multiple testing|p-?hacking|data snooping|replicat|out[- ]of[- ]sample|multitude"),
]
THEMES = [
    ("Cross-cutting themes", "Risk-based vs behavioral / mispricing", r"mispricing|risk[- ]based|behavioral|limits? to arbitrage|anomal"),
    ("Cross-cutting themes", "Transaction costs / arbitrage limits", r"transaction cost|trading cost|arbitrage|short[- ]sale|turnover"),
    ("Cross-cutting themes", "International / emerging markets", r"international|emerging market|\bchina\b|\bjapan\b|cross[- ]countr|global"),
    ("Cross-cutting themes", "Other asset classes (bonds/FX/commodities)", r"\bbond\b|fixed income|currenc|\bfx\b|commodit|corporate bond"),
]
AXES = FACTORS + MODELS + METHODS + THEMES


def main():
    src = sorted(glob.glob(str(PAPERS / "factor_alpha_papers_*_fulltext.parquet")))[-1]
    df = pd.read_parquet(src)
    df["yr"] = pd.to_numeric(df["year"], errors="coerce")
    blob = (df["title"].fillna("") + " " + df["abstract"].fillna("")).str.lower()
    for section, sub, pat in AXES:
        df[f"{section} :: {sub}"] = blob.str.contains(pat, regex=True)
    df.to_parquet(PAPERS / "factor_alpha_labeled.parquet", index=False)

    N = len(df)
    L = []; w = L.append
    w("# Factor / Cross-Section Asset Pricing — Literature Map (broad corpus)\n")
    w(f"*Corpus: {N} curated factor / cross-section asset-pricing papers, "
      f"{int(df['has_fulltext'].sum())} with full text. Years "
      f"{int(df['yr'].min())}–{int(df['yr'].max())}. Multi-label, auto-classified "
      f"from title+abstract.*\n")
    w("## Trend at a glance\n")
    for nm in ["Value", "Momentum", "Liquidity / illiquidity", "Profitability / quality"]:
        c = int(df[f"Factors / anomalies :: {nm}"].sum())
        w(f"- {nm}: {c} papers")
    ml = int(df["Methodology :: Machine learning"].sum())
    zoo = int(df["Methodology :: Factor zoo / multiple testing / replication"].sum())
    w(f"- Machine-learning methodology {ml}; factor-zoo / replication discipline {zoo}.\n")

    cur = None
    for section, sub, _ in AXES:
        if section != cur:
            w(f"\n## {section}\n"); cur = section
        key = f"{section} :: {sub}"
        sel = df[df[key]].sort_values("citations", ascending=False)
        if not len(sel):
            continue
        w(f"### {sub}  ·  {len(sel)} papers ({len(sel)*100//N}%)\n")
        for _, r in sel.head(6).iterrows():
            yy = "" if pd.isna(r["yr"]) else int(r["yr"])
            w(f"- **{str(r['title']).strip()}** ({yy}) — {int(r['citations'] or 0)} cites, {r['source']}")
        w("")

    out = PAPERS / "factor_alpha_taxonomy.md"
    out.write_text("\n".join(L), encoding="utf-8")
    print(f"Wrote {out}")
    print("\nSubsection counts:")
    for section, sub, _ in AXES:
        print(f"  {sub:48s} {int(df[f'{section} :: {sub}'].sum()):4d}")


if __name__ == "__main__":
    main()
