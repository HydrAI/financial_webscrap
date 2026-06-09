#!/usr/bin/env python3
"""Thematic taxonomy of the factor-combination / factor-alpha corpus.

Multi-label classifies each paper (title+abstract) by weighting scheme,
combination approach, factor style, task, and themes; emits a Markdown literature
map (counts + top-cited papers) + a per-paper labeled parquet.

Reads research_papers/factor_alpha_combine_papers_*_fulltext.parquet.
Output:
    research_papers/factor_alpha_combine_taxonomy.md
    research_papers/factor_alpha_combine_labeled.parquet
"""

import glob
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"

WEIGHTING = [
    ("Weighting scheme", "IC / IC-IR weighted", r"information coefficient|\bic[- ]?weight|ic-?ir|\bicir\b|rank ic"),
    ("Weighting scheme", "Equal weight", r"equal[- ]?weight|equally[- ]weighted"),
    ("Weighting scheme", "Optimization (mean-variance / max-IR)", r"mean[- ]variance|portfolio optimi|maximi[sz].{0,18}(?:information ratio|sharpe)|max(?:imum)? sharpe"),
    ("Weighting scheme", "ML-based weighting", r"machine learning|xgboost|gradient boost|neural network|deep learning|random forest|lightgbm"),
    ("Weighting scheme", "Regression / Fama-MacBeth", r"\bregression|fama[- ]?macbeth|cross[- ]sectional regression"),
    ("Weighting scheme", "Shrinkage / Bayesian", r"shrinkage|bayesian|\bridge\b|\blasso\b|elastic net|regulari[sz]"),
]
COMBINATION = [
    ("Combination approach", "Linear composite / scoring", r"composite|scoring|\bscore\b|linear combination|weighted sum|z-?score|standardi[sz]e"),
    ("Combination approach", "Dimensionality reduction (PCA / latent)", r"\bpca\b|principal component|dimension(?:ality)? reduction|latent factor|autoencoder"),
    ("Combination approach", "Ensemble / stacking", r"ensemble|stacking|bagging|boosting"),
    ("Combination approach", "Dynamic / conditional / timing", r"dynamic|conditional|factor timing|regime|time[- ]varying|adaptive|rotation"),
    ("Combination approach", "Nonlinear / interactions", r"nonlinear|interaction effect|deep learning|neural network"),
]
FACTORS = [
    ("Factor styles", "Value", r"\bvalue\b|book[- ]to[- ]market|earnings yield|\bb/m\b|valuation"),
    ("Factor styles", "Momentum", r"momentum"),
    ("Factor styles", "Quality / profitability", r"quality|profitab|gross profit|\broe\b|earnings quality"),
    ("Factor styles", "Size", r"\bsize\b|market cap|small[- ]cap|capitali[sz]ation"),
    ("Factor styles", "Low volatility / risk", r"low[- ]volatilit|low[- ]risk|idiosyncratic vol|\bbeta\b"),
    ("Factor styles", "Growth / investment", r"\bgrowth\b|investment factor|asset growth"),
]
TASKS = [
    ("Task", "Stock selection / ranking", r"stock selection|ranking|learning to rank|select stocks|top-?k"),
    ("Task", "Return / alpha prediction", r"return prediction|predict.{0,18}return|\balpha\b|expected return"),
    ("Task", "Portfolio construction", r"portfolio (?:construction|optimi|formation)|long[- ]short|asset allocation"),
    ("Task", "Performance / evaluation", r"performance evaluation|backtest|out[- ]of[- ]sample|sharpe ratio|information ratio"),
]
THEMES = [
    ("Cross-cutting themes", "Transaction costs / turnover", r"transaction cost|turnover|trading cost|implementation shortfall"),
    ("Cross-cutting themes", "Factor zoo / multiple testing", r"factor zoo|multiple testing|p-?hacking|data snooping|multitude|replicat"),
    ("Cross-cutting themes", "Machine learning", r"machine learning|deep learning|neural network|gradient boost"),
    ("Cross-cutting themes", "China / emerging market", r"\bchina\b|chinese|\bcsi\b|a[- ]share|\bindia\b|emerging market"),
]
AXES = WEIGHTING + COMBINATION + FACTORS + TASKS + THEMES


def main():
    src = sorted(glob.glob(str(PAPERS / "factor_alpha_combine_papers_*_fulltext.parquet")))[-1]
    df = pd.read_parquet(src)
    df["yr"] = pd.to_numeric(df["year"], errors="coerce")
    blob = (df["title"].fillna("") + " " + df["abstract"].fillna("")).str.lower()
    for section, sub, pat in AXES:
        df[f"{section} :: {sub}"] = blob.str.contains(pat, regex=True)
    df.to_parquet(PAPERS / "factor_alpha_combine_labeled.parquet", index=False)

    N = len(df)
    L = []; w = L.append
    w("# Factor Combination & Weighting for Equity Alpha — Literature Map\n")
    w(f"*Corpus: {N} curated papers on combining/weighting equity factors into a "
      f"composite alpha. Years {int(df['yr'].min())}–{int(df['yr'].max())}. "
      f"Auto-classified from title+abstract; multi-label.*\n")
    ic = df["Weighting scheme :: IC / IC-IR weighted"].sum()
    ml = df["Weighting scheme :: ML-based weighting"].sum()
    opt = df["Weighting scheme :: Optimization (mean-variance / max-IR)"].sum()
    w("## Trend at a glance\n")
    w(f"- Weighting schemes present: IC/IC-IR {ic}, optimization {opt}, ML-based {ml}.")
    w(f"- The corpus spans the classic linear-composite/scoring approach through "
      f"optimization and, most recently, machine-learning factor combination.\n")

    cur = None
    for section, sub, _ in AXES:
        if section != cur:
            w(f"\n## {section}\n"); cur = section
        key = f"{section} :: {sub}"
        sel = df[df[key]].sort_values("citations", ascending=False)
        if not len(sel):
            w(f"### {sub}  ·  0 papers\n"); continue
        w(f"### {sub}  ·  {len(sel)} papers ({len(sel)*100//N}%)\n")
        for _, r in sel.head(6).iterrows():
            yy = "" if pd.isna(r["yr"]) else int(r["yr"])
            w(f"- **{str(r['title']).strip()}** ({yy}) — {int(r['citations'] or 0)} cites, {r['source']}")
        w("")

    out = PAPERS / "factor_alpha_combine_taxonomy.md"
    out.write_text("\n".join(L), encoding="utf-8")
    print(f"Wrote {out}")
    print("\nSubsection counts:")
    for section, sub, _ in AXES:
        print(f"  {sub:46s} {int(df[f'{section} :: {sub}'].sum()):4d}")


if __name__ == "__main__":
    main()
