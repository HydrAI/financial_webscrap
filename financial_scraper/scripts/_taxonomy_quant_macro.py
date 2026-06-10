#!/usr/bin/env python3
"""Thematic taxonomy of the systematic / quant macro corpus.

Multi-label classifies each paper (title+abstract) by strategy type, asset class,
method, and themes; emits a Markdown literature map + labeled parquet.

Reads research_papers/quant_macro_papers_*_fulltext.parquet.
Output:
    research_papers/quant_macro_taxonomy.md
    research_papers/quant_macro_labeled.parquet
"""

import glob
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"

STRATEGIES = [
    ("Strategy type", "Trend following / time-series momentum", r"trend[- ]following|time[- ]series momentum|\bcta\b|managed futures|trend strateg"),
    ("Strategy type", "Carry", r"carry trade|currency carry|\bcarry\b"),
    ("Strategy type", "Value (cross-asset)", r"\bvalue\b|valuation|value premium"),
    ("Strategy type", "Cross-sectional momentum", r"cross[- ]sectional momentum|momentum (?:strateg|portfolio|everywhere)|relative momentum"),
    ("Strategy type", "Risk parity / vol targeting", r"risk parity|volatility target|vol[- ]target|equal risk contribution|risk budget"),
    ("Strategy type", "Regime-switching / tactical allocation", r"regime[- ]switch\w*|markov[- ]switch|tactical asset allocation|regime detection|dynamic asset allocation"),
    ("Strategy type", "Macro factor / nowcasting", r"macro factor|macroeconomic (?:factor|predictor|risk)|nowcast|business cycle|leading indicator"),
    ("Strategy type", "Mean reversion / relative value", r"mean[- ]reversion|relative value|statistical arbitrage|pairs trading"),
]
ASSETS = [
    ("Asset class", "Currencies / FX", r"currenc|foreign exchange|\bfx\b|exchange rate"),
    ("Asset class", "Rates / bonds", r"\bbond\b|fixed income|treasury|yield curve|interest rate|sovereign"),
    ("Asset class", "Commodities", r"commodit|crude|gold|futures market"),
    ("Asset class", "Equity indices", r"equity index|stock index|equity market|index futures|s&p"),
    ("Asset class", "Multi-asset / cross-asset", r"multi[- ]asset|cross[- ]asset|asset classes|diversified"),
    ("Asset class", "Credit", r"\bcredit\b|corporate bond|cds\b|default risk"),
]
METHODS = [
    ("Method", "Econometric / predictability regression", r"predictab|regression|vector autoregress|\bvar\b|cointegrat|garch"),
    ("Method", "Machine learning / deep learning", r"machine learning|deep learning|neural network|random forest|gradient boost|reinforcement learning"),
    ("Method", "Regime / Markov-switching models", r"markov[- ]switch|regime[- ]switch|hidden markov|state space"),
    ("Method", "Optimization / risk-based allocation", r"mean[- ]variance|portfolio optimi|risk budget|risk parity|maximum diversification"),
    ("Method", "Factor models", r"factor model|multi[- ]factor|factor premia|arbitrage pricing"),
]
THEMES = [
    ("Cross-cutting themes", "Risk management / vol targeting / drawdown", r"volatility target|drawdown|risk management|tail risk|vol[- ]target|downside"),
    ("Cross-cutting themes", "Crisis / tail hedging / safe havens", r"crisis|tail hedg|safe haven|flight to quality|crash|disaster"),
    ("Cross-cutting themes", "Transaction costs / capacity", r"transaction cost|turnover|capacity|implementation|liquidity cost"),
    ("Cross-cutting themes", "Machine learning", r"machine learning|deep learning|neural network"),
]
AXES = STRATEGIES + ASSETS + METHODS + THEMES


def main():
    src = sorted(glob.glob(str(PAPERS / "quant_macro_papers_*_fulltext.parquet")))[-1]
    df = pd.read_parquet(src)
    df["yr"] = pd.to_numeric(df["year"], errors="coerce")
    blob = (df["title"].fillna("") + " " + df["abstract"].fillna("")).str.lower()
    for section, sub, pat in AXES:
        df[f"{section} :: {sub}"] = blob.str.contains(pat, regex=True)
    df.to_parquet(PAPERS / "quant_macro_labeled.parquet", index=False)

    N = len(df)
    L = []; w = L.append
    w("# Systematic / Quant Macro — Literature Map\n")
    w(f"*Corpus: {N} curated systematic / quant macro papers, "
      f"{int(df['has_fulltext'].sum())} with full text. Years "
      f"{int(df['yr'].min())}–{int(df['yr'].max())}. Multi-label, auto-classified "
      f"from title+abstract.*\n")
    w("## Trend at a glance\n")
    for nm in ["Trend following / time-series momentum", "Carry", "Risk parity / vol targeting", "Regime-switching / tactical allocation"]:
        w(f"- {nm}: {int(df[f'Strategy type :: {nm}'].sum())} papers")
    ml = int(df["Method :: Machine learning / deep learning"].sum())
    w(f"- Machine-learning methodology: {ml} papers.\n")

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

    out = PAPERS / "quant_macro_taxonomy.md"
    out.write_text("\n".join(L), encoding="utf-8")
    print(f"Wrote {out}")
    print("\nSubsection counts:")
    for section, sub, _ in AXES:
        print(f"  {sub:48s} {int(df[f'{section} :: {sub}'].sum()):4d}")


if __name__ == "__main__":
    main()
