#!/usr/bin/env python3
"""Generic, --prefix-driven thematic taxonomy for a paper corpus.

Holds an axes registry per corpus prefix; classifies {prefix}_papers_*_fulltext
.parquet (multi-label, title+abstract) and emits {prefix}_taxonomy.md +
{prefix}_labeled.parquet. Feeds the generic review builder.

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_taxonomy_generic.py --prefix ml_futures
"""

import argparse
import glob
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"

AXES = {
    "ml_futures": [
        ("Method", "RNN / LSTM / GRU", r"\blstm\b|\bgru\b|recurrent neural|\brnn\b"),
        ("Method", "CNN", r"\bcnn\b|convolutional"),
        ("Method", "Transformer / attention", r"transformer|attention[- ]based|\bbert\b"),
        ("Method", "Reinforcement learning", r"reinforcement learning|\bdrl\b|deep q|q-learning|actor[- ]critic"),
        ("Method", "Classical ML (SVM/RF/boosting)", r"support vector|\bsvm\b|random forest|gradient boost|xgboost|\bknn\b|decision tree"),
        ("Method", "Hybrid / ensemble / decomposition", r"hybrid|ensemble|\bemd\b|wavelet|decomposition|fusion"),
        ("Method", "Genetic / evolutionary", r"genetic algorithm|evolutionary|genetic programming"),
        ("Prediction target", "Price / return forecasting", r"price (?:prediction|forecast)|return (?:prediction|forecast)|forecasting.{0,15}price"),
        ("Prediction target", "Direction / movement", r"direction|movement|up.{0,3}down|trend prediction"),
        ("Prediction target", "Volatility", r"volatilit"),
        ("Prediction target", "Trading strategy / signals", r"trading (?:strateg|system|signal|rule)|algorithmic trading|backtest"),
        ("Asset / market", "Energy (crude / gas)", r"crude|\boil\b|natural gas|wti|brent|energy futures"),
        ("Asset / market", "Metals (gold / copper)", r"gold|silver|copper|metal|nickel|aluminium"),
        ("Asset / market", "Agriculture", r"agricultur|corn|soybean|wheat|crop|grain|coffee|sugar"),
        ("Asset / market", "Equity index futures", r"index futures|stock index|s&p|csi 300|nifty|equity index"),
        ("Asset / market", "Interest-rate / bond futures", r"interest rate futures|bond futures|treasury futures"),
        ("Asset / market", "FX / crypto futures", r"currency futures|fx futures|bitcoin|crypto"),
        ("Cross-cutting themes", "High-frequency / intraday", r"high[- ]frequency|intraday|tick data|limit order"),
        ("Cross-cutting themes", "Technical indicators", r"technical (?:indicator|analysis)|moving average|\brsi\b|macd"),
        ("Cross-cutting themes", "Sentiment / news", r"sentiment|news|text"),
    ],
    "cta_trend": [
        ("Strategy", "Trend following", r"trend[- ]following|trend strateg|\bcta\b|managed futures"),
        ("Strategy", "Time-series momentum", r"time[- ]series momentum|\btsmom\b"),
        ("Strategy", "Cross-sectional momentum", r"cross[- ]sectional momentum|relative momentum"),
        ("Strategy", "Carry", r"\bcarry\b|carry trade"),
        ("Strategy", "Counter-trend / mean-reversion", r"counter[- ]trend|mean[- ]reversion|reversal"),
        ("Topics", "Crisis alpha / tail hedge", r"crisis alpha|tail (?:risk|hedg)|safe haven|crash|drawdown protection|divergent"),
        ("Topics", "Replication / style analysis", r"replicat|style analysis|clone|factor decomposition|return-based"),
        ("Topics", "Performance / survivorship bias", r"performance|survivorship|survivor bias|backfill|persistence"),
        ("Topics", "Risk management / vol scaling", r"volatility (?:scaling|target)|risk management|position sizing|risk budget"),
        ("Topics", "Fees / capacity / turnover", r"\bfee|capacity|turnover|transaction cost|smart leverage"),
        ("Topics", "Diversification", r"diversif|correlation"),
        ("Asset class", "Commodities", r"commodit|crude|metal|agricultur|energy"),
        ("Asset class", "Currencies / FX", r"currenc|\bfx\b|foreign exchange"),
        ("Asset class", "Rates / bonds", r"\bbond\b|fixed income|interest rate|treasury"),
        ("Asset class", "Equity index", r"equity index|stock index|\bs&p"),
        ("Asset class", "Multi-asset", r"multi[- ]asset|cross[- ]asset|diversified futures"),
        ("Method", "Econometric / regression", r"regression|garch|\bvar\b|cointegrat|predictab"),
        ("Method", "Machine learning", r"machine learning|deep learning|neural network|reinforcement learning"),
        ("Method", "Signal rules (MA / breakout)", r"moving average|breakout|channel|donchian|crossover"),
    ],
    "ga_ml": [
        ("Application", "Stock selection / prediction", r"stock (?:selection|prediction|price|return|market)|equity"),
        ("Application", "Trading strategy / system", r"trading (?:strateg|system|rule|signal)|algorithmic trading"),
        ("Application", "Portfolio optimization", r"portfolio (?:optimi|selection|management)|asset allocation"),
        ("Application", "Credit scoring / risk", r"credit (?:scoring|risk)|loan|default prediction"),
        ("Application", "Fraud detection", r"fraud"),
        ("Application", "Bankruptcy / distress prediction", r"bankrupt|financial distress|insolvenc"),
        ("Application", "Price / index forecasting", r"price (?:forecast|prediction)|index (?:forecast|prediction)|exchange rate (?:forecast|prediction)"),
        ("GA role", "Feature selection", r"feature selection|feature subset|variable selection|attribute selection"),
        ("GA role", "Hyperparameter / parameter optimization", r"hyperparameter|parameter (?:optimi|tuning|selection)|optimize.{0,15}parameter"),
        ("GA role", "Weight / portfolio optimization", r"weight optimi|portfolio optimi|optimal weight|asset allocation"),
        ("GA role", "Trading-rule evolution / GP", r"genetic programming|trading rule|evolve.{0,15}rule|rule (?:discovery|generation)"),
        ("GA role", "Neural architecture / topology", r"architecture|topology|neuroevolution|structure optimi"),
        ("Hybrid method", "GA + neural network", r"genetic algorithm.{0,30}neural|neural.{0,30}genetic|ga[- ]?bp|ga[- ]?ann"),
        ("Hybrid method", "GA + SVM", r"genetic.{0,20}support vector|genetic.{0,20}\bsvm\b|ga[- ]?svm"),
        ("Hybrid method", "GA + fuzzy", r"fuzzy"),
        ("Hybrid method", "GA + LSTM / deep", r"genetic.{0,25}(?:lstm|deep learning|cnn)|(?:lstm|deep learning).{0,25}genetic"),
        ("Market", "Forex / currency", r"forex|exchange rate|currenc|\bfx\b"),
        ("Market", "Cryptocurrency", r"crypto|bitcoin"),
        ("Market", "Commodity / futures", r"commodit|crude|gold|futures"),
        ("Market", "China / emerging", r"\bchina\b|chinese|\bcsi\b|india|emerging"),
    ],
}

TITLES = {
    "ml_futures": "Machine Learning for Futures",
    "cta_trend": "CTA / Managed Futures & Trend Following",
    "ga_ml": "Genetic Algorithms in Financial Machine Learning",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True, choices=list(AXES))
    args = ap.parse_args()
    p = args.prefix
    axes = AXES[p]

    src = sorted(glob.glob(str(PAPERS / f"{p}_papers_*_fulltext.parquet")))[-1]
    df = pd.read_parquet(src)
    df["yr"] = pd.to_numeric(df["year"], errors="coerce")
    blob = (df["title"].fillna("") + " " + df["abstract"].fillna("")).str.lower()
    for section, sub, pat in axes:
        df[f"{section} :: {sub}"] = blob.str.contains(pat, regex=True)
    df.to_parquet(PAPERS / f"{p}_labeled.parquet", index=False)

    N = len(df)
    L = []; w = L.append
    w(f"# {TITLES[p]} — Literature Map\n")
    w(f"*Corpus: {N} curated papers, {int(df['has_fulltext'].sum())} with full text. "
      f"Years {int(df['yr'].min())}–{int(df['yr'].max())}. Multi-label, "
      f"auto-classified from title+abstract.*\n")
    cur = None
    for section, sub, _ in axes:
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
    (PAPERS / f"{p}_taxonomy.md").write_text("\n".join(L), encoding="utf-8")
    print(f"Wrote {p}_taxonomy.md ({N} papers)")
    for section, sub, _ in axes:
        print(f"  {sub:46s} {int(df[f'{section} :: {sub}'].sum()):4d}")


if __name__ == "__main__":
    main()
