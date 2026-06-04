#!/usr/bin/env python3
"""Build a thematic taxonomy of the curated equity-sentiment corpus.

Multi-label classifies each paper (from title+abstract) along three axes —
method, data source, prediction target — plus cross-cutting themes, then emits
a structured Markdown literature map (counts + top-cited representative papers
per subsection) and a per-paper labeled parquet.

Output:
    research_papers/equity_sentiment_taxonomy.md
    research_papers/equity_sentiment_labeled.parquet

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_taxonomy_equity_sentiment.py
"""

import glob
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"

# (section, subsection, regex). A paper may match many.
METHODS = [
    ("Methods", "Lexicon / dictionary-based", r"lexicon|dictionary[- ]based|loughran|harvard.{0,6}(gi|iv)|sentiwordnet|\bvader\b|word ?list|opinion lexicon|bag[- ]of[- ]words"),
    ("Methods", "Classical ML (SVM/RF/NB/boosting)", r"naive bayes|support vector|\bsvm\b|random forest|logistic regression|gradient boost|xgboost|\bknn\b|decision tree|feature engineering"),
    ("Methods", "CNN", r"\bcnn\b|convolutional"),
    ("Methods", "RNN / LSTM / GRU", r"\blstm\b|\bgru\b|recurrent neural|\brnn\b"),
    ("Methods", "Transformers / BERT / FinBERT", r"\bbert\b|finbert|roberta|distilbert|\bxlnet\b|transformer|attention[- ]based"),
    ("Methods", "Large language models (GPT/LLM)", r"\bllm\b|\bgpt\b|gpt-?[34]|large language model|chatgpt|llama|mistral|generative (?:ai|pre-?trained)|in-?context learning|prompt"),
    ("Methods", "Graph-based (GNN / knowledge graph)", r"graph neural|\bgnn\b|knowledge graph|graph[- ]based"),
    ("Methods", "Word embeddings", r"word2vec|word embedding|\bglove\b|doc2vec|fasttext"),
    ("Methods", "Hybrid / ensemble / multimodal", r"hybrid|ensemble|fusion|multi[- ]?modal|multimodal"),
]
SOURCES = [
    ("Data sources", "Financial news & headlines", r"\bnews\b|headline|press release|reuters|bloomberg|news article"),
    ("Data sources", "Social media (Twitter/X, StockTwits, Reddit)", r"twitter|\btweets?\b|stocktwits|reddit|wallstreetbets|\bwsb\b|social media|weibo|message board|forum"),
    ("Data sources", "Earnings calls / transcripts", r"earnings call|conference call|earnings transcript|call transcript|management discussion|md&a"),
    ("Data sources", "Regulatory filings (10-K/8-K)", r"10-?k|10-?q|8-?k|annual report|sec filing|prospectus|regulatory filing"),
    ("Data sources", "Analyst reports", r"analyst report|analyst forecast|analyst recommendation|brokerage report|sell-side"),
    ("Data sources", "Central bank / policy communication", r"central bank|\bfomc\b|monetary policy|\becb\b|fed(?:eral reserve)? communication|policy statement"),
]
TARGETS = [
    ("Prediction targets", "Return / price / movement", r"return|price (?:prediction|forecast|movement|direction)|stock movement|directional"),
    ("Prediction targets", "Volatility", r"volatilit"),
    ("Prediction targets", "Trading volume / liquidity", r"trading volume|turnover|liquidity"),
    ("Prediction targets", "Trading strategy / portfolio", r"trading strateg|portfolio|algorithmic trading|backtest|profitab"),
]
THEMES = [
    ("Cross-cutting themes", "ESG / climate sentiment", r"\besg\b|climate|carbon|sustainab|greenwash|net[- ]zero"),
    ("Cross-cutting themes", "Explainability / interpretability", r"explainab|interpretab|\bxai\b|attention visuali|shap\b"),
    ("Cross-cutting themes", "Surveys / reviews / bibliometrics", r"\bsurvey\b|literature review|systematic review|bibliometric|review of"),
    ("Cross-cutting themes", "Multimodal / alternative data", r"multimodal|alternative data|satellite|audio|vocal|image-based"),
    ("Cross-cutting themes", "Emerging-market / non-English", r"chinese|china|india|arabic|korean|emerging market|non-english"),
]
AXES = METHODS + SOURCES + TARGETS + THEMES


def main():
    src = sorted(glob.glob(str(PAPERS / "equity_sentiment_papers_*_fulltext.parquet")))[-1]
    df = pd.read_parquet(src)
    df["yr"] = pd.to_numeric(df["year"], errors="coerce")
    blob = (df["title"].fillna("") + " " + df["abstract"].fillna("")).str.lower()

    # Tag
    labels: dict[str, pd.Series] = {}
    for section, sub, pat in AXES:
        key = f"{section} :: {sub}"
        labels[key] = blob.str.contains(pat, regex=True)
        df[key] = labels[key]
    df.to_parquet(PAPERS / "equity_sentiment_labeled.parquet", index=False)

    N = len(df)
    lines = []
    w = lines.append
    w(f"# Equity-Sentiment Literature Map\n")
    w(f"*Corpus: {N} curated papers (NLP sentiment for stocks/equities), "
      f"{int(df['has_fulltext'].sum())} with full text. "
      f"Years {int(df['yr'].min())}–{int(df['yr'].max())}. "
      f"Auto-classified from title+abstract; papers may appear in multiple "
      f"sections.*\n")

    # Temporal note
    yr = df.dropna(subset=["yr"]).copy(); yr["yr"] = yr["yr"].astype(int)
    recent = yr[yr["yr"] >= 2017]
    w("## Trend at a glance\n")
    w(f"- ~{int((yr['yr']>=2021).mean()*100)}% of the corpus is 2021 or later — the field is dominated by recent work.")
    bert = df["Methods :: Transformers / BERT / FinBERT"].sum()
    llm = df["Methods :: Large language models (GPT/LLM)"].sum()
    lex = df["Methods :: Lexicon / dictionary-based"].sum()
    w(f"- Method mix: lexicon-based {lex}, transformers/BERT {bert}, LLM/GPT {llm} "
      f"— the lexicon→transformer→LLM progression is visible.")
    news = df["Data sources :: Financial news & headlines"].sum()
    soc = df["Data sources :: Social media (Twitter/X, StockTwits, Reddit)"].sum()
    ec = df["Data sources :: Earnings calls / transcripts"].sum()
    w(f"- Dominant sources: news ({news}) and social media ({soc}); "
      f"earnings-call/transcript work is a smaller but distinct cluster ({ec}).\n")

    # Sections
    current = None
    for section, sub, _ in AXES:
        if section != current:
            w(f"\n## {section}\n")
            current = section
        key = f"{section} :: {sub}"
        sel = df[df[key]].sort_values("citations", ascending=False)
        if len(sel) == 0:
            continue
        w(f"### {sub}  ·  {len(sel)} papers ({len(sel)*100//N}%)\n")
        for _, r in sel.head(7).iterrows():
            yy = "" if pd.isna(r["yr"]) else int(r["yr"])
            cit = int(r["citations"]) if pd.notna(r["citations"]) else 0
            w(f"- **{str(r['title']).strip()}** ({yy}) — {cit} cites, {r['source']}")
        w("")

    out = PAPERS / "equity_sentiment_taxonomy.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {out}  ({len(lines)} lines)")
    # console summary
    print("\nSubsection counts:")
    for section, sub, _ in AXES:
        key = f"{section} :: {sub}"
        print(f"  {sub:46s} {int(df[key].sum()):4d}")


if __name__ == "__main__":
    main()
