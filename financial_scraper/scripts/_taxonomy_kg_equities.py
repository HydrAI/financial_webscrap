#!/usr/bin/env python3
"""Thematic taxonomy of the KG/GNN-for-equities corpus.

Multi-label classifies each paper (title+abstract) along axes specific to the
graph-based equity-prediction literature — graph model, relation/edge type,
data source, task, themes — and emits a Markdown literature map (counts +
top-cited papers) + a per-paper labeled parquet.

Output:
    research_papers/kg_equities_taxonomy.md
    research_papers/kg_equities_labeled.parquet
"""

import glob
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"

GRAPH_MODELS = [
    ("Graph model", "GCN (graph convolutional)", r"graph convolution|\bgcn\b"),
    ("Graph model", "GAT (graph attention)", r"graph attention|\bgat\b|attention.{0,20}graph"),
    ("Graph model", "Knowledge graph (entities/relations)", r"knowledge graph|entity (?:graph|embedding)|knowledge base|wikidata|dbpedia|ontolog"),
    ("Graph model", "Hypergraph", r"hypergraph"),
    ("Graph model", "Temporal / dynamic graph", r"temporal graph|dynamic graph|spatio[- ]?temporal|time[- ]aware graph"),
    ("Graph model", "Heterogeneous graph", r"heterogeneous graph|heterogeneous information network|\bhin\b|multi[- ]relational"),
    ("Graph model", "Graph embedding (node2vec/TransE/DeepWalk)", r"node2vec|deepwalk|transe|graph embedding|node embedding|network embedding"),
    ("Graph model", "GNN + sequence (LSTM/GRU/RNN)", r"(?:lstm|gru|\brnn\b|recurrent).{0,40}graph|graph.{0,40}(?:lstm|gru|recurrent)"),
    ("Graph model", "GNN + transformer", r"transformer.{0,30}graph|graph.{0,30}transformer"),
    ("Graph model", "Graph + reinforcement learning", r"reinforcement learning|\bdrl\b|deep q"),
]
RELATIONS = [
    ("Relation / edge type", "Price / return correlation", r"correlation|co[- ]?movement|price relation|return correlation|pearson"),
    ("Relation / edge type", "Supply chain / business relations", r"supply chain|supplier|customer relation|business relation|industrial chain|upstream|downstream"),
    ("Relation / edge type", "Ownership / shareholder / corporate", r"shareholder|ownership|subsidiar|parent company|board interlock|executive"),
    ("Relation / edge type", "Industry / sector", r"industry|sector|\bgics\b|same[- ]industry"),
    ("Relation / edge type", "News / event co-occurrence", r"event|co[- ]?occurrence|news[- ]based graph|mention|co[- ]mention"),
    ("Relation / edge type", "Wikidata / knowledge-base relations", r"wikidata|dbpedia|knowledge base|freebase"),
    ("Relation / edge type", "Social / investor relations", r"social media|investor relation|stocktwits|twitter|forum"),
]
SOURCES = [
    ("Data source", "Price / technical series", r"price (?:series|data)|technical indicator|ohlc|historical price|time series"),
    ("Data source", "Financial news", r"\bnews\b|headline|press release"),
    ("Data source", "Social media", r"twitter|tweet|stocktwits|reddit|social media|weibo"),
    ("Data source", "Filings / fundamentals", r"fundamental|10-?k|annual report|balance sheet|financial statement|filing"),
    ("Data source", "Knowledge bases", r"wikidata|dbpedia|freebase|knowledge base|encyclopedia"),
]
TASKS = [
    ("Task", "Movement / direction prediction", r"movement prediction|direction|trend prediction|up.{0,3}down|rise.{0,3}fall"),
    ("Task", "Return / price forecasting", r"return prediction|price (?:prediction|forecast)|stock prediction|price movement"),
    ("Task", "Stock ranking / selection", r"ranking|stock selection|learning to rank|top-?k|rank stocks"),
    ("Task", "Recommendation", r"recommend"),
    ("Task", "Portfolio / trading", r"portfolio|trading strateg|asset allocation|investment strateg"),
    ("Task", "Volatility / risk", r"volatilit|\brisk\b|systemic risk|contagion"),
]
THEMES = [
    ("Cross-cutting themes", "Explainability", r"explainab|interpretab|\bxai\b|attention weight"),
    ("Cross-cutting themes", "Multimodal (text + price + graph)", r"multimodal|multi[- ]modal|multi[- ]source|fusion"),
    ("Cross-cutting themes", "Surveys / reviews", r"\bsurvey\b|review of|systematic review|bibliometric"),
    ("Cross-cutting themes", "Emerging market / China", r"china|chinese|\bcsi\b|shanghai|shenzhen|india|\bnse\b"),
]
AXES = GRAPH_MODELS + RELATIONS + SOURCES + TASKS + THEMES


def main():
    src = sorted(glob.glob(str(PAPERS / "kg_equities_papers_*_fulltext.parquet")))[-1]
    df = pd.read_parquet(src)
    df["yr"] = pd.to_numeric(df["year"], errors="coerce")
    blob = (df["title"].fillna("") + " " + df["abstract"].fillna("")).str.lower()
    for section, sub, pat in AXES:
        df[f"{section} :: {sub}"] = blob.str.contains(pat, regex=True)
    df.to_parquet(PAPERS / "kg_equities_labeled.parquet", index=False)

    N = len(df)
    L = []; w = L.append
    w("# Knowledge-Graph / GNN for Equities — Literature Map\n")
    w(f"*Corpus: {N} curated papers (graph-based methods for equity prediction), "
      f"{int(df['has_fulltext'].sum())} with full text. Years "
      f"{int(df['yr'].min())}–{int(df['yr'].max())}. Auto-classified from "
      f"title+abstract; papers may appear in multiple sections.*\n")
    yr = df.dropna(subset=["yr"])
    w("## Trend at a glance\n")
    w(f"- ~{int((yr['yr']>=2021).mean()*100)}% of the corpus is 2021 or later.")
    kg = df['Graph model :: Knowledge graph (entities/relations)'].sum()
    gat = df['Graph model :: GAT (graph attention)'].sum()
    gcn = df['Graph model :: GCN (graph convolutional)'].sum()
    temp = df['Graph model :: Temporal / dynamic graph'].sum()
    w(f"- Graph models: GCN {gcn}, GAT {gat}, explicit knowledge graphs {kg}, "
      f"temporal/dynamic {temp}.")
    sc = df['Relation / edge type :: Supply chain / business relations'].sum()
    corr = df['Relation / edge type :: Price / return correlation'].sum()
    w(f"- Edge construction: correlation-based {corr}, supply-chain/business {sc} "
      f"— how the stock graph is wired is a core design axis.\n")

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

    out = PAPERS / "kg_equities_taxonomy.md"
    out.write_text("\n".join(L), encoding="utf-8")
    print(f"Wrote {out}")
    print("\nSubsection counts:")
    for section, sub, _ in AXES:
        print(f"  {sub:46s} {int(df[f'{section} :: {sub}'].sum()):4d}")


if __name__ == "__main__":
    main()
