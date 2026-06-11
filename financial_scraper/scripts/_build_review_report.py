#!/usr/bin/env python3
"""Generic literature-review PDF + BibTeX builder for any paper corpus.

Combines a corpus's taxonomy + chronological-review Markdown into one PDF, with a
References section and BibTeX appendix generated from corpus metadata. Generalizes
_build_sentiment_report.py via a --prefix that resolves all the per-corpus paths.

Output (for --prefix X):
    research_papers/X_review_report.pdf
    research_papers/X_cited.bib
    research_papers/X_corpus.bib

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_build_review_report.py \\
        --prefix kg_equities --title "Knowledge Graphs & GNNs for Equities"
"""

import argparse
import glob
import re
from io import BytesIO
from pathlib import Path

import markdown as md
import pandas as pd
from xhtml2pdf import pisa

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"


def norm(t): return re.sub(r"[^a-z0-9]+", " ", str(t).lower()).strip()


def citekey(row, used):
    auth = str(row.get("authors") or "").split(",")[0].strip()
    sur = re.sub(r"[^A-Za-z]", "", auth.split()[-1]) if auth else "anon"
    yr = str(row.get("year") or "nd")[:4]
    tw = re.sub(r"[^a-z0-9]", "", (norm(row.get("title", "")).split(" ") or ["x"])[0]) or "x"
    base, i = f"{sur.lower()}{yr}{tw}", 1
    key = base
    while key in used:
        i += 1; key = f"{base}{chr(96+i)}"
    used.add(key); return key


def bibtex(row, key):
    def esc(s): return str(s or "").replace("{", "").replace("}", "").replace("&amp;", "and").replace("&", "and")
    authors = " and ".join(a.strip() for a in str(row.get("authors") or "").split(",") if a.strip())
    venue = esc(row.get("venue")); doi = str(row.get("doi") or ""); arx = str(row.get("arxiv_id") or "")
    is_arxiv = bool(arx) and not venue
    f = [f"  author    = {{{authors}}}", f"  title     = {{{esc(row.get('title'))}}}",
         f"  year      = {{{str(row.get('year') or '')[:4]}}}"]
    if venue: f.append(f"  journal   = {{{venue}}}")
    if doi: f.append(f"  doi       = {{{doi}}}")
    if is_arxiv: f += [f"  eprint    = {{{arx}}}", "  archivePrefix = {arXiv}"]
    if row.get("link"): f.append(f"  url       = {{{row.get('link')}}}")
    return f"@{'misc' if is_arxiv else 'article'}{{{key},\n" + ",\n".join(f) + "\n}"


def ref_line(row, key):
    a = str(row.get("authors") or "Anon.")
    if a.count(",") > 3: a = ", ".join(a.split(",")[:3]) + ", et al."
    doi = str(row.get("doi") or "")
    tail = f" doi:{doi}" if doi else (f" arXiv:{row.get('arxiv_id')}" if row.get("arxiv_id") else "")
    return f"- [{key}] {a} ({str(row.get('year') or 'n.d.')[:4]}). *{str(row.get('title')).strip()}*. {row.get('venue') or row.get('source') or ''}.{tail}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True, help="Corpus prefix, e.g. kg_equities")
    ap.add_argument("--title", required=True)
    ap.add_argument("--subtitle", default="A Structured & Chronological Literature Review")
    ap.add_argument("--exec-summary", default=None,
                    help="Optional markdown file prepended as an executive-summary front section")
    ap.add_argument("--exec-title", default="Executive Summary — Practitioner Guidelines")
    args = ap.parse_args()
    p = args.prefix

    src = sorted(glob.glob(str(PAPERS / f"{p}_papers_*_fulltext.parquet")))[-1]
    corpus = pd.read_parquet(src)
    by_title = {norm(t): r for t, r in zip(corpus["title"], corpus.to_dict("records"))}
    taxo = (PAPERS / f"{p}_taxonomy.md").read_text(encoding="utf-8")
    chrono = (PAPERS / f"{p}_chronological_review.md").read_text(encoding="utf-8")

    cited = set()
    for txt in (taxo, chrono):
        cited |= {norm(m) for m in re.findall(r"\*\*(.+?)\*\*", txt)}
    cited |= {norm(m) for m in re.findall(r"\*([^*]+?)\*\s*\((?:19|20)\d\d", chrono)}

    used, refs, bibs = set(), [], []
    for nt in sorted(cited):
        row = by_title.get(nt)
        if not row: continue
        k = citekey(row, used); refs.append(ref_line(row, k)); bibs.append(bibtex(row, k))
    u2 = set()
    (PAPERS / f"{p}_corpus.bib").write_text("\n\n".join(bibtex(r, citekey(r, u2)) for r in corpus.to_dict("records")), encoding="utf-8")
    (PAPERS / f"{p}_cited.bib").write_text("\n\n".join(bibs), encoding="utf-8")

    title_block = (f"# {args.title}\n## {args.subtitle}\n\n"
                   f"*Auto-generated from a curated corpus of {len(corpus):,} papers "
                   f"({int(corpus['has_fulltext'].sum())} with full text). Part I maps the "
                   f"field; Part II narrates the innovation timeline; references include "
                   f"BibTeX keys with a BibTeX appendix.*\n\n")
    taxo_b = re.sub(r"^# .*\n", "", taxo, count=1)
    chrono_b = re.sub(r"^# .*\n", "", chrono, count=1)
    brk = '\n<div style="page-break-before:always"></div>\n\n'
    exec_b = ""
    if args.exec_summary:
        ex = Path(args.exec_summary).read_text(encoding="utf-8")
        ex = re.sub(r"^# .*\n", "", ex, count=1)  # drop its own H1; use --exec-title
        exec_b = brk + f"# {args.exec_title}\n\n" + ex
    combined = (title_block + exec_b + brk + "# Part I — Literature Map\n\n" + taxo_b
                + brk + "# Part II — Chronological Review\n\n" + chrono_b
                + brk + f"# References ({len(refs)} cited works)\n\n" + "\n".join(refs))
    body = md.markdown(combined, extensions=["tables", "fenced_code", "sane_lists"])
    bib_html = "<h1>Appendix — BibTeX</h1><pre class='bib'>" + "\n\n".join(bibs).replace("&", "&amp;").replace("<", "&lt;") + "</pre>"

    css = """@page { size: a4; margin: 1.7cm; }
    body{font-family:Helvetica;font-size:9.5pt;line-height:1.35;color:#111;}
    h1{font-size:18pt;color:#15396b;} h2{font-size:13pt;color:#15396b;border-bottom:1px solid #bbb;padding-bottom:2px;margin-top:16px;}
    h3{font-size:10.5pt;color:#333;margin-top:11px;} table{border-collapse:collapse;width:100%;font-size:8pt;margin:6px 0;}
    th,td{border:0.5px solid #888;padding:3px 4px;} th{background:#e9eef7;} ul{margin:3px 0 8px 0;} li{margin-bottom:2px;}
    pre.bib{font-family:Courier;font-size:7pt;white-space:pre-wrap;} em{color:#222;}"""
    html = f"<html><head><meta charset='utf-8'><style>{css}</style></head><body>{body}<div style='page-break-before:always'></div>{bib_html}</body></html>"

    out = PAPERS / f"{p}_review_report.pdf"
    with open(out, "wb") as f:
        res = pisa.CreatePDF(BytesIO(html.encode("utf-8")), dest=f)
    print(f"Cited matched: {len(refs)} | corpus bib: {len(corpus)}")
    print(f"PDF: {out} ({out.stat().st_size/1024:.0f} KB){'  [errors]' if res.err else ''}")


if __name__ == "__main__":
    main()
