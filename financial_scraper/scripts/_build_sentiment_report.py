#!/usr/bin/env python3
"""Combine the equity-sentiment taxonomy + chronological review into one PDF,
with a References section and a BibTeX appendix generated from corpus metadata.

Pipeline: corpus metadata -> BibTeX; two review .md files + references + bib
-> Markdown -> HTML (+ print CSS) -> PDF via xhtml2pdf.

Output:
    research_papers/equity_sentiment_review_report.pdf
    research_papers/equity_sentiment_cited.bib        (papers cited in the report)
    research_papers/equity_sentiment_corpus.bib       (all 1,172 papers)

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_build_sentiment_report.py
"""

import glob
import re
from io import BytesIO
from pathlib import Path

import markdown as md
import pandas as pd
from xhtml2pdf import pisa

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"
TAXO = PAPERS / "equity_sentiment_taxonomy.md"
CHRONO = PAPERS / "equity_sentiment_chronological_review.md"


def norm(t: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(t).lower()).strip()


def citekey(row, used: set) -> str:
    auth = str(row.get("authors") or "").split(",")[0].strip()
    sur = re.sub(r"[^A-Za-z]", "", auth.split()[-1]) if auth else "anon"
    yr = str(row.get("year") or "n.d.")[:4]
    tw = re.sub(r"[^a-z0-9]", "", norm(row.get("title", "")).split(" ")[0]) if row.get("title") else "x"
    base = f"{sur.lower()}{yr}{tw}"
    key, i = base, 1
    while key in used:
        i += 1; key = f"{base}{chr(96+i)}"
    used.add(key)
    return key


def bibtex(row, key: str) -> str:
    def esc(s): return str(s or "").replace("{", "").replace("}", "").replace("&amp;", "and").replace("&", "and")
    authors = " and ".join(a.strip() for a in str(row.get("authors") or "").split(",") if a.strip())
    title = esc(row.get("title"))
    year = str(row.get("year") or "")[:4]
    venue = esc(row.get("venue"))
    doi = str(row.get("doi") or "")
    arx = str(row.get("arxiv_id") or "")
    link = str(row.get("link") or "")
    is_arxiv = bool(arx) and not venue
    typ = "misc" if is_arxiv else "article"
    fields = [f"  author    = {{{authors}}}", f"  title     = {{{title}}}", f"  year      = {{{year}}}"]
    if venue:
        fields.append(f"  journal   = {{{venue}}}")
    if doi:
        fields.append(f"  doi       = {{{doi}}}")
    if is_arxiv:
        fields.append(f"  eprint    = {{{arx}}}")
        fields.append(f"  archivePrefix = {{arXiv}}")
    if link:
        fields.append(f"  url       = {{{link}}}")
    return f"@{typ}{{{key},\n" + ",\n".join(fields) + "\n}"


def reference_line(row, key: str) -> str:
    authors = str(row.get("authors") or "Anon.")
    if authors.count(",") > 3:
        authors = ", ".join(authors.split(",")[:3]) + ", et al."
    yr = str(row.get("year") or "n.d.")[:4]
    venue = str(row.get("venue") or row.get("source") or "")
    doi = str(row.get("doi") or "")
    tail = f" doi:{doi}" if doi else (f" arXiv:{row.get('arxiv_id')}" if row.get("arxiv_id") else "")
    return f"- [{key}] {authors} ({yr}). *{str(row.get('title')).strip()}*. {venue}.{tail}"


def main():
    src = sorted(glob.glob(str(PAPERS / "equity_sentiment_papers_*_fulltext.parquet")))[-1]
    corpus = pd.read_parquet(src)
    by_title = {norm(t): r for t, r in zip(corpus["title"], corpus.to_dict("records"))}

    taxo_txt = TAXO.read_text(encoding="utf-8")
    chrono_txt = CHRONO.read_text(encoding="utf-8")

    # Cited papers = bold titles across both reviews, matched to the corpus.
    cited_titles = set()
    for txt in (taxo_txt, chrono_txt):
        for m in re.findall(r"\*\*(.+?)\*\*", txt):
            cited_titles.add(norm(m))
    for m in re.findall(r"\*([^*]+?)\*\s*\((?:19|20)\d\d", chrono_txt):  # italic cites in prose
        cited_titles.add(norm(m))

    used: set = set()
    cited_rows, refs, bibs = [], [], []
    for nt in sorted(cited_titles):
        row = by_title.get(nt)
        if not row:
            continue
        k = citekey(row, used)
        cited_rows.append(row)
        refs.append(reference_line(row, k))
        bibs.append(bibtex(row, k))

    # Full-corpus bib (separate file)
    used2: set = set()
    corpus_bib = "\n\n".join(bibtex(r, citekey(r, used2)) for r in corpus.to_dict("records"))
    (PAPERS / "equity_sentiment_corpus.bib").write_text(corpus_bib, encoding="utf-8")
    cited_bib = "\n\n".join(bibs)
    (PAPERS / "equity_sentiment_cited.bib").write_text(cited_bib, encoding="utf-8")

    # Compose combined markdown
    title_block = (
        "# Equity-Sentiment Analysis with NLP\n"
        "## A Structured & Chronological Literature Review\n\n"
        f"*Auto-generated from a curated corpus of {len(corpus):,} papers "
        f"({int(corpus['has_fulltext'].sum())} with full text), 2007–2026. "
        "Part I maps the field by method/source/target; Part II narrates the "
        "innovation timeline crossing source × method; references are listed with "
        "BibTeX keys and a BibTeX appendix.*\n\n"
    )
    # strip the leading H1 of each sub-doc to avoid duplicate titles
    taxo_body = re.sub(r"^# .*\n", "", taxo_txt, count=1)
    chrono_body = re.sub(r"^# .*\n", "", chrono_txt, count=1)

    combined_md = (
        title_block
        + '\n<div style="page-break-before:always"></div>\n\n'
        + "# Part I — Literature Map\n\n" + taxo_body
        + '\n<div style="page-break-before:always"></div>\n\n'
        + "# Part II — Chronological Review\n\n" + chrono_body
        + '\n<div style="page-break-before:always"></div>\n\n'
        + f"# References ({len(refs)} cited works)\n\n" + "\n".join(refs)
    )

    body_html = md.markdown(combined_md, extensions=["tables", "fenced_code", "sane_lists"])
    bib_html = "<h1>Appendix — BibTeX</h1><pre class='bib'>" + \
        cited_bib.replace("&", "&amp;").replace("<", "&lt;") + "</pre>"

    css = """
    @page { size: a4; margin: 1.7cm; @frame footer {-pdf-frame-content: footer; bottom:1cm; height:1cm;} }
    body { font-family: Helvetica; font-size: 9.5pt; line-height: 1.35; color:#111; }
    h1 { font-size: 18pt; color:#15396b; margin-top:6px; }
    h2 { font-size: 13pt; color:#15396b; border-bottom:1px solid #bbb; padding-bottom:2px; margin-top:16px;}
    h3 { font-size: 10.5pt; color:#333; margin-top:11px; }
    table { border-collapse: collapse; width:100%; font-size:8pt; margin:6px 0;}
    th,td { border:0.5px solid #888; padding:3px 4px; }
    th { background:#e9eef7; }
    ul { margin:3px 0 8px 0; }
    li { margin-bottom:2px; }
    pre.bib { font-family: Courier; font-size:7pt; white-space:pre-wrap; }
    em { color:#222; }
    """
    html = (f"<html><head><meta charset='utf-8'><style>{css}</style></head><body>"
            f"<div id='footer' style='text-align:center;font-size:7pt;color:#888'>"
            f"Equity-Sentiment Literature Review · page <pdf:pagenumber></div>"
            f"{body_html}"
            f"<div style='page-break-before:always'></div>{bib_html}"
            f"</body></html>")

    out = PAPERS / "equity_sentiment_review_report.pdf"
    with open(out, "wb") as f:
        res = pisa.CreatePDF(BytesIO(html.encode("utf-8")), dest=f)
    size = out.stat().st_size / 1024
    print(f"Cited works matched: {len(refs)}  | corpus bib: {len(corpus)} entries")
    print(f"PDF: {out} ({size:.0f} KB){'  [errors]' if res.err else ''}")
    print(f"BibTeX: equity_sentiment_cited.bib ({len(bibs)}), equity_sentiment_corpus.bib ({len(corpus)})")


if __name__ == "__main__":
    main()
