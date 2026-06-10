#!/usr/bin/env python3
"""Zip the downloaded paper PDFs with human-readable <year>_<title>.pdf names.

The on-disk PDFs stay ID-keyed (arxiv_<id>.pdf / doi_<...>.pdf) so the
parquet `pdf_file` references remain valid; this only changes the names *inside*
the zip, mapped from each corpus's *_fulltext.parquet (pdf_file -> title/year).
Unmapped files keep their original name. Duplicate readable names get a suffix.

Output:
    research_papers/equity_sentiment_paper_pdfs.zip
    research_papers/ml_futures_and_cta_paper_pdfs.zip

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_zip_readable_pdfs.py
"""

import glob
import re
import zipfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PAPERS = ROOT / "research_papers"


def latest(pat: str) -> str | None:
    fs = sorted(glob.glob(str(PAPERS / pat)))
    return fs[-1] if fs else None


def readable(year: str, title: str) -> str:
    yr = str(year)[:4]
    slug = re.sub(r"[^A-Za-z0-9]+", "_", str(title)).strip("_")[:90]
    slug = slug or "untitled"
    return (f"{yr}_{slug}" if yr.isdigit() else slug) + ".pdf"


def _expected_pdf_name(row) -> str:
    """Reconstruct the downloader's safe_name() for an id-bearing row."""
    if str(row.get("arxiv_id") or ""):
        base = "arxiv_" + str(row["arxiv_id"])
    elif str(row.get("doi") or ""):
        base = "doi_" + str(row["doi"])
    else:
        return ""
    return re.sub(r"[^\w.\-]", "_", base)[:120] + ".pdf"


def build_map(parquet: str | None) -> dict[str, str]:
    """pdf_file basename -> readable name. Uses the `pdf_file` column when
    present, and also reconstructs expected filenames from arxiv_id/doi so a
    broader corpus can name PDFs a narrower fulltext parquet doesn't list."""
    out: dict[str, str] = {}
    if not parquet or not Path(parquet).exists():
        return out
    df = pd.read_parquet(parquet)
    for _, r in df.iterrows():
        nice = readable(r.get("year", ""), r.get("title", ""))
        if "pdf_file" in df.columns and str(r.get("pdf_file") or ""):
            out.setdefault(r["pdf_file"], nice)
        exp = _expected_pdf_name(r)
        if exp:
            out.setdefault(exp, nice)
    return out


def zip_corpus(z: zipfile.ZipFile, pdf_dir: Path, folder: str, name_map: dict[str, str]):
    used: set[str] = set()
    n = 0
    for f in sorted(pdf_dir.glob("*.pdf")):
        nice = name_map.get(f.name, f.name)
        # de-collide within the folder
        stem, ext = nice[:-4], ".pdf"
        cand, i = nice, 2
        while f"{folder}/{cand}" in used:
            cand = f"{stem}__{i}{ext}"
            i += 1
        used.add(f"{folder}/{cand}")
        z.write(f, arcname=f"{folder}/{cand}")
        n += 1
    return n


def zip_subset(z, pdf_dir, folder, name_map):
    """Like zip_corpus but only includes PDFs whose basename is in name_map
    (used to carve a focused sub-corpus out of a shared PDF directory)."""
    used, n = set(), 0
    for f in sorted(pdf_dir.glob("*.pdf")):
        if f.name not in name_map:
            continue
        nice = name_map[f.name]
        stem, cand, i = nice[:-4], nice, 2
        while f"{folder}/{cand}" in used:
            cand = f"{stem}__{i}.pdf"; i += 1
        used.add(f"{folder}/{cand}")
        z.write(f, arcname=f"{folder}/{cand}")
        n += 1
    return n


def main():
    # --- equity sentiment ---
    es_map = build_map(latest("equity_sentiment_papers_*_fulltext.parquet"))
    es_zip = PAPERS / "equity_sentiment_paper_pdfs.zip"
    with zipfile.ZipFile(es_zip, "w", zipfile.ZIP_DEFLATED) as z:
        n = zip_corpus(z, PAPERS / "pdfs_equity_sentiment", "equity_sentiment", es_map)
    print(f"{es_zip.name}: {n} PDFs ({es_zip.stat().st_size/1_048_576:.1f} MB)")

    # --- ml_futures + cta combined ---
    # Broader futures corpus (clean, ~639) covers the extra PDFs from the
    # original 354-download that the tightened 155 fulltext parquet omits.
    fut_map = build_map(latest("ml_futures_papers_*_clean.parquet"))
    fut_map.update(build_map(PAPERS / "papers_tierB_futures_fulltext.parquet"))
    cta_map = build_map(latest("cta_trend_papers_*_fulltext.parquet"))
    fc_zip = PAPERS / "ml_futures_and_cta_paper_pdfs.zip"
    with zipfile.ZipFile(fc_zip, "w", zipfile.ZIP_DEFLATED) as z:
        nf = zip_corpus(z, PAPERS / "pdfs", "ml_futures", fut_map)
        nc = zip_corpus(z, PAPERS / "pdfs_cta", "cta", cta_map)
    print(f"{fc_zip.name}: {nf+nc} PDFs (ml_futures={nf}, cta={nc}) "
          f"({fc_zip.stat().st_size/1_048_576:.1f} MB)")

    # --- KG / GNN for equities ---
    kg_map = build_map(latest("kg_equities_papers_*_fulltext.parquet"))
    if (PAPERS / "pdfs_kg_equities").exists():
        kg_zip = PAPERS / "kg_equities_paper_pdfs.zip"
        with zipfile.ZipFile(kg_zip, "w", zipfile.ZIP_DEFLATED) as z:
            nk = zip_corpus(z, PAPERS / "pdfs_kg_equities", "kg_equities", kg_map)
        print(f"{kg_zip.name}: {nk} PDFs ({kg_zip.stat().st_size/1_048_576:.1f} MB)")

    # --- Genetic algorithms in ML (finance-scoped) ---
    ga_map = build_map(latest("ga_ml_papers_*_fulltext.parquet"))
    if (PAPERS / "pdfs_ga_ml").exists():
        ga_zip = PAPERS / "ga_ml_paper_pdfs.zip"
        with zipfile.ZipFile(ga_zip, "w", zipfile.ZIP_DEFLATED) as z:
            ng = zip_corpus(z, PAPERS / "pdfs_ga_ml", "ga_ml", ga_map)
        print(f"{ga_zip.name}: {ng} PDFs ({ga_zip.stat().st_size/1_048_576:.1f} MB)")

    # --- factor alpha: broad corpus + focused (combination/weighting) subset ---
    if (PAPERS / "pdfs_factor_alpha").exists():
        fa_map = build_map(latest("factor_alpha_papers_*_fulltext.parquet"))
        fa_zip = PAPERS / "factor_alpha_paper_pdfs.zip"
        with zipfile.ZipFile(fa_zip, "w", zipfile.ZIP_DEFLATED) as z:
            nb = zip_corpus(z, PAPERS / "pdfs_factor_alpha", "factor_alpha", fa_map)
        print(f"{fa_zip.name}: {nb} PDFs ({fa_zip.stat().st_size/1_048_576:.1f} MB)")

        fc_map = build_map(latest("factor_alpha_combine_papers_*_fulltext.parquet"))
        fc_zip = PAPERS / "factor_alpha_combine_paper_pdfs.zip"
        # Prefer the dedicated focused PDF dir (augmented corpus, incl. regularization
        # papers); fall back to carving the subset out of the broad dir.
        with zipfile.ZipFile(fc_zip, "w", zipfile.ZIP_DEFLATED) as z:
            if (PAPERS / "pdfs_factor_alpha_combine").exists():
                nfc = zip_corpus(z, PAPERS / "pdfs_factor_alpha_combine", "factor_alpha_combine", fc_map)
            else:
                nfc = zip_subset(z, PAPERS / "pdfs_factor_alpha", "factor_alpha_combine", fc_map)
        print(f"{fc_zip.name}: {nfc} PDFs ({fc_zip.stat().st_size/1_048_576:.1f} MB)")

    # show a few example readable names
    print("\nExamples:")
    for k in list(es_map)[:6]:
        print(f"  {k}  ->  {es_map[k]}")


if __name__ == "__main__":
    main()
