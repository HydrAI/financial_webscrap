#!/usr/bin/env python3
"""Download full-text PDFs for the Tier-B futures papers and extract their text.

Reads research_papers/papers_tierB_futures.parquet, downloads each paper's PDF
(arXiv OA links are reliable; many publisher links are paywalled and will fail),
saves the raw PDF, and extracts text via the project's PDFExtractor.

The abstract is PRESERVED in its own `abstract` column. `full_text` becomes the
extracted PDF text when available, else falls back to the abstract.

Output:
    research_papers/pdfs/<id>.pdf
    research_papers/papers_tierB_futures_fulltext.parquet (+ .jsonl)

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_download_paper_pdfs.py
    ... --limit 20 --delay 2
"""

import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "financial_scraper" / "src"))
from financial_scraper.extract.pdf import PDFExtractor  # noqa: E402

OUT = ROOT / "research_papers"
PDF_DIR = OUT / "pdfs"
INPUT = OUT / "papers_tierB_futures.parquet"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
    "Accept": "application/pdf,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}
MIN_FULLTEXT_CHARS = 500


def safe_name(row) -> str:
    if row.get("arxiv_id"):
        base = "arxiv_" + str(row["arxiv_id"])
    elif row.get("doi"):
        base = "doi_" + str(row["doi"])
    else:
        base = "u_" + hashlib.sha256(str(row["pdf_url"]).encode()).hexdigest()[:12]
    base = re.sub(r"[^\w.\-]", "_", base)[:120]
    return base + ".pdf"


def download(url: str, timeout: int) -> bytes | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code != 200:
            return None
        data = r.content
        ct = r.headers.get("content-type", "").lower()
        if "text/html" in ct or b"%PDF-" not in data[:1024]:
            return None  # anti-bot HTML page, not a real PDF
        return data
    except requests.RequestException:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(INPUT))
    ap.add_argument("--out", default="",
                    help="Output parquet (default: <input stem w/o _clean>_fulltext.parquet)")
    ap.add_argument("--pdf-dir", default=str(PDF_DIR),
                    help="Dir for raw PDFs (default: research_papers/pdfs)")
    ap.add_argument("--delay", type=float, default=2.0)
    ap.add_argument("--timeout", type=int, default=40)
    ap.add_argument("--limit", type=int, default=0, help="Process only first N (0=all)")
    args = ap.parse_args()

    in_path = Path(args.input)
    out_pq = Path(args.out) if args.out else (
        OUT / (in_path.stem.replace("_clean", "") + "_fulltext.parquet"))
    pdf_dir = Path(args.pdf_dir)

    df = pd.read_parquet(args.input)
    if args.limit:
        df = df.head(args.limit).copy()
    pdf_dir.mkdir(parents=True, exist_ok=True)
    extractor = PDFExtractor()

    # Preserve the abstract; full_text currently holds it.
    df["abstract"] = df["full_text"].fillna("").astype(str)
    df["pdf_file"] = ""
    df["pdf_status"] = ""
    df["has_fulltext"] = False
    df["words"] = 0

    n_ok = n_nolink = n_dlfail = n_extractfail = 0
    total = len(df)
    for i, idx in enumerate(df.index):
        row = df.loc[idx]
        url = str(row.get("pdf_url") or "").strip()
        if not url:
            df.at[idx, "pdf_status"] = "no_link"
            n_nolink += 1
            continue
        data = download(url, args.timeout)
        if not data:
            df.at[idx, "pdf_status"] = "download_failed"
            n_dlfail += 1
            time.sleep(args.delay)
            continue
        fname = safe_name(row)
        (pdf_dir / fname).write_bytes(data)
        ex = extractor.extract(data, url)
        if ex.extraction_method != "failed" and len(ex.text) >= MIN_FULLTEXT_CHARS:
            df.at[idx, "full_text"] = ex.text          # replace abstract w/ full text
            df.at[idx, "has_fulltext"] = True
            df.at[idx, "words"] = ex.word_count
            df.at[idx, "pdf_status"] = "ok"
            df.at[idx, "pdf_file"] = fname
            n_ok += 1
        else:
            df.at[idx, "pdf_status"] = "extract_failed"
            df.at[idx, "pdf_file"] = fname
            n_extractfail += 1
        if (i + 1) % 25 == 0:
            print(f"  {i+1}/{total} | full-text {n_ok} | dl-fail {n_dlfail} | "
                  f"extract-fail {n_extractfail} | no-link {n_nolink}", flush=True)
        time.sleep(args.delay)

    df.to_parquet(out_pq, index=False)
    df.to_json(out_pq.with_suffix(".jsonl"), orient="records", lines=True, force_ascii=False)

    print(f"\n=== Done ===")
    print(f"Papers: {total}")
    print(f"  full text extracted: {n_ok}")
    print(f"  download failed (paywall/block): {n_dlfail}")
    print(f"  extract failed: {n_extractfail}")
    print(f"  no PDF link: {n_nolink}")
    print(f"  (all {total} keep their abstract; {total-n_ok} fall back to abstract as full_text)")
    print(f"Raw PDFs: {pdf_dir}")
    print(f"Output: {out_pq}")


if __name__ == "__main__":
    main()
