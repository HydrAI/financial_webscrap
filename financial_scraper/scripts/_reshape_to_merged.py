"""Reshape every scraper parquet in the project into the KG's 8-col merged format.

Target schema (from Downloads/merged_2002.parquet):
    company, title, link, snippet, date(timestamp[ns]), source, full_text, source_file

Output:
    kg_input/merged_{YYYY}.parquet       — partitioned by date.year
    kg_input/merged_undated.parquet      — rows with null date
    kg_input/merged_provenance.parquet   — sidecar keyed on `link` for dropped cols

Strategy:
    - One source file at a time → reshape → explode to <=5000-char chunks
      → partition by year → write to kg_input/_staging/{source_id}/{year}.parquet
    - Large files (>500 MB) stream via pyarrow row-group batches to avoid OOM
    - Final pass: per year, concat all staging files → merged_{year}.parquet
    - Provenance rows streamed into _staging/_provenance/{source_id}.parquet
"""

import functools
import hashlib
import shutil
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

print = functools.partial(print, flush=True)  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "kg_input"
STAGE = OUT / "_staging"
PROV_STAGE = STAGE / "_provenance"

TARGET_COLS = ["company", "title", "link", "snippet", "date",
               "source", "full_text", "source_file"]
CHUNK_CHARS = 5000
SNIPPET_CHARS = 200
LARGE_FILE_MB = 500
BATCH_ROWS = 50_000


# ---------- helpers ----------

def parse_dates(s: pd.Series) -> pd.Series:
    out = pd.to_datetime(s, errors="coerce", utc=True, format="ISO8601")
    mask = out.isna() & s.notna() & (s.astype(str).str.len() > 0)
    if mask.any():
        fb = pd.to_datetime(s[mask], errors="coerce", utc=True, format="mixed")
        out.loc[mask] = fb
    return out.dt.tz_localize(None)


def chunk_text(text: str, size: int = CHUNK_CHARS) -> list[str]:
    if not text:
        return [""]
    text = str(text)
    if len(text) <= size:
        return [text]
    chunks = []
    i = 0
    n = len(text)
    while i < n:
        end = min(i + size, n)
        if end < n:
            br = text.rfind("\n\n", i, end)
            if br == -1 or br - i < size // 2:
                br = text.rfind(". ", i, end)
            if br != -1 and br - i >= size // 2:
                end = br + 2
        chunks.append(text[i:end])
        i = end
    return chunks


def explode_chunks(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["full_text"] = df["full_text"].fillna("").astype(str)
    df["full_text"] = df["full_text"].apply(lambda t: chunk_text(t, CHUNK_CHARS))
    df = df.explode("full_text", ignore_index=True)
    df["snippet"] = df["full_text"].str.slice(0, SNIPPET_CHARS)
    df["date"] = parse_dates(df["date"])
    for c in TARGET_COLS:
        if c == "date":
            continue
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)
        else:
            df[c] = ""
    return df[TARGET_COLS]


_BATCH_COUNTER: dict[str, int] = {}


def safe_source_id(source_id: str) -> str:
    """Flatten path separators AND strip .parquet suffix so staging dirs
    don't get picked up as parquet datasets by rglob."""
    s = source_id.replace("/", "_").replace("\\", "_")
    if s.endswith(".parquet"):
        s = s[:-len(".parquet")]
    return s


def write_partitioned(df: pd.DataFrame, source_id: str):
    """Partition a dataframe by year. Each call writes a new counter-suffixed
    batch file per year so we never rewrite existing staging files."""
    if len(df) == 0:
        return
    stage_dir = STAGE / safe_source_id(source_id)
    stage_dir.mkdir(parents=True, exist_ok=True)
    batch = _BATCH_COUNTER.get(source_id, 0)
    _BATCH_COUNTER[source_id] = batch + 1
    years = df["date"].dt.year
    undated = years.isna()
    if undated.any():
        (df[undated]).to_parquet(stage_dir / f"undated_{batch:05d}.parquet", index=False)
    for year, part in df[~undated].groupby(years[~undated]):
        part.to_parquet(stage_dir / f"{int(year)}_{batch:05d}.parquet", index=False)


def write_provenance(prov: pd.DataFrame, source_id: str):
    if len(prov) == 0:
        return
    PROV_STAGE.mkdir(parents=True, exist_ok=True)
    prov.to_parquet(PROV_STAGE / f"{safe_source_id(source_id)}.parquet", index=False)


# ---------- reshapers ----------

def reshape_scraper_passthrough(path: Path, source_id: str):
    """8-col scraper files — direct pass-through (optionally with company_canonical)."""
    size_mb = path.stat().st_size / 1_048_576
    stream = size_mb > LARGE_FILE_MB
    pf = pq.ParquetFile(path)
    print(f"  {source_id}: {size_mb:.0f}MB, {pf.metadata.num_rows:,} rows, "
          f"{'streaming' if stream else 'in-memory'}")

    def handle(df: pd.DataFrame):
        if "company_canonical" in df.columns:
            cc = df["company_canonical"].fillna("").astype(str)
            df["company"] = cc.where(cc.str.len() > 0, df["company"])
            df = df.drop(columns=["company_canonical"])
        for c in TARGET_COLS:
            if c not in df.columns:
                df[c] = "" if c != "date" else pd.NaT
        if not df["source_file"].astype(str).str.len().gt(0).all():
            df["source_file"] = df["source_file"].where(
                df["source_file"].astype(str).str.len() > 0, path.name
            )
        out = explode_chunks(df[TARGET_COLS])
        write_partitioned(out, source_id)

    if stream:
        for i, batch in enumerate(pf.iter_batches(batch_size=BATCH_ROWS)):
            df = batch.to_pandas()
            handle(df)
            if i % 10 == 0:
                print(f"    batch {i+1}: {(i+1)*BATCH_ROWS:,} rows processed")
    else:
        handle(pf.read().to_pandas())


def reshape_sec(path: Path, source_id: str):
    df = pd.read_parquet(path)
    print(f"  {source_id}: {len(df):,} rows")
    out = pd.DataFrame({
        "company":     df["company"].fillna(""),
        "title":       df["form"].fillna("") + " " + df["filing_date"].fillna(""),
        "link":        df["url"].fillna(""),
        "snippet":     "",
        "date":        parse_dates(df["filing_date"]),
        "source":      "SEC EDGAR " + df["form"].fillna(""),
        "full_text":   df["full_text"].fillna(""),
        "source_file": source_id,
    })
    write_partitioned(explode_chunks(out), source_id)
    prov = pd.DataFrame({
        "link":           df["url"].fillna(""),
        "source_parquet": source_id,
        "ticker":         df.get("ticker", pd.Series([""] * len(df))).fillna("").astype(str),
        "form":           df["form"].fillna("").astype(str),
        "words":          df.get("words", pd.Series([0] * len(df))).fillna(0).astype("int64"),
    })
    write_provenance(prov, source_id)


def reshape_fca_nsm(path: Path, source_id: str):
    df = pd.read_parquet(path)
    print(f"  {source_id}: {len(df):,} rows")
    out = pd.DataFrame({
        "company":     df["csv_company"].fillna(df["hit_company"]).fillna(""),
        "title":       df["headline"].fillna(""),
        "link":        df["download_url"].fillna(""),
        "snippet":     "",
        "date":        parse_dates(df["publication_date"]),
        "source":      "FCA NSM " + df["type_code"].fillna(""),
        "full_text":   df["full_text"].fillna(""),
        "source_file": source_id,
    })
    write_partitioned(explode_chunks(out), source_id)
    prov = pd.DataFrame({
        "link":           df["download_url"].fillna(""),
        "source_parquet": source_id,
        "lei":            df["csv_lei"].fillna("").astype(str),
        "hit_lei":        df["hit_lei"].fillna("").astype(str),
        "disclosure_id":  df["disclosure_id"].fillna("").astype(str),
        "type_code":      df["type_code"].fillna("").astype(str),
        "type":           df["type"].fillna("").astype(str),
        "file_kind":      df["file_kind"].fillna("").astype(str),
        "file_size_mb":   df["file_size_mb"].fillna(0.0).astype(float),
        "document_date":  df["document_date"].fillna("").astype(str),
        "words":          df["words"].fillna(0).astype("int64"),
    })
    write_provenance(prov, source_id)


def reshape_uk_ch(path: Path, source_id: str):
    df = pd.read_parquet(path)
    print(f"  {source_id}: {len(df):,} rows")
    link = df["company_number"].fillna("").apply(
        lambda cn: (f"https://find-and-update.company-information.service.gov.uk"
                    f"/company/{cn}/filing-history") if cn else ""
    )
    out = pd.DataFrame({
        "company":     df["company"].fillna(""),
        "title":       df["filing_type"].fillna("") + " " + df["filing_date"].fillna(""),
        "link":        link,
        "snippet":     df.get("description", pd.Series([""] * len(df))).fillna(""),
        "date":        parse_dates(df["filing_date"]),
        "source":      "Companies House " + df["filing_type"].fillna(""),
        "full_text":   df["full_text"].fillna(""),
        "source_file": source_id,
    })
    write_partitioned(explode_chunks(out), source_id)
    prov = pd.DataFrame({
        "link":           link,
        "source_parquet": source_id,
        "company_number": df["company_number"].fillna("").astype(str),
        "filing_type":    df["filing_type"].fillna("").astype(str),
        "pages":          df.get("pages", pd.Series([0] * len(df))).fillna(0).astype("int64"),
        "paper_filed":    df.get("paper_filed", pd.Series([False] * len(df))).fillna(False).astype(bool),
        "pdf_size_mb":    df.get("pdf_size_mb", pd.Series([0.0] * len(df))).fillna(0.0).astype(float),
        "words":          df.get("words", pd.Series([0] * len(df))).fillna(0).astype("int64"),
    })
    write_provenance(prov, source_id)


def reshape_quantica(path: Path, source_id: str):
    df = pd.read_parquet(path)
    print(f"  {source_id}: {len(df):,} rows")
    out = pd.DataFrame({
        "company":     "Quantica",
        "title":       df["title"].fillna(""),
        "link":        df["url"].fillna(""),
        "snippet":     "",
        "date":        pd.NaT,
        "source":      "quantica.com",
        "full_text":   df["text"].fillna(""),
        "source_file": source_id,
    })
    out["date"] = pd.to_datetime(out["date"])
    write_partitioned(explode_chunks(out), source_id)
    prov = pd.DataFrame({
        "link":           df["url"].fillna(""),
        "source_parquet": source_id,
        "words":          df.get("words", pd.Series([0] * len(df))).fillna(0).astype("int64"),
    })
    write_provenance(prov, source_id)


# ---------- source registry ----------

SOURCES = [
    # scraper 8-col
    (reshape_scraper_passthrough, "financial_scraper/runs/20260218_104852/scrape_20260218_104852.parquet"),
    (reshape_scraper_passthrough, "financial_scraper/runs/20260218_111016/scrape_20260218_111016.parquet"),
    (reshape_scraper_passthrough, "financial_scraper/runs/20260219_121938/scrape_20260219_121938.parquet"),
    (reshape_scraper_passthrough, "financial_scraper/runs/20260219_160338/scrape_20260219_160338.parquet"),
    (reshape_scraper_passthrough, "financial_scraper/runs/20260228_153240/scrape_20260228_153240.parquet"),
    (reshape_scraper_passthrough, "output/oklo_20260314_180835/patents_20260314_180835.parquet"),
    (reshape_scraper_passthrough, "financial_scraper/patent_data/bq_patents_20260315_091835/patents_20260315_091835.parquet"),
    (reshape_scraper_passthrough, "financial_scraper/patent_data/bq_patents_20260315_123552/patents_20260315_123552.parquet"),
    (reshape_scraper_passthrough, "financial_scraper/output/patents_converted/patents_converted_20260316_195027.parquet"),
    (reshape_scraper_passthrough, "financial_scraper/output/mega_scrape_20260317_191736.parquet"),
    (reshape_scraper_passthrough, "supply_chain_top100/20260331_113744/supply_chain_20260331_113744.parquet"),
    # scraper 9-col with company_canonical
    (reshape_scraper_passthrough, "financial_scraper/output/mega_scrape_20260317_191736_clean.parquet"),
    (reshape_scraper_passthrough, "financial_scraper/output/china_tech_news.parquet"),
    # SEC
    (reshape_sec, "sec_filings_test/sec_filings.parquet"),
    (reshape_sec, "sec_filings_top100/sec_filings.parquet"),
    (reshape_sec, "sec_test_kg/sec_filings.parquet"),
    (reshape_sec, "sec_filings_uk/sec_filings.parquet"),
    # FCA NSM (merged already includes the misses recovery)
    (reshape_fca_nsm, "fca_nsm_uk/fca_nsm.parquet"),
    # UK Companies House
    (reshape_uk_ch, "uk_filings_test/uk_filings.parquet"),
    # Quantica
    (reshape_quantica, "quantica_output/direct/quantica.parquet"),
]


# ---------- main ----------

def main():
    if OUT.exists():
        print(f"Wiping {OUT}")
        shutil.rmtree(OUT)
    OUT.mkdir()
    STAGE.mkdir()

    for reshaper, rel in SOURCES:
        path = ROOT / rel.replace("/", "\\") if "\\" not in rel else ROOT / rel
        path = ROOT / rel  # pathlib handles forward slashes on Windows
        if not path.exists():
            print(f"SKIP (missing): {rel}")
            continue
        source_id = rel  # keep the relative path as identifier
        print(f"\n>>> {rel}")
        try:
            reshaper(path, source_id)
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            continue

    # ---- final merge per year ----
    print("\n=== merging staging into final year files ===", flush=True)
    years: dict[str, list[Path]] = {}
    for p in STAGE.rglob("*.parquet"):
        if not p.is_file():
            continue
        if PROV_STAGE in p.parents or p.parent == PROV_STAGE:
            continue
        # filename is "{year}_{batch:05d}.parquet" or "undated_{batch:05d}.parquet"
        stem = p.stem.rsplit("_", 1)[0]
        years.setdefault(stem, []).append(p)

    for year in sorted(years):
        parts = [pd.read_parquet(p) for p in years[year]]
        df = pd.concat(parts, ignore_index=True)
        if year == "undated":
            out_path = OUT / "merged_undated.parquet"
        else:
            out_path = OUT / f"merged_{year}.parquet"
        df.to_parquet(out_path, index=False)
        print(f"  {out_path.name}: {len(df):,} rows", flush=True)

    # ---- provenance merge ----
    print("\n=== merging provenance sidecar ===")
    prov_parts = [pd.read_parquet(p) for p in PROV_STAGE.glob("*.parquet")]
    if prov_parts:
        prov = pd.concat(prov_parts, ignore_index=True)
        prov = prov.drop_duplicates(subset=["link", "source_parquet"])
        prov.to_parquet(OUT / "merged_provenance.parquet", index=False)
        print(f"  merged_provenance.parquet: {len(prov):,} rows, "
              f"{len(prov.columns)} cols")

    # ---- cleanup ----
    shutil.rmtree(STAGE)
    print("\nDone.")


if __name__ == "__main__":
    main()
