"""Merge all transcript parquets into a single deduplicated file."""
import logging
from pathlib import Path
from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MAIN = Path("runs/us10002/transcripts.parquet")
WAYBACK = Path("runs/us10002/wayback_transcripts.parquet")
RESEARCH4 = Path("runs/us10002/research4_transcripts.parquet")
OUTPUT = Path("runs/us10002/transcripts_merged.parquet")

SCHEMA = pa.schema([
    ("company", pa.string()),
    ("title", pa.string()),
    ("link", pa.string()),
    ("snippet", pa.string()),
    ("date", pa.timestamp("ns")),
    ("source", pa.string()),
    ("full_text", pa.string()),
    ("source_file", pa.string()),
])

tables = []
for label, path in [("main", MAIN), ("wayback", WAYBACK), ("research4", RESEARCH4)]:
    if not path.exists():
        log.warning(f"{label}: NOT FOUND at {path}")
        continue
    t = pq.read_table(path)
    log.info(f"{label}: {t.num_rows} rows, {path.stat().st_size/1024/1024:.1f} MB")
    # Cast to common schema
    t = t.cast(SCHEMA)
    tables.append(t)

combined = pa.concat_tables(tables)
log.info(f"Combined (before dedup): {combined.num_rows} rows")

# Deduplicate by (company, year, quarter) — keep first occurrence (main > wayback > research4)
seen = set()
keep_indices = []
companies = combined.column("company").to_pylist()
dates = combined.column("date").to_pylist()
sources = combined.column("source").to_pylist()

dupes_by_source = Counter()
for i in range(combined.num_rows):
    company = companies[i]
    date = str(dates[i]) if dates[i] else ""
    if not company or len(date) < 7:
        keep_indices.append(i)
        continue
    yr = date[:4]
    mo_str = date[5:7]
    mo = int(mo_str) if mo_str.isdigit() else 0
    q = f"Q{(mo - 1) // 3 + 1}" if mo else None
    if not q:
        keep_indices.append(i)
        continue
    key = (company, yr, q)
    if key in seen:
        dupes_by_source[sources[i]] += 1
        continue
    seen.add(key)
    keep_indices.append(i)

deduped = combined.take(keep_indices)
log.info(f"After dedup: {deduped.num_rows} rows (removed {combined.num_rows - deduped.num_rows} duplicates)")
for src, cnt in dupes_by_source.most_common():
    log.info(f"  Dupes removed from '{src}': {cnt}")

# Write
pq.write_table(deduped, OUTPUT)
log.info(f"Written: {OUTPUT} ({deduped.num_rows} rows, {OUTPUT.stat().st_size/1024/1024:.1f} MB)")

# Source breakdown
source_counts = Counter(deduped.column("source").to_pylist())
log.info("\nFinal source breakdown:")
for src, cnt in source_counts.most_common():
    log.info(f"  {src}: {cnt}")
