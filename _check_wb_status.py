"""Check wayback fetch status."""
import json
from pathlib import Path
from collections import Counter

# Checkpoint
cp = Path("runs/us10002/wayback_checkpoint.json")
if cp.exists():
    data = json.loads(cp.read_bytes())
    fetched = len(data.get("fetched", []))
    failed = len(data.get("failed", []))
    skipped = len(data.get("skipped", []))
    total = fetched + failed + skipped
    print(f"Checkpoint: {fetched} fetched, {failed} failed, {skipped} skipped = {total} total")
else:
    print("No checkpoint found")

# Wayback parquet
try:
    import pyarrow.parquet as pq
    wp = Path("runs/us10002/wayback_transcripts.parquet")
    if wp.exists():
        t = pq.read_table(wp)
        print(f"\nWayback parquet: {t.num_rows} rows, {wp.stat().st_size/1024/1024:.1f} MB")
        companies = set(t.column("company").to_pylist())
        print(f"Unique companies: {len(companies)}")
        dates = t.column("date").to_pylist()
        years = Counter(str(d)[:4] for d in dates if d)
        print("\nYear breakdown:")
        for yr in sorted(years):
            print(f"  {yr}: {years[yr]}")
    else:
        print("No wayback parquet found")
except Exception as e:
    print(f"Parquet error: {e}")

# CDX cache size
cdx = Path("runs/us10002/wayback_cdx_cache.json")
if cdx.exists():
    entries = json.loads(cdx.read_bytes())
    print(f"\nCDX cache: {len(entries)} entries")
