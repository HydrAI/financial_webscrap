"""Final wayback fetch status check."""
import json
from pathlib import Path
from collections import Counter

# Checkpoint
cp = Path("runs/us10002/wayback_checkpoint.json")
data = json.loads(cp.read_bytes())
fetched = len(data.get("fetched", []))
empty = len(data.get("empty", []))
failed = len(data.get("failed", []))
skipped_ticker = len(data.get("skipped_ticker", []))
total = fetched + empty + failed + skipped_ticker
print(f"Checkpoint totals:")
print(f"  Fetched (US10002 match): {fetched}")
print(f"  Empty (no content):      {empty}")
print(f"  Failed (HTTP error):     {failed}")
print(f"  Skipped (wrong ticker):  {skipped_ticker}")
print(f"  TOTAL processed:         {total}")
print(f"  CDX cache size:          8712")
print(f"  Remaining unprocessed:   {8712 - total}")

# Wayback parquet
try:
    import pyarrow.parquet as pq
    wp = Path("runs/us10002/wayback_transcripts.parquet")
    if wp.exists():
        t = pq.read_table(wp)
        print(f"\nWayback parquet: {t.num_rows} rows, {wp.stat().st_size/1024/1024:.1f} MB")
        companies = set(t.column("company").to_pylist())
        print(f"Unique companies: {len(companies)}")

        # Year breakdown
        dates = t.column("date").to_pylist()
        years = Counter(str(d)[:4] for d in dates if d)
        print("\nYear breakdown:")
        for yr in sorted(years):
            print(f"  {yr}: {years[yr]}")

        # Quarter breakdown
        print("\nQuarter breakdown (top 20):")
        quarters = Counter()
        for d in dates:
            if d:
                ds = str(d)
                yr = ds[:4]
                mo = int(ds[5:7]) if len(ds) >= 7 else 0
                q = (mo - 1) // 3 + 1 if mo else 0
                if q:
                    quarters[f"{yr}-Q{q}"] += 1
        for qtr, cnt in sorted(quarters.items())[:20]:
            print(f"  {qtr}: {cnt}")
        print(f"  ... ({len(quarters)} quarters total)")

        # Sample companies
        print(f"\nSample companies: {sorted(list(companies))[:30]}")
except Exception as e:
    print(f"Parquet error: {e}")

# Also check main parquet for wayback rows
mp = Path("runs/us10002/transcripts.parquet")
if mp.exists():
    try:
        t2 = pq.read_table(mp)
        sources = t2.column("source").to_pylist()
        wb_main = sum(1 for s in s if "wayback" in str(s).lower() or "seekingalpha" in str(s).lower())
        print(f"\nMain parquet: {t2.num_rows} rows")
        sc = Counter(sources)
        for s, c in sc.most_common():
            print(f"  {s}: {c}")
    except Exception as e:
        print(f"Main parquet error: {e}")
