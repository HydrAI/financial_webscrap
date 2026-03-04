"""Comprehensive coverage analysis on merged transcript parquet."""
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

TICKERS_PATH = Path("financial_scraper/config/us10002_active_tickers.txt")
PARQUET = Path("runs/us10002/transcripts.parquet")

target_tickers = set(TICKERS_PATH.read_text().strip().splitlines())
print(f"Target universe: {len(target_tickers)} tickers")

t = pq.read_table(PARQUET)
print(f"Parquet: {t.num_rows} rows, {PARQUET.stat().st_size/1024/1024:.1f} MB")

companies = t.column("company").to_pylist()
dates = t.column("date").to_pylist()
sources = t.column("source").to_pylist()

# Build combos
combos = {}
for i in range(t.num_rows):
    company = companies[i]
    date = str(dates[i]) if dates[i] else ""
    source = sources[i]
    if not company or len(date) < 7:
        continue
    yr = date[:4]
    mo = int(date[5:7]) if date[5:7].isdigit() else 0
    q = f"Q{(mo - 1) // 3 + 1}" if mo else None
    if not q:
        continue
    combos[(company, yr, q)] = source

print(f"Unique (company, year, quarter) combos: {len(combos)}")

# ── Source breakdown ──
print("\n" + "=" * 60)
print("SOURCE BREAKDOWN")
print("=" * 60)
source_counts = Counter(combos.values())
for src, cnt in source_counts.most_common():
    print(f"  {src}: {cnt}")

# ── Year x Source matrix ──
year_source = defaultdict(Counter)
year_counts = Counter()
for (company, yr, q), src in combos.items():
    year_counts[yr] += 1
    year_source[yr][src] += 1

print("\n" + "=" * 100)
print(f"{'Year':<6} {'Total':>6} {'fool':>8} {'alpha':>8} {'wayback':>8} {'res4':>8}")
print("=" * 100)
for yr in sorted(year_counts.keys()):
    total = year_counts[yr]
    fool = sum(v for k, v in year_source[yr].items() if "fool" in k.lower())
    alpha = sum(v for k, v in year_source[yr].items() if "alphastreet" in k.lower())
    wayback = sum(v for k, v in year_source[yr].items() if "wayback" in k.lower())
    res4 = sum(v for k, v in year_source[yr].items() if "research4" in k.lower())
    print(f"{yr:<6} {total:>6} {fool:>8} {alpha:>8} {wayback:>8} {res4:>8}")

total_all = len(combos)
fool_t = sum(v for k, v in source_counts.items() if "fool" in k.lower())
alpha_t = sum(v for k, v in source_counts.items() if "alphastreet" in k.lower())
wayback_t = sum(v for k, v in source_counts.items() if "wayback" in k.lower())
res4_t = sum(v for k, v in source_counts.items() if "research4" in k.lower())
print("-" * 100)
print(f"{'TOTAL':<6} {total_all:>6} {fool_t:>8} {alpha_t:>8} {wayback_t:>8} {res4_t:>8}")

# ── Company coverage by year ──
print(f"\n{'='*100}")
print("COMPANY COVERAGE BY YEAR")
print(f"{'='*100}")
year_tickers = defaultdict(set)
for (company, yr, q) in combos:
    if company in target_tickers:
        year_tickers[yr].add(company)

print(f"{'Year':<6} {'Tickers':>8} {'/ Univ':>8} {'Pct':>7}  Bar")
print("-" * 70)
for yr in sorted(year_tickers.keys()):
    n = len(year_tickers[yr])
    pct = n / len(target_tickers) * 100
    bar = "#" * int(pct / 2)
    print(f"{yr:<6} {n:>8} /{len(target_tickers):<6} {pct:>6.1f}%  {bar}")

# ── Overall company stats ──
all_covered = set()
for tickers in year_tickers.values():
    all_covered.update(tickers)
missing = sorted(target_tickers - all_covered)
print(f"\nOverall: {len(all_covered)}/{len(target_tickers)} tickers covered ({len(all_covered)/len(target_tickers)*100:.1f}%)")
print(f"Missing: {len(missing)} tickers")
if missing:
    # Print in rows of 15
    for i in range(0, len(missing), 15):
        print(f"  {', '.join(missing[i:i+15])}")

# ── Quarterly detail ──
print(f"\n{'='*100}")
print("QUARTERLY BREAKDOWN")
print(f"{'='*100}")
print(f"{'Year':<6} {'Q1':>6} {'Q2':>6} {'Q3':>6} {'Q4':>6} {'Total':>7} {'Avg':>6}")
print("-" * 50)
for yr in sorted(year_tickers.keys()):
    yr_str = yr
    q_counts = []
    for q in ["Q1", "Q2", "Q3", "Q4"]:
        n = sum(1 for (c, y, qq) in combos if y == yr_str and qq == q)
        q_counts.append(n)
    total = sum(q_counts)
    nonzero = sum(1 for c in q_counts if c > 0)
    avg = total / nonzero if nonzero else 0
    print(f"{yr:<6} {q_counts[0]:>6} {q_counts[1]:>6} {q_counts[2]:>6} {q_counts[3]:>6} {total:>7} {avg:>6.0f}")

# ── Per-company depth ──
print(f"\n{'='*100}")
print("TRANSCRIPT DEPTH PER COMPANY")
print(f"{'='*100}")
company_counts = Counter()
for (company, yr, q) in combos:
    if company in target_tickers:
        company_counts[company] += 1

depths = list(company_counts.values())
if depths:
    print(f"Companies with transcripts: {len(depths)}")
    print(f"  Min: {min(depths)}, Max: {max(depths)}, Mean: {sum(depths)/len(depths):.1f}, Median: {sorted(depths)[len(depths)//2]}")

    brackets = [(1, 1), (2, 4), (5, 10), (11, 20), (21, 40), (41, 60), (61, 100)]
    print(f"\n  {'Range':<12} {'Count':>6} {'Pct':>7}")
    print(f"  {'-'*28}")
    for lo, hi in brackets:
        n = sum(1 for d in depths if lo <= d <= hi)
        pct = n / len(depths) * 100
        print(f"  {lo:>3}-{hi:<5} {n:>6} {pct:>6.1f}%")

# ── Top 20 most covered companies ──
print(f"\n  Top 20 most covered:")
for ticker, cnt in company_counts.most_common(20):
    print(f"    {ticker:<6} {cnt} transcripts")

# ── Least covered (>0) ──
print(f"\n  Bottom 20 least covered:")
for ticker, cnt in company_counts.most_common()[:-21:-1]:
    print(f"    {ticker:<6} {cnt} transcripts")
