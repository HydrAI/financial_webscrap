# CLI Cookbook

Copy-paste examples using the query files included in this repo. On GitHub, hover over any code block and click the copy icon.

> **Windows note:** If `financial-scraper` gives "Access is denied", replace it with `python -m financial_scraper` in all commands below.

---

## Quick Start

### Smoke test (3 queries, ~2 min)

```bash
financial-scraper \
  --queries-file config/test_commodities.txt \
  --search-type news \
  --max-results 5 \
  --output-dir ./runs
```

### Verify output

```bash
python -c "
import pandas as pd, glob
f = sorted(glob.glob('runs/**/*.parquet', recursive=True))[-1]
df = pd.read_parquet(f)
print(f'{len(df)} docs from {df[\"source\"].nunique()} domains')
print(df[['company','source','title']].head())
"
```

---

## By Query File

### Commodities - starter (10 queries)

```bash
financial-scraper \
  --queries-file config/queries_example.txt \
  --search-type news \
  --max-results 10 \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt \
  --jsonl
```

### Commodities - validated (50 queries)

```bash
financial-scraper \
  --queries-file config/commodities_50.txt \
  --search-type news \
  --max-results 20 \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt \
  --stealth \
  --resume \
  --jsonl
```

### Commodities - comprehensive (300 queries)

```bash
financial-scraper \
  --queries-file config/commodities_300.txt \
  --search-type news \
  --max-results 20 \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt \
  --stealth \
  --use-tor \
  --resume \
  --jsonl
```

### Financial research (35 queries)

```bash
financial-scraper \
  --queries-file docs/examples/queries_financial.txt \
  --search-type news \
  --max-results 15 \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt \
  --resume \
  --jsonl
```

### ESG & sustainability (20 queries)

```bash
financial-scraper \
  --queries-file docs/examples/queries_esg.txt \
  --search-type news \
  --max-results 15 \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt \
  --resume \
  --jsonl
```

### Equities & credit (65 queries)

```bash
financial-scraper \
  --queries-file docs/examples/queries_equities_credit.txt \
  --search-type news \
  --max-results 15 \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt \
  --stealth \
  --resume \
  --jsonl
```

### Fund analysis (75 queries)

```bash
financial-scraper \
  --queries-file docs/examples/queries_fund_analysis.txt \
  --search-type news \
  --max-results 15 \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt \
  --stealth \
  --resume \
  --jsonl
```

### China companies (90 queries)

```bash
financial-scraper \
  --queries-file docs/examples/queries_china_companies.txt \
  --search-type news \
  --max-results 15 \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt \
  --stealth \
  --resume \
  --jsonl
```

---

## By Use Case

### Date-filtered news (e.g. Q1 2026 only)

```bash
financial-scraper \
  --queries-file docs/examples/queries_financial.txt \
  --search-type news \
  --date-from 2026-01-01 \
  --date-to 2026-03-31 \
  --output-dir ./runs
```

### Text search (research papers, SEC filings)

```bash
financial-scraper \
  --queries-file docs/examples/queries_financial.txt \
  --search-type text \
  --max-results 20 \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt
```

### Last 24 hours only

```bash
financial-scraper \
  --queries-file config/queries_example.txt \
  --search-type news \
  --timelimit d \
  --output-dir ./runs
```

### Last week only

```bash
financial-scraper \
  --queries-file config/queries_example.txt \
  --search-type news \
  --timelimit w \
  --output-dir ./runs
```

### Markdown export (human-readable)

```bash
financial-scraper \
  --queries-file config/queries_example.txt \
  --search-type news \
  --output-dir ./runs \
  --markdown \
  --jsonl
```

### English content only

```bash
financial-scraper \
  --queries-file docs/examples/queries_equities_credit.txt \
  --search-type news \
  --target-language en \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt
```

### Low word-count threshold (catch shorter articles)

```bash
financial-scraper \
  --queries-file config/queries_example.txt \
  --search-type news \
  --min-words 50 \
  --output-dir ./runs
```

---

## Tor Examples

### Tor Browser (port 9150, default)

```bash
financial-scraper \
  --queries-file config/commodities_50.txt \
  --search-type news \
  --use-tor \
  --output-dir ./runs \
  --resume
```

### Tor daemon (port 9050)

```bash
financial-scraper \
  --queries-file config/commodities_50.txt \
  --search-type news \
  --use-tor \
  --tor-socks-port 9050 \
  --output-dir ./runs \
  --resume
```

### Tor + stealth + frequent circuit renewal

```bash
financial-scraper \
  --queries-file config/commodities_300.txt \
  --search-type news \
  --use-tor \
  --tor-renew-every 10 \
  --stealth \
  --resume \
  --output-dir ./runs \
  --exclude-file config/exclude_domains.txt \
  --jsonl
```

---

## Resume & Recovery

### Resume an interrupted run

```bash
financial-scraper \
  --queries-file config/commodities_50.txt \
  --search-type news \
  --resume \
  --output-dir ./runs
```

### Resume with a custom checkpoint file

```bash
financial-scraper \
  --queries-file config/commodities_50.txt \
  --search-type news \
  --resume \
  --checkpoint my_checkpoint.json \
  --output-dir ./runs
```

---

## Analyze Output

### Quick stats from Python

```bash
python docs/examples/analyze_output.py runs/latest_folder/scrape_*.parquet
```

### One-liner: count documents per query

```bash
python -c "
import pandas as pd, glob
f = sorted(glob.glob('runs/**/*.parquet', recursive=True))[-1]
df = pd.read_parquet(f)
print(df['company'].value_counts().to_string())
"
```

### One-liner: list top domains

```bash
python -c "
import pandas as pd, glob
f = sorted(glob.glob('runs/**/*.parquet', recursive=True))[-1]
df = pd.read_parquet(f)
print(df['source'].value_counts().head(20).to_string())
"
```

---

## Available Query Files

| File | Queries | Domain |
|------|---------|--------|
| [`config/test_commodities.txt`](../financial_scraper/config/test_commodities.txt) | 3 | Smoke test |
| [`config/queries_example.txt`](../financial_scraper/config/queries_example.txt) | 10 | Commodity basics |
| [`config/commodities_50.txt`](../financial_scraper/config/commodities_50.txt) | 50 | Commodities (validated) |
| [`config/commodities_300.txt`](../financial_scraper/config/commodities_300.txt) | 305 | Commodities (comprehensive) |
| [`docs/examples/queries_financial.txt`](examples/queries_financial.txt) | 35 | Earnings, SEC, macro |
| [`docs/examples/queries_esg.txt`](examples/queries_esg.txt) | 20 | ESG & sustainability |
| [`docs/examples/queries_equities_credit.txt`](examples/queries_equities_credit.txt) | 65 | Equities & credit |
| [`docs/examples/queries_fund_analysis.txt`](examples/queries_fund_analysis.txt) | 75 | Funds, ETFs, PE/VC |
| [`docs/examples/queries_china_companies.txt`](examples/queries_china_companies.txt) | 90 | China top companies |
