# CLI Cookbook

Copy-paste examples using the query files included in this repo. On GitHub, hover over any code block and click the clipboard icon to copy.

> **All commands are single-line** so they copy-paste correctly on Windows (CMD, PowerShell, Git Bash) and Unix alike. Multi-line `\` continuations break on Windows when pasted from a browser.

> **Windows note:** If `financial-scraper` gives "Access is denied", replace it with `python -m financial_scraper` in all commands below.

---

## Quick Start

### Smoke test (3 queries, ~2 min)

```bash
financial-scraper --queries-file config/test_commodities.txt --search-type news --max-results 5 --output-dir ./runs
```

### Verify output

```bash
python docs/examples/analyze_output.py runs/latest_folder/scrape_*.parquet
```

---

## By Query File

### Commodities - starter (10 queries)

```bash
financial-scraper --queries-file config/queries_example.txt --search-type news --max-results 10 --output-dir ./runs --exclude-file config/exclude_domains.txt --jsonl
```

### Commodities - validated (50 queries)

```bash
financial-scraper --queries-file config/commodities_50.txt --search-type news --max-results 20 --output-dir ./runs --exclude-file config/exclude_domains.txt --stealth --resume --jsonl
```

### Commodities - comprehensive (300 queries)

```bash
financial-scraper --queries-file config/commodities_300.txt --search-type news --max-results 20 --output-dir ./runs --exclude-file config/exclude_domains.txt --stealth --use-tor --resume --jsonl
```

### Financial research (35 queries)

```bash
financial-scraper --queries-file docs/examples/queries_financial.txt --search-type news --max-results 15 --output-dir ./runs --exclude-file config/exclude_domains.txt --resume --jsonl
```

### ESG & sustainability (20 queries)

```bash
financial-scraper --queries-file docs/examples/queries_esg.txt --search-type news --max-results 15 --output-dir ./runs --exclude-file config/exclude_domains.txt --resume --jsonl
```

### Equities & credit (65 queries)

```bash
financial-scraper --queries-file docs/examples/queries_equities_credit.txt --search-type news --max-results 15 --output-dir ./runs --exclude-file config/exclude_domains.txt --stealth --resume --jsonl
```

### Fund analysis (75 queries)

```bash
financial-scraper --queries-file docs/examples/queries_fund_analysis.txt --search-type news --max-results 15 --output-dir ./runs --exclude-file config/exclude_domains.txt --stealth --resume --jsonl
```

---

## By Use Case

### Date-filtered news (e.g. Q1 2026 only)

```bash
financial-scraper --queries-file docs/examples/queries_financial.txt --search-type news --date-from 2026-01-01 --date-to 2026-03-31 --output-dir ./runs
```

### Text search (research papers, SEC filings)

```bash
financial-scraper --queries-file docs/examples/queries_financial.txt --search-type text --max-results 20 --output-dir ./runs --exclude-file config/exclude_domains.txt
```

### Last 24 hours only

```bash
financial-scraper --queries-file config/queries_example.txt --search-type news --timelimit d --output-dir ./runs
```

### Last week only

```bash
financial-scraper --queries-file config/queries_example.txt --search-type news --timelimit w --output-dir ./runs
```

### Markdown export (human-readable)

```bash
financial-scraper --queries-file config/queries_example.txt --search-type news --output-dir ./runs --markdown --jsonl
```

### English content only

```bash
financial-scraper --queries-file docs/examples/queries_equities_credit.txt --search-type news --target-language en --output-dir ./runs --exclude-file config/exclude_domains.txt
```

### Low word-count threshold (catch shorter articles)

```bash
financial-scraper --queries-file config/queries_example.txt --search-type news --min-words 50 --output-dir ./runs
```

---

## Tor Examples

### Tor Browser (port 9150, default)

```bash
financial-scraper --queries-file config/commodities_50.txt --search-type news --use-tor --output-dir ./runs --resume
```

### Tor daemon (port 9050)

```bash
financial-scraper --queries-file config/commodities_50.txt --search-type news --use-tor --tor-socks-port 9050 --output-dir ./runs --resume
```

### Tor + stealth + frequent circuit renewal

```bash
financial-scraper --queries-file config/commodities_300.txt --search-type news --use-tor --tor-renew-every 10 --stealth --resume --output-dir ./runs --exclude-file config/exclude_domains.txt --jsonl
```

---

## Deep Crawl

Deep crawl follows same-domain links from fetched pages to discover additional content beyond the initial search results. Useful for finding related articles, "more on this topic" links, and deeper site content.

### Quick crawl test (depth 1, small cap)

```bash
financial-scraper --queries-file config/test_commodities3.txt --search-type news --crawl --crawl-depth 1 --max-results 3 --max-pages-per-domain 3 --output-dir ./runs
```

### News crawl (depth 1, moderate)

```bash
financial-scraper --queries-file config/queries_example.txt --search-type news --crawl --crawl-depth 1 --max-pages-per-domain 5 --output-dir ./runs --exclude-file config/exclude_domains.txt --jsonl
```

### Deep crawl (depth 2, production)

```bash
financial-scraper --queries-file config/commodities_50.txt --search-type news --crawl --crawl-depth 2 --max-pages-per-domain 10 --stealth --resume --output-dir ./runs --exclude-file config/exclude_domains.txt --jsonl
```

### Text search + crawl (research papers, reports)

```bash
financial-scraper --queries-file docs/examples/queries_financial.txt --search-type text --crawl --crawl-depth 1 --max-pages-per-domain 5 --output-dir ./runs --exclude-file config/exclude_domains.txt
```

### Crawl with Markdown export

```bash
financial-scraper --queries-file config/test_commodities3.txt --search-type news --crawl --crawl-depth 1 --max-pages-per-domain 5 --output-dir ./runs --markdown --jsonl
```

> **Tip:** Start with `--crawl-depth 1 --max-pages-per-domain 3` for testing. News sites can have hundreds of links per page, so keeping the cap low prevents long waits. Scale up once you've verified the results.

---

## Resume & Recovery

### Resume an interrupted run

```bash
financial-scraper --queries-file config/commodities_50.txt --search-type news --resume --output-dir ./runs
```

### Resume with a custom checkpoint file

```bash
financial-scraper --queries-file config/commodities_50.txt --search-type news --resume --checkpoint my_checkpoint.json --output-dir ./runs
```

### Re-run all queries (keep URL history)

All queries completed but you want fresh search results? Use `--reset-queries` with `--resume` to re-search while skipping already-fetched URLs:

```bash
financial-scraper --queries-file config/commodities_50.txt --search-type news --resume --reset-queries --output-dir ./runs --exclude-file config/exclude_domains.txt
```

### Re-run with stealth + Tor (keep URL history)

```bash
financial-scraper --queries-file config/commodities_300.txt --search-type news --resume --reset-queries --stealth --use-tor --output-dir ./runs --exclude-file config/exclude_domains.txt --jsonl
```

### Full checkpoint reset (start from scratch)

Delete the checkpoint entirely and run as if it's the first time:

```bash
financial-scraper --queries-file config/commodities_50.txt --search-type news --reset --output-dir ./runs --exclude-file config/exclude_domains.txt --jsonl
```

---

## Analyze Output

### Quick stats

```bash
python docs/examples/analyze_output.py runs/latest_folder/scrape_*.parquet
```

### Count documents per query

```bash
python -c "import pandas as pd, glob; f=sorted(glob.glob('runs/**/*.parquet',recursive=True))[-1]; df=pd.read_parquet(f); print(df['company'].value_counts().to_string())"
```

### List top domains

```bash
python -c "import pandas as pd, glob; f=sorted(glob.glob('runs/**/*.parquet',recursive=True))[-1]; df=pd.read_parquet(f); print(df['source'].value_counts().head(20).to_string())"
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
