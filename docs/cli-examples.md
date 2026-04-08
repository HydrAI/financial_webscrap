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

## URL Deep-Crawl (`crawl` subcommand)

The `crawl` subcommand skips search entirely. Provide a file of seed URLs and crawl4ai's headless browser discovers and extracts content from each site. Requires `pip install -e ".[crawl]"`.

### Seed URL file format

One URL per line, `#` comments:

```text
# Financial news sites
https://reuters.com/business
https://bloomberg.com/markets

# Company investor pages
https://investor.apple.com
```

### Quick crawl test (depth 1)

```bash
financial-scraper crawl --urls-file config/seed_urls.txt --max-depth 1 --max-pages 10 --output-dir ./runs
```

### Production crawl (depth 2, with exclusions)

```bash
financial-scraper crawl --urls-file config/seed_urls.txt --max-depth 2 --max-pages 50 --exclude-file config/exclude_domains.txt --output-dir ./runs --jsonl
```

### Resume an interrupted crawl

```bash
financial-scraper crawl --urls-file config/seed_urls.txt --resume --output-dir ./runs
```

### Crawl with Markdown export

```bash
financial-scraper crawl --urls-file config/seed_urls.txt --max-depth 1 --output-dir ./runs --markdown --jsonl
```

### Stealth crawl (reduced concurrency)

```bash
financial-scraper crawl --urls-file config/seed_urls.txt --max-depth 2 --stealth --output-dir ./runs
```

### Crawl PDF-heavy sites (e.g. SEC filings, annual reports)

Seed URLs can point directly to PDFs. The pipeline detects PDFs by URL extension or content-type header:

```text
# seed_pdfs.txt
https://www.sec.gov/Archives/edgar/data/320193/000032019323000106/aapl-20230930.htm
https://www.sec.gov/Archives/edgar/data/320193/000032019323000106/aapl-20230930-10k.pdf
```

```bash
# Auto backend (Docling if installed, otherwise pdfplumber)
financial-scraper crawl --urls-file seed_pdfs.txt --max-depth 1 --output-dir ./runs

# Docling for layout-aware extraction (tables, structure)
financial-scraper crawl --urls-file seed_pdfs.txt --max-depth 1 --pdf-extractor docling --output-dir ./runs

# pdfplumber for lightweight extraction (no ML dependencies)
financial-scraper crawl --urls-file seed_pdfs.txt --max-depth 1 --pdf-extractor pdfplumber --output-dir ./runs
```

> **Tip:** The `company` field in crawl output is set to the seed URL's domain (e.g. `reuters.com`), and `source_file` tags use the `_crawl_` prefix (e.g. `reuters_com_crawl_2026Q1.parquet`).

---

## Earnings Transcripts (`transcripts` subcommand)

Download structured earnings call transcripts by ticker symbol. The built-in pipeline discovers URLs via Motley Fool sitemaps. Standalone backfill scripts extend coverage to AlphaStreet and Seeking Alpha (via Wayback Machine) for 2007-2026 historical reach.

### Single ticker, all quarters

```bash
financial-scraper transcripts --tickers AAPL --year 2025 --output-dir ./runs
```

### Multiple tickers, specific quarters

```bash
financial-scraper transcripts --tickers AAPL MSFT NVDA --quarters Q1 Q4 --output-dir ./runs --jsonl
```

### From a ticker file

```bash
financial-scraper transcripts --tickers-file config/tickers.txt --year 2025 --output-dir ./runs
```

### Large-scale run (1,500 tickers)

```bash
financial-scraper transcripts --tickers-file config/us10002_active_tickers.txt --year 2025 --resume --output-dir ./runs
```

### Resume an interrupted transcript download

```bash
financial-scraper transcripts --tickers AAPL MSFT GOOG AMZN META --year 2025 --resume --output-dir ./runs
```

> **Tip:** Transcript discovery scans monthly sitemaps (one HTTP request per month), so runs targeting a single year take ~30s for discovery + a few seconds per transcript page fetched.

> **Coverage:** The built-in pipeline covers Motley Fool (2013-2026). For broader historical coverage, standalone scripts can backfill from AlphaStreet (2019-2026) and Seeking Alpha via Wayback Machine (2007-2020). See the [User Guide](user-guide.md#earnings-transcripts-transcripts-subcommand) for details.

---

## Regulatory Filings

Direct downloaders for official annual-report repositories. Each subcommand reads a company-list CSV, queries the authoritative regulator endpoint, downloads filings, and writes extracted text to a single parquet. All support `--limit-companies`, `--skip-companies`, `--max-filings`, `--output-dir`, and `--resume`.

### SEC EDGAR (`sec-filings`) — US 10-K / 20-F

```bash
financial-scraper sec-filings --csv kg_liquid_companies.csv \
  --ticker-column ticker --company-column company_name \
  --max-filings 5 --output-dir sec_filings_us
```

Non-US tickers can be resolved to their US ADR via OpenFIGI using ISIN (authoritative when `--isin-column` is set):

```bash
financial-scraper sec-filings --csv kg_liquid_companies.csv \
  --isin-column isin --company-column company_name \
  --country-column country_code --country-filter GB \
  --max-filings 5 --output-dir sec_filings_uk_adrs
```

### UK Companies House (`uk-filings`) — statutory accounts

```bash
export COMPANIES_HOUSE_API_KEY=...   # or pass --ch-api-key
financial-scraper uk-filings --csv kg_liquid_companies.csv \
  --company-column company_name --company-number-column company_number \
  --country-column country_code --country-filter GB \
  --max-filings 3 --output-dir uk_filings
```

> Scope: CH hosts *statutory* accounts (often scanned PDFs for legacy filings). For text-extractable DTR 6.4 annual reports from UK-listed issuers, prefer `fca-nsm` below.

### FCA National Storage Mechanism (`fca-nsm`) — UK listed-issuer annual reports

```bash
financial-scraper fca-nsm --csv kg_liquid_companies.csv \
  --company-column company_name \
  --country-column country_code --country-filter GB \
  --max-filings 5 --output-dir fca_nsm_uk --resume
```

Handles three payload types automatically:
- **PDF** → `pdfplumber`
- **ESEF iXBRL zip** → unzipped, parsed with `lxml.etree` (recover + huge_tree) — handles 100+ MB single-file reports
- **HTML (RNS)** → `trafilatura`

Optional filters: `--lei-column` (exact LEI match, preferred over name), `--headline "Annual Financial Report"` (default; set empty for all disclosures), `--from-date`/`--to-date` (`DD/MM/YYYY`).

### EDINET (`edinet-filings`) — Japan annual securities reports

```bash
export EDINET_API_KEY=...   # or pass --edinet-api-key
financial-scraper edinet-filings --csv kg_liquid_companies.csv \
  --company-column company_name --ticker-column ticker \
  --scan-days 730 --max-filings 3 --output-dir edinet_jp
```

---

## Patents (`patents` subcommand)

Discover and fetch patent data by assignee, topic keywords, or explicit IDs. Data acquisition only — no downstream signal extraction (that lives in the separate KG project).

### By assignee (discover via Google Patents)

```bash
financial-scraper patents --assignee "Droneshield LLC" \
  --company "DroneShield" --max-discovery 50 --output-dir patent_data
```

### By topic keywords

```bash
financial-scraper patents --search-queries "drone acoustic detection patent" \
  --company "topic-drones" --max-discovery 100 --output-dir patent_data
```

### CPC classification filter

```bash
financial-scraper patents --assignee "Raytheon Technologies" \
  --cpc-filter G01S H04 --max-discovery 200 --output-dir patent_data
```

### Batch mode (multiple companies / themes)

```bash
financial-scraper patents --targets-file config/patent_targets.json --output-dir patent_data
```

### BigQuery source (large-scale)

```bash
financial-scraper patents --source bigquery \
  --bq-csv kg_liquid_companies.csv --bq-company-column company_name \
  --bq-country US --bq-dry-run   # estimate cost first
```

---

## Supply-Chain Queries (`supply-chain` subcommand)

Generates 5 supply-chain search queries per company from a CSV and runs them through the standard search pipeline.

```bash
financial-scraper supply-chain --csv kg_liquid_companies.csv \
  --company-column company_name --ticker-column ticker \
  --limit-companies 100 --output-dir supply_chain_top100 --resume
```

---

## Saving Raw Downloads (`--save-raw`)

Available on `search`, `crawl`, and `supply-chain`. Persists the raw PDFs and HTML alongside the parquet for downstream inspection or re-extraction:

```bash
financial-scraper --queries-file config/commodities_50.txt \
  --save-raw --output-dir ./runs
# → runs/<ts>/pdfs/  and  runs/<ts>/html/
```

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
