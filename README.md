# financial_webscrap

Helps researchers and analysts gather large-scale text datasets from financial news and web sources for NLP, sentiment analysis, and market research projects. Searches DuckDuckGo, fetches pages async with fingerprint rotation, extracts clean text via trafilatura/pdfplumber, and outputs to Parquet. Features per-domain adaptive throttling, Tor proxy support, checkpoint/resume, and content deduplication.

---

## Repository Structure

```
wscrap/
│
├── financial_scraper/              Main package (v0.1.0)
│   ├── pyproject.toml              Package definition & dependencies
│   ├── README.md                   Package-level documentation
│   ├── DOCUMENTATION.md            Full module reference & dev history
│   ├── HOW_TO_BUILD_A_PYTHON_PACKAGE.md
│   ├── config/                     Query files & domain exclusion list
│   ├── src/financial_scraper/      Source code (17 modules)
│   ├── runs/                       Scrape output directory
│   └── tests/                      Unit tests (placeholder)
│
├── 20250930_scrapping_theme_names.py   Standalone Google News scraper for
│                                       company/theme names (567 entities)
├── compliant_scraper_v18.py        Legacy monolithic scraper (reference only)
│
├── SCRAPER_PROJECT_PROMPT_v2.md    Architecture spec & project brief
├── KPI_PROMPT.txt                  LLM prompt for financial KPI extraction
├── RECAP.md                        Full project recap
│
└── _archive/                       Historical files (legacy scripts, debug
                                    artifacts, patent data, PDFs, SEC filings)
```

### What's Active vs Archived

| Component | Status | Description |
|-----------|--------|-------------|
| `financial_scraper/` | **Active** | Modular package — the main codebase |
| `20250930_scrapping_theme_names.py` | **Active** | Standalone scraper for 567 company/theme entities via Google News |
| `compliant_scraper_v18.py` | **Reference** | Monolithic predecessor (2183 lines) — kept for reference |
| `KPI_PROMPT.txt` | **Future** | LLM prompt template for downstream KPI extraction |
| `_archive/` | **Archived** | Legacy scripts, debug outputs, patent/PDF/SEC data |

---

## financial_scraper Package

The core of this repo. A modular Python package that replaces the monolithic v18 scraper.

### Pipeline

```
Query file  -->  DDG Search  -->  Async Fetch  -->  Extract  -->  Dedup  -->  Parquet
                 (text/news)     (aiohttp +        (trafilatura    (URL +     (append mode,
                  + retry +       fingerprints,      + pdfplumber    content    merged_by_year
                  cooldown)       throttle,          + cleanup)      SHA256)    schema)
                                  robots.txt,
                                  optional Tor)
```

### Key Features

- **DuckDuckGo search** — text and news modes, tenacity retry with exponential backoff, adaptive cooldown on rate limits
- **Async HTTP** — aiohttp with 5 rotating browser fingerprint profiles, per-domain adaptive throttling, robots.txt compliance
- **Tor integration** — SOCKS5 proxy, automatic circuit renewal every N queries or on rate limit
- **Content extraction** — trafilatura 2-pass (precision then fallback), pdfplumber for PDFs, boilerplate regex cleanup
- **Deduplication** — URL normalization + content SHA256 hashing
- **Output** — Parquet (snappy compression, append mode) + optional JSONL, schema-compatible with `merged_by_year` pipeline
- **Checkpoint/resume** — atomic JSON saves after each query, crash recovery
- **Stealth mode** — reduced concurrency + longer delays preset

### Quick Start

```bash
cd financial_scraper
pip install -e .

# Run a news search
financial-scraper --queries-file config/commodities_50.txt --search-type news --output-dir runs --jsonl

# Stealth + Tor for large runs
financial-scraper --queries-file config/commodities_300.txt --stealth --use-tor --resume --output-dir runs
```

See [`financial_scraper/README.md`](financial_scraper/README.md) for full CLI reference and usage examples.

---

## Theme Names Scraper

`20250930_scrapping_theme_names.py` is a standalone Google News scraper that:

- Searches for **567 company/entity names** across configurable year/quarter ranges
- Uses Google News RSS with quarter-aware date filtering
- Parallel article fetching with global rate limiting and backoff
- Extracts full text with Google cache fallback
- Outputs to Parquet with the same schema as the main pipeline

This script predates the `financial_scraper` package and operates independently.

---

## KPI Extraction (Future)

`KPI_PROMPT.txt` defines a structured LLM system prompt for extracting financial KPIs from scraped text. The schema covers:

- Numeric metrics (revenue, TAM, cash flow, ratios)
- Ranges and multi-year series
- Metadata: period, scenario (actual/forecast/guidance), subject (company/segment/geo), confidence score

This is intended as a downstream processing step on the Parquet output. Not yet integrated into code.

---

## Constraints

- **Python 3.11+**, Windows (Spyder IDE) primary environment
- **DuckDuckGo only** for the main package (no Google API). Theme names script uses Google News RSS.
- **No Selenium/Playwright** in the main package — lightweight async only
- **No LLM/AI APIs** in the scraping pipeline

---

## Documentation

| File | Content |
|------|---------|
| [`financial_scraper/README.md`](financial_scraper/README.md) | Installation, quick start, CLI reference, output format, tips |
| [`financial_scraper/DOCUMENTATION.md`](financial_scraper/DOCUMENTATION.md) | Full module reference, design decisions, testing results, development history |
| [`financial_scraper/HOW_TO_BUILD_A_PYTHON_PACKAGE.md`](financial_scraper/HOW_TO_BUILD_A_PYTHON_PACKAGE.md) | Tutorial on Python packaging using this project as example |
| [`SCRAPER_PROJECT_PROMPT_v2.md`](SCRAPER_PROJECT_PROMPT_v2.md) | Original architecture spec with module-by-module design and code templates |
| [`RECAP.md`](RECAP.md) | Full project recap — architecture, status, gaps, TODOs |
| [`_archive/ARCHIVE_INDEX.md`](_archive/ARCHIVE_INDEX.md) | Index of all archived materials |

---

## License

For research and educational purposes.
