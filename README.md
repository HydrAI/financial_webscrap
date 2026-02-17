# financial_webscrap

Helps researchers and analysts gather large-scale text datasets from financial news and web sources for NLP, sentiment analysis, and market research projects. Searches DuckDuckGo, fetches pages async with fingerprint rotation, extracts clean text via trafilatura/pdfplumber, and outputs to Parquet. Features per-domain adaptive throttling, Tor proxy support, checkpoint/resume, and content deduplication.

---

## Repository Structure

```
financial_webscrap/
│
├── financial_scraper/              Main package (v0.1.0)
│   ├── pyproject.toml              Package definition & dependencies
│   ├── README.md                   Package-level documentation
│   ├── DOCUMENTATION.md            Full module reference & dev history
│   ├── config/                     Query files & domain exclusion list
│   ├── src/financial_scraper/      Source code (17 modules)
│   └── _run_50.py                  Standalone runner script
│
└── README.md                       This file
```

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

## Constraints

- **Python 3.11+**, Windows (Spyder IDE) primary environment
- **DuckDuckGo only** — no Google API, no API keys
- **No Selenium/Playwright** — lightweight async only
- **No LLM/AI APIs** in the scraping pipeline

---

## Documentation

| File | Content |
|------|---------|
| [`financial_scraper/README.md`](financial_scraper/README.md) | Installation, quick start, CLI reference, output format, tips |
| [`financial_scraper/DOCUMENTATION.md`](financial_scraper/DOCUMENTATION.md) | Full module reference, design decisions, testing results, development history |

---

## License

For research and educational purposes.
