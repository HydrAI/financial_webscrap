# Architecture

## Pipeline Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Query File │────>│  DDG Search │────>│ Async Fetch │────>│   Extract   │────>│ Dedup+Store │
│             │     │             │     │             │     │             │     │             │
│ One query   │     │ text/news   │     │ aiohttp +   │     │ trafilatura │     │ URL + SHA256│
│ per line    │     │ retry +     │     │ fingerprints│     │ pdfplumber  │     │ Parquet     │
│             │     │ cooldown    │     │ throttle    │     │ cleanup     │     │ JSONL       │
│             │     │             │     │ robots.txt  │     │ date filter │     │ checkpoint  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

## Module Map

```
src/financial_scraper/
├── __init__.py          Package exports (ScraperConfig, ScraperPipeline)
├── __main__.py          python -m financial_scraper entry point
├── main.py              CLI argument parsing (argparse)
├── config.py            ScraperConfig frozen dataclass
├── pipeline.py          Orchestrator — wires all stages together
├── checkpoint.py        Atomic JSON checkpoint for crash recovery
├── search/
│   └── duckduckgo.py    DDG text+news search, tenacity retry, Tor circuit renewal
├── fetch/
│   ├── client.py        Async HTTP client with session management
│   ├── fingerprints.py  5 browser fingerprint profiles (UA, headers, TLS)
│   ├── throttle.py      Per-domain adaptive rate limiter
│   ├── robots.py        robots.txt fetching and compliance checking
│   └── tor.py           Tor SOCKS5 proxy and circuit renewal via stem
├── extract/
│   ├── html.py          trafilatura 2-pass extraction (precision then fallback)
│   ├── pdf.py           pdfplumber text extraction
│   ├── clean.py         10 regex patterns for boilerplate removal
│   └── date_filter.py   Post-extraction date range filtering
└── store/
    ├── dedup.py          URL normalization + content SHA256 deduplication
    └── output.py         Parquet (snappy, append) and JSONL writers
```

## Data Flow

Each query passes through four typed result stages:

1. **Search** — query string → `list[SearchResult]` (title, URL, snippet)
2. **Fetch** — `SearchResult` → `FetchResult` (raw HTML/PDF bytes, status code)
3. **Extract** — `FetchResult` → `ExtractionResult` (clean text, metadata, date)
4. **Store** — `ExtractionResult` → Parquet row (deduplicated, schema-enforced)

## Design Rationale

**DuckDuckGo only** — No API keys, no billing, works through Tor. News mode produces strong results for financial content without rate-limit pressure.

**trafilatura over BeautifulSoup** — Purpose-built for main content extraction with metadata (title, date, author). The 2-pass strategy (precision mode first, then relaxed fallback) maximizes recall without sacrificing quality.

**Adaptive per-domain throttling** — Each domain gets its own delay that adjusts dynamically: successful fetches halve the delay (floor 0.5s), 429/503 responses double it (ceiling 60s). This converges to the optimal rate per site without a single slow domain blocking the whole pipeline.

**Frozen dataclass config** — `ScraperConfig` is immutable after creation. Stealth mode creates a new instance with overrides rather than mutating state during async execution.

**Checkpoint per query** — Queries are the natural unit of work (5-20 pages each). Atomic JSON writes (write to temp file, then rename) prevent corruption on crash.

**Parquet output** — Columnar format with snappy compression. Schema matches the downstream `merged_by_year` pipeline for compatibility. Append mode allows incremental writes.
