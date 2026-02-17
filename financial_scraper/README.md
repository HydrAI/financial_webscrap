# financial-scraper

Helps researchers and analysts gather large-scale text datasets from financial news and web sources for NLP, sentiment analysis, and market research projects. Searches DuckDuckGo, fetches pages async with fingerprint rotation, extracts clean text via trafilatura/pdfplumber, and outputs to Parquet. Features per-domain adaptive throttling, Tor proxy support, checkpoint/resume, and content deduplication.

---

## How It Works

```
queries.txt
    |
    v
 DDG Search         Search DuckDuckGo (text or news mode) with retry + adaptive cooldown
    |
    v
 Filter             Exclude blocked domains, skip already-seen URLs
    |
    v
 Async Fetch        aiohttp with browser fingerprints, per-domain throttling, robots.txt
    |                Optional Tor proxy for IP rotation
    v
 Extract            trafilatura for HTML (2-pass: precision then fallback)
                    pdfplumber for PDFs
                    Boilerplate regex cleanup
    |
    v
 Dedup + Store      URL + content SHA256 dedup
                    Parquet (append mode) + optional JSONL
                    Atomic checkpoint after each query
```

---

## Installation

Requires **Python 3.11+** and runs on **Windows**, macOS, and Linux.

```bash
cd financial_scraper
pip install -e .
```

This installs all dependencies and registers the `financial-scraper` CLI command.

### Dependencies

| Package | Purpose |
|---------|---------|
| duckduckgo-search | DuckDuckGo search API (text + news) |
| aiohttp | Async HTTP client |
| aiohttp-socks | SOCKS5 proxy support (Tor) |
| aiolimiter | Per-domain leaky bucket rate limiter |
| trafilatura | HTML content extraction + metadata |
| beautifulsoup4 + lxml | HTML parsing |
| pdfplumber | PDF text extraction |
| pyarrow + pandas | Parquet I/O and DataFrame ops |
| chardet | Encoding detection |
| tenacity | Retry with exponential backoff |
| stem | Tor circuit renewal |

---

## Quick Start

### 1. Create a query file

One query per line, `#` for comments:

```text
# Energy
crude oil futures market outlook
natural gas futures winter forecast

# Metals
gold futures price prediction
copper futures demand supply
```

### 2. Run a search

```bash
# News search (recommended for financial content)
financial-scraper --queries-file queries.txt --search-type news

# With explicit output path
financial-scraper --queries-file queries.txt --output results.parquet

# With timestamped output folder + JSONL
financial-scraper --queries-file queries.txt --output-dir ./runs --search-type news --jsonl
```

### 3. Run from Python

```python
import asyncio
from pathlib import Path
from financial_scraper.config import ScraperConfig
from financial_scraper.pipeline import ScraperPipeline

config = ScraperConfig(
    queries_file=Path("queries.txt"),
    search_type="news",
    max_results_per_query=10,
)

pipeline = ScraperPipeline(config)
asyncio.run(pipeline.run())
```

---

## Usage

### Basic

```bash
# News search with domain exclusions
financial-scraper \
  --queries-file config/commodities_50.txt \
  --search-type news \
  --exclude-file config/exclude_domains.txt \
  --output-dir runs \
  --jsonl
```

### Stealth Mode

Reduces concurrency, increases delays to avoid rate limits:

```bash
financial-scraper \
  --queries-file queries.txt \
  --search-type news \
  --stealth \
  --resume
```

Stealth overrides: concurrency 10 -> 4, per-domain 3 -> 2, delays 3-6s -> 5-8s.

### Large-Scale Runs (300+ queries)

For large runs, combine stealth + Tor + resume:

```bash
financial-scraper \
  --queries-file config/commodities_300.txt \
  --search-type text \
  --stealth \
  --use-tor \
  --resume \
  --max-results 20 \
  --output-dir runs \
  --exclude-file config/exclude_domains.txt \
  --jsonl
```

Tor rotates your IP every 20 queries and auto-renews on rate limits. `--resume` picks up where it left off if interrupted.

---

## CLI Reference

```
REQUIRED
  --queries-file FILE          Text file with one query per line

OUTPUT
  --output FILE.parquet        Explicit parquet path
  --output-dir DIR             Base dir for timestamped folders (default: cwd)
  --jsonl                      Also write JSONL alongside parquet

SEARCH
  --search-type {text,news}    DDG search mode (default: text)
  --max-results N              Results per query (default: 20)
  --timelimit {d,w,m,y}        DDG time filter
  --region CODE                DDG region (default: wt-wt = worldwide)
  --backend {auto,api,html,lite}  DDG backend (default: auto)
  --proxy URL                  HTTP/SOCKS5 proxy for searches

TOR
  --use-tor                    Route through Tor (requires Tor Browser or daemon)
  --tor-socks-port PORT        SOCKS5 port (default: 9150)
  --tor-control-port PORT      Control port (default: 9051)
  --tor-password PASS          Control port password
  --tor-renew-every N          Renew circuit every N queries (default: 20)

FETCH
  --concurrent N               Max parallel fetches (default: 10)
  --per-domain N               Max concurrent per domain (default: 3)
  --timeout SECS               Fetch timeout (default: 20)
  --stealth                    Low-profile mode (lower concurrency, higher delays)
  --no-robots                  Skip robots.txt checking

EXTRACT
  --min-words N                Minimum word count to keep (default: 100)
  --target-language LANG       ISO language filter (e.g. en)
  --no-favor-precision         Disable trafilatura precision mode
  --date-from YYYY-MM-DD       Keep only pages after this date
  --date-to YYYY-MM-DD         Keep only pages before this date

RESUME
  --resume                     Resume from last checkpoint
  --checkpoint FILE            Checkpoint path (default: .scraper_checkpoint.json)
  --exclude-file FILE          Domain exclusion list
```

---

## Output Format

### Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| `company` | string | Search query (maps to entity/topic) |
| `title` | string | Page title |
| `link` | string | Source URL |
| `snippet` | string | First 300 characters of content |
| `date` | timestamp[ns] | Publication date |
| `source` | string | Domain name |
| `full_text` | string | Full extracted text |
| `source_file` | string | Provenance tag |

### Output Structure

```
# Default (timestamped folder)
./20260215_143000/
  scrape_20260215_143000.parquet
  scrape_20260215_143000.jsonl

# Explicit path
./results.parquet
```

---

## Project Structure

```
financial_scraper/
├── pyproject.toml
├── config/
│   ├── exclude_domains.txt        # 60 blocked domains
│   ├── queries_example.txt        # Example queries
│   ├── commodities_50.txt         # 50 commodity queries
│   └── commodities_300.txt        # 305 commodity queries
├── src/financial_scraper/
│   ├── __init__.py
│   ├── __main__.py                # python -m financial_scraper
│   ├── main.py                    # CLI entry point (argparse)
│   ├── config.py                  # ScraperConfig frozen dataclass
│   ├── pipeline.py                # Orchestrator
│   ├── checkpoint.py              # Crash-resume with atomic JSON writes
│   ├── search/
│   │   └── duckduckgo.py          # DDG search + retry + Tor integration
│   ├── fetch/
│   │   ├── client.py              # Async HTTP client
│   │   ├── fingerprints.py        # 5 browser fingerprint profiles
│   │   ├── throttle.py            # Per-domain adaptive rate limiter
│   │   ├── robots.py              # robots.txt compliance
│   │   └── tor.py                 # Tor SOCKS5 proxy + circuit renewal
│   ├── extract/
│   │   ├── html.py                # trafilatura 2-pass extraction
│   │   ├── pdf.py                 # pdfplumber extraction
│   │   ├── clean.py               # Boilerplate removal (10 regex patterns)
│   │   └── date_filter.py         # Post-extraction date range filter
│   └── store/
│       ├── dedup.py               # URL + content SHA256 dedup
│       └── output.py              # Parquet/JSONL writers (append mode)
└── tests/
```

---

## Key Design Decisions

**DuckDuckGo only** — No API keys, no billing, works with Tor. News mode gives strong results for financial content.

**trafilatura over BeautifulSoup** — Purpose-built for content extraction. The 2-pass approach (precision then fallback) maximizes extraction quality.

**Adaptive per-domain throttling** — Success halves delay, 429 doubles it, converging to the optimal rate per site without slowing everything for one blocked domain.

**Frozen dataclass config** — Immutable during async execution. Stealth mode creates a new instance with overrides.

**Checkpoint per query** — Queries are the natural unit of work (5-20 pages each). Atomic JSON writes prevent corruption on crash.

**Parquet output** — Schema matches the existing `merged_by_year` pipeline for downstream compatibility. Snappy compression, enforced column types.

---

## Tips

- **Use `--search-type news`** for financial content. It's less rate-limited and returns more relevant results than text search.
- **Expect 30-50% fetch failures.** Many financial sites block automated access (Cloudflare, bot detection). This is normal; the pipeline logs and continues.
- **Niche queries returning 0 results?** Try simpler phrasing or broader terms (`grain market` instead of `oats commodity analysis`).
- **Windows users:** The asyncio event loop policy is set automatically. No manual configuration needed.
- **Tor users:** Start Tor Browser (port 9150) or the Tor daemon (port 9050) before running with `--use-tor`.

---

## License

For research and educational purposes.
