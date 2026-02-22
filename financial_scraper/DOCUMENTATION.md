# Financial Scraper - Complete Documentation

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Installation](#3-installation)
4. [Usage (CLI)](#4-usage-cli)
5. [Output Format](#5-output-format)
6. [Module Reference](#6-module-reference)
7. [Configuration Reference](#7-configuration-reference)
8. [Design Decisions](#8-design-decisions)
9. [Known Issues & Limitations](#9-known-issues--limitations)
10. [Development History](#10-development-history)

---

## 1. Project Overview

**financial_scraper** is a modular Python package for financial content acquisition. It supports three modes: DuckDuckGo search (text/news), deep URL crawling (crawl4ai), and earnings call transcript downloading (Motley Fool). All modes output to Parquet format compatible with the `merged_by_year` data pipeline.

### What it does

1. Reads a list of search queries from a text file (one per line)
2. Searches DuckDuckGo (text or news mode) for each query
3. Fetches resulting URLs asynchronously with browser fingerprint rotation
4. Extracts clean text using trafilatura (HTML) or pdfplumber (PDF)
5. Deduplicates by URL and content hash
6. Writes results to Parquet with schema matching `C:\_DATA\doc_parquet\merged_by_year\`

### Origin

This package replaces a monolithic 2183-line scraper (`compliant_scraper_v18.py`) with a modular, maintainable architecture. The design follows `SCRAPER_PROJECT_PROMPT_v2.md` (the original project brief) while incorporating battle-tested patterns from v18 and other legacy scripts.

---

## 2. Architecture

```
financial_scraper/
├── pyproject.toml                     # Package definition & dependencies
├── examples/
│   ├── 01_search_text.ipynb           # Jupyter notebook: text search mode
│   ├── 02_search_news.ipynb           # Jupyter notebook: news search mode
│   └── 03_crawl_urls.ipynb            # Jupyter notebook: URL deep-crawl mode
├── config/
│   ├── exclude_domains.txt            # 74 blocked domains (social, video, paywalled, low-quality)
│   ├── queries_example.txt            # Example query file
│   ├── commodities_50.txt             # 50 commodity queries (energy, metals, grains, softs, livestock)
│   ├── commodities_300.txt            # 305 commodity queries (comprehensive coverage)
│   ├── test_commodities.txt           # Test queries (set 1)
│   ├── test_commodities2.txt          # Test queries (set 2)
│   └── test_commodities3.txt          # Test queries (set 3)
├── src/financial_scraper/
│   ├── __init__.py
│   ├── __main__.py                    # Enables `python -m financial_scraper`
│   ├── main.py                        # CLI entry point (argparse)
│   ├── config.py                      # ScraperConfig frozen dataclass
│   ├── pipeline.py                    # Orchestrator: search -> fetch -> extract -> store
│   ├── checkpoint.py                  # Resume capability with atomic JSON writes
│   ├── search/
│   │   ├── __init__.py
│   │   └── duckduckgo.py              # DDG search with retry + rate limit handling
│   ├── fetch/
│   │   ├── __init__.py
│   │   ├── client.py                  # Async HTTP client (aiohttp)
│   │   ├── fingerprints.py            # 5 browser fingerprint profiles
│   │   ├── throttle.py                # Per-domain adaptive rate limiter
│   │   ├── robots.py                  # robots.txt compliance
│   │   └── tor.py                     # Tor SOCKS5 proxy + circuit renewal
│   ├── crawl/
│   │   ├── __init__.py
│   │   ├── config.py                  # CrawlConfig frozen dataclass
│   │   ├── pipeline.py                # CrawlPipeline: BFS crawl orchestrator
│   │   └── strategy.py                # URL scoring (financial keywords, path depth, freshness)
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── html.py                    # trafilatura two-pass extraction
│   │   ├── pdf.py                     # pdfplumber/Docling extraction + get_pdf_extractor()
│   │   ├── clean.py                   # Boilerplate removal + content-type filtering
│   │   ├── date_filter.py             # Post-extraction date range filtering
│   │   └── links.py                   # BFS link extraction + same-domain filtering
│   ├── store/
│   │   ├── __init__.py
│   │   ├── dedup.py                   # URL + content SHA256 + MinHash LSH fuzzy deduplication
│   │   └── output.py                  # Parquet/JSONL writers (merged_by_year schema)
│   └── transcripts/
│       ├── __init__.py                # Lazy import for TranscriptPipeline
│       ├── config.py                  # TranscriptConfig frozen dataclass
│       ├── discovery.py               # Sitemap-based transcript URL discovery
│       ├── extract.py                 # HTML transcript extraction (speakers, sections, metadata)
│       └── pipeline.py                # TranscriptPipeline: discover -> fetch -> extract -> store
└── tests/                             # 350 tests (pytest)
```

### Data Flow

```
queries.txt
    │
    ▼
┌─────────────────┐
│  DDGSearcher     │  DuckDuckGo text/news search with retry
│  (duckduckgo.py) │  Adaptive cooldown on rate limits
└────────┬────────┘
         │ List[SearchResult]
         ▼
┌─────────────────┐
│  Domain Filter   │  Exclude blocked domains + already-seen URLs
│  + Dedup Check   │
└────────┬────────┘
         │ Filtered URLs
         ▼
┌─────────────────┐
│  FetchClient     │  Async HTTP with fingerprints, throttling, robots.txt
│  (client.py)     │  Optional Tor proxy, auto-retry on 403/429
└────────┬────────┘
         │ FetchResult (html/pdf bytes)
         ▼
┌─────────────────┐
│  HTMLExtractor   │  trafilatura bare_extraction (precision + fallback)
│  or PDFExtractor │  pdfplumber for PDF content
│  + TextCleaner   │  Boilerplate regex removal
└────────┬────────┘
         │ ExtractionResult
         ▼
┌─────────────────┐
│  Content Filter  │  Reject ticker pages, Nature Index profiles
│  (TextCleaner)   │  Post-extraction content-type detection
└────────┬────────┘
         │ Filtered results
         ▼
┌─────────────────┐
│  DateFilter      │  Optional date range filtering
│  + Content Dedup │  SHA256 exact + MinHash LSH fuzzy
└────────┬────────┘
         │ Final records
         ▼
┌─────────────────┐
│  ParquetWriter   │  Append-mode, merged_by_year schema
│  + JSONLWriter   │  Optional JSONL alongside
│  + Checkpoint    │  Atomic save after each query
└─────────────────┘
```

### Transcript Data Flow

```
tickers (CLI or file)
    │
    ▼
┌─────────────────┐
│  discover_       │  Scan fool.com/sitemap/YYYY/MM monthly sitemaps
│  transcripts()   │  Filter by ticker, year, quarter via URL slug regex
└────────┬────────┘
         │ List[TranscriptInfo]
         ▼
┌─────────────────┐
│  requests.get()  │  Fetch transcript HTML pages
│  (synchronous)   │  1.5s polite delay between requests
└────────┬────────┘
         │ HTML text
         ▼
┌─────────────────┐
│  extract_        │  Parse JSON-LD metadata (ticker, date, headline)
│  transcript()    │  Extract article-body sections (participants, transcript)
│                  │  Detect speakers, split prepared remarks vs Q&A
└────────┬────────┘
         │ TranscriptResult
         ▼
┌─────────────────┐
│  Deduplicator    │  URL + content SHA256 + MinHash LSH fuzzy
│  + Checkpoint    │  Skip already-fetched URLs on resume
└────────┬────────┘
         │ Final records
         ▼
┌─────────────────┐
│  ParquetWriter   │  merged_by_year schema, source=fool.com
│  + JSONLWriter   │  source_file={TICKER}_transcript_{Q}_{YEAR}
└─────────────────┘
```

---

## 3. Installation

```bash
cd C:/_T/code/wscrap/financial_scraper
pip install -e .
```

This installs the package in editable mode with all dependencies:

| Dependency | Version | Purpose |
|---|---|---|
| duckduckgo-search | >=8.0 | DDG search API |
| aiohttp | >=3.9 | Async HTTP client |
| aiohttp-socks | >=0.8 | SOCKS5 proxy for Tor |
| aiolimiter | >=1.1 | Leaky bucket rate limiting |
| trafilatura | >=2.0 | HTML content extraction |
| beautifulsoup4 | >=4.12 | HTML parsing |
| lxml | >=5.0 | Fast XML/HTML parser |
| pdfplumber | >=0.10 | PDF text extraction |
| pyarrow | >=15.0 | Parquet I/O |
| pandas | >=2.0 | DataFrame operations |
| chardet | >=5.0 | Character encoding detection |
| tenacity | >=8.0 | Retry logic |
| stem | >=1.8 | Tor control protocol |
| datasketch | >=1.6 | MinHash LSH near-duplicate detection |

---

## 4. Usage (CLI)

### Running the scraper

After installing (`pip install -e .`), you can run via either method:

```bash
# Via entry point
financial-scraper --queries-file config/queries_example.txt --search-type news

# Via python -m (no install required if running from project root)
python -m financial_scraper --queries-file config/queries_example.txt --search-type news
```

### Basic usage

```bash
# News search (recommended for financial content)
financial-scraper --queries-file config/queries_example.txt --search-type news

# Text search with explicit output
financial-scraper --queries-file queries.txt --output results.parquet

# Timestamped output folder
financial-scraper --queries-file queries.txt --output-dir ./runs --search-type news --jsonl
# Creates: ./runs/20260215_143000/scrape_20260215_143000.parquet
```

### Large-scale runs (300+ queries)

For 300+ queries, use stealth mode with Tor and resume to handle rate limiting:

```bash
financial-scraper --queries-file config/commodities_300.txt --search-type text --stealth --use-tor --resume --max-results 20 --output-dir runs --exclude-file config/exclude_domains.txt --jsonl
```

**What this enables**:
- `--stealth`: Reduces concurrency to 4, per-domain to 2, delays to 5-8s
- `--use-tor`: Rotates IP every 20 searches, auto-renews on ratelimit
- `--resume`: Picks up where it left off if interrupted (safe to re-run the same command)

**Estimated time for 305 queries**: ~55-90 min (stealth + Tor), depending on ratelimit frequency.

For runs without Tor (100 queries or fewer):

```bash
financial-scraper --queries-file config/commodities_50.txt --search-type text --stealth --resume --max-results 20 --output-dir runs --exclude-file config/exclude_domains.txt --jsonl
```

### All CLI flags

```
REQUIRED:
  --queries-file FILE       Text file with one query per line (# comments OK)

OUTPUT:
  --output FILE.parquet     Explicit parquet path (overrides --output-dir)
  --output-dir DIR          Base dir for timestamped folders (default: cwd)
  --jsonl                   Also write JSONL output alongside parquet

SEARCH:
  --search-type {text,news} DDG search mode (default: text, recommended: news)
  --max-results N           Max results per query (default: 20)
  --timelimit {d,w,m,y}     DDG time filter (day/week/month/year)
  --region REGION           DDG region code (default: wt-wt = worldwide)
  --backend BACKEND         DDG backend: auto, api, html, lite (default: auto)
  --proxy URL               HTTP/SOCKS5 proxy for DDG searches

TOR:
  --use-tor                 Route through Tor (requires Tor Browser/daemon)
  --tor-socks-port PORT     SOCKS5 port (default: 9150 = Tor Browser)
  --tor-control-port PORT   Control port (default: 9051)
  --tor-password PASS       Control port password
  --tor-renew-every N       Renew circuit every N queries (default: 20)

FETCH:
  --concurrent N            Max total concurrent fetches (default: 10)
  --per-domain N            Max concurrent per domain (default: 3)
  --timeout SECS            Fetch timeout seconds (default: 20)
  --stealth                 Stealth mode: lower concurrency, higher delays
  --no-robots               Skip robots.txt checking

EXTRACT:
  --min-words N             Minimum word count to keep page (default: 100)
  --target-language LANG    ISO language code filter (e.g., en)
  --no-favor-precision      Disable trafilatura precision mode
  --date-from YYYY-MM-DD    Keep only pages after this date
  --date-to YYYY-MM-DD      Keep only pages before this date

RESUME:
  --checkpoint FILE         Checkpoint file path (default: .scraper_checkpoint.json)
  --resume                  Resume from last checkpoint
  --reset                   Delete checkpoint before running (fresh start)
  --reset-queries           Clear completed queries but keep URL history
  --exclude-file FILE       Domain exclusion list
```

### Transcript usage

```bash
# Download transcripts for specific tickers
python -m financial_scraper transcripts --tickers AAPL MSFT --year 2025 --output-dir ./runs

# Filter to specific quarters
python -m financial_scraper transcripts --tickers NVDA --quarters Q1 Q4 --output-dir ./runs

# From a file of tickers
python -m financial_scraper transcripts --tickers-file tickers.txt --year 2025 --jsonl --output-dir ./runs
```

### Transcript CLI flags

```
INPUT (at least one required):
  --tickers AAPL MSFT     Ticker symbols
  --tickers-file FILE     File with tickers (one per line, # comments OK)

FILTERS:
  --year YYYY             Fiscal year (default: current year)
  --quarters Q1 Q2 ...    Filter to specific quarters (default: all)

FETCH:
  --concurrent N          Max parallel fetches (default: 5)

OUTPUT:
  --output-dir DIR        Base dir for timestamped output folders
  --jsonl                 Also write JSONL output

RESUME:
  --checkpoint FILE       Checkpoint file (default: .transcript_checkpoint.json)
  --resume                Resume from last checkpoint
  --reset                 Delete checkpoint before running
```

### Query file format

```
# Comments are ignored
wheat futures price
silver commodity forecast 2025
copper demand supply chain
```

---

## 5. Output Format

### Parquet Schema (merged_by_year compatible)

Output is schema-compatible with `C:\_DATA\doc_parquet\merged_by_year\merged_YYYY.parquet`:

| Column | Type | Description |
|---|---|---|
| `company` | string | The search query (maps to entity/topic) |
| `title` | string | Page title extracted by trafilatura |
| `link` | string | Source URL |
| `snippet` | string | First 300 characters of extracted content |
| `date` | timestamp[ns] | Publication date (parsed from extraction) |
| `source` | string | Domain name (e.g., `nasdaq.com`) |
| `full_text` | string | Full extracted text content |
| `source_file` | string | Auto-generated provenance tag |

### source_file naming convention

Mirrors the `themename0001_gnews_2025Q4.parquet` pattern from the existing pipeline:

```
{query_slug}_{search_mode}_{year}Q{quarter}.parquet
```

Examples:
- `wheat_futures_price_ddgnews_2026Q1.parquet`
- `copper_demand_supply_chain_ddgtext_2025Q3.parquet`
- `silver_commodity_forecast_ddgnews_2025Q4.parquet`

### Output directory structure

Without `--output`:
```
./YYYYMMDD_HHMMSS/
├── scrape_YYYYMMDD_HHMMSS.parquet
└── scrape_YYYYMMDD_HHMMSS.jsonl     (if --jsonl)
```

With `--output results.parquet`:
```
./results.parquet  (single file, append mode)
```

---

## 6. Module Reference

### 6.1 `search/duckduckgo.py` - DDGSearcher

**Purpose**: Query DuckDuckGo and return structured search results.

**Key features**:
- Supports both `ddgs.text()` and `ddgs.news()` modes
- 3-attempt retry with exponential backoff (10s, 20s, 40s) on RatelimitException
- Adaptive cooldown: consecutive ratelimit counter * 15s extra delay
- Tor circuit renewal on ratelimit (if Tor enabled)
- Windows asyncio policy fix for curl-cffi compatibility
- Handles both `duckduckgo_search` and `ddgs` package names

**Classes**:
- `SearchResult` (frozen dataclass): `url`, `title`, `snippet`, `search_rank`, `query`
- `DDGSearcher`: Main search class with `search()` and `search_news()` methods

### 6.2 `fetch/client.py` - FetchClient

**Purpose**: Async HTTP client with anti-detection and compliance.

**Key features**:
- aiohttp-based async context manager
- Deterministic browser fingerprint per domain
- Per-domain throttling via DomainThrottler
- robots.txt compliance via RobotChecker
- Global concurrency semaphore
- Auto-retry on 403/429 with different fingerprint profile
- Automatic encoding detection via chardet
- PDF binary content support
- Optional Tor proxy via aiohttp-socks ProxyConnector

**Classes**:
- `FetchResult` (dataclass): `url`, `status`, `html`, `content_type`, `content_bytes`, `error`, `response_headers`
- `FetchClient`: `fetch(url)`, `fetch_batch(urls)`

### 6.3 `fetch/fingerprints.py` - Browser Fingerprints

**Purpose**: Rotate browser identity to avoid detection.

**Profiles** (5 total):
1. Chrome 122 Windows
2. Chrome 122 macOS
3. Firefox 123 Windows
4. Safari 17 macOS
5. Edge 122 Windows

Each profile includes: User-Agent, Accept, Accept-Language, Accept-Encoding, Sec-CH-UA headers, Sec-Fetch-* headers, Upgrade-Insecure-Requests.

**Selection**: `get_fingerprint_for_domain(domain)` uses `hash(domain) % 5` for deterministic, consistent profile per domain.

### 6.4 `fetch/throttle.py` - DomainThrottler

**Purpose**: Prevent rate limiting via per-domain adaptive delays.

**Mechanism**:
- AsyncLimiter (leaky bucket) per domain
- Semaphore for concurrent request limiting per domain
- Adaptive extra delays:
  - Success: halve delay (min 0)
  - 429: double delay (or use Retry-After header)
  - 403: 1.5x delay
  - 5xx: 1.25x delay
- Max delay cap: 30 seconds

### 6.5 `fetch/robots.py` - RobotChecker

**Purpose**: Respect robots.txt directives.

**Features**:
- Async fetch of robots.txt per domain
- In-memory cache (one fetch per domain per session)
- Permissive default: if robots.txt is missing or fails, allow access
- Crawl-delay extraction

### 6.6 `fetch/tor.py` - TorManager

**Purpose**: Route traffic through Tor for IP rotation.

**Features**:
- SOCKS5 proxy on port 9150 (Tor Browser) or 9050 (Tor daemon)
- Circuit renewal via stem NEWNYM signal
- Minimum 15s between renewal signals (Tor requirement)
- Auto-renewal every N queries (configurable)
- Renewal on ratelimit (configurable)
- Availability check via check.torproject.org

### 6.7 `extract/html.py` - HTMLExtractor

**Purpose**: Extract clean text, title, author, date from HTML.

**Strategy** (two-pass):
1. **Primary**: `trafilatura.bare_extraction()` with `favor_precision=True` - extracts main content, ignores boilerplate
2. **Fallback**: If word count < min_word_count, retry with `favor_precision=False` - more permissive extraction

**Features**:
- Metadata extraction: title, author, publication date
- Language detection support
- Periodic cache reset (every 500 pages) to prevent memory bloat
- Handles both dict and object return types from trafilatura

### 6.8 `extract/pdf.py` - PDFExtractor

**Purpose**: Extract text from PDF response bytes.

**Implementation**: Dispatches between two backends via `get_pdf_extractor()`:
- **pdfplumber** (default): Page-by-page text extraction with metadata (title) from PDF properties. Lightweight, always available.
- **Docling** (`--pdf-extractor docling`): Layout-aware extraction with table detection and hierarchical structure. Better for complex PDFs (SEC filings, annual reports). Requires `pip install -e ".[docling]"`.

The `auto` mode (default) uses Docling if installed, falling back to pdfplumber.

### 6.9 `extract/clean.py` - TextCleaner

**Purpose**: Remove boilerplate text that trafilatura missed, and detect non-article page types.

**Boilerplate patterns removed** (24 regex):
- Cookie policy/consent notices
- Newsletter subscription prompts
- Social media share buttons
- Copyright notices
- Follow-us sections
- Terms of service/privacy links
- Bare URLs
- Advertisement/sponsored content markers
- TipRanks promotional blocks ("Claim X% Off", "Meet Your ETF AI Analyst", "Stock Analysis page", "Smart Investor Newsletter", etc.)
- Trending/related/recommended article blocks
- PR wire disclaimer blocks (MENAFN-style "We do not accept any responsibility or liability..." multi-line disclaimers)

**Post-extraction content-type filters**:
- `is_ticker_page(text)`: Detects stock quote/profile pages by matching 3+ of 4 fingerprints (`52 Week`, `EPS (TTM)`, `P/E (TTM)`, `Prev Close`). These pages contain structured market data, not article content.
- `is_nature_index_page(text)`: Detects Nature Index research collaboration profiles by matching `Nature Index` + `Collaboration Score`. These are academic metrics pages, not financial news.

Both filters are called by the pipeline after extraction to reject non-article pages before storage.

**Additional**: Unicode NFKC normalization, whitespace collapse, per-line stripping.

### 6.10 `extract/links.py` - Link Extraction

**Purpose**: Extract and filter links from HTML pages for BFS deep crawling.

**Functions**:
- `extract_links(html, base_url)` - Parse HTML with BeautifulSoup/lxml, extract `<a href>` links, resolve relative URLs, strip fragments, skip `javascript:`/`mailto:`/asset extensions, deduplicate
- `filter_links_same_domain(links, source_domain, exclusions, seen_urls, domain_page_counts, max_pages_per_domain)` - Filter to same base domain, check exclusions, seen URLs, per-domain cap
- `_base_domain(hostname)` - Extract base domain (e.g., `blog.reuters.com` -> `reuters.com`)

**Asset extensions skipped**: `.jpg`, `.jpeg`, `.png`, `.gif`, `.svg`, `.ico`, `.webp`, `.bmp`, `.css`, `.js`, `.woff`, `.woff2`, `.ttf`, `.eot`, `.mp3`, `.mp4`, `.avi`, `.mov`, `.wmv`, `.flv`, `.zip`, `.gz`, `.tar`, `.rar`, `.7z`, `.exe`, `.dmg`, `.msi`

### 6.11 `crawl/config.py` - CrawlConfig

**Purpose**: Configuration for the crawl subcommand as a frozen dataclass.

**Key fields**: `urls_file`, `max_depth`, `max_pages`, `semaphore_count`, `pdf_extractor` (`auto`/`docling`/`pdfplumber`), `min_word_count`, `check_robots_txt`, `stealth`, `resume`, `checkpoint_file`, `output_dir`, `output_path`.

### 6.12 `crawl/pipeline.py` - CrawlPipeline

**Purpose**: BFS crawl orchestrator using crawl4ai's `AsyncWebCrawler` + `BestFirstCrawlingStrategy`.

**Key features**:
- PDF detection by URL extension (`.pdf`) and `Content-Type` header — checked before crawl4ai success status
- Direct PDF download with browser-style headers (User-Agent, Accept, Referer) and `Accept-Encoding: gzip, deflate` to prevent Brotli decoding errors
- Dispatches to Docling or pdfplumber via `get_pdf_extractor()`
- PDF date extraction: `max(content_regex, metadata)` for most accurate release date
- Resume/checkpoint support with atomic JSON saves
- Parquet + JSONL output with merged_by_year schema

### 6.13 `crawl/strategy.py` - URL Scoring

**Purpose**: Financial keyword scoring for BFS URL prioritization.

**Scoring components**:
- Financial keyword relevance (weight 0.8) — URLs containing `earnings`, `quarterly`, `report`, `sec`, `filings`, etc.
- Path depth (weight 0.3) — shorter paths (closer to site root) preferred
- Freshness (weight 0.15) — URLs containing the current year score higher

**Filtering**: Non-content URLs (login, contact, career, legal, privacy, etc.) are filtered before scoring.

### 6.14 `extract/date_filter.py` - DateFilter

**Purpose**: Post-extraction filtering by publication date range.

**Supported date formats**: `YYYY-MM-DDTHH:MM:SS`, `YYYY-MM-DD`, `YYYY-MM`, `YYYY`

**Policy**: Pages without dates are kept (permissive). Only pages with parseable dates outside the range are filtered out.

### 6.15 `store/dedup.py` - Deduplicator

**Purpose**: Prevent duplicate pages by URL and content (exact and near-duplicate).

**Implementation** (two-layer content dedup):
- **Layer 1 — Exact dedup**: SHA256 of first 2000 characters. O(1) lookup, catches identical content instantly.
- **Layer 2 — Fuzzy dedup** (MinHash LSH): Runs only when exact check passes. Detects near-duplicates like syndicated news rewrites (same article republished across sites with minor edits, different headers, added disclaimers).
- URL: normalize (defrag, lowercase, strip trailing slash) then SHA256
- In-memory sets + LSH index with save/load to JSON

**Fuzzy dedup details**:

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `num_perm` | 128 | Standard accuracy/memory tradeoff |
| LSH threshold | 0.85 | Catches syndicated rewrites, avoids false positives on same-topic articles |
| Shingle size | 3 words | Word-level 3-grams capture phrase-level similarity; financial articles share boilerplate but differ in entity names |

**Graceful degradation**: If `datasketch` is not installed, fuzzy dedup is silently disabled — exact dedup still works.

**Persistence format** (inside existing JSON):
```json
{
  "urls": ["sha256..."],
  "content": ["sha256..."],
  "minhash": {"0": "hex_digest...", "1": "hex_digest..."}
}
```
On load, MinHash objects are reconstructed from hex digests and re-inserted into a fresh LSH index.

### 6.16 `store/output.py` - ParquetWriter / JSONLWriter

**Purpose**: Write results in merged_by_year-compatible Parquet.

**ParquetWriter**:
- Append mode: reads existing file, concatenates, rewrites
- Schema enforcement via PyArrow
- Date strings auto-parsed to timestamp[ns] via pandas
- Snappy compression
- `source_file` tag auto-generated with `make_source_file_tag()`

**JSONLWriter**:
- Simple append-mode JSON Lines
- UTF-8, no ASCII escaping

### 6.17 `checkpoint.py` - Checkpoint

**Purpose**: Resume interrupted scrape sessions.

**Tracks**:
- Completed queries (set)
- Fetched URLs (set)
- Failed URLs with retry counts (dict)
- Running statistics (total queries, pages, words, failures)

**Atomic saves**: Write to `.tmp` then `os.replace()` to prevent corruption.

**Reset modes**:
- `--reset`: Deletes the checkpoint file entirely before running (full fresh start)
- `--reset-queries`: Clears completed queries and stats but keeps fetched/failed URL history. Re-runs all queries while avoiding re-fetching the same URLs. Requires `--resume` to load the checkpoint first.

### 6.18 `pipeline.py` - ScraperPipeline

**Purpose**: Main orchestrator wiring all modules together.

**Flow per query**:
1. Load exclusions + checkpoint + dedup state
2. DDG search -> filter exclusions -> filter already-seen URLs
3. BFS crawl loop (depth 0 = search results, depth 1+ = crawled links):
   a. Async fetch batch with throttling + fingerprints + robots
   b. Extract (HTML via trafilatura or PDF via pdfplumber)
   c. Content-type filter: reject ticker/profile pages and Nature Index profiles
   d. If `crawl=True` and `depth < crawl_depth`: extract same-domain links for next depth
   e. Post-clean, date filter, content dedup
4. Write all records to Parquet + JSONL + Markdown
5. Checkpoint save (atomic)
6. Print summary

### 6.19 `config.py` - ScraperConfig

**Purpose**: Centralized configuration as frozen dataclass.

**Stealth mode** (`--stealth`): Automatically overrides:
- `max_concurrent_total`: 10 -> 4
- `max_concurrent_per_domain`: 3 -> 2
- `search_delay_min`: 3.0 -> 5.0
- `search_delay_max`: 6.0 -> 8.0

### 6.20 `__main__.py` - Module Runner

**Purpose**: Enable `python -m financial_scraper` invocation.

Delegates to `main.main()`. Required because the package uses a `src/` layout,without this file, `python -m` fails with "No module named financial_scraper.__main__".

### 6.21 `main.py` - CLI Entry Point

**Purpose**: Parse CLI arguments and launch pipeline.

**Features**:
- Three subcommands: `search`, `crawl`, `transcripts`
- Datetime-stamped output folders (default) or explicit file path
- Windows asyncio policy fix
- Noisy logger suppression (DDG, urllib3, trafilatura)
- Elapsed time reporting

### 6.22 `transcripts/discovery.py` - Sitemap-based Transcript Discovery

**Purpose**: Discover earnings call transcript URLs from Motley Fool sitemaps.

**Implementation**:
- Fetches monthly XML sitemaps at `https://www.fool.com/sitemap/YYYY/MM`
- Filters URLs matching `/earnings/call-transcripts/` containing the target ticker
- Parses URL slug via regex to extract ticker, quarter, fiscal year, publication date
- Scans target year + following year (Q4 transcripts published in Jan/Feb of next year)

**Classes**:
- `TranscriptInfo` (frozen dataclass): `url`, `ticker`, `quarter`, `year`, `pub_date`

**Functions**:
- `discover_transcripts(ticker, year, quarters)` — main discovery entry point
- `_fetch_sitemap_urls(year, month)` — fetch and parse a single monthly sitemap
- `_parse_transcript_url(url)` — extract metadata from URL slug

### 6.23 `transcripts/extract.py` - Transcript HTML Extraction

**Purpose**: Parse Motley Fool transcript HTML into structured content.

**Implementation**:
- Extracts JSON-LD structured data for metadata (headline, ticker, publication date)
- Finds `<div class="article-body">` and extracts sections by H2 headings
- Sections: DATE, CALL PARTICIPANTS, Full Conference Call Transcript
- Speaker detection regex: `^([A-Z][A-Za-z\s.\-']+?)(?:\s*,\s*[A-Za-z\s]+)?:`
- Splits transcript into prepared remarks vs Q&A at operator markers ("question-and-answer session", etc.)
- Fallback: if no transcript section found, grabs all `<p>` text from article body

**Classes**:
- `TranscriptResult` (dataclass): `company`, `ticker`, `quarter`, `year`, `date`, `full_text`, `speakers`, `participants`, `prepared_remarks`, `qa_section`

### 6.24 `transcripts/pipeline.py` - TranscriptPipeline

**Purpose**: Orchestrate transcript discovery, fetching, extraction, and storage.

**Flow**:
1. Load tickers from CLI args or file
2. For each ticker: discover transcript URLs via sitemaps
3. Filter already-fetched URLs (checkpoint + dedup)
4. Fetch each transcript page with `requests` (1.5s polite delay)
5. Extract structured content
6. Content dedup (SHA256 + MinHash LSH)
7. Write to Parquet + optional JSONL
8. Save checkpoint after each ticker

**Reuses**: `Deduplicator`, `ParquetWriter`, `JSONLWriter`, `Checkpoint` from existing infrastructure.

### 6.25 `transcripts/config.py` - TranscriptConfig

**Purpose**: Configuration for the transcripts subcommand as a frozen dataclass.

**Key fields**:

| Field | Type | Default | CLI Flag |
|-------|------|---------|----------|
| `tickers` | `tuple[str, ...]` | `()` | `--tickers` |
| `tickers_file` | `Path\|None` | `None` | `--tickers-file` |
| `year` | `int\|None` | `None` (current year) | `--year` |
| `quarters` | `tuple[str, ...]` | `()` (all) | `--quarters` |
| `concurrent` | `int` | `5` | `--concurrent` |
| `output_dir` | `Path` | `.` | `--output-dir` |
| `output_path` | `Path` | `output.parquet` | (auto-generated) |
| `jsonl_path` | `Path\|None` | `None` | `--jsonl` |
| `checkpoint_file` | `Path` | `.transcript_checkpoint.json` | `--checkpoint` |
| `resume` | `bool` | `False` | `--resume` |

---

## 7. Configuration Reference

### ScraperConfig Fields

| Field | Type | Default | CLI Flag | Description |
|---|---|---|---|---|
| queries_file | Path | queries.txt | --queries-file | Query list file |
| max_results_per_query | int | 20 | --max-results | Results per DDG query |
| search_delay_min | float | 3.0 | (internal) | Min seconds between searches |
| search_delay_max | float | 6.0 | (internal) | Max seconds between searches |
| ddg_region | str | wt-wt | --region | DDG region code |
| ddg_timelimit | str/None | None | --timelimit | d/w/m/y filter |
| ddg_backend | str | auto | --backend | DDG backend |
| search_type | str | text | --search-type | text or news |
| proxy | str/None | None | --proxy | HTTP/SOCKS proxy |
| use_tor | bool | False | --use-tor | Enable Tor routing |
| tor_socks_port | int | 9150 | --tor-socks-port | SOCKS5 port |
| tor_control_port | int | 9051 | --tor-control-port | Control port |
| tor_password | str | "" | --tor-password | Control auth |
| tor_renew_every | int | 20 | --tor-renew-every | Queries per circuit |
| tor_renew_on_ratelimit | bool | True | (internal) | Renew on ratelimit |
| max_concurrent_total | int | 10 | --concurrent | Global concurrency |
| max_concurrent_per_domain | int | 3 | --per-domain | Per-domain concurrency |
| fetch_timeout | int | 20 | --timeout | Fetch timeout (s) |
| stealth | bool | False | --stealth | Stealth overrides |
| respect_robots | bool | True | --no-robots | robots.txt check |
| crawl | bool | False | --crawl | Follow links from fetched pages (BFS) |
| crawl_depth | int | 2 | --crawl-depth | Max BFS link-following depth |
| max_pages_per_domain | int | 50 | --max-pages-per-domain | Cap pages fetched per domain during crawl |
| min_word_count | int | 100 | --min-words | Min words to keep |
| target_language | str/None | None | --target-language | Language filter |
| include_tables | bool | True | (internal) | Extract tables |
| favor_precision | bool | True | --no-favor-precision | trafilatura mode |
| date_from | str/None | None | --date-from | Date range start |
| date_to | str/None | None | --date-to | Date range end |
| output_dir | Path | . | --output-dir | Output base dir |
| output_path | Path | output.parquet | --output | Parquet file path |
| jsonl_path | Path/None | None | --jsonl | JSONL output |
| exclude_file | Path/None | built-in list | --exclude-file | Blocked domains file (74 domains, use --no-exclude to disable) |
| checkpoint_file | Path | .scraper_checkpoint.json | --checkpoint | Checkpoint file |
| resume | bool | False | --resume | Resume from checkpoint |
| reset_queries | bool | False | --reset-queries | Clear completed queries, keep URL history |

---

## 8. Design Decisions

### Why DuckDuckGo only (no Google)?
- No API key required, no billing
- The `duckduckgo-search` library provides reliable access
- News mode (`ddgs.news()`) gives good results for financial content
- Tor integration works seamlessly with DDG

### Why trafilatura over BeautifulSoup?
- trafilatura is purpose-built for main content extraction
- Handles boilerplate removal, metadata extraction, date parsing
- `bare_extraction()` returns structured data (title, author, date, text)
- The two-pass approach (precision then fallback) maximizes extraction rate

### Why Parquet over CSV?
- Column types enforced (timestamp[ns] for dates)
- Compression (snappy) reduces file size ~3-5x
- Schema compatibility with existing `merged_by_year` pipeline
- Fast columnar reads with PyArrow

### Why frozen dataclass for config?
- Immutability prevents accidental mutation during pipeline execution
- `apply_stealth()` creates a new instance with overrides (functional style)
- All fields have defaults, making it easy to construct programmatically

### Why adaptive throttling?
- Static delays waste time on permissive domains
- Adaptive delays (halve on success, double on 429) converge to optimal rate
- Per-domain independence prevents one blocked domain from slowing everything

### Why checkpoint after each query (not each page)?
- Queries are the natural unit of work (5-20 pages each)
- Atomic JSON write prevents corruption on crash
- Resume skips completed queries, re-fetches incomplete ones

---

## 9. Known Issues & Limitations

### DDG Rate Limiting
DDG aggressively rate-limits automated searches. Mitigations:
- Randomized delays (3-6s baseline, up to 15s * consecutive_ratelimits)
- Tor circuit renewal on ratelimit
- Stealth mode (lower concurrency, higher delays)
- **Best practice**: Use `--search-type news` which is less rate-limited

### Fetch Failures (~30-50% of URLs)
Many financial sites block automated access via:
- Cloudflare challenges (no JavaScript execution in aiohttp)
- SSL certificate issues
- Aggressive bot detection

This is expected behavior. The pipeline logs failures and moves on.

### Windows-Specific
- Requires `asyncio.WindowsSelectorEventLoopPolicy()` for curl-cffi (DDG library)
- Forward slashes in paths work fine on Windows Python
- Console encoding issues with non-ASCII content (resolved via utf-8 stdout wrapper)

### Package Rename
`duckduckgo_search` has been renamed to `ddgs`. The code handles both import paths.

---

## 10. Development History

### Phase 1: Analysis (from existing codebase)

**Source files analyzed**:
- `SCRAPER_PROJECT_PROMPT_v2.md` (1401 lines) - Master architecture document
- `compliant_scraper_v18.py` (2183 lines) - Monolithic scraper being replaced
- `20250811_scrapping_news.py` - Google News scraper with paywall handling
- `20250811_scraping_3pass.py` - 3-pass architecture (fast/Playwright/PDF)
- `20250813_headless_playwright.py` - Playwright-based Google search
- `webscrapp_news_code/pdf_parsing02.py` - PDF parser with OCR fallback
- `KPI_PROMPT.txt` - KPI extraction prompt for future NLP step

**Key patterns salvaged from v18**:
- BrowserFingerprint class with 5 profiles -> `fetch/fingerprints.py`
- Adaptive delay system -> `fetch/throttle.py`
- Parquet append writer -> `store/output.py`
- DDG search with retry -> `search/duckduckgo.py`
- Content cleaning regexes -> `extract/clean.py`
- Checkpoint/resume system -> `checkpoint.py`

### Phase 2: Implementation

Built all modules following the project brief. Key adaptations:
- Used `trafilatura.bare_extraction()` instead of v18's custom BeautifulSoup logic
- Used `aiolimiter.AsyncLimiter` instead of custom token bucket
- Used frozen dataclass instead of mutable config dict
- Added `make_source_file_tag()` for merged_by_year compatibility

### Phase 3: Testing (4 rounds)

| Test | Queries | Pages | Mode | Time | Result |
|---|---|---|---|---|---|
| 1 | 3 | 5 | text | ~1m | Some DDG queries returned 0 results |
| 2 | 5 | 6 | text | ~1m | Rate limiting caused empty results |
| 3 | 4 | 13 | news | ~1m | Good results, real commodity content |
| **4** | **50** | **120** | **news** | **8m 55s** | **Production-scale validation** |

**Test 3** (news mode) produced 13 rows with 14,760 total words across 11 unique domains including nasdaq.com, morningstar.com, marketwatch.com, fxempire.com.

**Test 4** (50-query large-scale run on 2026-02-15),full commodity coverage:
- **50 queries** across energy, precious metals, base metals, grains, softs, livestock, exotic/niche, agricultural specialty
- **120 pages extracted**, 83,895 total words, 699 avg words/page
- **41 unique domains**,top sources: nasdaq.com (27), theglobeandmail.com (11), marketwatch.com (8), news.metal.com (8), oilprice.com (7), reuters.com (7), fxempire.com (5), mining.com (4), cnbc.com (3)
- **38/50 queries** produced results (76%). The 12 that returned 0 were niche commodities with sparse DDG news coverage (feeder cattle, lead, lumber, manganese, oats, palladium, platinum, rice, rubber, soybean oil, wheat specific phrasing, wool)
- **Rate limiting**: Adaptive cooldown triggered a few times but recovered,pipeline completed all 50 queries without manual intervention
- **Config used**: `max_results=10`, `search_delay=4-7s`, `concurrent=8`, `per_domain=2`, `min_words=80`
- **Output**: `runs/20260215_235519/commodities_50_20260215_235519.parquet` + `.jsonl`

### Phase 4: Schema Alignment

Adapted output to match `C:\_DATA\doc_parquet\merged_by_year\merged_YYYY.parquet`:
- Renamed columns: query->company, url->link, domain->source, content->full_text
- Added snippet (first 300 chars) and source_file (provenance tag)
- Changed date from string to timestamp[ns]
- Added datetime-stamped output folders

### Phase 5: Folder Cleanup

Organized the root `wscrap/` directory:
- Created `_archive/` with categorized subfolders (legacy_scripts, debug_artifacts, patent_data, pdf_data, sample_docs, sec_filings, SAMPLE_scrapping, STEP_2_NLP_investor_report)
- Moved 6 old scraping scripts (Aug 2025), `webscrapp_news_code/` folder, debug screenshots/logs/CSVs, patent JSON/text files, PDF extraction outputs (~200MB), sample financial documents, SEC filings
- Wrote `_archive/ARCHIVE_INDEX.md` describing each subfolder
- Cleaned test parquet outputs and `__pycache__` from the package

**Repository structure** (committed files):
```
financial_webscrap/
├── README.md                    # Repo-level documentation
├── financial_scraper/           # Active package
│   ├── DOCUMENTATION.md         # This file
│   ├── pyproject.toml
│   ├── examples/                # Jupyter notebooks (search, news, crawl)
│   ├── config/                  # Query files + exclusion list
│   └── src/financial_scraper/   # Source code (22 modules)
```

### Phase 6: `__main__.py` + 300-Query File

- Added `src/financial_scraper/__main__.py` to enable `python -m financial_scraper` invocation (previously failed with "No module named financial_scraper.__main__")
- Created `config/commodities_300.txt` with 305 queries across 11 categories: energy (crude, gas, refined, coal, nuclear, carbon), precious metals, base/industrial metals, battery/EV metals, fertilizers/chemicals, grains/oilseeds, softs/tropical, livestock/dairy, specialty agriculture, petrochemicals, and environmental/misc commodities

### Phase 7: Content Quality Filtering (2026-02-19)

Analysis of 1,923 merged articles from two scrape runs (Feb 18-19) revealed that ~24% of content was low-value: stock ticker pages, TipRanks promotional blurbs, paywall-truncated articles, and Nature Index profiles. Four improvements were implemented:

1. **TipRanks promo text removal** (`extract/clean.py`): Added 10 regex patterns to strip TipRanks promotional blocks ("Claim X% Off", "Meet Your ETF AI Analyst", "Stock Analysis page", etc.) that were embedded in nasdaq.com, theglobeandmail.com, and businessinsider.com articles. Cleans 109+ articles per run.

2. **Post-extraction content-type filter** (`extract/clean.py` + `pipeline.py`): New `is_ticker_page()` method detects stock quote/profile pages by matching 3+ of 4 fingerprints (`52 Week`, `EPS (TTM)`, `P/E (TTM)`, `Prev Close`). New `is_nature_index_page()` detects Nature Index research profiles. Pipeline rejects these after extraction instead of storing them.

3. **Trending/Related articles removal** (`extract/clean.py`): Added patterns for "Trending Articles", "Related Stories", "Recommended Stories" blocks that appeared as trailing content in 72+ articles.

4. **PR wire disclaimer removal** (`extract/clean.py`): Two regex patterns strip MENAFN-style multi-line disclaimer blocks ("We do not accept any responsibility or liability...kindly contact the provider above").

5. **Domain exclusions** (`config/exclude_domains.txt`): Added 8 domains with >50% bad content rate: `caixinglobal.com` (100% paywall), `cnbc.com` (89% ticker pages), `theglobeandmail.com` (88% TipRanks blurbs), `nature.com` (84% non-financial), `bloomberg.com` (82% paywall), `zawya.com` (53% truncated), `scmp.com`, `law.com`. Total excluded domains: 74.

**Impact**: In future scrapes, ~466 low-value articles per run will be blocked at domain level, ~32 ticker pages and ~32 Nature Index pages caught by post-extraction filter, and remaining articles have cleaner text.

### Phase 8: Crawl Subcommand (2026-02-20/21)

Added a standalone `crawl` subcommand that deep-crawls seed URLs using crawl4ai's headless browser with BFS financial-keyword scoring. Key features:

1. **crawl4ai integration** (`crawl/pipeline.py`, `crawl/strategy.py`): `AsyncWebCrawler` + `BestFirstCrawlingStrategy` with financial keyword scoring (weight 0.8), path depth (0.3), and freshness (0.15). Non-content URL filtering.

2. **Docling PDF extraction** (`extract/pdf.py`): Added `get_pdf_extractor()` dispatch between pdfplumber and Docling. Docling provides layout-aware table detection for complex PDFs (SEC filings, annual reports). Auto mode uses Docling if installed.

3. **PDF date extraction** (`crawl/pipeline.py`): Extracts dates from both PDF content (regex patterns) and PDF metadata, takes `max()` for the most accurate release date.

4. **PDF detection fix**: Check URL extension and `Content-Type` header for PDFs before checking crawl4ai success status, since crawl4ai marks PDF responses as failures.

5. **Browser headers for PDF downloads**: Direct PDF downloads use User-Agent, Accept, and Referer headers to avoid 403s.

6. **Accept-Encoding fix**: Send `Accept-Encoding: gzip, deflate` instead of allowing Brotli, which aiohttp can't decode without the optional `brotli` package.

7. **CrawlConfig** (`crawl/config.py`): Frozen dataclass with fields for urls_file, max_depth, max_pages, semaphore_count, pdf_extractor, etc.

8. **Resume/checkpoint**: Crawl sessions support checkpoint/resume with atomic JSON saves.

9. **53 new tests** for crawl pipeline, bringing the total to 327.

### Phase 9: Earnings Call Transcript Downloader (2026-02-21)

Added a `transcripts` subcommand that downloads structured earnings call transcripts from Motley Fool. This replaces the broken `discountingcashflows.com` API used by the legacy `FinNLP` library (now returns 403).

1. **Source research**: Evaluated multiple transcript sources. Motley Fool publishes full transcripts publicly (no paywall, no API key). URL pattern: `/earnings/call-transcripts/YYYY/MM/DD/{slug}/`.

2. **Discovery via sitemaps** (`transcripts/discovery.py`): Monthly XML sitemaps at `fool.com/sitemap/YYYY/MM` contain all published URLs. Scanning and filtering by ticker slug is more reliable than DDG search or listing page pagination.

3. **HTML extraction** (`transcripts/extract.py`): Parses JSON-LD metadata (ticker from `about[].tickerSymbol`, date from `datePublished`), article-body H2 sections (DATE, CALL PARTICIPANTS, Full Conference Call Transcript), speaker names, and Q&A split.

4. **Pipeline** (`transcripts/pipeline.py`): Synchronous fetch with `requests` (polite 1.5s delay), reuses existing `Deduplicator`, `ParquetWriter`, `JSONLWriter`, `Checkpoint`.

5. **Verified output**: AAPL Q1 2025 transcript — 8,136 words, 48,422 chars, full speaker list, prepared remarks + Q&A sections extracted correctly.

6. **18 new tests** for discovery URL parsing, config, HTML extraction, JSON-LD parsing, and ticker extraction. Total: 350 tests.

### Errors Fixed During Development

1. **pyproject.toml**: `setuptools.backends._legacy:_Backend` doesn't exist -> changed to `setuptools.build_meta`
2. **Windows shell escaping**: Multiline Python in bash failed -> wrote separate .py files
3. **DDG empty results**: Text search poor for niche queries -> news mode recommended
4. **Package renamed**: `duckduckgo_search` -> `ddgs` -> code handles both
5. **Missing `__main__.py`**: `python -m financial_scraper` failed because `src/` layout requires explicit `__main__.py`

---

## 11. Recommended Settings for Production

Based on the 50-query test run, these settings balance throughput vs. rate limiting:

```python
ScraperConfig(
    search_type="news",           # Much better than "text" for financial content
    max_results_per_query=10,     # 10-20 sweet spot; higher = more rate limits
    search_delay_min=4.0,         # 4-7s delay avoids most DDG blocks
    search_delay_max=7.0,
    max_concurrent_total=8,       # 8 parallel fetches
    max_concurrent_per_domain=2,  # Don't hammer individual sites
    fetch_timeout=25,             # Some financial sites are slow
    min_word_count=80,            # Lower than default to catch short news
)
```

**Throughput**: ~50 queries in ~9 minutes (~5.6 queries/min), yielding ~120 pages.

**Niche queries**: If DDG news returns 0 results, try:
1. Simpler phrasing (e.g., "oats price" instead of "oats commodity market")
2. `--search-type text` as fallback
3. Broader terms (e.g., "grain market" catches wheat, oats, barley together)

---

## Appendix A: Excluded Domains (config/exclude_domains.txt)

74 domains blocked by default across these categories:

| Category | Domains |
|---|---|
| Social media | twitter.com, x.com, facebook.com, reddit.com, linkedin.com, instagram.com, tiktok.com, pinterest.com, tumblr.com, snapchat.com |
| Video | youtube.com, vimeo.com, dailymotion.com |
| App stores | play.google.com, itunes.apple.com, apps.apple.com |
| Shopping | amazon.com/.co.uk/.fr/.de, ebay.com, walmart.com, etsy.com, aliexpress.com |
| Paywalled | wsj.com, ft.com, barrons.com, bloomberg.com, caixinglobal.com, scmp.com, law.com |
| Low quality financial | substack.com, seekingalpha.com |
| Ticker/TipRanks blurbs (>50% non-article) | marketwatch.com, cnbc.com, theglobeandmail.com, zawya.com |
| Non-financial content | nature.com |
| Russian/non-English noise | rbc.ru, mediametrics.ru, trk.mail.ru |
| Aggregators | quora.com |
| Job sites | indeed.com, glassdoor.com |
| Search engines | bing.com, google.com, duckduckgo.com, yahoo.com |

## Appendix B: Query Files

| File | Queries | Purpose |
|---|---|---|
| `config/queries_example.txt` | ~10 | General example |
| `config/test_commodities.txt` | 3 | Quick smoke test |
| `config/test_commodities2.txt` | 5 | Specific futures queries |
| `config/test_commodities3.txt` | 4 | Simple broad queries |
| `config/commodities_50.txt` | 50 | Full commodity coverage (energy, metals, grains, softs, livestock, niche) |
| `config/commodities_300.txt` | 305 | Comprehensive commodity coverage (energy, precious/base/battery metals, fertilizers, chemicals, grains, oilseeds, softs, livestock, dairy, specialty agriculture, environmental) |
