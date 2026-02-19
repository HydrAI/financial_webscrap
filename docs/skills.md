# Technical Skills Demonstrated

A breakdown of the engineering competencies showcased in this project, organized by domain.

---

## Python & Async Programming

| Skill | Where it shows |
|-------|----------------|
| **asyncio / async-await** | Full async pipeline: `FetchClient` uses `aiohttp` with batched `gather()` and `return_exceptions=True` for fault-tolerant parallel I/O |
| **Concurrency control** | Dual semaphore pattern -- global cap (`max_concurrent_total`) + per-domain cap (`max_concurrent_per_domain`) prevent both server overload and self-DoS |
| **Frozen dataclasses** | `ScraperConfig` is immutable; stealth mode rebuilds via field extraction rather than mutation -- safe for async contexts |
| **Type hints** | Python 3.10+ union syntax (`str | None`), typed NamedTuples for inter-stage data (`SearchResult`, `FetchResult`, `ExtractionResult`) |
| **Package structure** | Proper `src/` layout with `pyproject.toml`, console script entry points, optional dependency groups (`[dev]`, `[mcp]`) |

---

## Network Engineering & Web Scraping

| Skill | Where it shows |
|-------|----------------|
| **Adaptive rate limiting** | `DomainThrottler` uses aiolimiter leaky-bucket with feedback: 200 OK halves delay, 429/503 doubles it -- each domain converges to its max sustainable rate independently |
| **Browser fingerprint rotation** | 5 profiles (Chrome/Win, Chrome/Mac, Firefox, Safari, Edge) with realistic headers, User-Agent, Accept-Encoding; automatic swap on 403/429 |
| **Tor integration** | SOCKS5 proxy via `aiohttp-socks`, automatic circuit renewal via `stem` control protocol every N queries or on rate-limit detection |
| **robots.txt compliance** | `RobotChecker` fetches and caches robots.txt per domain, respects disallow rules before fetching |
| **Retry strategies** | DDG search: 3 attempts with exponential backoff (10s, 20s, 40s) on `RatelimitException`; HTTP fetch: fingerprint rotation + retry on 403/429 |

---

## Data Engineering

| Skill | Where it shows |
|-------|----------------|
| **ETL pipeline design** | Clean 4-stage pipeline: Search -> Fetch -> Extract -> Store, each stage a separate module with typed inputs/outputs |
| **Parquet output** | PyArrow-based writer with snappy compression, append mode, schema compatible with downstream `merged_by_year` pipeline |
| **Content deduplication** | Two-layer dedup: URL normalization (exact match) + SHA256 hash of first 2000 chars (near-duplicate detection) |
| **BFS web crawling** | Breadth-first link discovery with depth limits, same-domain filtering, per-domain page caps, seen-URL tracking |
| **Text extraction** | trafilatura two-pass strategy (precision mode first, relaxed fallback if insufficient content) + pdfplumber for PDFs + 10-pattern regex cleanup |
| **Date filtering** | Post-extraction date range filter with flexible date parsing from trafilatura metadata |

---

## Resilience & Fault Tolerance

| Skill | Where it shows |
|-------|----------------|
| **Checkpoint / resume** | Atomic JSON checkpoint (write to `.tmp`, then `os.replace`) after each query. Tracks completed queries, fetched URLs, failed URLs with retry counts, cumulative stats |
| **Graceful degradation** | Tor unavailable? Falls back to direct. Extraction fails? Logs and continues. PDF instead of HTML? Switches extractor. No search results? Marks done and moves on |
| **Crash recovery** | `--resume` reloads checkpoint and skips completed work. `--reset-queries` re-runs queries while keeping URL-level dedup. `--reset` starts fully fresh |
| **Failure tracking** | Per-URL failure counts with configurable max retries (`should_retry`). Domain-level page counts cap runaway crawls |

---

## Software Architecture

| Skill | Where it shows |
|-------|----------------|
| **Modular design** | 5 independent packages (`search/`, `fetch/`, `extract/`, `store/`, `mcp/`) + core orchestrator. Any layer can be replaced or tested independently |
| **Configuration management** | Single frozen dataclass with 30+ fields, CLI mapping via argparse, stealth mode as a config transform (not runtime mutation) |
| **Separation of concerns** | `pipeline.py` orchestrates but doesn't implement: search logic in `duckduckgo.py`, HTTP in `client.py`, extraction in `html.py`/`pdf.py`, storage in `output.py` |
| **MCP server** | Exposes scraper capabilities as Model Context Protocol tools for LLM integration -- search, fetch, extract, scrape as callable tools |

---

## Testing

| Skill | Where it shows |
|-------|----------------|
| **Comprehensive coverage** | 19 test files, 60+ tests, 89% coverage across all modules |
| **Async test support** | `pytest-asyncio` with strict mode for testing async pipeline, fetch client, and Tor integration |
| **Mocking external services** | DDG API, HTTP responses, Tor daemon, file system -- all mocked for deterministic, fast tests |
| **Fixture design** | Shared `conftest.py` with `sample_config` (zero-delay test defaults), `tmp_parquet`, `tmp_jsonl` |
| **Edge case coverage** | Empty results, failed extractions, duplicate content, missing files, PDF content types, crawl depth limits |

---

## DevOps & Tooling

| Skill | Where it shows |
|-------|----------------|
| **CLI design** | 30+ flags organized by concern (search, fetch, Tor, crawl, extract, store), sensible defaults, mutual exclusivity where needed |
| **Cross-platform** | Windows `asyncio.WindowsSelectorEventLoopPolicy` workaround, forward-slash path handling, platform-aware entry points |
| **Logging** | Structured logging with timestamps, log level, module name. Noisy third-party loggers suppressed (`duckduckgo_search`, `trafilatura`, `urllib3`) |
| **Output organization** | Timestamped run directories (`YYYYMMDD_HHMMSS/`), provenance tagging (`source_file` column tracks query + mode + quarter) |

---

## Security & Ethics

| Skill | Where it shows |
|-------|----------------|
| **robots.txt compliance** | Checked before every fetch (with caching). Opt-out via `--no-robots` for authorized testing only |
| **Rate limiting** | Per-domain adaptive throttle + configurable search delays (3-8s default). Stealth mode presets for large runs |
| **Domain exclusions** | Configurable blocklist (48 domains: social media, paywalled, video, ticker/TipRanks, non-financial) |
| **Tor privacy** | Optional SOCKS5 routing with circuit renewal to avoid IP-based blocking and protect researcher identity |
| **Anti-fingerprinting awareness** | Realistic browser profiles, not just random User-Agents -- matching Accept, Accept-Language, Accept-Encoding headers per browser |

---

## By the Numbers

| Metric | Value |
|--------|-------|
| Source lines | ~1,800 |
| Modules | 23 |
| Test files | 19 |
| Test cases | 274 |
| Coverage | 89% |
| CLI options | 30+ |
| Browser profiles | 5 |
| Output formats | 3 (Parquet, JSONL, Markdown) |
| Dependencies | 13 core + 3 dev |

---

## Usage Examples

### Basic news scrape

```bash
financial-scraper --queries-file queries.txt --search-type news --output-dir ./runs
```

### Production run with stealth + Tor + resume

```bash
financial-scraper --queries-file config/commodities_300.txt --search-type news --stealth --use-tor --resume --output-dir ./runs --exclude-file config/exclude_domains.txt --jsonl
```

### Re-run queries with fresh search results (keep URL dedup)

```bash
financial-scraper --queries-file queries.txt --resume --reset-queries --output-dir ./runs
```

### Deep crawl with depth-limited BFS

```bash
financial-scraper --queries-file queries.txt --search-type news --crawl --crawl-depth 2 --max-pages-per-domain 10 --output-dir ./runs
```

### Date-filtered extraction

```bash
financial-scraper --queries-file queries.txt --search-type news --date-from 2025-01-01 --date-to 2025-12-31 --output-dir ./runs
```

### Python API

```python
import asyncio
from pathlib import Path
from financial_scraper import ScraperConfig, ScraperPipeline

config = ScraperConfig(
    queries_file=Path("queries.txt"),
    search_type="news",
    max_results_per_query=20,
    stealth=True,
    resume=True,
    crawl=True,
    crawl_depth=1,
    max_pages_per_domain=5,
    output_dir=Path("./runs"),
    exclude_file=Path("config/exclude_domains.txt"),
)

pipeline = ScraperPipeline(config)
asyncio.run(pipeline.run())
```

---

## Further Reading

- [README](../README.md) -- project overview and quick start
- [User Guide](user-guide.md) -- installation, configuration reference, scaling guide
- [CLI Cookbook](cli-examples.md) -- copy-paste commands for every use case
- [Architecture](architecture.md) -- module map, data flow diagrams, design rationale
- [Ethical Scraping](ethical-scraping.md) -- rate limiting strategy and best practices
