# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.1] - 2026-02-28

### Added

- **`--all-formats` flag** (`search` and `crawl` subcommands): shorthand for `--jsonl --markdown`, writes all three output formats (Parquet + JSONL + Markdown) in one run
- **`read_output` MCP tool: `as_markdown` parameter**: set `as_markdown=True` to get a formatted Markdown report from a Parquet file instead of raw JSON rows; returns `{total_rows, returned_rows, markdown}`
- **GitHub Actions CI** (`.github/workflows/pytest.yml`): runs `pytest` on push to `main` and on PRs, Python 3.11 and 3.12 matrix
- **55 new tests** (405 total): `TranscriptPipeline` integration suite (fetch, extract, HTTP failure, JSONL output, tickers file), `discover_transcripts` with mocked sitemaps, `_fetch_sitemap_urls` error paths, `build_crawl_config`, `build_transcript_config`, `_run_search` / `_run_crawl` / `_run_transcripts`, `main()` subcommand routing and backward-compat, `--all-formats` / `--markdown` flag tests, `read_output(as_markdown=True)`, pipeline markdown integration

### Changed

- `.gitignore` re-added to version control (self-exclusion line removed); file now ships with the repo
- Coverage: 79% → 91% (`main.py` 28% → 94%, `transcripts/pipeline.py` 0% → 92%, `transcripts/discovery.py` 37% → 95%)
- `docs/user-guide.md`: added `markdown_path` to output settings table, updated directory structure diagram to show `.md` and `markdown/` outputs, added Markdown format section with `--all-formats` example
- `docs/mcp-setup.md`: added `export_markdown` tool reference (parameters, return shape, usage pattern), added `as_markdown` parameter to `read_output` reference, added three new workflow examples (export session as Markdown, read past run as Markdown)

## [0.4.0] - 2026-02-21

### Added

- **Earnings call transcript downloader** (`transcripts` subcommand): Downloads and extracts structured earnings call transcripts from Motley Fool. Discovers transcript URLs via monthly XML sitemaps, extracts metadata (company, ticker, quarter, year), speakers, prepared remarks, and Q&A sections. Outputs to Parquet in `merged_by_year` schema.
- `transcripts/discovery.py`: Sitemap-based URL discovery — scans `fool.com/sitemap/YYYY/MM` monthly sitemaps, filters by ticker/year/quarter via URL slug regex
- `transcripts/extract.py`: HTML extraction — parses JSON-LD metadata, article-body sections (DATE, CALL PARTICIPANTS, Full Conference Call Transcript), speaker detection regex, Q&A split
- `transcripts/pipeline.py`: `TranscriptPipeline` orchestrator — load tickers, discover, fetch, extract, dedup, write Parquet/JSONL with checkpoint/resume support
- `transcripts/config.py`: `TranscriptConfig` frozen dataclass with tickers, year, quarters, concurrent, output paths
- CLI: `--tickers`, `--tickers-file`, `--year`, `--quarters`, `--jsonl`, `--checkpoint`, `--resume`, `--reset`
- 18 new tests for transcripts module (350 total)

## [0.3.2] - 2026-02-21

### Added

- **Example notebooks** (`examples/`): 3 Jupyter notebooks with detailed walkthroughs for all scraper modes — text search, news search, and URL deep-crawl. Each includes config, run, result analysis, and CLI equivalents.
- **Default domain exclusion**: `--exclude-file` now defaults to the built-in `config/exclude_domains.txt` (previously required explicit flag every run)
- `--no-exclude` CLI flag to disable domain filtering entirely

### Changed

- Updated DOCUMENTATION.md architecture tree with `examples/` directory
- Updated user-guide.md Next Steps with notebook links
- Removed `.gitignore` from tracked files (local-only)

## [0.3.1] - 2026-02-21

### Added

- **MinHash LSH fuzzy deduplication**: Near-duplicate content detection using MinHash with Locality-Sensitive Hashing (128 permutations, 0.85 Jaccard threshold, 3-word shingles). Catches syndicated news rewrites — same article republished across sites with minor edits, different headers, or added disclaimers. Runs as a second layer on top of existing SHA256 exact dedup.
- `datasketch>=1.6` added to dependencies
- Graceful degradation: fuzzy dedup silently disabled if `datasketch` not installed
- MinHash state persisted in dedup JSON (hex-encoded digests), restored on load
- 5 new tests for fuzzy dedup (332 total)

### Changed

- `store/dedup.py` now uses two-layer content dedup: SHA256 exact check (O(1)) then MinHash LSH fuzzy check
- Updated DOCUMENTATION.md, user-guide.md with fuzzy dedup details

## [0.3.0] - 2026-02-21

### Added

- `crawl` subcommand with crawl4ai headless browser, BFS financial-keyword scoring, seed URL files
- Docling PDF extraction backend (`--pdf-extractor docling`) with layout-aware table detection
- PDF date extraction: `max(content_regex, metadata)` for most accurate release date
- PDF detection before crawl4ai success check — catches PDFs by URL extension/content-type header
- Browser-style headers for direct PDF downloads (User-Agent, Accept, Referer)
- `Accept-Encoding: gzip, deflate` header to prevent Brotli decoding errors on PDF downloads
- `CrawlConfig` frozen dataclass, `CrawlPipeline`, `BestFirstCrawlingStrategy` integration
- Resume/checkpoint support for crawl sessions
- 53 new tests for crawl pipeline (327 total)

### Changed

- `extract/pdf.py` now dispatches between pdfplumber and Docling via `get_pdf_extractor()`
- Updated README, user-guide, cli-examples, architecture docs with crawl subcommand reference

## [0.2.2] - 2026-02-19

### Added

- **TipRanks promo text removal**: 10 new regex patterns in `TextCleaner` strip TipRanks promotional blocks ("Claim X% Off TipRanks Premium", "Meet Your ETF AI Analyst", "Stock Analysis page", "Smart Investor Newsletter", etc.)
- **Post-extraction content-type filters**: `is_ticker_page()` detects stock quote/profile pages (matching 3+ of: `52 Week`, `EPS (TTM)`, `P/E (TTM)`, `Prev Close`); `is_nature_index_page()` detects Nature Index research profiles. Pipeline rejects both after extraction.
- **Trending/Related article block removal**: New patterns strip "Trending Articles", "Related Stories", "Recommended Stories" trailing blocks
- **PR wire disclaimer removal**: Two regex patterns strip MENAFN-style multi-line disclaimer blocks ("We do not accept any responsibility or liability...")
- 8 new domains in `exclude_domains.txt`: `caixinglobal.com`, `cnbc.com`, `theglobeandmail.com`, `zawya.com`, `nature.com`, `marketwatch.com`, `scmp.com`, `law.com`

### Changed

- `TextCleaner` boilerplate patterns expanded from 10 to 24 regex patterns
- Pipeline now filters out non-article page types (ticker pages, Nature Index profiles) before storage
- Domain exclusion list expanded from 40 to 48 domains
- Updated DOCUMENTATION.md with content quality filtering details (Phase 7)

## [0.2.1] - 2026-02-19

### Added

- **`--reset` flag**: Deletes the checkpoint file before running for a full fresh start
- **`--reset-queries` flag**: Clears completed queries from checkpoint but keeps URL history, allowing re-runs without re-fetching the same URLs
- `reset_queries()` method on `Checkpoint` class
- `reset_queries` field on `ScraperConfig` frozen dataclass
- **[Technical Skills](docs/skills.md)** document: engineering competencies demonstrated by this project, organized across 8 domains (async programming, network engineering, data engineering, resilience, architecture, testing, devops, security)

### Changed

- Updated all documentation (README, User Guide, CLI Cookbook, DOCUMENTATION, Architecture) with reset flag references and examples
- Test suite expanded from 265 to 274 tests (9 new tests for reset features)

## [0.2.0] - 2026-02-18

### Added

- **Deep crawl** (`--crawl`): BFS link-following from fetched pages to discover additional same-domain content
- `--crawl-depth N` flag (default: 2) to control max link-following depth
- `--max-pages-per-domain N` flag (default: 50) to cap pages fetched per domain during crawl
- New module `extract/links.py` with `extract_links()` and `filter_links_same_domain()`
- Per-depth progress logging (`[depth 0] Fetching N URLs`, `[depth 0] Discovered N links for depth 1`)
- Hard cap on total crawl URLs per depth to prevent runaway crawls on link-heavy sites
- `_domain_page_counts` counter in pipeline for domain-level page tracking
- 20 unit tests for link extraction and filtering (`tests/test_links.py`)
- 3 integration tests for crawl feature (`tests/test_pipeline.py::TestCrawlFeature`)
- CLI flag wiring test for crawl options (`tests/test_main.py`)
- Deep crawl examples in CLI Cookbook, User Guide, and README

### Changed

- Pipeline fetch-process section refactored into a BFS `while urls_to_fetch` loop
- When `crawl=False` (default), loop runs exactly once with zero behavioral change
- Updated `.gitignore` to cover `_test_*.py` and `_run_*.py` standalone scripts
- Test suite expanded from 245 to 265 tests (all passing)

## [0.1.0] - 2025-10-01

### Added

- Modular package structure replacing legacy monolithic scraper
- DuckDuckGo search integration (text and news modes) with retry and adaptive cooldown
- Async HTTP fetching with aiohttp and 5 rotating browser fingerprint profiles
- Per-domain adaptive throttling (success halves delay, 429 doubles it)
- robots.txt compliance checking
- Tor proxy support with automatic circuit renewal
- Content extraction via trafilatura (2-pass: precision then fallback)
- PDF extraction via pdfplumber
- Boilerplate removal with 10 regex cleanup patterns
- URL normalization and content SHA256 deduplication
- Parquet output with snappy compression and append mode
- Optional JSONL output
- Checkpoint/resume with atomic JSON saves per query
- Stealth mode preset (reduced concurrency, longer delays)
- Date range filtering for extracted content
- Domain exclusion list support
- CLI entry point (`financial-scraper`)
- Comprehensive test suite (195 tests, 89% coverage)
