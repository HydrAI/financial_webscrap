# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
