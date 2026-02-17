# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
