# Transcript Coverage Improvement — Agent Resume Guide

## Current State (2026-03-04, updated mid-session)

**Main parquet:** `runs/us10002/transcripts.parquet` — 25,690 rows (+2,764 from AlphaStreet re-run)
**CDX cache:** `runs/us10002/wayback_cdx_cache.json` — 63,669 entries (+44,844 from deep CDX expansion)
**Wayback parquet:** `runs/us10002/wayback_transcripts.parquet` — ~4,000+ rows (fill script running, 44,730 URLs to process)
**Wayback fill in progress:** `_fill_wayback_gaps.py` running with checkpoint/resume, ~23 req/min

### Source Breakdown
| Source | Transcripts | Share |
|--------|------------|-------|
| fool.com | 14,758 | 67.0% |
| seekingalpha.com (wayback) | 3,546 | 16.1% |
| alphastreet.com | 2,054 | 9.3% |
| seekingalpha.com (research4) | 1,656 | 7.5% |

### Year Coverage (transcripts / ticker coverage %)
| Year | Rows | Tickers | % |
|------|------|---------|---|
| 2007 | 27 | 27 | 1.8% |
| 2008 | 331 | 331 | 22.0% |
| 2009 | 491 | 307 | 20.4% |
| 2010 | 90 | 86 | 5.7% |
| 2011 | 135 | 97 | 6.4% |
| 2012 | 147 | 133 | 8.8% |
| 2013 | 932 | 552 | 36.7% |
| 2014 | 113 | 113 | 7.5% |
| 2015 | 275 | 254 | 16.9% |
| 2016 | 381 | 280 | 18.6% |
| 2017 | 367 | 139 | 9.2% |
| 2018 | 1,425 | 678 | 45.0% |
| 2019 | 3,345 | 927 | 61.6% |
| 2020 | 3,544 | 1,058 | 70.3% |
| 2021 | 3,365 | 986 | 65.5% |
| 2022 | 1,906 | 606 | 40.3% |
| 2023 | 1,570 | 707 | 47.0% |
| 2024 | 1,012 | 308 | 20.5% |
| 2025 | 1,470 | 1,022 | 67.9% |
| 2026 | 1,083 | 1,083 | 72.0% |

### Biggest Gaps (priority order)
1. **2010, 2011, 2012, 2014** — under 10% coverage each
2. **2015-2017** — 10-19% coverage (improved from near-zero)
3. **2022-2024** — 20-47% (Fool paywall changes reduced discovery)
4. **117 missing tickers** — mostly SPACs, OTC, preferred shares, recent IPOs

---

## Completed Work

### Sources Tried and Exhausted
- **Motley Fool pipeline** (7 runs) — primary source, 14,758 transcripts. Fool sitemaps → FMP fallback → browser fallback. Coverage drops before 2019 and in 2022-2024. Re-run 2022-2024 found 0 new (sitemaps empty, paywall confirmed).
- **AlphaStreet standalone script** (`_fill_alphastreet_gaps.py`) — **4,818 transcripts** (2,054 + 2,764 from re-run). Good for 2019-2026. Re-run filled 2,764 gaps with 99.96% hit rate. All sitemaps processed.
- **Wayback Machine + Seeking Alpha** (`_fill_wayback_gaps.py`) — **~4,000+ transcripts** (growing). CDX cache expanded from 18,825 → 63,669 entries via `_expand_cdx_deep.py`. Fill script in progress with 44,730 new URLs to process. Best source for pre-2017.
- **Research4 text files** (`_convert_research4.py`) — 1,656 transcripts from `research_4/Earnings-Calls-NLP/transcripts/sandp500/` (5,238 SA transcript .txt files, 2015-2020, S&P 500). Already fully converted.

### Sources Probed and Ruled Out
- **FMP API** — requires paid API key, demo key returns 401. Not viable without signup.
- **EarningsCall.biz** — app-only site, no public transcript pages, all URLs 404.
- **Benzinga** — transcripts are TRUNCATED (2-5K chars vs 40-60K full). Only 2025+ available in sitemaps. Not viable.
- **SEC EDGAR** — EFTS search finds ~50 filings/year with transcript keyword. Too sparse.
- **Yahoo Finance** — paywalled.
- **GuruFocus** — paywalled.
- **Quartr** — app-only, no web access.
- **Investing.com** — AI summaries only, blocked by anti-bot.
- **Rev.com** — (probed 2026-03-04) No longer hosts earnings call transcripts. `/blog/transcripts` redirects to generic service page. All transcript URLs return 404. Zero earnings content.

---

## Remaining Actions to Improve Coverage

### 1. Expand Wayback CDX Discovery Further — DONE (2026-03-04)
Ran `_expand_cdx_deep.py` with 4 phases:
- Phase 1: 3-digit sub-prefixes (60 queries) → 21,953 new URLs
- Phase 2: Year-filtered CDX for 2010-2014, 2017 → 216 new URLs
- Phase 3: Sub-prefix + year filter for 2015-2017 → 16,603 new URLs
- Phase 4: Additional ranges (article/1x, 5x) → 6,072 new URLs
**Total: 44,844 new CDX entries. Cache: 18,825 → 63,669.**

### 2. Wayback CDX with Per-Year Crawl Filtering — DONE (2026-03-04)
Combined with task 1 into `_expand_cdx_deep.py`. Phase 3 used sub-prefix + year approach. 2017 alone yielded 13,063 new transcript URLs.

### 3. Motley Fool Pipeline Re-run for 2022-2024 — DONE (2026-03-04)
Scanned 60 monthly sitemaps. **0 new transcripts found.** Fool sitemaps no longer list 2022-2024 transcripts (paywall changes confirmed). FMP API key needed for further progress.

### 4. AlphaStreet Re-run for Recent Quarters — DONE (2026-03-04)
Ran `_fill_alphastreet_gaps.py`. Found 2,765 gaps, filled 2,764 (99.96% hit rate). Covered 2019-2026. Main parquet: 22,926 → 25,690 rows.

### 5. Rev.com Transcripts — RULED OUT (2026-03-04)
Ran `_probe_rev.py`. Rev.com removed all earnings call transcripts. `/blog/transcripts` redirects to generic service page, all transcript URLs 404, zero earnings content.

### 6. Wayback Fill of Expanded CDX — IN PROGRESS
`_fill_wayback_gaps.py` running on 44,730 new URLs (19,309 known-ticker gaps + 25,421 unknown-ticker). Checkpoint/resume enabled. ~23 req/min. Estimated ~24-32 hours total.
- To resume: `python -u _fill_wayback_gaps.py --delay 1.5 --batch-size 25`
- To check progress: `python _check_wb_status.py`

### 7. Missing Tickers Deep Dive
117 tickers have zero transcripts. Many are legitimate misses (SPACs, OTC, preferred shares), but some may have transcripts under different names/tickers:
- `BRK.A` → Berkshire Hathaway (may be under `BRK-A` or `BRK.B`)
- `EXPD` → Expeditors International (S&P 500 company, should have transcripts)
- `THO` → Thor Industries (S&P 600, likely has transcripts)
- `SEDG` → SolarEdge (was S&P 500)
- `NVR` → NVR Inc (homebuilder, S&P 500)
- Script needed: cross-reference missing tickers against known SA/Fool URL slugs with fuzzy matching

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `runs/us10002/transcripts.parquet` | Main merged transcript file (THE output) |
| `runs/us10002/wayback_transcripts.parquet` | Wayback-only transcripts (pre-merge) |
| `runs/us10002/research4_transcripts.parquet` | Research4-only transcripts (pre-merge) |
| `runs/us10002/wayback_cdx_cache.json` | 63,669 archived SA transcript URLs from CDX |
| `runs/us10002/wayback_checkpoint.json` | Checkpoint for wayback fill script (18,748 processed) |
| `financial_scraper/config/us10002_active_tickers.txt` | 1,505 target tickers |
| `_fill_wayback_gaps.py` | Wayback+SA backfill script (CDX discovery + fetch) |
| `_fill_alphastreet_gaps.py` | AlphaStreet backfill script |
| `_convert_research4.py` | Research4 text file → parquet converter |
| `_expand_cdx_deep.py` | Combined CDX expansion (3-digit prefixes + year filters + extra ranges) |
| `_expand_cdx_2015_2017.py` | CDX cache expansion with granular sub-prefixes (superseded by _expand_cdx_deep.py) |
| `_expand_cdx_cache.py` | CDX cache expansion with article ID prefixes (superseded by _expand_cdx_deep.py) |
| `_probe_rev.py` | Rev.com probe script (result: not viable) |
| `_merge_parquets.py` | Merge main + wayback + research4 parquets (deduplicates) |
| `_analysis_merged.py` | Full coverage analysis on merged parquet |
| `_check_wb_status.py` | Quick checkpoint + parquet status check |
| `_check_wb_final.py` | Detailed checkpoint + year/quarter breakdown |

## Parquet Schema
```python
pa.schema([
    ("company", pa.string()),     # Ticker symbol (e.g., "AAPL")
    ("title", pa.string()),       # Transcript title
    ("link", pa.string()),        # Source URL
    ("snippet", pa.string()),     # First ~300 chars
    ("date", pa.timestamp("ns")), # Earnings call date
    ("source", pa.string()),      # e.g., "fool.com", "seekingalpha.com (wayback)"
    ("full_text", pa.string()),   # Full transcript text (typically 20-80K chars)
    ("source_file", pa.string()), # Original filename reference
])
```

## Deduplication Logic
Deduplicate by `(company, year, quarter)` — keep first occurrence. Merge order: main parquet (Fool + AlphaStreet) → wayback → research4. This means Fool/AlphaStreet take priority when the same combo exists in multiple sources.

## Wayback Fetch Notes
- Archive URL format: `https://web.archive.org/web/{timestamp}id_/{original_url}` (the `id_` flag returns original page without Wayback toolbar)
- SA content selectors vary by era: `div#a-body` (newer), `div.article_body` (older), `article` (fallback)
- Ticker extraction: URL-based first (fast), then content-based via `(TICKER)` or `(EXCHANGE:TICKER)` patterns in first 2000 chars
- Rate: ~30 req/min with 1.5s delay. Use `timeout=(10, 45)` to avoid hung connections
- CDX API: `web.archive.org/cdx/search/cdx` with `matchType=prefix`, `collapse=urlkey`, `filter=statuscode:200`
- CDX frequently returns 504 — retry with 30s wait, up to 3 attempts
