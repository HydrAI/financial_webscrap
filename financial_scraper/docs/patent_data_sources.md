# Patent Data Sources

## Overview

The patent pipeline uses multiple data sources for discovery and full-text retrieval. Google Patents is the primary source but rate-limits aggressively. This document covers all available sources, setup instructions, and the bulk download tool.

## Live Pipeline Sources

### 1. Google Patents (Primary)

- **Endpoint**: `patents.google.com/xhr/query` (discovery) + HTML pages (fetch)
- **Auth**: None
- **Rate limit**: ~5-6 requests before 503; cooldown ~30-60 min
- **Coverage**: Global (US, EP, WO, CN, JP, KR, etc.)
- **Returns**: Full patent text (abstract, claims, description via HTML)

No setup required. This is the default source.

### 2. EPO Open Patent Services (Fallback)

- **Endpoint**: `ops.epo.org/3.2/rest-services/`
- **Auth**: OAuth2 (free registration)
- **Rate limit**: 3.5 GB/week, throttled per-minute rolling window
- **Coverage**: International (EP, WO, many national offices)
- **Returns**: Structured XML with separate endpoints for `/abstract`, `/claims`, `/description`

#### Setup

1. Go to https://developers.epo.org
2. Click **Register** and create a free account (email + password)
3. Once logged in, go to **My Apps** > **Add a new app**
4. Name it anything (e.g. `patent_scraper`), select **OPS** as the API
5. Copy your **Consumer Key** and **Consumer Secret**
6. Paste them into `config/secrets.json`:

```json
{
  "epo_ops": {
    "consumer_key": "YOUR_KEY_HERE",
    "consumer_secret": "YOUR_SECRET_HERE"
  },
  "patentsview": {
    "api_key": ""
  }
}
```

The `python-epo-ops-client` package handles OAuth token exchange automatically.

#### Rate Limit Behavior

EPO OPS uses a color-coded throttling system:
- **Green** (<50% quota): full speed
- **Yellow** (50-75%): slow down
- **Red** (75-100%): minimal requests
- **Black** (>100%): blocked until quota resets

Quota info is returned in response headers (`X-IndividualQuotaPerHour`, `X-RegisteredQuotaPerWeek`).

### 3. PatentsView Local Cache (Fallback)

- **Source**: PatentsView S3 bulk TSV tables
- **Auth**: None
- **Rate limit**: None (local file lookup)
- **Coverage**: All US patents
- **Returns**: Title, abstract, dates, assignee, CPC codes (no claims/description)

Build the cache once, then the live pipeline uses it as a fallback when Google Patents and EPO are unavailable:

```bash
python scripts/bulk_patents.py --build-cache --cache-dir .patent_cache
```

This downloads ~1.1 GB of TSV tables from PatentsView S3 (free, no auth), joins them, and creates a local parquet cache file.

### 4. DuckDuckGo (Discovery Only)

- **Usage**: Auto-fallback when Google Patents discovery returns 0 results
- **Auth**: None
- **Returns**: Patent IDs extracted from search results (no full text)

### 5. Justia (Discovery Only)

- **Usage**: Best-effort assignee page scraping
- **Auth**: None
- **Status**: Frequently blocked by Cloudflare (403)

## Bulk Download Tool

`scripts/bulk_patents.py` downloads patent metadata in bulk from PatentsView S3 data tables hosted on AWS. These are free, require no authentication, and have no rate limits.

### Available Tables

| Table | Size | Contents | Status |
|-------|------|----------|--------|
| `g_patent.tsv.zip` | ~219 MB | title, abstract, date, type, num_claims | Free |
| `g_assignee_disambiguated.tsv.zip` | ~342 MB | assignee organizations | Free |
| `g_cpc_current.tsv.zip` | ~472 MB | CPC classification codes | Free |
| `g_application.tsv.zip` | ~68 MB | filing/application dates | Free |
| `g_claim.tsv.zip` | - | claim text | Requires API key |
| `g_detail_desc_text.tsv.zip` | - | detailed description | Requires API key |

### Usage

```bash
# Download + filter by assignee, output parquet + JSONL
python scripts/bulk_patents.py \
  --assignee "NVIDIA" \
  --output-dir runs/bulk_patents \
  --parquet --jsonl

# Filter by CPC codes and date range
python scripts/bulk_patents.py \
  --cpc-filter G06F H04L \
  --date-from 2020-01-01 \
  --assignee "Google" \
  --limit 1000 \
  --output-dir runs/bulk_patents \
  --parquet

# Build local cache for live pipeline fallback
python scripts/bulk_patents.py --build-cache --cache-dir .patent_cache

# Use already-downloaded TSV files (skip re-download)
python scripts/bulk_patents.py \
  --download-dir /path/to/tsvs \
  --assignee "Tesla" \
  --output-dir runs/bulk_patents \
  --parquet
```

TSV files are cached after the first download. Subsequent runs skip the download step.

## Retired / Non-functional Sources (2026)

| Source | Status | Notes |
|--------|--------|-------|
| USPTO PatFT | DNS dead (10.10.10.10) | Retired, replaced by ODP |
| USPTO AppFT | DNS does not resolve | Retired |
| USPTO ODP API | Behind AWS WAF | Angular SPA only, no public REST API |
| PatentsView API v1 | 410 Gone | Discontinued May 2025 |
| USPTO BDSS | 404 | Retired |
| USPTO PEDS | Connection refused | Retired or moved behind ODP |

## Secrets File

`config/secrets.json` stores API credentials. This file must **never** be committed to git.

```json
{
  "epo_ops": {
    "consumer_key": "",
    "consumer_secret": ""
  },
  "patentsview": {
    "api_key": ""
  }
}
```
