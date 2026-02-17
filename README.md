# financial-scraper

Ethical, async web scraper for financial research. Searches DuckDuckGo, fetches pages with fingerprint rotation, extracts clean text via trafilatura, and outputs to Parquet.

![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
![Tests](https://img.shields.io/github/actions/workflow/status/HydrAI/financial-scraper/tests.yml?label=tests)
![Coverage](https://img.shields.io/badge/coverage-89%25-brightgreen)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

---

## Features

- **Ethical by default**, robots.txt compliance, adaptive per-domain rate limiting
- **DuckDuckGo search**, text and news modes, no API keys required
- **Async HTTP**, aiohttp with configurable concurrency and per-domain throttling
- **HTML + PDF extraction**, trafilatura 2-pass for HTML, pdfplumber for PDFs
- **Content deduplication**, URL normalization + SHA256 content hashing
- **Checkpoint/resume**, atomic saves after each query, crash recovery
- **Fingerprint rotation**, 5 browser profiles to reduce bot detection
- **Parquet + JSONL output**, columnar storage with snappy compression
- **Date filtering**, keep only pages within a date range
- **Tor support**, SOCKS5 proxy with automatic circuit renewal
- **Stealth mode**, reduced concurrency + longer delays preset

---

## Quick Start

```bash
pip install -e ./financial_scraper
financial-scraper --queries-file queries.txt --search-type news --output-dir ./runs
```

---

## Prerequisites

| Requirement | Minimum | Check with |
|-------------|---------|------------|
| Python | 3.10+ | `python --version` |
| pip | 21.0+ | `pip --version` |
| git | any | `git --version` |

---

## Installation

### From source (recommended)

```bash
git clone https://github.com/HydrAI/financial-scraper.git
cd financial-scraper

# Create a virtual environment (recommended)
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate

cd financial_scraper
pip install -e .
```

### Verify installation

```bash
python -m financial_scraper --help
```

### With dev dependencies (for running tests)

```bash
pip install -e ".[dev]"
python -m pytest tests/ -v
```

### Tor setup (optional)

Install [Tor Browser](https://www.torproject.org/) (uses port 9150) or the Tor daemon (port 9050), then add `--use-tor` to your commands. See the [User Guide](docs/user-guide.md#using-tor) for detailed setup instructions.

---

## Usage Examples

### Basic query file scraping

```bash
financial-scraper --queries-file queries.txt --output-dir ./runs
```

### Date-filtered news scraping

```bash
financial-scraper \
  --queries-file queries.txt \
  --search-type news \
  --date-from 2025-01-01 \
  --date-to 2025-12-31 \
  --output-dir ./runs
```

### Tor-routed privacy mode

```bash
financial-scraper \
  --queries-file queries.txt \
  --use-tor \
  --tor-socks-port 9150 \
  --output-dir ./runs
```

### Stealth mode for large runs

```bash
financial-scraper \
  --queries-file queries.txt \
  --stealth \
  --resume \
  --output-dir ./runs
```

### Resume an interrupted job

```bash
financial-scraper \
  --queries-file queries.txt \
  --resume \
  --output-dir ./runs
```

### JSONL output alongside Parquet

```bash
financial-scraper \
  --queries-file queries.txt \
  --output-dir ./runs \
  --jsonl
```

---

## Query File Format

One query per line. Lines starting with `#` are comments:

```text
# Energy
crude oil futures market outlook
natural gas futures winter forecast

# Metals
gold futures price prediction
copper futures demand supply

# Macro
Federal Reserve interest rate decision
treasury yield curve inversion signal
```

See [`docs/examples/`](docs/examples/) for ready-to-use query files.

---

## Output Format

### Parquet schema

| Column | Type | Description |
|--------|------|-------------|
| `company` | string | Search query (maps to entity/topic) |
| `title` | string | Page title |
| `link` | string | Source URL |
| `snippet` | string | First 300 characters of content |
| `date` | timestamp | Publication date |
| `source` | string | Domain name |
| `full_text` | string | Full extracted text |
| `source_file` | string | Provenance tag |

### Reading output

```python
import pandas as pd

df = pd.read_parquet("runs/20260215_143000/scrape_20260215_143000.parquet")
print(f"{len(df)} documents from {df['source'].nunique()} domains")
```

### JSONL

When `--jsonl` is enabled, a `.jsonl` file is written alongside the Parquet file with the same schema (one JSON object per line).

---

## Python API

```python
import asyncio
from pathlib import Path
from financial_scraper import ScraperConfig, ScraperPipeline

config = ScraperConfig(
    queries_file=Path("queries.txt"),
    search_type="news",
    max_results_per_query=10,
    output_dir=Path("./runs"),
    stealth=True,
    date_from="2025-01-01",
    date_to="2025-12-31",
    resume=True,
)

pipeline = ScraperPipeline(config)
asyncio.run(pipeline.run())
```

`ScraperConfig` is a frozen dataclass with 30+ fields covering search, fetch, extraction, Tor, and output settings. See the [User Guide, Configuration Reference](docs/user-guide.md#configuration-reference) for the full field table with types and defaults.

See [`docs/examples/`](docs/examples/) for more Python examples.

---

## CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--queries-file FILE` | *required* | Text file with one query per line |
| `--output FILE` | - | Explicit `.parquet` output path |
| `--output-dir DIR` | `.` | Base directory for timestamped output folders |
| `--jsonl` | off | Also write JSONL alongside Parquet |
| `--search-type {text,news}` | `text` | DuckDuckGo search mode |
| `--max-results N` | `20` | Results per query |
| `--timelimit {d,w,m,y}` | - | DDG time filter |
| `--region CODE` | `wt-wt` | DDG region code |
| `--backend {auto,api,html,lite}` | `auto` | DDG backend |
| `--proxy URL` | - | HTTP/SOCKS5 proxy for searches |
| `--use-tor` | off | Route fetches through Tor |
| `--tor-socks-port PORT` | `9150` | Tor SOCKS5 port |
| `--tor-control-port PORT` | `9051` | Tor control port |
| `--tor-password PASS` | - | Tor control password |
| `--tor-renew-every N` | `20` | Renew Tor circuit every N queries |
| `--concurrent N` | `10` | Max parallel fetches |
| `--per-domain N` | `3` | Max concurrent fetches per domain |
| `--timeout SECS` | `20` | Fetch timeout |
| `--stealth` | off | Low-profile mode (concurrency 4, delays 5-8s) |
| `--no-robots` | off | Skip robots.txt checking |
| `--min-words N` | `100` | Minimum word count to keep |
| `--target-language LANG` | - | ISO language filter (e.g. `en`) |
| `--no-favor-precision` | off | Disable trafilatura precision mode |
| `--date-from YYYY-MM-DD` | - | Keep pages after this date |
| `--date-to YYYY-MM-DD` | - | Keep pages before this date |
| `--resume` | off | Resume from last checkpoint |
| `--checkpoint FILE` | `.scraper_checkpoint.json` | Checkpoint file path |
| `--exclude-file FILE` | - | Domain exclusion list |

---

## Tips

- **Use `--search-type news`** for financial content, less rate-limited and more relevant than text search
- **Expect 30-50% fetch failures**, many financial sites block automated access (Cloudflare, paywalls). This is normal; the pipeline logs and continues
- **Niche queries returning 0 results?** Simplify your phrasing (e.g., `grain market` instead of `oats commodity analysis`)
- **Start small**, test with 5-10 queries before scaling to hundreds
- **Windows users:** If `financial-scraper` gives "Access is denied", use `python -m financial_scraper` instead
- **Tor users:** Start Tor Browser (port 9150) or the Tor daemon (port 9050) *before* running with `--use-tor`

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `Found 0 results` for all queries | DuckDuckGo rate limiting | Wait 5-10 min, enable `--use-tor` or `--stealth` |
| `ModuleNotFoundError` | Package not installed | Activate venv, run `pip install -e .` from `financial_scraper/` dir |
| High fetch failure (>60%) | Sites blocking scrapers | Use `--search-type news`, add domains to exclusion list |
| Empty Parquet output | Word count filter or date filter too strict | Try `--min-words 50`, broaden date range |

For detailed troubleshooting, see the [User Guide, Troubleshooting & FAQ](docs/user-guide.md#troubleshooting--faq).

---

## Architecture

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'primaryColor': '#4a90d9', 'lineColor': '#5c6bc0', 'fontSize': '14px'}}}%%
flowchart LR
    A([" 📄 Query File\n<i>one per line</i> "]):::blue
    B([" 🔍 DDG Search\n<i>text / news · retry</i> "]):::orange
    C([" 🌐 Async Fetch\n<i>aiohttp · throttle · Tor</i> "]):::purple
    D([" 📝 Extract\n<i>trafilatura · pdfplumber</i> "]):::green
    E([" 💾 Dedup + Store\n<i>Parquet · JSONL · checkpoint</i> "]):::red

    A --> B --> C --> D --> E

    classDef blue fill:#4a90d9,stroke:#2c5f8a,color:#fff,stroke-width:2px
    classDef orange fill:#f5a623,stroke:#c47d0e,color:#fff,stroke-width:2px
    classDef purple fill:#7b68ee,stroke:#5a4bc7,color:#fff,stroke-width:2px
    classDef green fill:#50c878,stroke:#3a9a5c,color:#fff,stroke-width:2px
    classDef red fill:#e74c3c,stroke:#c0392b,color:#fff,stroke-width:2px

    linkStyle 0 stroke:#5c6bc0,stroke-width:3px
    linkStyle 1 stroke:#5c6bc0,stroke-width:3px
    linkStyle 2 stroke:#5c6bc0,stroke-width:3px
    linkStyle 3 stroke:#5c6bc0,stroke-width:3px
```

The pipeline is modular, each stage is an independent module under `src/financial_scraper/`. See [`docs/architecture.md`](docs/architecture.md) for the full module map, data flow types, and design rationale.

---

## Documentation

- **[User Guide](docs/user-guide.md)**, detailed installation, configuration reference, scaling guide, and troubleshooting
- **[Architecture](docs/architecture.md)**, module map, data flow, and design rationale
- **[Ethical Scraping](docs/ethical-scraping.md)**, rate limiting strategy, robots.txt, best practices

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and PR workflow.

---

## License

[MIT](LICENSE)
