# MCP Server Setup

Use financial-scraper as a tool server for LLMs via the [Model Context Protocol (MCP)](https://modelcontextprotocol.io). The MCP server exposes the same scraping pipeline as the CLI, but over stdio so that AI assistants (Claude Desktop, etc.) can search, fetch, and extract financial content on demand.

---

## Installation

```bash
cd financial_scraper
pip install -e ".[mcp]"
```

This installs the `mcp` Python package alongside the existing scraper dependencies.

### Verify

```bash
# MCP entry point exists
financial-scraper-mcp --help

# Or via module
python -m financial_scraper.mcp
```

The server starts on stdio and waits for MCP messages — you won't see output in the terminal (that's normal). Press `Ctrl+C` to stop.

---

## Claude Desktop Configuration

Add the following to your Claude Desktop config file:

| OS | Config path |
|----|-------------|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |

```json
{
  "mcpServers": {
    "financial-scraper": {
      "command": "financial-scraper-mcp"
    }
  }
}
```

If you installed into a virtual environment, use the full path:

```json
{
  "mcpServers": {
    "financial-scraper": {
      "command": "/path/to/venv/bin/financial-scraper-mcp"
    }
  }
}
```

Restart Claude Desktop after editing the config.

---

## Tools Reference

### `search`

Search DuckDuckGo for financial content.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | string | *required* | Search query |
| `max_results` | int | 10 | Maximum results to return |
| `search_type` | string | `"text"` | `"text"` or `"news"` |
| `region` | string | `"wt-wt"` | DuckDuckGo region code |
| `timelimit` | string | null | Time filter: `"d"`, `"w"`, `"m"`, `"y"` |

**Returns:** Array of `{url, title, snippet, search_rank, query}`.

---

### `fetch`

Fetch URLs with ethical rate limiting, browser fingerprints, and robots.txt compliance. Results are cached in memory for `extract` to use.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `urls` | array[string] | *required* | URLs to fetch |
| `timeout` | int | 20 | Fetch timeout in seconds |
| `respect_robots` | bool | true | Check robots.txt before fetching |

**Returns:** Array of `{url, status, content_type, error, has_html, has_pdf_bytes, html_length}`.

Raw HTML is not returned (too large). Call `extract` to get clean text.

---

### `extract`

Extract clean text from previously fetched content. **You must call `fetch` first.**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `urls` | array[string] | *required* | URLs to extract (must be in fetch cache) |
| `min_word_count` | int | 100 | Minimum words to keep an article |

**Returns:** Array of `{url, title, author, date, text, word_count, extraction_method, error}`.

---

### `scrape`

Convenience tool that chains search, fetch, and extract in one call.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | string | *required* | Search query |
| `max_results` | int | 10 | Max search results |
| `search_type` | string | `"text"` | `"text"` or `"news"` |
| `min_word_count` | int | 100 | Minimum words per article |
| `region` | string | `"wt-wt"` | DuckDuckGo region |
| `timelimit` | string | null | Time filter |

**Returns:** `{query, results_found, articles_extracted, articles: [{url, title, author, date, word_count, snippet, full_text}]}`.

---

### `read_output`

Read a Parquet file from a previous CLI scrape run.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `file_path` | string | *required* | Path to `.parquet` file |
| `limit` | int | 50 | Max rows to return |

**Returns:** `{total_rows, returned_rows, columns, rows: [...]}`.

---

## Typical Workflows

### Quick research (one-shot)

Ask the LLM: *"Search for recent news about crude oil futures and give me a summary."*

The LLM will call `scrape` with `search_type="news"`, get full article texts, and summarize.

### Step-by-step (more control)

1. `search` — find URLs
2. `fetch` — download pages (LLM can inspect status codes, filter)
3. `extract` — pull clean text from successful fetches

### Analyze past runs

Ask the LLM: *"Read the Parquet file at runs/20260215_235519/scrape_20260215_235519.parquet and show me the top 10 articles by word count."*

The LLM calls `read_output` and processes the data.

---

## Notes

- The MCP server is a long-lived stdio process. Fetch results are cached in memory (up to 500 URLs, LRU eviction) so that `extract` can access them without re-fetching.
- Content deduplication is active within a session — duplicate articles are filtered automatically.
- The existing CLI (`financial-scraper`) is completely unaffected. Both entry points use the same underlying classes.
- Search calls are synchronous (DuckDuckGo client limitation) but wrapped in `asyncio.to_thread()` so the MCP event loop stays responsive.
