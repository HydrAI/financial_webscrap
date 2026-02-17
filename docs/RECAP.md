# Session Recap

## 2026-02-17

### Markdown output format (CLI + MCP)

Added markdown as a third output format alongside Parquet and JSONL.

**New file:**
- `src/financial_scraper/store/markdown.py` — `MarkdownWriter` class, `format_record_md()`, `format_records_md()` helpers

**Modified:**
- `config.py` — added `markdown_path` field
- `pipeline.py` — wired `MarkdownWriter` (mirrors JSONL pattern)
- `main.py` — added `--markdown` CLI flag, updated `_resolve_output_paths` to return markdown path
- `mcp/server.py` — added `_extract_cache` (populated by `extract` and `scrape` tools), new `export_markdown` MCP tool
- `tests/test_main.py` — updated for new 4-tuple return signature
- `tests/test_mcp_server.py` — added 4 tests for `export_markdown`

**New test file:**
- `tests/test_markdown.py` — 17 tests covering slug generation, single/combined formatting, writer I/O

**Result:** 241 tests passing. Commits: `adf1473`, `f71c749`, `0a918b4`.

### Documentation

- Added "Why use this instead of built-in web search?" cost comparison section to `docs/mcp-setup.md`
- Updated MCP link description in `README.md`

### Housekeeping

- Added `.coverage`, `.coverage.*`, `htmlcov/` to `.gitignore`
