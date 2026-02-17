# TODO — 2026-02-18

## High priority

- [ ] **Live test markdown output** — run CLI with `--markdown --max-results 3` against real queries, verify combined `.md` and individual `markdown/*.md` files look correct
- [ ] **Live test `export_markdown` MCP tool** — run `_test_mcp_live.py export_markdown` end-to-end
- [ ] **Update CHANGELOG** — add v0.2.0 entry covering MCP server and markdown output

## Medium priority

- [ ] **Document `--markdown` flag** in `docs/user-guide.md` (CLI reference section)
- [ ] **Document `export_markdown` tool** in `docs/mcp-setup.md` (tool listing)
- [ ] **Add markdown output tests to pipeline integration tests** — `test_pipeline.py` currently only checks parquet/JSONL writes; add a case with `markdown_path` set
- [ ] **Consider `--all-formats` convenience flag** — shorthand for `--jsonl --markdown`

## Low priority / ideas

- [ ] **Markdown export for `read_output`** — allow converting an existing parquet file to markdown via MCP (read parquet → format_records_md)
- [ ] **Configurable markdown template** — let users customize the report header, metadata table fields, or grouping strategy
- [ ] **Coverage target** — current suite is at 89%; aim for 92%+ with the new markdown paths included
- [ ] **CI pipeline** — set up GitHub Actions for automated `pytest` on push
