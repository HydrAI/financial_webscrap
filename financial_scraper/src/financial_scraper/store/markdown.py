"""Markdown writer for combined and individual article output."""

import logging
import re
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def _slugify(text: str, max_len: int = 40) -> str:
    """Lowercase, replace non-alphanumeric with underscore, truncate."""
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    return slug[:max_len]


def _word_count(text: str) -> int:
    return len(text.split())


def format_record_md(record: dict, include_query: bool = True) -> str:
    """Format a single record dict as a standalone markdown article."""
    title = record.get("title") or "Untitled"
    source = record.get("source", "")
    url = record.get("link", "")
    date = record.get("date", "")
    text = record.get("full_text", "")
    query = record.get("company", "")
    words = _word_count(text) if text else 0

    lines = [f"# {title}", ""]

    # Metadata table
    lines.append("| | |")
    lines.append("|---|---|")
    if source:
        lines.append(f"| **Source** | {source} |")
    if url:
        lines.append(f"| **URL** | {url} |")
    if date:
        lines.append(f"| **Date** | {date} |")
    lines.append(f"| **Words** | {words:,} |")
    if include_query and query:
        lines.append(f"| **Query** | {query} |")

    lines.append("")
    if text:
        lines.append(text)
    lines.append("")

    return "\n".join(lines)


def format_records_md(records: list[dict]) -> str:
    """Format multiple records as a combined markdown document, grouped by query."""
    if not records:
        return ""

    # Gather stats
    queries = set()
    sources = set()
    for r in records:
        if r.get("company"):
            queries.add(r["company"])
        if r.get("source"):
            sources.add(r["source"])

    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        "# Financial Scraper Report",
        "",
        f"> {len(records)} articles \u00b7 {len(sources)} sources \u00b7 {today}",
        "",
    ]

    # Group by query (company field)
    from collections import OrderedDict
    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for r in records:
        key = r.get("company", "")
        groups.setdefault(key, []).append(r)

    for query, group in groups.items():
        lines.append("---")
        lines.append("")
        if query:
            lines.append(f"## {query}")
            lines.append("")

        for r in group:
            title = r.get("title") or "Untitled"
            source = r.get("source", "")
            url = r.get("link", "")
            date = r.get("date", "")
            text = r.get("full_text", "")
            words = _word_count(text) if text else 0

            lines.append(f"### {title}")
            lines.append("")
            lines.append("| | |")
            lines.append("|---|---|")
            if source:
                lines.append(f"| **Source** | {source} |")
            if url:
                lines.append(f"| **URL** | {url} |")
            if date:
                lines.append(f"| **Date** | {date} |")
            lines.append(f"| **Words** | {words:,} |")
            lines.append("")
            if text:
                lines.append(text)
            lines.append("")

    return "\n".join(lines)


class MarkdownWriter:
    """Writes combined .md file and individual per-article .md files."""

    def __init__(self, path: Path):
        self._path = Path(path)
        self._md_dir = self._path.parent / "markdown"
        self._counter: dict[str, int] = {}  # per-query slug counter

    def append(self, records: list[dict]) -> None:
        if not records:
            return

        # 1. Append to combined file
        block = format_records_md(records)
        self._path.parent.mkdir(parents=True, exist_ok=True)

        if self._path.exists():
            # Append just the article sections (skip repeated header)
            # Re-render as grouped sections without the top header
            with open(self._path, "a", encoding="utf-8") as f:
                f.write("\n" + block)
        else:
            with open(self._path, "w", encoding="utf-8") as f:
                f.write(block)

        logger.info(f"Wrote {len(records)} articles to {self._path}")

        # 2. Write individual files
        self._md_dir.mkdir(parents=True, exist_ok=True)
        for r in records:
            query = r.get("company", "unknown")
            slug = _slugify(query)
            self._counter[slug] = self._counter.get(slug, 0) + 1
            idx = self._counter[slug]
            filename = f"{slug}_{idx:03d}.md"
            filepath = self._md_dir / filename
            content = format_record_md(r, include_query=True)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)

        logger.info(f"Wrote {len(records)} individual files to {self._md_dir}")
