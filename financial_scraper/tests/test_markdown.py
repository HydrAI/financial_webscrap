"""Tests for financial_scraper.store.markdown."""

from pathlib import Path

from financial_scraper.store.markdown import (
    MarkdownWriter,
    format_record_md,
    format_records_md,
    _slugify,
)


def _make_record(**overrides):
    base = {
        "company": "oil futures",
        "title": "Oil Market Update",
        "link": "https://example.com/oil",
        "snippet": "Snippet about oil.",
        "date": "2025-06-15",
        "source": "reuters.com",
        "full_text": "Full article about oil markets and trading.",
        "source_file": "oil_futures_ddgtext_2025Q2.parquet",
    }
    base.update(overrides)
    return base


class TestSlugify:
    def test_basic(self):
        assert _slugify("Hello World") == "hello_world"

    def test_special_chars(self):
        assert _slugify("crude oil / futures (2025)") == "crude_oil_futures_2025"

    def test_truncation(self):
        result = _slugify("a" * 100, max_len=10)
        assert len(result) == 10

    def test_empty(self):
        assert _slugify("") == ""


class TestFormatRecordMd:
    def test_contains_title(self):
        md = format_record_md(_make_record())
        assert "# Oil Market Update" in md

    def test_contains_metadata_table(self):
        md = format_record_md(_make_record())
        assert "| **Source** | reuters.com |" in md
        assert "| **URL** | https://example.com/oil |" in md
        assert "| **Date** | 2025-06-15 |" in md

    def test_contains_word_count(self):
        md = format_record_md(_make_record())
        assert "**Words**" in md

    def test_contains_full_text(self):
        md = format_record_md(_make_record())
        assert "Full article about oil markets" in md

    def test_includes_query_by_default(self):
        md = format_record_md(_make_record())
        assert "| **Query** | oil futures |" in md

    def test_excludes_query_when_disabled(self):
        md = format_record_md(_make_record(), include_query=False)
        assert "**Query**" not in md

    def test_untitled_fallback(self):
        md = format_record_md(_make_record(title=""))
        assert "# Untitled" in md


class TestFormatRecordsMd:
    def test_header(self):
        md = format_records_md([_make_record()])
        assert "# Financial Scraper Report" in md

    def test_article_count_in_summary(self):
        records = [_make_record(), _make_record(title="Second")]
        md = format_records_md(records)
        assert "2 articles" in md

    def test_groups_by_query(self):
        records = [
            _make_record(company="oil futures", title="Oil A"),
            _make_record(company="gold price", title="Gold A"),
        ]
        md = format_records_md(records)
        assert "## oil futures" in md
        assert "## gold price" in md
        assert "### Oil A" in md
        assert "### Gold A" in md

    def test_empty_records(self):
        assert format_records_md([]) == ""

    def test_contains_full_text(self):
        md = format_records_md([_make_record()])
        assert "Full article about oil markets" in md


class TestMarkdownWriter:
    def test_creates_combined_file(self, tmp_path):
        md_path = tmp_path / "report.md"
        w = MarkdownWriter(md_path)
        w.append([_make_record()])

        assert md_path.exists()
        content = md_path.read_text(encoding="utf-8")
        assert "Oil Market Update" in content

    def test_creates_individual_files(self, tmp_path):
        md_path = tmp_path / "report.md"
        w = MarkdownWriter(md_path)
        w.append([_make_record()])

        md_dir = tmp_path / "markdown"
        assert md_dir.exists()
        files = list(md_dir.glob("*.md"))
        assert len(files) == 1
        assert "oil_futures_001.md" == files[0].name

    def test_individual_file_content(self, tmp_path):
        md_path = tmp_path / "report.md"
        w = MarkdownWriter(md_path)
        w.append([_make_record()])

        individual = (tmp_path / "markdown" / "oil_futures_001.md").read_text(encoding="utf-8")
        assert "# Oil Market Update" in individual
        assert "| **Query** | oil futures |" in individual

    def test_slugified_names_with_counter(self, tmp_path):
        md_path = tmp_path / "report.md"
        w = MarkdownWriter(md_path)
        w.append([
            _make_record(title="Article One"),
            _make_record(title="Article Two"),
        ])

        md_dir = tmp_path / "markdown"
        names = sorted(f.name for f in md_dir.glob("*.md"))
        assert names == ["oil_futures_001.md", "oil_futures_002.md"]

    def test_empty_records_is_noop(self, tmp_path):
        md_path = tmp_path / "report.md"
        w = MarkdownWriter(md_path)
        w.append([])

        assert not md_path.exists()
        assert not (tmp_path / "markdown").exists()

    def test_multiple_appends(self, tmp_path):
        md_path = tmp_path / "report.md"
        w = MarkdownWriter(md_path)
        w.append([_make_record(title="First")])
        w.append([_make_record(title="Second")])

        content = md_path.read_text(encoding="utf-8")
        assert "First" in content
        assert "Second" in content

        md_dir = tmp_path / "markdown"
        assert len(list(md_dir.glob("*.md"))) == 2
