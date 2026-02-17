"""Tests for financial_scraper.store.output."""

import json
from unittest.mock import patch

import pandas as pd
import pyarrow.parquet as pq

from financial_scraper.store.output import (
    SCHEMA,
    JSONLWriter,
    ParquetWriter,
    _parse_date,
    make_source_file_tag,
)


class TestParseDate:
    def test_iso_datetime(self):
        result = _parse_date("2024-06-15T14:30:00")
        assert result == pd.Timestamp("2024-06-15 14:30:00")

    def test_date_only(self):
        result = _parse_date("2024-06-15")
        assert result == pd.Timestamp("2024-06-15")

    def test_year_month(self):
        result = _parse_date("2024-06")
        assert result == pd.Timestamp("2024-06-01")

    def test_year_only(self):
        result = _parse_date("2024")
        assert result == pd.Timestamp("2024-01-01")

    def test_none_input(self):
        assert _parse_date(None) is None

    def test_unparseable(self):
        assert _parse_date("not-a-date") is None


class TestMakeSourceFileTag:
    def test_slug_generation(self):
        tag = make_source_file_tag("My Query Here", "2024-03-15", "text")
        assert tag.startswith("my_query_here_")
        assert "ddgtext" in tag

    def test_quarter_from_date(self):
        tag = make_source_file_tag("q", "2024-03-15", "text")
        assert "2024Q1" in tag

    def test_quarter_q2(self):
        tag = make_source_file_tag("q", "2024-06-01", "text")
        assert "2024Q2" in tag

    def test_news_search_type(self):
        tag = make_source_file_tag("q", "2024-01-01", "news")
        assert "ddgnews" in tag

    def test_text_search_type(self):
        tag = make_source_file_tag("q", "2024-01-01", "text")
        assert "ddgtext" in tag

    @patch("financial_scraper.store.output._current_quarter_tag", return_value="2026Q1")
    def test_no_date_uses_current_quarter(self, mock_qt):
        tag = make_source_file_tag("q", None, "text")
        assert "2026Q1" in tag

    def test_ends_with_parquet(self):
        tag = make_source_file_tag("q", "2024-01-01", "text")
        assert tag.endswith(".parquet")


def _make_record(**overrides):
    base = {
        "company": "TestCorp",
        "title": "Test Article",
        "link": "https://example.com/article",
        "snippet": "A test snippet.",
        "date": "2024-06-15",
        "source": "ddg",
        "full_text": "Full article text here with enough words.",
        "source_file": "test_ddgtext_2024Q2.parquet",
    }
    base.update(overrides)
    return base


class TestParquetWriter:
    def test_creates_new_file(self, tmp_parquet):
        w = ParquetWriter(tmp_parquet)
        w.append([_make_record()])
        assert tmp_parquet.exists()
        table = pq.read_table(tmp_parquet)
        assert len(table) == 1

    def test_appends_to_existing(self, tmp_parquet):
        w = ParquetWriter(tmp_parquet)
        w.append([_make_record(title="First")])
        w.append([_make_record(title="Second")])
        table = pq.read_table(tmp_parquet)
        assert len(table) == 2

    def test_empty_list_is_noop(self, tmp_parquet):
        w = ParquetWriter(tmp_parquet)
        w.append([])
        assert not tmp_parquet.exists()

    def test_schema_columns(self, tmp_parquet):
        w = ParquetWriter(tmp_parquet)
        w.append([_make_record()])
        table = pq.read_table(tmp_parquet)
        expected_cols = [f.name for f in SCHEMA]
        assert table.column_names == expected_cols


class TestJSONLWriter:
    def test_creates_file(self, tmp_jsonl):
        w = JSONLWriter(tmp_jsonl)
        w.append([_make_record()])
        assert tmp_jsonl.exists()
        lines = tmp_jsonl.read_text().strip().split("\n")
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["company"] == "TestCorp"

    def test_appends(self, tmp_jsonl):
        w = JSONLWriter(tmp_jsonl)
        w.append([_make_record(title="First")])
        w.append([_make_record(title="Second")])
        lines = tmp_jsonl.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_empty_list_is_noop(self, tmp_jsonl):
        w = JSONLWriter(tmp_jsonl)
        w.append([])
        assert not tmp_jsonl.exists()
