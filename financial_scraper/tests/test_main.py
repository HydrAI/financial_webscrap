"""Tests for financial_scraper.main."""

import argparse
from pathlib import Path
from unittest.mock import patch

from financial_scraper.main import _resolve_output_paths, build_config


def _make_args(**overrides):
    defaults = {
        "queries_file": "queries.txt",
        "output": None,
        "output_dir": None,
        "max_results": 20,
        "search_type": "text",
        "timelimit": None,
        "region": "wt-wt",
        "backend": "auto",
        "proxy": None,
        "use_tor": False,
        "tor_socks_port": 9150,
        "tor_control_port": 9051,
        "tor_password": "",
        "tor_renew_every": 20,
        "concurrent": 10,
        "per_domain": 3,
        "timeout": 20,
        "stealth": False,
        "no_robots": False,
        "min_words": 100,
        "target_language": None,
        "no_favor_precision": False,
        "date_from": None,
        "date_to": None,
        "jsonl": False,
        "markdown": False,
        "exclude_file": None,
        "checkpoint": ".scraper_checkpoint.json",
        "resume": False,
        "reset_queries": False,
        "crawl": False,
        "crawl_depth": 2,
        "max_pages_per_domain": 50,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


class TestResolveOutputPaths:
    def test_explicit_parquet_path(self, tmp_path):
        out_file = str(tmp_path / "my_output.parquet")
        args = _make_args(output=out_file, jsonl=False)
        out_dir, out_path, jsonl_path, _ = _resolve_output_paths(args)
        assert out_path == Path(out_file)
        assert out_dir == Path(out_file).parent
        assert jsonl_path is None

    def test_explicit_parquet_with_jsonl(self, tmp_path):
        out_file = str(tmp_path / "out.parquet")
        jsonl_file = str(tmp_path / "out.jsonl")
        args = _make_args(output=out_file, jsonl=jsonl_file)
        _, _, jsonl_path, _ = _resolve_output_paths(args)
        assert jsonl_path == Path(jsonl_file)

    def test_timestamped_folder(self, tmp_path):
        args = _make_args(output_dir=str(tmp_path))
        out_dir, out_path, jsonl_path, _ = _resolve_output_paths(args)
        assert out_dir.parent == tmp_path
        assert out_path.suffix == ".parquet"
        assert "scrape_" in out_path.name

    def test_timestamped_folder_with_jsonl(self, tmp_path):
        args = _make_args(output_dir=str(tmp_path), jsonl=True)
        _, _, jsonl_path, _ = _resolve_output_paths(args)
        assert jsonl_path is not None
        assert jsonl_path.suffix == ".jsonl"

    def test_default_cwd_when_no_output_args(self, tmp_path):
        args = _make_args()
        out_dir, out_path, jsonl_path, _ = _resolve_output_paths(args)
        assert out_path.suffix == ".parquet"


class TestBuildConfig:
    def test_basic_config(self, tmp_path):
        out_file = str(tmp_path / "out.parquet")
        args = _make_args(output=out_file, max_results=15, stealth=True)
        cfg = build_config(args)
        assert cfg.max_results_per_query == 15
        assert cfg.stealth is True
        assert cfg.output_path == Path(out_file)

    def test_tor_config(self, tmp_path):
        out_file = str(tmp_path / "out.parquet")
        args = _make_args(output=out_file, use_tor=True, tor_password="secret")
        cfg = build_config(args)
        assert cfg.use_tor is True
        assert cfg.tor_password == "secret"

    def test_no_robots_flag(self, tmp_path):
        out_file = str(tmp_path / "out.parquet")
        args = _make_args(output=out_file, no_robots=True)
        cfg = build_config(args)
        assert cfg.respect_robots is False

    def test_date_filters(self, tmp_path):
        out_file = str(tmp_path / "out.parquet")
        args = _make_args(output=out_file, date_from="2024-01-01", date_to="2024-12-31")
        cfg = build_config(args)
        assert cfg.date_from == "2024-01-01"
        assert cfg.date_to == "2024-12-31"

    def test_exclude_file(self, tmp_path):
        out_file = str(tmp_path / "out.parquet")
        args = _make_args(output=out_file, exclude_file="exclude.txt")
        cfg = build_config(args)
        assert cfg.exclude_file == Path("exclude.txt")

    def test_no_favor_precision(self, tmp_path):
        out_file = str(tmp_path / "out.parquet")
        args = _make_args(output=out_file, no_favor_precision=True)
        cfg = build_config(args)
        assert cfg.favor_precision is False

    def test_crawl_flags(self, tmp_path):
        out_file = str(tmp_path / "out.parquet")
        args = _make_args(
            output=out_file, crawl=True, crawl_depth=3, max_pages_per_domain=25,
        )
        cfg = build_config(args)
        assert cfg.crawl is True
        assert cfg.crawl_depth == 3
        assert cfg.max_pages_per_domain == 25

    def test_reset_queries_flag(self, tmp_path):
        out_file = str(tmp_path / "out.parquet")
        args = _make_args(output=out_file, reset_queries=True)
        cfg = build_config(args)
        assert cfg.reset_queries is True

    def test_reset_queries_default_false(self, tmp_path):
        out_file = str(tmp_path / "out.parquet")
        args = _make_args(output=out_file)
        cfg = build_config(args)
        assert cfg.reset_queries is False
