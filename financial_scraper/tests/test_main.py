"""Tests for financial_scraper.main."""

import argparse
import sys
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, AsyncMock

from financial_scraper.main import (
    _resolve_output_paths, _resolve_exclude_file,
    build_config, build_crawl_config, build_transcript_config,
    _add_search_args, _add_crawl_args, _add_transcript_args,
    _run_search, _run_crawl, _run_transcripts,
    main,
)


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
        "all_formats": False,
        "no_exclude": False,
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

    def test_markdown_flag(self, tmp_path):
        args = _make_args(output_dir=str(tmp_path), markdown=True)
        _, _, jsonl_path, markdown_path = _resolve_output_paths(args)
        assert markdown_path is not None
        assert markdown_path.suffix == ".md"
        assert jsonl_path is None

    def test_all_formats_flag(self, tmp_path):
        args = _make_args(output_dir=str(tmp_path), all_formats=True)
        _, _, jsonl_path, markdown_path = _resolve_output_paths(args)
        assert jsonl_path is not None
        assert jsonl_path.suffix == ".jsonl"
        assert markdown_path is not None
        assert markdown_path.suffix == ".md"


class TestResolveExcludeFile:
    def test_no_exclude_returns_none(self, tmp_path):
        args = _make_args(no_exclude=True, exclude_file=None)
        assert _resolve_exclude_file(args) is None

    def test_explicit_exclude_file(self, tmp_path):
        ef = tmp_path / "exclude.txt"
        ef.write_text("example.com\n")
        args = _make_args(exclude_file=str(ef))
        result = _resolve_exclude_file(args)
        assert result == Path(str(ef))

    def test_no_exclude_file_and_default_missing(self, tmp_path):
        args = _make_args(exclude_file=None)
        # Default file doesn't exist in tmp env, returns None or default
        result = _resolve_exclude_file(args)
        # Either None (if default missing) or a Path (if default found)
        assert result is None or isinstance(result, Path)


def _make_crawl_args(**overrides):
    defaults = {
        "urls_file": "urls.txt",
        "output_dir": None,
        "max_depth": 2,
        "max_pages": 50,
        "semaphore_count": 2,
        "min_words": 100,
        "target_language": None,
        "no_favor_precision": False,
        "date_from": None,
        "date_to": None,
        "jsonl": False,
        "markdown": False,
        "all_formats": False,
        "no_exclude": False,
        "exclude_file": None,
        "checkpoint": ".crawl_checkpoint.json",
        "resume": False,
        "pdf_extractor": "auto",
        "no_robots": False,
        "stealth": False,
    }
    defaults.update(overrides)
    import argparse
    return argparse.Namespace(**defaults)


class TestBuildCrawlConfig:
    def test_basic_crawl_config(self, tmp_path):
        args = _make_crawl_args(output_dir=str(tmp_path))
        cfg = build_crawl_config(args)
        assert cfg.max_depth == 2
        assert cfg.max_pages == 50
        assert cfg.pdf_extractor == "auto"

    def test_crawl_config_with_jsonl(self, tmp_path):
        args = _make_crawl_args(output_dir=str(tmp_path), jsonl=True)
        cfg = build_crawl_config(args)
        assert cfg.jsonl_path is not None

    def test_crawl_config_all_formats(self, tmp_path):
        args = _make_crawl_args(output_dir=str(tmp_path), all_formats=True)
        cfg = build_crawl_config(args)
        assert cfg.jsonl_path is not None
        assert cfg.markdown_path is not None


def _make_transcript_args(**overrides):
    defaults = {
        "tickers": ["AAPL"],
        "tickers_file": None,
        "year": 2025,
        "quarters": None,
        "concurrent": 5,
        "output_dir": None,
        "jsonl": False,
        "checkpoint": ".transcript_checkpoint.json",
        "resume": False,
    }
    defaults.update(overrides)
    import argparse
    return argparse.Namespace(**defaults)


class TestBuildTranscriptConfig:
    def test_basic_transcript_config(self, tmp_path):
        args = _make_transcript_args(output_dir=str(tmp_path))
        cfg = build_transcript_config(args)
        assert cfg.tickers == ("AAPL",)
        assert cfg.year == 2025
        assert cfg.jsonl_path is None

    def test_transcript_config_with_jsonl(self, tmp_path):
        args = _make_transcript_args(output_dir=str(tmp_path), jsonl=True)
        cfg = build_transcript_config(args)
        assert cfg.jsonl_path is not None

    def test_transcript_config_tickers_uppercased(self, tmp_path):
        args = _make_transcript_args(output_dir=str(tmp_path), tickers=["aapl", "msft"])
        cfg = build_transcript_config(args)
        assert "AAPL" in cfg.tickers
        assert "MSFT" in cfg.tickers


class TestAddArgsFunctions:
    def test_add_search_args_registers_required(self):
        p = argparse.ArgumentParser()
        _add_search_args(p)
        args = p.parse_args(["--queries-file", "q.txt"])
        assert args.queries_file == "q.txt"
        assert args.max_results == 20
        assert args.search_type == "text"
        assert args.jsonl is False
        assert args.markdown is False
        assert args.all_formats is False

    def test_add_crawl_args_registers_required(self):
        p = argparse.ArgumentParser()
        _add_crawl_args(p)
        args = p.parse_args(["--urls-file", "urls.txt"])
        assert args.urls_file == "urls.txt"
        assert args.max_depth == 2
        assert args.pdf_extractor == "auto"

    def test_add_transcript_args_registers_defaults(self):
        p = argparse.ArgumentParser()
        _add_transcript_args(p)
        args = p.parse_args(["--tickers", "AAPL"])
        assert args.tickers == ["AAPL"]
        assert args.concurrent == 5
        assert args.jsonl is False


class TestRunFunctions:
    def test_run_search_calls_pipeline(self, tmp_path):
        qf = tmp_path / "q.txt"
        qf.write_text("test query\n")
        args = _make_args(output_dir=str(tmp_path), queries_file=str(qf))
        args.reset = False

        with patch("financial_scraper.pipeline.ScraperPipeline") as MockPipeline:
            MockPipeline.return_value = MagicMock()
            with patch("financial_scraper.main.asyncio.run"):
                _run_search(args)
        MockPipeline.assert_called_once()

    def test_run_search_with_reset(self, tmp_path):
        qf = tmp_path / "q.txt"
        qf.write_text("test\n")
        cp = tmp_path / "cp.json"
        cp.write_text("{}")
        args = _make_args(output_dir=str(tmp_path), queries_file=str(qf),
                          checkpoint=str(cp))
        args.reset = True

        with patch("financial_scraper.pipeline.ScraperPipeline") as MockPipeline:
            MockPipeline.return_value = MagicMock()
            with patch("financial_scraper.main.asyncio.run"):
                _run_search(args)
        assert not cp.exists()

    def test_run_crawl_calls_pipeline(self, tmp_path):
        uf = tmp_path / "urls.txt"
        uf.write_text("https://example.com\n")
        args = _make_crawl_args(output_dir=str(tmp_path), urls_file=str(uf))
        args.reset = False

        with patch("financial_scraper.crawl.pipeline.CrawlPipeline") as MockPipeline:
            MockPipeline.return_value = MagicMock()
            with patch("financial_scraper.main.asyncio.run"):
                _run_crawl(args)
        MockPipeline.assert_called_once()

    def test_run_transcripts_no_tickers_exits(self, tmp_path):
        args = _make_transcript_args(tickers=None, tickers_file=None,
                                     output_dir=str(tmp_path))
        args.reset = False
        with pytest.raises(SystemExit):
            _run_transcripts(args)

    def test_run_transcripts_calls_pipeline(self, tmp_path):
        args = _make_transcript_args(output_dir=str(tmp_path))
        args.reset = False
        with patch("financial_scraper.transcripts.pipeline.TranscriptPipeline") as MockPipeline:
            MockPipeline.return_value = MagicMock()
            _run_transcripts(args)
        MockPipeline.assert_called_once()


class TestMain:
    def test_main_no_args_prints_help(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["financial-scraper"])
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1

    def test_main_search_subcommand(self, tmp_path, monkeypatch):
        qf = tmp_path / "q.txt"
        qf.write_text("test\n")
        monkeypatch.setattr(sys, "argv", [
            "financial-scraper", "search",
            "--queries-file", str(qf),
            "--output-dir", str(tmp_path),
            "--max-results", "1",
        ])
        with patch("financial_scraper.pipeline.ScraperPipeline") as MockPipeline:
            MockPipeline.return_value = MagicMock()
            with patch("financial_scraper.main.asyncio.run"):
                main()
        MockPipeline.assert_called_once()

    def test_main_backward_compat_no_subcommand(self, tmp_path, monkeypatch):
        qf = tmp_path / "q.txt"
        qf.write_text("test\n")
        monkeypatch.setattr(sys, "argv", [
            "financial-scraper",
            "--queries-file", str(qf),
            "--output-dir", str(tmp_path),
        ])
        with patch("financial_scraper.pipeline.ScraperPipeline") as MockPipeline:
            MockPipeline.return_value = MagicMock()
            with patch("financial_scraper.main.asyncio.run"):
                main()
        MockPipeline.assert_called_once()
