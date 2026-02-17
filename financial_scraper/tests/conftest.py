"""Shared fixtures for financial_scraper tests."""

import pytest

from financial_scraper.config import ScraperConfig


@pytest.fixture
def sample_config() -> ScraperConfig:
    """ScraperConfig with test-friendly defaults."""
    return ScraperConfig(
        max_results_per_query=5,
        search_delay_min=0.0,
        search_delay_max=0.0,
        fetch_timeout=5,
        min_word_count=10,
    )


@pytest.fixture
def tmp_parquet(tmp_path):
    return tmp_path / "test_output.parquet"


@pytest.fixture
def tmp_jsonl(tmp_path):
    return tmp_path / "test_output.jsonl"
