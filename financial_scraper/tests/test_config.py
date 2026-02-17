"""Tests for financial_scraper.config."""

import dataclasses

import pytest

from financial_scraper.config import ScraperConfig, apply_stealth


class TestScraperConfigDefaults:
    def test_default_max_results(self):
        cfg = ScraperConfig()
        assert cfg.max_results_per_query == 20

    def test_default_search_type(self):
        cfg = ScraperConfig()
        assert cfg.search_type == "text"

    def test_default_stealth_off(self):
        cfg = ScraperConfig()
        assert cfg.stealth is False

    def test_default_respect_robots(self):
        cfg = ScraperConfig()
        assert cfg.respect_robots is True

    def test_default_min_word_count(self):
        cfg = ScraperConfig()
        assert cfg.min_word_count == 100


class TestApplyStealth:
    def test_stealth_false_returns_unchanged(self):
        cfg = ScraperConfig(stealth=False, max_concurrent_total=10)
        result = apply_stealth(cfg)
        assert result is cfg  # same object

    def test_stealth_true_overrides_concurrency(self):
        cfg = ScraperConfig(stealth=True, max_concurrent_total=10)
        result = apply_stealth(cfg)
        assert result.max_concurrent_total == 4
        assert result.max_concurrent_per_domain == 2

    def test_stealth_true_overrides_delays(self):
        cfg = ScraperConfig(stealth=True)
        result = apply_stealth(cfg)
        assert result.search_delay_min == 5.0
        assert result.search_delay_max == 8.0

    def test_stealth_preserves_other_fields(self):
        cfg = ScraperConfig(stealth=True, ddg_region="us-en", min_word_count=50)
        result = apply_stealth(cfg)
        assert result.ddg_region == "us-en"
        assert result.min_word_count == 50

    def test_stealth_returns_new_instance(self):
        cfg = ScraperConfig(stealth=True)
        result = apply_stealth(cfg)
        assert result is not cfg


class TestFrozenDataclass:
    def test_reject_mutation(self):
        cfg = ScraperConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.max_results_per_query = 99
