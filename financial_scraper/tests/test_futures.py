"""Tests for the futures subcommand."""

import json
import os
from pathlib import Path

import pytest

from financial_scraper.futures.model import FuturesContract
from financial_scraper.futures.config import FuturesConfig
from financial_scraper.futures.lme_fetcher import LmeFetcher

# Path to real LME HTML fixtures from the commodities crawl
_FIXTURES_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "commodities_crawl" / "exchanges" / "20260409_115652" / "html"
)

# Steel Rebar FOB Turkey (Platts) — 204e8492
_LME_REBAR_HTML = _FIXTURES_DIR / "204e8492_contract-specifications.html"
# Steel Scrap CFR Turkey (Platts) — 276606c8
_LME_SCRAP_HTML = _FIXTURES_DIR / "276606c8_contract-specifications.html"
# Cobalt (Fastmarkets MB) — bdf44e3a
_LME_COBALT_HTML = _FIXTURES_DIR / "bdf44e3a_contract-specifications.html"
# Lithium Hydroxide CIF (Fastmarkets MB) — cee8ea65
_LME_LITHIUM_HTML = _FIXTURES_DIR / "cee8ea65_contract-specifications.html"


class TestFuturesContract:
    def test_defaults(self):
        c = FuturesContract()
        assert c.exchange == ""
        assert c.extra_specs == {}
        assert c.error == ""

    def test_fields(self):
        c = FuturesContract(
            exchange="LME",
            product_name="Copper",
            ticker="CA",
            asset_class="metals",
        )
        assert c.exchange == "LME"
        assert c.ticker == "CA"


class TestFuturesConfig:
    def test_frozen(self):
        cfg = FuturesConfig()
        with pytest.raises(AttributeError):
            cfg.delay = 5.0  # type: ignore[misc]

    def test_defaults(self):
        cfg = FuturesConfig()
        assert cfg.exchanges == ("lme", "cme", "ice")
        assert cfg.delay == 3.0
        assert cfg.resume is False


class TestLmeFetcher:
    """Test LME parser against real crawled HTML fixtures."""

    @pytest.fixture()
    def fetcher(self):
        return LmeFetcher()

    @pytest.mark.skipif(
        not _LME_REBAR_HTML.exists(),
        reason="LME fixture not found on disk",
    )
    def test_parse_steel_rebar(self, fetcher):
        html = _LME_REBAR_HTML.read_text(encoding="utf-8")
        url = "https://www.lme.com/metals/ferrous/lme-steel-rebar-fob-turkey-platts/contract-specifications"

        c = fetcher.parse_contract(html, url)

        assert c.exchange == "LME"
        assert c.ticker == "SR"
        assert c.settlement_type == "Cash settled"
        assert c.contract_size == "10 tonnes"
        assert "US dollars" in c.quote_currency
        assert c.asset_class == "metals"
        assert c.source_url == url
        assert c.error == ""
        # Trading months should mention "15 months"
        assert "15 months" in c.trading_months.lower() or "15" in c.trading_months

    @pytest.mark.skipif(
        not _LME_SCRAP_HTML.exists(),
        reason="LME fixture not found on disk",
    )
    def test_parse_steel_scrap(self, fetcher):
        html = _LME_SCRAP_HTML.read_text(encoding="utf-8")
        url = "https://www.lme.com/metals/ferrous/lme-steel-scrap-cfr-turkey-platts/contract-specifications"

        c = fetcher.parse_contract(html, url)

        assert c.exchange == "LME"
        assert c.ticker  # should have a contract code
        assert c.asset_class == "metals"
        assert c.contract_size  # should have lot size

    @pytest.mark.skipif(
        not _LME_COBALT_HTML.exists(),
        reason="LME fixture not found on disk",
    )
    def test_parse_cobalt(self, fetcher):
        html = _LME_COBALT_HTML.read_text(encoding="utf-8")
        url = "https://www.lme.com/metals/ev/lme-cobalt-fastmarkets-mb/contract-specifications"

        c = fetcher.parse_contract(html, url)

        assert c.exchange == "LME"
        assert c.asset_class == "metals"
        assert c.ticker

    @pytest.mark.skipif(
        not _LME_LITHIUM_HTML.exists(),
        reason="LME fixture not found on disk",
    )
    def test_parse_lithium(self, fetcher):
        html = _LME_LITHIUM_HTML.read_text(encoding="utf-8")
        url = "https://www.lme.com/metals/ev/lme-lithium-hydroxide-cif-fastmarkets-mb/contract-specifications"

        c = fetcher.parse_contract(html, url)

        assert c.exchange == "LME"
        assert c.asset_class == "metals"

    @pytest.mark.skipif(
        not _LME_REBAR_HTML.exists(),
        reason="LME fixture not found on disk",
    )
    def test_full_text_json_roundtrip(self, fetcher):
        """Verify the contract can be serialized to JSON (for full_text column)."""
        html = _LME_REBAR_HTML.read_text(encoding="utf-8")
        url = "https://www.lme.com/metals/ferrous/lme-steel-rebar-fob-turkey-platts/contract-specifications"

        c = fetcher.parse_contract(html, url)
        serialized = json.dumps(c.__dict__, ensure_ascii=False, default=str)
        roundtrip = json.loads(serialized)

        assert roundtrip["exchange"] == "LME"
        assert roundtrip["ticker"] == "SR"


class TestLmeFetcherAllFixtures:
    """Smoke-test: parse all 10 LME fixtures without errors."""

    @pytest.mark.skipif(
        not _FIXTURES_DIR.exists(),
        reason="Fixtures directory not found",
    )
    def test_parse_all_lme_fixtures(self):
        fetcher = LmeFetcher()
        fixtures = list(_FIXTURES_DIR.glob("*_contract-specifications.html"))
        assert len(fixtures) >= 10, f"Expected >=10 fixtures, found {len(fixtures)}"

        for fixture in fixtures:
            html = fixture.read_text(encoding="utf-8")
            # Extract canonical URL from the HTML
            import re
            match = re.search(r'<link rel="canonical" href="([^"]+)"', html)
            url = match.group(1) if match else f"https://www.lme.com/unknown/{fixture.stem}"

            c = fetcher.parse_contract(html, url)

            assert c.exchange == "LME", f"Failed for {fixture.name}"
            assert c.ticker, f"No ticker for {fixture.name}"
            assert c.contract_size, f"No contract_size for {fixture.name}"
            assert c.asset_class == "metals", f"Wrong asset_class for {fixture.name}"
