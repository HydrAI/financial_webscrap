"""CME Group contract-specification fetcher.

Covers CME, CBOT, NYMEX, and COMEX products.

Strategy:
  1. Try the CmeWS JSON API for product catalog + contract specs (no HTML parsing).
  2. Fall back to HTML spec pages if JSON is unavailable.
"""

import json
import logging
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .model import FuturesContract

logger = logging.getLogger(__name__)

# CME product catalog API (V2 product slate)
_CATALOG_API = "https://www.cmegroup.com/CmeWS/mvc/ProductSlate/V2/List/All/Exchange/CME"
_SPEC_API_TPL = "https://www.cmegroup.com/CmeWS/mvc/Quotes/ContractSpecifications/{product_id}"

# Category pages for HTML fallback
CME_CATEGORY_URLS = {
    "energy": "https://www.cmegroup.com/markets/energy.html",
    "metals": "https://www.cmegroup.com/markets/metals.html",
    "agriculture": "https://www.cmegroup.com/markets/agriculture.html",
    "financials": "https://www.cmegroup.com/markets/interest-rates.html",
    "emissions": "https://www.cmegroup.com/markets/energy/emissions.html",
}

_CME_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/html",
}

# Map CME product group names to our asset classes
_GROUP_TO_ASSET = {
    "energy": "energy",
    "metals": "metals",
    "agriculture": "agriculture",
    "interest rates": "financials",
    "interest-rates": "financials",
    "equity index": "financials",
    "fx": "financials",
    "emissions": "emissions",
}


class CmeFetcher:
    exchange_name = "CME"
    domain = "www.cmegroup.com"

    def discover_contracts(self, session, throttler, categories=None):
        """Discover CME contract-spec page URLs.

        Tries JSON catalog first, falls back to HTML category pages.
        Returns list of spec page URLs (or product-ID-based API URLs).
        """
        urls = self._discover_via_api(session, throttler, categories)
        if urls:
            return urls

        logger.info("CME JSON catalog unavailable, falling back to HTML discovery")
        return self._discover_via_html(session, throttler, categories)

    def _discover_via_api(self, session, throttler, categories):
        """Use CmeWS product slate API to get product IDs."""
        throttler.acquire(self.domain)
        try:
            resp = session.get(
                _CATALOG_API, headers=_CME_HEADERS, timeout=30,
            )
            throttler.release(self.domain)
            if resp.status_code != 200:
                throttler.report_failure(self.domain, resp.status_code)
                return []
            throttler.report_success(self.domain)
        except Exception as exc:
            throttler.release(self.domain)
            logger.warning("CME catalog API failed: %s", exc)
            return []

        try:
            products = resp.json()
        except (json.JSONDecodeError, ValueError):
            return []

        if not isinstance(products, list):
            return []

        urls = []
        cat_filter = set(categories) if categories else None

        for prod in products:
            if not isinstance(prod, dict):
                continue
            product_id = prod.get("id")
            group = (prod.get("group") or prod.get("productGroup") or "").lower()
            asset = _GROUP_TO_ASSET.get(group, group)

            if cat_filter and asset not in cat_filter:
                continue

            if product_id:
                spec_url = _SPEC_API_TPL.format(product_id=product_id)
                urls.append(spec_url)

        logger.info("CME API discovery: %d products", len(urls))
        return urls

    def _discover_via_html(self, session, throttler, categories):
        """Crawl CME category HTML pages for product spec links."""
        cats = categories or list(CME_CATEGORY_URLS.keys())
        urls = []

        for cat in cats:
            cat_url = CME_CATEGORY_URLS.get(cat)
            if not cat_url:
                continue

            throttler.acquire(self.domain)
            try:
                resp = session.get(cat_url, headers=_CME_HEADERS, timeout=30)
                throttler.release(self.domain)
                if resp.status_code != 200:
                    throttler.report_failure(self.domain, resp.status_code)
                    continue
                throttler.report_success(self.domain)
            except Exception as exc:
                throttler.release(self.domain)
                logger.warning("CME category %s failed: %s", cat, exc)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/contract-specs" in href or "contractSpecs" in href:
                    abs_url = urljoin(cat_url, href)
                    if abs_url not in urls:
                        urls.append(abs_url)

        logger.info("CME HTML discovery: %d spec URLs from %d categories", len(urls), len(cats))
        return urls

    def parse_contract(self, html: str, url: str) -> FuturesContract:
        """Parse a CME contract spec response (JSON API or HTML)."""
        # Try JSON first (CmeWS API response)
        try:
            data = json.loads(html)
            if isinstance(data, dict):
                return self._parse_json_spec(data, url)
        except (json.JSONDecodeError, ValueError):
            pass

        # Fall back to HTML parsing
        return self._parse_html_spec(html, url)

    def _parse_json_spec(self, data: dict, url: str) -> FuturesContract:
        """Parse CmeWS JSON contract specification."""
        specs = data.get("specs", data)
        if isinstance(specs, list) and specs:
            specs = specs[0]

        product_name = (
            specs.get("productName")
            or specs.get("name")
            or data.get("productName")
            or ""
        )
        ticker = specs.get("productCode") or specs.get("globexCode") or ""
        asset_class = _GROUP_TO_ASSET.get(
            (specs.get("group") or specs.get("productGroup") or "").lower(),
            "financials",
        )

        return FuturesContract(
            exchange="CME",
            product_name=product_name,
            ticker=ticker,
            asset_class=asset_class,
            contract_size=specs.get("contractSize") or specs.get("contractUnit") or "",
            quote_currency=specs.get("priceQuotation") or "",
            tick_size=specs.get("minimumFluctuation") or "",
            trading_months=specs.get("listedContracts") or specs.get("productListing") or "",
            settlement_type=specs.get("settlementMethod") or specs.get("settlement") or "",
            trading_hours=specs.get("tradingHours") or "",
            last_trade_date_rule=specs.get("terminationOfTrading") or specs.get("lastTradeDate") or "",
            source_url=url,
            extra_specs={
                k: v for k, v in specs.items()
                if k not in {
                    "productName", "name", "productCode", "globexCode",
                    "group", "productGroup", "contractSize", "contractUnit",
                    "priceQuotation", "minimumFluctuation", "listedContracts",
                    "productListing", "settlementMethod", "settlement",
                    "tradingHours", "terminationOfTrading", "lastTradeDate",
                } and isinstance(v, (str, int, float, bool))
            },
        )

    def _parse_html_spec(self, html: str, url: str) -> FuturesContract:
        """Parse CME HTML contract spec page."""
        soup = BeautifulSoup(html, "html.parser")

        product_name = ""
        title_tag = soup.find("title")
        if title_tag:
            product_name = re.sub(
                r"\s*[-|]\s*CME Group\s*$", "",
                title_tag.get_text(strip=True),
            )

        specs: dict[str, str] = {}
        # CME uses <div class="description-table"> or standard <table> elements
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(" ", strip=True)
                    specs[label] = value

        return FuturesContract(
            exchange="CME",
            product_name=product_name,
            ticker=specs.get("product code", specs.get("globex code", "")),
            asset_class=self._guess_asset_class(url),
            contract_size=specs.get("contract size", specs.get("contract unit", "")),
            quote_currency=specs.get("price quotation", ""),
            tick_size=specs.get("minimum fluctuation", specs.get("minimum price fluctuation", "")),
            trading_months=specs.get("listed contracts", specs.get("product listing", "")),
            settlement_type=specs.get("settlement method", specs.get("settlement", "")),
            trading_hours=specs.get("trading hours", ""),
            last_trade_date_rule=specs.get("termination of trading", specs.get("last trade date", "")),
            source_url=url,
            extra_specs={},
        )

    @staticmethod
    def _guess_asset_class(url: str) -> str:
        for key, asset in _GROUP_TO_ASSET.items():
            if key.replace(" ", "-") in url or key.replace(" ", "") in url:
                return asset
        return "financials"
