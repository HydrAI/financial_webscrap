"""ICE (Intercontinental Exchange) contract-specification fetcher.

Primary strategy: use ICE's CSV product catalog API at
  https://www.ice.com/api/productguide/info/codes/all/csv
which returns a full product list with names, tickers, groups, and product page URLs.

Fallback: crawl HTML category pages.
"""

import csv
import io
import logging
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .model import FuturesContract

logger = logging.getLogger(__name__)

_CSV_API = "https://www.ice.com/api/productguide/info/codes/all/csv"

# ICE category pages (fallback)
ICE_CATEGORY_URLS = {
    "energy": "https://www.ice.com/products/Futures-Options/Energy",
    "agriculture": "https://www.ice.com/products/Futures-Options/Agriculture",
    "financials": "https://www.ice.com/products/Futures-Options/Financial",
    "emissions": "https://www.ice.com/products/Futures-Options/Emissions",
    "metals": "https://www.ice.com/products/Futures-Options/Metals",
}

_ICE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html, application/vnd.ms-excel",
}

# Map ICE GROUP column values → our asset_class
_GROUP_TO_ASSET = {
    "crude oil and refined products": "energy",
    "natural gas": "energy",
    "power": "energy",
    "coal": "energy",
    "lng": "energy",
    "agriculture": "agriculture",
    "softs": "softs",
    "sugar": "softs",
    "coffee": "softs",
    "cocoa": "softs",
    "cotton": "agriculture",
    "grains and oilseeds": "agriculture",
    "dairy": "agriculture",
    "lumber": "agriculture",
    "emissions": "emissions",
    "environmental": "emissions",
    "metals": "metals",
    "battery materials": "metals",
    "precious metals": "metals",
    "interest rates": "financials",
    "equity index": "financials",
    "fx": "financials",
    "digital assets": "financials",
    "credit": "financials",
    "bonds": "financials",
    "freight": "energy",
    "electricity": "energy",
    "biofuels": "energy",
    "petrochemicals": "energy",
    "msci indices": "financials",
    "single stock options": "financials",
    "mini single stocks": "financials",
    "cross rates": "financials",
    "emerging markets": "financials",
    "ftse indices": "financials",
    "swapnotes": "financials",
    "coindesk indices": "financials",
    "majors": "financials",
    "ifus indices": "financials",
    "ifsg indices": "financials",
    "ice u.s. dollar index (usdx)": "financials",
    "mortgage rates": "financials",
    "bitcoin": "financials",
    "digital assets": "financials",
    "canola": "agriculture",
    "frozen orange juice": "agriculture",
}

# Map ICE table labels → FuturesContract fields (for HTML spec pages)
_LABEL_MAP = {
    "contract symbol": "ticker",
    "symbol": "ticker",
    "contract size": "contract_size",
    "unit of trading": "contract_size",
    "price quotation": "quote_currency",
    "currency": "quote_currency",
    "minimum price fluctuation": "tick_size",
    "minimum tick": "tick_size",
    "tick size": "tick_size",
    "tick value": "tick_size",
    "contract months": "trading_months",
    "contract series": "trading_months",
    "listed contracts": "trading_months",
    "settlement": "settlement_type",
    "settlement method": "settlement_type",
    "delivery": "settlement_type",
    "last trading day": "last_trade_date_rule",
    "last trade date": "last_trade_date_rule",
    "expiration": "last_trade_date_rule",
    "trading hours": "trading_hours",
    "trading period": "trading_hours",
}


class IceFetcher:
    exchange_name = "ICE"
    domain = "www.ice.com"

    def discover_contracts(self, session, throttler, categories=None):
        """Discover ICE contracts via CSV catalog API.

        Returns list of product page URLs.
        Falls back to HTML category crawling if CSV is unavailable.
        """
        result = self._discover_via_csv(session, throttler, categories)
        if result:
            return result

        logger.info("ICE CSV API unavailable, falling back to HTML discovery")
        return self._discover_via_html(session, throttler, categories)

    def discover_contracts_csv(self, session, throttler, categories=None):
        """Discover AND parse contracts directly from the CSV catalog.

        Returns (urls, contracts) — contracts are pre-built from CSV data,
        so spec pages don't need to be fetched individually.
        """
        throttler.acquire(self.domain)
        try:
            resp = session.get(_CSV_API, headers=_ICE_HEADERS, timeout=30)
            throttler.release(self.domain)
            if resp.status_code != 200:
                throttler.report_failure(self.domain, resp.status_code)
                return [], []
            throttler.report_success(self.domain)
        except Exception as exc:
            throttler.release(self.domain)
            logger.warning("ICE CSV API failed: %s", exc)
            return [], []

        contracts = self._parse_csv_catalog(resp.text, categories)
        urls = [c.source_url for c in contracts]
        return urls, contracts

    def _discover_via_csv(self, session, throttler, categories):
        """Fetch CSV catalog and return product page URLs."""
        throttler.acquire(self.domain)
        try:
            resp = session.get(_CSV_API, headers=_ICE_HEADERS, timeout=30)
            throttler.release(self.domain)
            if resp.status_code != 200:
                throttler.report_failure(self.domain, resp.status_code)
                return []
            throttler.report_success(self.domain)
        except Exception as exc:
            throttler.release(self.domain)
            logger.warning("ICE CSV API failed: %s", exc)
            return []

        contracts = self._parse_csv_catalog(resp.text, categories)
        logger.info("ICE CSV discovery: %d products", len(contracts))
        # Store parsed contracts for later retrieval
        self._csv_contracts = contracts
        return [c.source_url for c in contracts]

    def _parse_csv_catalog(self, csv_text: str, categories=None) -> list[FuturesContract]:
        """Parse the ICE product catalog CSV into FuturesContract objects."""
        cat_filter = set(categories) if categories else None
        contracts = []

        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            # Extract product URL from the HYPERLINK formula
            product_col = row.get("PRODUCT (Click to open in Browser)", "")
            url_match = re.search(r'HYPERLINK\("([^"]+)"', product_col)
            product_url = url_match.group(1) if url_match else ""

            # Extract product name from the HYPERLINK formula
            name_match = re.search(r'HYPERLINK\("[^"]+","([^"]+)"', product_col)
            product_name = name_match.group(1) if name_match else ""

            ticker = row.get("SYMBOL CODE", "") or row.get("PHYSICAL", "")
            group = (row.get("GROUP", "") or "").lower().strip()
            asset_class = self._map_group_to_asset(group)
            market_type = row.get("MARKET TYPE NAME", "")

            if cat_filter and asset_class not in cat_filter:
                continue

            contracts.append(FuturesContract(
                exchange="ICE",
                product_name=product_name,
                ticker=ticker,
                asset_class=asset_class,
                source_url=product_url,
                extra_specs={
                    "product_id": row.get("PRODUCT ID", ""),
                    "physical_code": row.get("PHYSICAL", ""),
                    "logical_code": row.get("LOGICAL", ""),
                    "group": row.get("GROUP", ""),
                    "clearing_venue": row.get("CLEARING VENUE", ""),
                    "mic_code": row.get("MIC CODE", ""),
                    "market_type": market_type,
                },
            ))

        return contracts

    @staticmethod
    def _map_group_to_asset(group: str) -> str:
        """Map ICE GROUP value to our normalized asset_class."""
        group_lower = group.lower().strip()
        if group_lower in _GROUP_TO_ASSET:
            return _GROUP_TO_ASSET[group_lower]
        # Fuzzy match
        for key, asset in _GROUP_TO_ASSET.items():
            if key in group_lower or group_lower in key:
                return asset
        return ""

    def get_csv_contracts(self) -> list[FuturesContract]:
        """Return contracts parsed from CSV (available after discover_contracts)."""
        return getattr(self, "_csv_contracts", [])

    def _discover_via_html(self, session, throttler, categories):
        """Crawl ICE category HTML pages for product spec links (fallback)."""
        cats = categories or list(ICE_CATEGORY_URLS.keys())
        urls: list[str] = []

        for cat in cats:
            cat_url = ICE_CATEGORY_URLS.get(cat)
            if not cat_url:
                continue

            throttler.acquire(self.domain)
            try:
                resp = session.get(cat_url, headers=_ICE_HEADERS, timeout=30)
                throttler.release(self.domain)
                if resp.status_code != 200:
                    throttler.report_failure(self.domain, resp.status_code)
                    continue
                throttler.report_success(self.domain)
            except Exception as exc:
                throttler.release(self.domain)
                logger.warning("ICE category %s failed: %s", cat, exc)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if any(kw in href.lower() for kw in (
                    "/productguide/", "/product/", "/contract-spec",
                    "/futures/", "/productspec",
                )):
                    abs_url = urljoin(cat_url, href)
                    if abs_url not in urls:
                        urls.append(abs_url)

        logger.info("ICE HTML discovery: %d product URLs", len(urls))
        return urls

    def parse_contract(self, html: str, url: str) -> FuturesContract:
        """Parse an ICE contract-specification HTML page."""
        soup = BeautifulSoup(html, "html.parser")

        product_name = self._extract_product_name(soup, url)
        asset_class = self._derive_asset_class(url)
        specs = self._parse_spec_tables(soup)

        return FuturesContract(
            exchange="ICE",
            product_name=product_name,
            ticker=specs.pop("ticker", ""),
            asset_class=asset_class,
            contract_size=specs.pop("contract_size", ""),
            quote_currency=specs.pop("quote_currency", ""),
            tick_size=specs.pop("tick_size", ""),
            trading_months=specs.pop("trading_months", ""),
            settlement_type=specs.pop("settlement_type", ""),
            trading_hours=specs.pop("trading_hours", ""),
            last_trade_date_rule=specs.pop("last_trade_date_rule", ""),
            source_url=url,
            extra_specs=specs,
        )

    @staticmethod
    def _extract_product_name(soup: BeautifulSoup, url: str) -> str:
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            title = re.sub(r"\s*[-|]\s*ICE\s*$", "", title, flags=re.IGNORECASE)
            title = re.sub(r"\s*[-|]\s*Intercontinental Exchange\s*$", "", title, flags=re.IGNORECASE)
            if title:
                return title

        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True)

        return ""

    @staticmethod
    def _derive_asset_class(url: str) -> str:
        url_lower = url.lower()
        for keyword in ("energy", "oil", "gas", "power"):
            if keyword in url_lower:
                return "energy"
        for keyword in ("agriculture", "agri", "sugar", "coffee", "cocoa", "cotton"):
            if keyword in url_lower:
                return "agriculture"
        for keyword in ("emission", "carbon"):
            if keyword in url_lower:
                return "emissions"
        for keyword in ("metal",):
            if keyword in url_lower:
                return "metals"
        for keyword in ("financial", "interest", "equity", "fx", "index"):
            if keyword in url_lower:
                return "financials"
        return ""

    @staticmethod
    def _parse_spec_tables(soup: BeautifulSoup) -> dict:
        """Parse all tables on the page for spec-like label/value rows."""
        specs: dict[str, str] = {}

        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(strip=True).lower()
                if not label or label in ("specification", "details", "description"):
                    continue
                value = " | ".join(
                    c.get_text(" ", strip=True) for c in cells[1:] if c.get_text(strip=True)
                )
                field_name = _LABEL_MAP.get(label)
                if field_name:
                    specs[field_name] = value
                else:
                    clean_key = re.sub(r"[^a-z0-9_]", "_", label).strip("_")
                    if clean_key:
                        specs[clean_key] = value

        return specs
