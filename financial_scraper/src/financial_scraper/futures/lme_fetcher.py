"""LME contract-specification fetcher and parser.

LME pages are server-rendered HTML with a simple <table> layout:
  <tr><td><strong>Label</strong></td><td colspan="3">Value</td></tr>
"""

import logging
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .model import FuturesContract

logger = logging.getLogger(__name__)

# LME category pages that list metal products
LME_CATEGORY_URLS = {
    "non-ferrous": "https://www.lme.com/metals/non-ferrous",
    "ferrous": "https://www.lme.com/metals/ferrous",
    "precious": "https://www.lme.com/metals/precious",
    "ev": "https://www.lme.com/metals/ev",
}

# Map LME URL path segments to asset_class
_PATH_TO_ASSET = {
    "non-ferrous": "metals",
    "ferrous": "metals",
    "precious": "metals",
    "ev": "metals",
}

# Map LME table labels → FuturesContract field names
_LABEL_MAP = {
    "contract code": "ticker",
    "lot size": "contract_size",
    "deliver type": "settlement_type",
    "delivery type": "settlement_type",
    "price quotation": "quote_currency",
    "contract period": "trading_months",
    "termination of trading": "last_trade_date_rule",
}


class LmeFetcher:
    exchange_name = "LME"
    domain = "www.lme.com"

    def discover_contracts(self, session, throttler, categories=None):
        """Fetch LME category pages and extract contract-spec URLs.

        Returns a list of absolute URLs ending in /contract-specifications.
        """
        cats = categories or list(LME_CATEGORY_URLS.keys())
        urls: list[str] = []

        for cat in cats:
            cat_url = LME_CATEGORY_URLS.get(cat)
            if not cat_url:
                logger.warning("Unknown LME category: %s", cat)
                continue

            throttler.acquire(self.domain)
            try:
                resp = session.get(cat_url, timeout=30)
                throttler.release(self.domain)
                if resp.status_code != 200:
                    throttler.report_failure(self.domain, resp.status_code)
                    logger.warning("LME category %s returned %d", cat, resp.status_code)
                    continue
                throttler.report_success(self.domain)
            except Exception as exc:
                throttler.release(self.domain)
                logger.warning("LME category %s fetch failed: %s", cat, exc)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/contract-specifications" in href:
                    abs_url = urljoin(cat_url, href)
                    if abs_url not in urls:
                        urls.append(abs_url)

        logger.info("LME discovery: %d contract-spec URLs from %d categories", len(urls), len(cats))
        return urls

    def parse_contract(self, html: str, url: str) -> FuturesContract:
        """Parse an LME contract-specifications HTML page into a FuturesContract."""
        soup = BeautifulSoup(html, "html.parser")

        # Extract product name from <title> or canonical URL
        product_name = self._extract_product_name(soup, url)

        # Derive asset class from URL path
        asset_class = self._derive_asset_class(url)

        # Parse the spec table
        specs = self._parse_spec_table(soup)

        # Build tick_size from the tick-size rows (may have multiple sub-rows)
        tick_size = specs.pop("_tick_size", "")

        # Extract trading hours
        trading_hours = specs.pop("_trading_hours", "")

        # Map known labels to fields
        contract = FuturesContract(
            exchange="LME",
            product_name=product_name,
            ticker=specs.pop("ticker", ""),
            asset_class=asset_class,
            contract_size=specs.pop("contract_size", ""),
            quote_currency=specs.pop("quote_currency", ""),
            tick_size=tick_size,
            trading_months=specs.pop("trading_months", ""),
            settlement_type=specs.pop("settlement_type", ""),
            trading_hours=trading_hours,
            last_trade_date_rule=specs.pop("last_trade_date_rule", ""),
            source_url=url,
            extra_specs=specs,  # anything not mapped
        )
        return contract

    @staticmethod
    def _extract_product_name(soup: BeautifulSoup, url: str) -> str:
        """Get product name from <title> or URL slug."""
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Remove " | London Metal Exchange" suffix and "Contract specifications |" prefix
            title = re.sub(r"\s*\|\s*London Metal Exchange\s*$", "", title)
            title = re.sub(r"^Contract specifications\s*\|\s*", "", title)
            if title and title != "Contract specifications":
                return title

        # Fallback: derive from URL slug
        # e.g. /metals/ferrous/lme-steel-rebar-fob-turkey-platts/contract-specifications
        match = re.search(r"/metals/[^/]+/([^/]+)/contract-specifications", url)
        if match:
            slug = match.group(1)
            return slug.replace("-", " ").title()

        return ""

    @staticmethod
    def _derive_asset_class(url: str) -> str:
        """Map URL path to asset class."""
        for segment, asset in _PATH_TO_ASSET.items():
            if f"/metals/{segment}/" in url or f"/metals/{segment}" in url:
                return asset
        return "metals"  # LME is all metals

    @staticmethod
    def _parse_spec_table(soup: BeautifulSoup) -> dict:
        """Parse the first spec <table> on the page.

        Returns a dict with mapped field names + extras.
        Multi-row fields (tick size, trading hours) are concatenated.
        """
        specs: dict[str, str] = {}

        table = soup.find("table")
        if not table:
            return specs

        rows = table.find_all("tr")
        i = 0
        while i < len(rows):
            row = rows[i]
            cells = row.find_all("td")
            if not cells:
                i += 1
                continue

            # Check if first cell has a <strong> label
            strong = cells[0].find("strong")
            if not strong:
                i += 1
                continue

            label = strong.get_text(strip=True).lower()

            # Check for rowspan — indicates multi-row field
            rowspan_attr = cells[0].get("rowspan")
            rowspan = int(rowspan_attr) if rowspan_attr else 1

            # Collect value from remaining cells in this row + spanned rows
            value_parts = []
            for cell in cells[1:]:
                text = cell.get_text(" ", strip=True)
                if text:
                    value_parts.append(text)

            # Collect text from additional spanned rows
            for j in range(1, rowspan):
                if i + j < len(rows):
                    extra_row = rows[i + j]
                    extra_cells = extra_row.find_all("td")
                    for cell in extra_cells:
                        text = cell.get_text(" ", strip=True)
                        if text:
                            value_parts.append(text)

            value = " | ".join(value_parts) if value_parts else ""

            # Handle tick size specially
            if "tick size" in label or "minimum price fluctuation" in label:
                specs["_tick_size"] = value
            elif "trading hours" in label:
                specs["_trading_hours"] = value
            else:
                # Map to known field or store as extra
                field_name = _LABEL_MAP.get(label)
                if field_name:
                    specs[field_name] = value
                else:
                    # Store unmapped fields as extras
                    clean_key = re.sub(r"[^a-z0-9_]", "_", label).strip("_")
                    specs[clean_key] = value

            i += rowspan

        return specs
