"""Financial Modeling Prep (FMP) fallback transcript source.

Free tier: 250 requests/day, historical data back to 2013.
Get a free API key at: https://financialmodelingprep.com/register

Usage:
    source = FMPSource(api_key="your_key")          # or set FMP_API_KEY env var
    result = source.get_transcript("AAPL", "Q1", 2024, session)
"""

import logging
import os

import requests

from ..extract import TranscriptResult

logger = logging.getLogger(__name__)

_BASE_URL = "https://financialmodelingprep.com/stable/earning-call-transcript"


class FMPSource:
    """Fetches earnings call transcripts from the FMP API."""

    def __init__(self, api_key: str = ""):
        self.api_key = api_key or os.environ.get("FMP_API_KEY", "")

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def get_transcript(
        self,
        ticker: str,
        quarter: str,
        year: int,
        session: requests.Session | None = None,
    ) -> TranscriptResult | None:
        """Fetch a single transcript from FMP.

        Args:
            ticker:  Ticker symbol, e.g. "AAPL"
            quarter: Quarter string, e.g. "Q1" / "Q2" / "Q3" / "Q4"
            year:    Fiscal year, e.g. 2024
            session: Optional requests.Session to reuse (shares connection pool).

        Returns:
            TranscriptResult if found and non-empty, else None.
        """
        if not self.available:
            logger.debug("FMP API key not set — skipping fallback")
            return None

        q_num = int(quarter[1])  # "Q3" -> 3
        sess = session or requests.Session()

        try:
            resp = sess.get(
                _BASE_URL,
                params={
                    "symbol": ticker,
                    "quarter": q_num,
                    "year": year,
                    "apikey": self.api_key,
                },
                timeout=30,
            )
        except requests.RequestException as e:
            logger.warning(f"FMP request error for {ticker} {quarter} {year}: {e}")
            return None

        if resp.status_code == 401:
            logger.warning("FMP API key invalid or expired")
            return None

        if resp.status_code == 429:
            logger.warning("FMP daily limit reached (250 req/day on free tier)")
            return None

        if resp.status_code != 200:
            logger.warning(f"FMP HTTP {resp.status_code} for {ticker} {quarter} {year}")
            return None

        try:
            data = resp.json()
        except ValueError:
            logger.warning(f"FMP returned non-JSON for {ticker} {quarter} {year}")
            return None

        if not data:
            logger.debug(f"FMP returned empty result for {ticker} {quarter} {year}")
            return None

        item = data[0] if isinstance(data, list) else data
        content = (item.get("content") or "").strip()
        if not content:
            logger.debug(f"FMP transcript content empty for {ticker} {quarter} {year}")
            return None

        date_raw = str(item.get("date") or "")
        date = date_raw[:10] if date_raw else ""  # "2024-01-28 17:00:00" -> "2024-01-28"

        logger.info(f"  FMP: fetched {ticker} {quarter} {year} ({len(content)} chars)")
        return TranscriptResult(
            company=ticker,
            ticker=ticker,
            quarter=quarter,
            year=year,
            date=date,
            full_text=content,
        )
