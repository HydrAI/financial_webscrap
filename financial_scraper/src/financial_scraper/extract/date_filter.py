"""Post-extraction date range filtering."""

import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)


class DateFilter:
    """Filter extracted pages by publication date."""

    def __init__(self, date_from: str | None = None, date_to: str | None = None):
        self._date_from = self._parse_bound(date_from) if date_from else None
        self._date_to = self._parse_bound(date_to) if date_to else None
        self._stats = {"passed": 0, "filtered_out": 0, "no_date_kept": 0}

    @staticmethod
    def _parse_bound(date_str: str) -> date:
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    def _parse_extracted_date(self, date_str: str | None) -> date | None:
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y-%m", "%Y"):
            try:
                return datetime.strptime(date_str[:len(fmt.replace('%', 'X'))],
                                         fmt).date()
            except (ValueError, IndexError):
                continue
        # Try common lengths
        for length, fmt in [(19, "%Y-%m-%dT%H:%M:%S"), (10, "%Y-%m-%d"),
                            (7, "%Y-%m"), (4, "%Y")]:
            try:
                return datetime.strptime(date_str[:length], fmt).date()
            except (ValueError, IndexError):
                continue
        return None

    def passes(self, extracted_date: str | None) -> bool:
        if not self.is_active:
            return True
        parsed = self._parse_extracted_date(extracted_date)
        if parsed is None:
            self._stats["no_date_kept"] += 1
            return True  # keep pages without dates
        if self._date_from and parsed < self._date_from:
            self._stats["filtered_out"] += 1
            return False
        if self._date_to and parsed > self._date_to:
            self._stats["filtered_out"] += 1
            return False
        self._stats["passed"] += 1
        return True

    @property
    def is_active(self) -> bool:
        return self._date_from is not None or self._date_to is not None

    def get_stats(self) -> dict:
        return dict(self._stats)
