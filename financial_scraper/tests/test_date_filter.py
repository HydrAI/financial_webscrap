"""Tests for financial_scraper.extract.date_filter."""

from financial_scraper.extract.date_filter import DateFilter


class TestIsActive:
    def test_no_bounds_not_active(self):
        f = DateFilter()
        assert f.is_active is False

    def test_date_from_only_active(self):
        f = DateFilter(date_from="2024-01-01")
        assert f.is_active is True

    def test_date_to_only_active(self):
        f = DateFilter(date_to="2024-12-31")
        assert f.is_active is True


class TestPassesNoBounds:
    def test_always_true(self):
        f = DateFilter()
        assert f.passes("2024-06-15") is True
        assert f.passes(None) is True
        assert f.passes("garbage") is True


class TestPassesDateFromOnly:
    def test_rejects_before(self):
        f = DateFilter(date_from="2024-06-01")
        assert f.passes("2024-05-31") is False

    def test_accepts_on(self):
        f = DateFilter(date_from="2024-06-01")
        assert f.passes("2024-06-01") is True

    def test_accepts_after(self):
        f = DateFilter(date_from="2024-06-01")
        assert f.passes("2024-07-15") is True


class TestPassesDateToOnly:
    def test_rejects_after(self):
        f = DateFilter(date_to="2024-06-30")
        assert f.passes("2024-07-01") is False

    def test_accepts_on(self):
        f = DateFilter(date_to="2024-06-30")
        assert f.passes("2024-06-30") is True

    def test_accepts_before(self):
        f = DateFilter(date_to="2024-06-30")
        assert f.passes("2024-05-01") is True


class TestPassesBothBounds:
    def test_in_range(self):
        f = DateFilter(date_from="2024-01-01", date_to="2024-12-31")
        assert f.passes("2024-06-15") is True

    def test_before_range(self):
        f = DateFilter(date_from="2024-01-01", date_to="2024-12-31")
        assert f.passes("2023-12-31") is False

    def test_after_range(self):
        f = DateFilter(date_from="2024-01-01", date_to="2024-12-31")
        assert f.passes("2025-01-01") is False


class TestNoneDateInput:
    def test_none_kept(self):
        f = DateFilter(date_from="2024-01-01")
        assert f.passes(None) is True

    def test_unparseable_kept(self):
        f = DateFilter(date_from="2024-01-01")
        assert f.passes("not-a-date") is True


class TestDateFormats:
    def test_iso_datetime(self):
        f = DateFilter(date_from="2024-06-01")
        assert f.passes("2024-06-15T14:30:00") is True

    def test_date_only(self):
        f = DateFilter(date_from="2024-06-01")
        assert f.passes("2024-06-15") is True

    def test_year_month(self):
        f = DateFilter(date_from="2024-06-01")
        assert f.passes("2024-07") is True

    def test_year_only(self):
        f = DateFilter(date_from="2024-06-01")
        assert f.passes("2025") is True


class TestGetStats:
    def test_accumulates(self):
        f = DateFilter(date_from="2024-06-01")
        f.passes("2024-07-01")  # passed
        f.passes("2024-05-01")  # filtered_out
        f.passes(None)          # no_date_kept
        stats = f.get_stats()
        assert stats["passed"] == 1
        assert stats["filtered_out"] == 1
        assert stats["no_date_kept"] == 1
