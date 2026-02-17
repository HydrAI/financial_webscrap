"""Tests for financial_scraper.fetch.throttle."""

import asyncio

import pytest

from financial_scraper.fetch.throttle import DomainThrottler


class TestReportSuccess:
    def test_halves_extra_delay(self):
        t = DomainThrottler()
        t._extra_delays["example.com"] = 4.0
        t.report_success("example.com")
        assert t._extra_delays["example.com"] == 2.0

    def test_halves_to_zero(self):
        t = DomainThrottler()
        t._extra_delays["example.com"] = 0.5
        t.report_success("example.com")
        assert t._extra_delays["example.com"] == 0.25
        t.report_success("example.com")
        assert t._extra_delays["example.com"] == 0.125


class TestReportFailure429:
    def test_doubles_delay(self):
        t = DomainThrottler()
        t._extra_delays["example.com"] = 2.0
        t.report_failure("example.com", 429)
        assert t._extra_delays["example.com"] == 4.0

    def test_caps_at_max(self):
        t = DomainThrottler(max_delay=10.0)
        t._extra_delays["example.com"] = 8.0
        t.report_failure("example.com", 429)
        assert t._extra_delays["example.com"] == 10.0

    def test_retry_after_respected(self):
        t = DomainThrottler()
        t.report_failure("example.com", 429, retry_after=15.0)
        assert t._extra_delays["example.com"] == 15.0

    def test_retry_after_capped(self):
        t = DomainThrottler(max_delay=10.0)
        t.report_failure("example.com", 429, retry_after=20.0)
        assert t._extra_delays["example.com"] == 10.0


class TestReportFailure403:
    def test_1_5x_delay(self):
        t = DomainThrottler()
        t._extra_delays["example.com"] = 2.0
        t.report_failure("example.com", 403)
        assert t._extra_delays["example.com"] == 3.0


class TestReportFailure5xx:
    def test_1_25x_delay(self):
        t = DomainThrottler()
        t._extra_delays["example.com"] = 4.0
        t.report_failure("example.com", 500)
        assert t._extra_delays["example.com"] == 5.0

    def test_503(self):
        t = DomainThrottler()
        t._extra_delays["example.com"] = 4.0
        t.report_failure("example.com", 503)
        assert t._extra_delays["example.com"] == 5.0


class TestMaxDelayCap:
    def test_cannot_exceed_max(self):
        t = DomainThrottler(max_delay=5.0)
        t._extra_delays["example.com"] = 4.0
        t.report_failure("example.com", 429)
        assert t._extra_delays["example.com"] == 5.0
        t.report_failure("example.com", 429)
        assert t._extra_delays["example.com"] == 5.0
