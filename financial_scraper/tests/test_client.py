"""Tests for financial_scraper.fetch.client."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from financial_scraper.config import ScraperConfig
from financial_scraper.fetch.client import FetchClient, FetchResult
from financial_scraper.fetch.throttle import DomainThrottler
from financial_scraper.fetch.robots import RobotChecker


class TestFetchResult:
    def test_construction(self):
        r = FetchResult(
            url="https://example.com", status=200, html="<html></html>",
            content_type="text/html", content_bytes=None,
            error=None, response_headers={},
        )
        assert r.url == "https://example.com"
        assert r.status == 200

    def test_error_result(self):
        r = FetchResult(
            url="https://example.com", status=0, html=None,
            content_type="", content_bytes=None,
            error="Connection refused", response_headers=None,
        )
        assert r.error == "Connection refused"


def _make_client(**config_overrides):
    defaults = {"fetch_timeout": 5, "max_concurrent_total": 2,
                "max_concurrent_per_domain": 1, "respect_robots": False}
    defaults.update(config_overrides)
    cfg = ScraperConfig(**defaults)
    throttler = MagicMock(spec=DomainThrottler)
    throttler.acquire = AsyncMock()
    throttler.release = MagicMock()
    throttler.report_success = MagicMock()
    throttler.report_failure = MagicMock()
    robot_checker = MagicMock(spec=RobotChecker)
    robot_checker.is_allowed = AsyncMock(return_value=True)
    return FetchClient(cfg, throttler, robot_checker)


def _mock_response(status=200, content_type="text/html", text="<html>ok</html>",
                   headers=None, read_bytes=None):
    resp = AsyncMock()
    resp.status = status
    resp.headers = headers or {"Content-Type": content_type}
    resp.text = AsyncMock(return_value=text)
    resp.read = AsyncMock(return_value=read_bytes or b"")
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


class TestFetchSuccess:
    def test_html_200(self):
        client = _make_client()
        mock_resp = _mock_response(200, "text/html", "<html>content</html>")
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session.close = AsyncMock()
        client._session = mock_session

        result = asyncio.run(client.fetch("https://example.com/page"))
        assert result.status == 200
        assert result.html == "<html>content</html>"
        assert result.error is None

    def test_pdf_200(self):
        client = _make_client()
        pdf_bytes = b"%PDF-1.4 fake content"
        mock_resp = _mock_response(
            200, headers={"Content-Type": "application/pdf"},
            read_bytes=pdf_bytes,
        )
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session.close = AsyncMock()
        client._session = mock_session

        result = asyncio.run(client.fetch("https://example.com/report.pdf"))
        assert result.status == 200
        assert result.content_bytes == pdf_bytes
        assert result.html is None


class TestFetchRobotsBlocked:
    def test_robots_disallowed(self):
        client = _make_client(respect_robots=True)
        client._robot_checker.is_allowed = AsyncMock(return_value=False)
        client._session = AsyncMock()

        result = asyncio.run(client.fetch("https://example.com/secret"))
        assert result.status == 0
        assert result.error == "robots.txt disallowed"


class TestFetchErrors:
    def test_http_error_status(self):
        client = _make_client()
        mock_resp = _mock_response(500, headers={"Content-Type": "text/html"})
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session.close = AsyncMock()
        client._session = mock_session

        result = asyncio.run(client.fetch("https://example.com/page"))
        assert result.status == 500
        assert result.error == "HTTP 500"

    def test_exception_during_fetch(self):
        client = _make_client()
        mock_session = MagicMock()
        mock_session.get.side_effect = Exception("Connection timeout")
        mock_session.close = AsyncMock()
        client._session = mock_session

        result = asyncio.run(client.fetch("https://example.com/page"))
        assert result.status == 0
        assert "Connection timeout" in result.error


class TestFetch429Retry:
    def test_retries_on_429(self):
        client = _make_client()
        resp_429 = _mock_response(429, headers={"Content-Type": "text/html"})
        resp_200 = _mock_response(200, "text/html", "<html>ok</html>")
        mock_session = MagicMock()
        mock_session.get.side_effect = [resp_429, resp_200]
        mock_session.close = AsyncMock()
        client._session = mock_session

        result = asyncio.run(client.fetch("https://example.com/page"))
        assert result.status == 200

    def test_retries_on_403(self):
        client = _make_client()
        resp_403 = _mock_response(403, headers={"Content-Type": "text/html"})
        resp_200 = _mock_response(200, "text/html", "<html>ok</html>")
        mock_session = MagicMock()
        mock_session.get.side_effect = [resp_403, resp_200]
        mock_session.close = AsyncMock()
        client._session = mock_session

        result = asyncio.run(client.fetch("https://example.com/page"))
        assert result.status == 200

    def test_429_no_retry_on_second_failure(self):
        client = _make_client()
        resp_429a = _mock_response(429, headers={"Content-Type": "text/html"})
        resp_429b = _mock_response(429, headers={"Content-Type": "text/html"})
        mock_session = MagicMock()
        mock_session.get.side_effect = [resp_429a, resp_429b]
        mock_session.close = AsyncMock()
        client._session = mock_session

        result = asyncio.run(client.fetch("https://example.com/page"))
        assert result.status == 429


class TestFetchBatch:
    def test_batch_returns_list(self):
        client = _make_client()
        mock_resp = _mock_response(200, "text/html", "<html>ok</html>")
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session.close = AsyncMock()
        client._session = mock_session

        results = asyncio.run(client.fetch_batch(["https://a.com", "https://b.com"]))
        assert len(results) == 2
        assert all(r.status == 200 for r in results)

    def test_batch_handles_exception(self):
        client = _make_client()
        mock_session = MagicMock()
        mock_session.get.side_effect = Exception("fail")
        mock_session.close = AsyncMock()
        client._session = mock_session

        results = asyncio.run(client.fetch_batch(["https://a.com"]))
        assert len(results) == 1
        assert results[0].error is not None


class TestDecode:
    def test_unicode_decode_error_fallback(self):
        client = _make_client()
        mock_resp = AsyncMock()
        mock_resp.text.side_effect = UnicodeDecodeError("utf-8", b"", 0, 1, "bad")
        mock_resp.read = AsyncMock(return_value="héllo".encode("latin-1"))

        with patch("financial_scraper.fetch.client.chardet") as mock_chardet:
            mock_chardet.detect.return_value = {"encoding": "latin-1"}
            result = asyncio.run(client._decode(mock_resp))

        assert "héllo" in result


class TestContextManager:
    def test_aenter_aexit(self):
        client = _make_client()

        async def _test():
            with patch("financial_scraper.fetch.client.aiohttp.TCPConnector"):
                with patch("financial_scraper.fetch.client.aiohttp.ClientSession") as MockSession:
                    mock_session = AsyncMock()
                    MockSession.return_value = mock_session
                    async with client as c:
                        assert c._session is not None
                    mock_session.close.assert_awaited_once()

        asyncio.run(_test())
