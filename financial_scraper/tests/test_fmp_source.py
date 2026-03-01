"""Tests for the FMP fallback transcript source."""

import pytest
from unittest.mock import MagicMock, patch

from financial_scraper.transcripts.sources.fmp import FMPSource


# ---------------------------------------------------------------------------
# FMPSource.available
# ---------------------------------------------------------------------------

class TestFMPAvailability:
    def test_available_with_key(self):
        assert FMPSource(api_key="test123").available is True

    def test_not_available_without_key(self):
        with patch.dict("os.environ", {}, clear=True):
            src = FMPSource(api_key="")
            # Only unavailable if env var also absent
            import os
            os.environ.pop("FMP_API_KEY", None)
            assert FMPSource(api_key="").available is False

    def test_reads_env_var(self):
        with patch.dict("os.environ", {"FMP_API_KEY": "env_key"}):
            assert FMPSource().available is True
            assert FMPSource().api_key == "env_key"

    def test_explicit_key_overrides_env(self):
        with patch.dict("os.environ", {"FMP_API_KEY": "env_key"}):
            assert FMPSource(api_key="explicit").api_key == "explicit"


# ---------------------------------------------------------------------------
# FMPSource.get_transcript — happy path
# ---------------------------------------------------------------------------

_SAMPLE_RESPONSE = [
    {
        "symbol": "AAPL",
        "quarter": 1,
        "year": 2024,
        "date": "2024-01-28 17:00:00",
        "content": "Operator: Good afternoon. Welcome to Apple's Q1 2024 earnings call. " * 20,
    }
]


def _mock_resp(status=200, json_data=None, text=""):
    m = MagicMock()
    m.status_code = status
    m.json.return_value = json_data if json_data is not None else []
    m.text = text
    return m


class TestFMPGetTranscript:
    def _src(self):
        return FMPSource(api_key="testkey")

    def test_returns_transcript_result_on_success(self):
        mock_sess = MagicMock()
        mock_sess.get.return_value = _mock_resp(200, _SAMPLE_RESPONSE)

        result = self._src().get_transcript("AAPL", "Q1", 2024, mock_sess)

        assert result is not None
        assert result.ticker == "AAPL"
        assert result.quarter == "Q1"
        assert result.year == 2024
        assert result.date == "2024-01-28"
        assert "Apple" in result.full_text

    def test_correct_api_params_sent(self):
        mock_sess = MagicMock()
        mock_sess.get.return_value = _mock_resp(200, _SAMPLE_RESPONSE)

        self._src().get_transcript("MSFT", "Q3", 2023, mock_sess)

        call_kwargs = mock_sess.get.call_args
        params = call_kwargs[1]["params"] if call_kwargs[1] else call_kwargs[0][1]
        assert params["symbol"] == "MSFT"
        assert params["quarter"] == 3
        assert params["year"] == 2023
        assert params["apikey"] == "testkey"

    def test_quarter_string_converted_to_int(self):
        mock_sess = MagicMock()
        mock_sess.get.return_value = _mock_resp(200, _SAMPLE_RESPONSE)
        self._src().get_transcript("AAPL", "Q4", 2022, mock_sess)
        params = mock_sess.get.call_args[1]["params"]
        assert params["quarter"] == 4

    def test_date_truncated_to_date_only(self):
        data = [{**_SAMPLE_RESPONSE[0], "date": "2024-01-28 17:00:00"}]
        mock_sess = MagicMock()
        mock_sess.get.return_value = _mock_resp(200, data)
        result = self._src().get_transcript("AAPL", "Q1", 2024, mock_sess)
        assert result.date == "2024-01-28"

    def test_handles_dict_response_not_list(self):
        mock_sess = MagicMock()
        mock_sess.get.return_value = _mock_resp(200, _SAMPLE_RESPONSE[0])
        result = self._src().get_transcript("AAPL", "Q1", 2024, mock_sess)
        assert result is not None
        assert result.full_text


# ---------------------------------------------------------------------------
# FMPSource.get_transcript — error cases
# ---------------------------------------------------------------------------

class TestFMPErrors:
    def _src(self):
        return FMPSource(api_key="testkey")

    def test_returns_none_when_no_api_key(self):
        src = FMPSource(api_key="")
        with patch.dict("os.environ", {}, clear=True):
            import os; os.environ.pop("FMP_API_KEY", None)
            result = FMPSource(api_key="").get_transcript("AAPL", "Q1", 2024)
        assert result is None

    def test_returns_none_on_401(self):
        mock_sess = MagicMock()
        mock_sess.get.return_value = _mock_resp(401)
        assert self._src().get_transcript("AAPL", "Q1", 2024, mock_sess) is None

    def test_returns_none_on_429(self):
        mock_sess = MagicMock()
        mock_sess.get.return_value = _mock_resp(429)
        assert self._src().get_transcript("AAPL", "Q1", 2024, mock_sess) is None

    def test_returns_none_on_500(self):
        mock_sess = MagicMock()
        mock_sess.get.return_value = _mock_resp(500)
        assert self._src().get_transcript("AAPL", "Q1", 2024, mock_sess) is None

    def test_returns_none_on_empty_list(self):
        mock_sess = MagicMock()
        mock_sess.get.return_value = _mock_resp(200, [])
        assert self._src().get_transcript("AAPL", "Q1", 2024, mock_sess) is None

    def test_returns_none_on_empty_content(self):
        mock_sess = MagicMock()
        data = [{**_SAMPLE_RESPONSE[0], "content": ""}]
        mock_sess.get.return_value = _mock_resp(200, data)
        assert self._src().get_transcript("AAPL", "Q1", 2024, mock_sess) is None

    def test_returns_none_on_network_error(self):
        import requests
        mock_sess = MagicMock()
        mock_sess.get.side_effect = requests.RequestException("timeout")
        assert self._src().get_transcript("AAPL", "Q1", 2024, mock_sess) is None

    def test_returns_none_on_invalid_json(self):
        mock_sess = MagicMock()
        resp = MagicMock(status_code=200)
        resp.json.side_effect = ValueError("bad json")
        mock_sess.get.return_value = resp
        assert self._src().get_transcript("AAPL", "Q1", 2024, mock_sess) is None


# ---------------------------------------------------------------------------
# Pipeline integration: FMP fallback triggered on http_error
# ---------------------------------------------------------------------------

class TestPipelineFMPFallback:
    """Verify _fetch_one falls back to FMP when fool.com returns a permanent error."""

    def _make_config(self, tmp_path, fmp_key="testkey"):
        from financial_scraper.transcripts.config import TranscriptConfig
        return TranscriptConfig(
            tickers=("AAPL",),
            year=2024,
            concurrent=1,
            output_dir=tmp_path,
            output_path=tmp_path / "out.parquet",
            checkpoint_file=tmp_path / "cp.json",
            fmp_api_key=fmp_key,
        )

    def test_fmp_fallback_used_on_404(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from financial_scraper.transcripts.discovery import TranscriptInfo
        from financial_scraper.transcripts.extract import TranscriptResult
        import pyarrow.parquet as pq

        info = TranscriptInfo(
            url="https://www.fool.com/earnings/call-transcripts/2024/01/28/aapl-q1-2024/",
            ticker="AAPL", quarter="Q1", year=2024, pub_date="2024-01-28",
        )

        fmp_result = TranscriptResult(
            company="AAPL", ticker="AAPL", quarter="Q1", year=2024,
            date="2024-01-28",
            full_text="Apple Q1 2024 strong results. Revenue beat expectations. " * 20,
        )

        fool_resp = MagicMock(status_code=404)
        cfg = self._make_config(tmp_path)
        pipeline = TranscriptPipeline(cfg)

        with patch("financial_scraper.transcripts.pipeline.discover_transcripts",
                   return_value=[info]):
            with patch.object(pipeline._session, "get", return_value=fool_resp):
                with patch.object(pipeline._fmp, "get_transcript",
                                  return_value=fmp_result):
                    with patch("financial_scraper.transcripts.pipeline.time.sleep"):
                        pipeline.run()

        table = pq.read_table(tmp_path / "out.parquet")
        assert table.num_rows == 1
        assert table.column("source")[0].as_py() == "fool.com"

    def test_fmp_not_called_when_fool_succeeds(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from financial_scraper.transcripts.discovery import TranscriptInfo
        from financial_scraper.transcripts.extract import TranscriptResult

        info = TranscriptInfo(
            url="https://www.fool.com/earnings/call-transcripts/2024/01/28/aapl-q1-2024/",
            ticker="AAPL", quarter="Q1", year=2024, pub_date="2024-01-28",
        )
        fool_result = TranscriptResult(
            company="AAPL", ticker="AAPL", quarter="Q1", year=2024,
            date="2024-01-28",
            full_text="Apple Q1 2024 earnings call transcript content. " * 20,
        )
        cfg = self._make_config(tmp_path)
        pipeline = TranscriptPipeline(cfg)

        fool_resp = MagicMock(status_code=200, text="<html></html>")
        with patch("financial_scraper.transcripts.pipeline.discover_transcripts",
                   return_value=[info]):
            with patch.object(pipeline._session, "get", return_value=fool_resp):
                with patch("financial_scraper.transcripts.pipeline.extract_transcript",
                           return_value=fool_result):
                    with patch.object(pipeline._fmp, "get_transcript") as mock_fmp:
                        with patch("financial_scraper.transcripts.pipeline.time.sleep"):
                            pipeline.run()

        mock_fmp.assert_not_called()

    def test_fmp_not_called_when_no_api_key(self, tmp_path):
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        from financial_scraper.transcripts.discovery import TranscriptInfo

        info = TranscriptInfo(
            url="https://www.fool.com/earnings/call-transcripts/2024/01/28/aapl-q1-2024/",
            ticker="AAPL", quarter="Q1", year=2024, pub_date="2024-01-28",
        )
        cfg = self._make_config(tmp_path, fmp_key="")
        pipeline = TranscriptPipeline(cfg)

        fool_resp = MagicMock(status_code=404)
        with patch("financial_scraper.transcripts.pipeline.discover_transcripts",
                   return_value=[info]):
            with patch.object(pipeline._session, "get", return_value=fool_resp):
                with patch.object(pipeline._fmp, "get_transcript") as mock_fmp:
                    with patch("financial_scraper.transcripts.pipeline.time.sleep"):
                        pipeline.run()

        mock_fmp.assert_not_called()
