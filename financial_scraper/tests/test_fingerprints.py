"""Tests for financial_scraper.fetch.fingerprints."""

from financial_scraper.fetch.fingerprints import (
    ALL_FINGERPRINTS,
    FIREFOX_WINDOWS,
    get_fingerprint_for_domain,
)


class TestToHeaders:
    def test_includes_required_keys(self):
        for fp in ALL_FINGERPRINTS:
            h = fp.to_headers()
            assert "User-Agent" in h
            assert "Accept" in h
            assert "Accept-Language" in h
            assert "Accept-Encoding" in h
            assert "Sec-Fetch-Site" in h
            assert "Sec-Fetch-Mode" in h
            assert "Sec-Fetch-Dest" in h
            assert "Upgrade-Insecure-Requests" in h

    def test_excludes_none_values_firefox(self):
        h = FIREFOX_WINDOWS.to_headers()
        assert "Sec-CH-UA" not in h
        assert "Sec-CH-UA-Mobile" not in h
        assert "Sec-CH-UA-Platform" not in h

    def test_chrome_includes_sec_ch_ua(self):
        from financial_scraper.fetch.fingerprints import CHROME_WINDOWS
        h = CHROME_WINDOWS.to_headers()
        assert "Sec-CH-UA" in h
        assert "Sec-CH-UA-Mobile" in h
        assert "Sec-CH-UA-Platform" in h


class TestGetFingerprintForDomain:
    def test_deterministic(self):
        fp1 = get_fingerprint_for_domain("example.com")
        fp2 = get_fingerprint_for_domain("example.com")
        assert fp1 is fp2

    def test_returns_from_all_fingerprints(self):
        for domain in ["a.com", "b.com", "c.com", "d.com", "e.com",
                        "f.com", "g.com", "h.com", "i.com", "j.com"]:
            fp = get_fingerprint_for_domain(domain)
            assert fp in ALL_FINGERPRINTS


class TestAllFingerprints:
    def test_five_profiles_exist(self):
        assert len(ALL_FINGERPRINTS) == 5
