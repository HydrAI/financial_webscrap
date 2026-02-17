"""Browser fingerprint profiles for anti-detection."""

from dataclasses import dataclass


@dataclass(frozen=True)
class BrowserFingerprint:
    name: str
    user_agent: str
    accept: str
    accept_language: str
    accept_encoding: str
    sec_ch_ua: str | None
    sec_ch_ua_mobile: str | None
    sec_ch_ua_platform: str | None
    sec_fetch_site: str
    sec_fetch_mode: str
    sec_fetch_dest: str
    upgrade_insecure_requests: str

    def to_headers(self) -> dict[str, str]:
        """Return headers dict, excluding None values."""
        h: dict[str, str] = {
            "User-Agent": self.user_agent,
            "Accept": self.accept,
            "Accept-Language": self.accept_language,
            "Accept-Encoding": self.accept_encoding,
            "Sec-Fetch-Site": self.sec_fetch_site,
            "Sec-Fetch-Mode": self.sec_fetch_mode,
            "Sec-Fetch-Dest": self.sec_fetch_dest,
            "Upgrade-Insecure-Requests": self.upgrade_insecure_requests,
        }
        if self.sec_ch_ua is not None:
            h["Sec-CH-UA"] = self.sec_ch_ua
        if self.sec_ch_ua_mobile is not None:
            h["Sec-CH-UA-Mobile"] = self.sec_ch_ua_mobile
        if self.sec_ch_ua_platform is not None:
            h["Sec-CH-UA-Platform"] = self.sec_ch_ua_platform
        return h


CHROME_WINDOWS = BrowserFingerprint(
    name="Chrome 122 Windows",
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    accept_language="en-US,en;q=0.9",
    accept_encoding="gzip, deflate, br",
    sec_ch_ua='"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    sec_ch_ua_mobile="?0",
    sec_ch_ua_platform='"Windows"',
    sec_fetch_site="none",
    sec_fetch_mode="navigate",
    sec_fetch_dest="document",
    upgrade_insecure_requests="1",
)

CHROME_MAC = BrowserFingerprint(
    name="Chrome 122 macOS",
    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    accept_language="en-US,en;q=0.9",
    accept_encoding="gzip, deflate, br",
    sec_ch_ua='"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    sec_ch_ua_mobile="?0",
    sec_ch_ua_platform='"macOS"',
    sec_fetch_site="none",
    sec_fetch_mode="navigate",
    sec_fetch_dest="document",
    upgrade_insecure_requests="1",
)

FIREFOX_WINDOWS = BrowserFingerprint(
    name="Firefox 123 Windows",
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    accept_language="en-US,en;q=0.5",
    accept_encoding="gzip, deflate, br",
    sec_ch_ua=None,
    sec_ch_ua_mobile=None,
    sec_ch_ua_platform=None,
    sec_fetch_site="none",
    sec_fetch_mode="navigate",
    sec_fetch_dest="document",
    upgrade_insecure_requests="1",
)

SAFARI_MAC = BrowserFingerprint(
    name="Safari 17 macOS",
    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    accept_language="en-US,en;q=0.9",
    accept_encoding="gzip, deflate, br",
    sec_ch_ua=None,
    sec_ch_ua_mobile=None,
    sec_ch_ua_platform=None,
    sec_fetch_site="none",
    sec_fetch_mode="navigate",
    sec_fetch_dest="document",
    upgrade_insecure_requests="1",
)

EDGE_WINDOWS = BrowserFingerprint(
    name="Edge 122 Windows",
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    accept="text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    accept_language="en-US,en;q=0.9",
    accept_encoding="gzip, deflate, br",
    sec_ch_ua='"Chromium";v="122", "Not(A:Brand";v="24", "Microsoft Edge";v="122"',
    sec_ch_ua_mobile="?0",
    sec_ch_ua_platform='"Windows"',
    sec_fetch_site="none",
    sec_fetch_mode="navigate",
    sec_fetch_dest="document",
    upgrade_insecure_requests="1",
)

ALL_FINGERPRINTS = [CHROME_WINDOWS, CHROME_MAC, FIREFOX_WINDOWS, SAFARI_MAC, EDGE_WINDOWS]


def get_fingerprint_for_domain(domain: str) -> BrowserFingerprint:
    """Deterministic: hash(domain) -> consistent profile per domain."""
    return ALL_FINGERPRINTS[hash(domain) % len(ALL_FINGERPRINTS)]
