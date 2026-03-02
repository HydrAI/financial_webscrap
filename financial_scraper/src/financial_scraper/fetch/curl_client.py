"""HTTP client wrapping curl_cffi for TLS fingerprint impersonation.

curl_cffi impersonates real browser TLS fingerprints (JA3/JA4 hash, HTTP/2
settings, cipher suites) — the #1 technique for bypassing bot detection
without a full browser.

Falls back to ``requests.Session`` when curl_cffi is not installed.
"""

import logging
import random
import threading

logger = logging.getLogger(__name__)

_BROWSERS = ("chrome", "firefox", "safari", "edge")


def _select_random_browser() -> str:
    return random.choice(_BROWSERS)


try:
    from curl_cffi.requests import Session as _CurlSession

    _HAS_CURL_CFFI = True
except ImportError:
    _CurlSession = None  # type: ignore[assignment,misc]
    _HAS_CURL_CFFI = False


class CurlSession:
    """Sync HTTP session with browser TLS fingerprint impersonation.

    Thread-safe: each thread gets its own underlying session via
    ``threading.local()``, so the class works safely with
    ``ThreadPoolExecutor``.
    """

    def __init__(
        self,
        browser: str | None = None,
        proxy: str | None = None,
        headers: dict[str, str] | None = None,
    ):
        self._browser = browser or _select_random_browser()
        self._proxy = proxy
        self._headers: dict[str, str] = dict(headers) if headers else {}
        self._local = threading.local()

    # ---- public API (requests.Session compatible) -------------------------

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    def get(self, url: str, **kwargs):
        """Send a GET request. Returns a response with .status_code, .text, .headers."""
        return self._get_session().get(url, **kwargs)

    def post(self, url: str, **kwargs):
        return self._get_session().post(url, **kwargs)

    def set_proxy(self, proxy: str | None):
        """Update proxy for all future sessions (existing thread-local sessions are recreated)."""
        self._proxy = proxy
        # Force re-creation on next access
        if hasattr(self._local, "session"):
            try:
                self._local.session.close()
            except Exception:
                pass
            del self._local.session

    def rotate_browser(self):
        """Switch to a different random browser impersonation."""
        if not _HAS_CURL_CFFI:
            return
        new = _select_random_browser()
        while new == self._browser and len(_BROWSERS) > 1:
            new = _select_random_browser()
        self._browser = new
        # Force re-creation of thread-local sessions
        if hasattr(self._local, "session"):
            try:
                self._local.session.close()
            except Exception:
                pass
            del self._local.session

    def close(self):
        if hasattr(self._local, "session"):
            try:
                self._local.session.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- internals --------------------------------------------------------

    def _get_session(self):
        """Return a thread-local session, creating one if needed."""
        if not hasattr(self._local, "session"):
            self._local.session = self._build_session()
        return self._local.session

    def _build_session(self):
        if _HAS_CURL_CFFI:
            s = _CurlSession(impersonate=self._browser)
            logger.debug("curl_cffi session (impersonate=%s)", self._browser)
        else:
            import requests

            s = requests.Session()
            logger.debug("Fallback to requests.Session (curl_cffi unavailable)")

        s.headers.update(self._headers)
        if self._proxy:
            s.proxies = {"http": self._proxy, "https": self._proxy}
        return s
