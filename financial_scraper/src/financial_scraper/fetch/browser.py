"""Playwright-based fetcher for JS-rendered or Cloudflare-protected pages.

Optional module — requires ``playwright`` to be installed.
Used as a final fallback when static HTTP fetching fails.
"""

import logging

logger = logging.getLogger(__name__)

try:
    from playwright.sync_api import sync_playwright

    _HAS_PLAYWRIGHT = True
except ImportError:
    _HAS_PLAYWRIGHT = False


class BrowserFetcher:
    """Fetch pages using a real browser via Playwright.

    Args:
        headless: Run browser in headless mode (default True).
        proxy: Optional proxy URL (e.g. ``"http://host:port"``).
    """

    def __init__(self, headless: bool = True, proxy: str | None = None):
        if not _HAS_PLAYWRIGHT:
            raise ImportError(
                "playwright is required for BrowserFetcher. "
                "Install with: pip install playwright && playwright install chromium"
            )
        self._headless = headless
        self._proxy = proxy
        self._pw = None
        self._browser = None

    def _ensure_browser(self):
        """Lazily launch browser on first use."""
        if self._browser is not None:
            return

        self._pw = sync_playwright().start()
        launch_kwargs = {"headless": self._headless}
        if self._proxy:
            launch_kwargs["proxy"] = {"server": self._proxy}

        self._browser = self._pw.chromium.launch(**launch_kwargs)
        logger.info("Playwright browser launched (headless=%s)", self._headless)

    def fetch(
        self,
        url: str,
        wait_selector: str | None = None,
        timeout: int = 30_000,
    ) -> str | None:
        """Fetch a URL and return the rendered HTML.

        Args:
            url: The URL to fetch.
            wait_selector: Optional CSS selector to wait for before capturing HTML.
            timeout: Navigation timeout in milliseconds.

        Returns:
            Rendered HTML string, or None on failure.
        """
        self._ensure_browser()
        context = None
        page = None
        try:
            context = self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                java_script_enabled=True,
            )
            page = context.new_page()
            page.goto(url, timeout=timeout, wait_until="domcontentloaded")

            if wait_selector:
                page.wait_for_selector(wait_selector, timeout=timeout)
            else:
                # Default: wait for network to settle
                page.wait_for_load_state("networkidle", timeout=timeout)

            html = page.content()
            logger.info("Browser fetched %s (%d bytes)", url, len(html))
            return html

        except Exception as e:
            logger.warning("Browser fetch failed for %s: %s", url, e)
            return None
        finally:
            if page:
                try:
                    page.close()
                except Exception:
                    pass
            if context:
                try:
                    context.close()
                except Exception:
                    pass

    def close(self):
        """Shut down the browser."""
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._pw:
            try:
                self._pw.stop()
            except Exception:
                pass
            self._pw = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
