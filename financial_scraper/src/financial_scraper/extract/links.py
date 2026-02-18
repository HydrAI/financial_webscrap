"""Extract and filter links from HTML pages for crawling."""

import logging
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

ASSET_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp", ".bmp",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv", ".flv",
    ".zip", ".gz", ".tar", ".rar", ".7z",
    ".exe", ".dmg", ".msi",
})


def _base_domain(hostname: str) -> str:
    """Extract base domain from hostname (e.g. 'blog.reuters.com' -> 'reuters.com')."""
    parts = hostname.lower().split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return hostname.lower()


def extract_links(html: str, base_url: str) -> list[str]:
    """Extract unique absolute URLs from HTML anchor tags.

    - Resolves relative URLs against base_url
    - Strips fragments
    - Skips javascript:, mailto:, and asset extensions
    - Deduplicates
    """
    soup = BeautifulSoup(html, "lxml")
    seen = set()
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()

        if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue

        absolute = urljoin(base_url, href)

        # Strip fragment
        parsed = urlparse(absolute)
        if parsed.scheme not in ("http", "https"):
            continue

        clean = parsed._replace(fragment="").geturl()

        # Skip asset extensions
        path_lower = parsed.path.lower()
        if any(path_lower.endswith(ext) for ext in ASSET_EXTENSIONS):
            continue

        if clean not in seen:
            seen.add(clean)
            links.append(clean)

    return links


def filter_links_same_domain(
    links: list[str],
    source_domain: str,
    exclusions: set[str],
    seen_urls: set[str],
    domain_page_counts: dict[str, int],
    max_pages_per_domain: int,
) -> list[str]:
    """Filter links to same base domain, applying exclusions and caps.

    Args:
        links: Candidate URLs to filter.
        source_domain: The domain of the page these links were found on.
        exclusions: Set of excluded domain strings.
        seen_urls: URLs already fetched or queued.
        domain_page_counts: Counter of pages fetched per domain.
        max_pages_per_domain: Cap per domain.

    Returns:
        Filtered list of URLs.
    """
    source_base = _base_domain(source_domain)
    filtered = []

    for url in links:
        parsed = urlparse(url)
        hostname = parsed.netloc.lower()
        link_base = _base_domain(hostname)

        # Same base domain check
        if link_base != source_base:
            continue

        # Exclusion check
        domain_clean = hostname.replace("www.", "").split(":")[0]
        if domain_clean in exclusions:
            continue

        # Already seen
        if url in seen_urls:
            continue

        # Domain cap
        if domain_page_counts.get(hostname, 0) >= max_pages_per_domain:
            continue

        # Asset extension (double-check)
        path_lower = parsed.path.lower()
        if any(path_lower.endswith(ext) for ext in ASSET_EXTENSIONS):
            continue

        filtered.append(url)

    return filtered
