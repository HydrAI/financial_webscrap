#!/usr/bin/env python3
"""Discover sitemaps and generate seed URLs for commodity company crawls.

Reads commodities_companies.csv, fetches robots.txt → sitemap XML for each
domain, extracts content URLs (filtering out careers/login/cookie junk),
and writes per-sector + combined seed files.
"""

import csv
import re
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parents[2]  # financial_webscrap/
CONFIG = ROOT / "financial_scraper" / "config"
CSV_PATH = CONFIG / "commodities_companies.csv"
OUTPUT_DIR = CONFIG

# URL path fragments to skip
SKIP_FRAGMENTS = [
    "/career", "/jobs", "/vacancies", "/applicant", "/talent",
    "/login", "/signin", "/register", "/account",
    "/cookie", "/privacy", "/terms", "/legal", "/disclaimer",
    "/search", "/faq",
    "/media-library", "/image-gallery",
    "/video/",
]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; FinScraper/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})


def fetch(url: str, timeout: int = 15) -> str | None:
    """GET with retry on 429."""
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if r.status_code >= 400:
                return None
            return r.text
        except requests.RequestException:
            return None
    return None


def extract_sitemap_urls_from_robots(robots_text: str, base_url: str) -> list[str]:
    """Parse Sitemap: directives from robots.txt."""
    urls = []
    for line in robots_text.splitlines():
        line = line.strip()
        if line.lower().startswith("sitemap:"):
            url = line.split(":", 1)[1].strip()
            if url:
                urls.append(url)
    return urls


def parse_sitemap(xml_text: str) -> list[str]:
    """Extract <loc> URLs from a sitemap or sitemap index XML."""
    urls = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return urls

    # Strip namespace
    ns = re.match(r"\{.*\}", root.tag)
    ns_prefix = ns.group() if ns else ""

    for loc in root.iter(f"{ns_prefix}loc"):
        if loc.text:
            urls.append(loc.text.strip())
    return urls


def is_sitemap_index(xml_text: str) -> bool:
    """Check if the XML is a sitemap index (contains sub-sitemaps)."""
    return "<sitemapindex" in xml_text[:500].lower()


def should_skip(url: str) -> bool:
    """Return True if the URL path matches a skip fragment."""
    path = urlparse(url).path.lower()
    return any(frag in path for frag in SKIP_FRAGMENTS)


def discover_seeds(domain: str) -> list[str]:
    """Discover seed URLs for a domain via robots.txt → sitemap chain."""
    base = f"https://{domain}"

    # 1. Try robots.txt
    robots = fetch(f"{base}/robots.txt")
    sitemap_urls = []
    if robots:
        sitemap_urls = extract_sitemap_urls_from_robots(robots, base)

    # 2. If no sitemap in robots, try common sitemap locations
    if not sitemap_urls:
        for path in ["/sitemap.xml", "/sitemap_index.xml", "/sitemap/"]:
            test_url = f"{base}{path}"
            resp = fetch(test_url)
            if resp and "<urlset" in resp[:500].lower() or (resp and "<sitemapindex" in resp[:500].lower()):
                sitemap_urls = [test_url]
                break

    if not sitemap_urls:
        print(f"  [{domain}] No sitemap found, using homepage as seed")
        return [f"{base}/"]

    # 3. Fetch sitemaps (handle sitemap index → sub-sitemaps)
    all_urls = []
    visited_sitemaps = set()

    def process_sitemap(url: str, depth: int = 0):
        if url in visited_sitemaps or depth > 2:
            return
        visited_sitemaps.add(url)
        xml = fetch(url, timeout=30)
        if not xml:
            return
        if is_sitemap_index(xml):
            sub_urls = parse_sitemap(xml)
            print(f"  [{domain}] Sitemap index {url} -> {len(sub_urls)} sub-sitemaps")
            for sub in sub_urls[:20]:  # cap to avoid huge index trees
                process_sitemap(sub, depth + 1)
                time.sleep(0.3)
        else:
            urls = parse_sitemap(xml)
            print(f"  [{domain}] Sitemap {url} -> {len(urls)} URLs")
            all_urls.extend(urls)

    for sm_url in sitemap_urls:
        process_sitemap(sm_url)
        time.sleep(0.5)

    # 4. Filter out junk URLs
    filtered = [u for u in all_urls if not should_skip(u)]

    # 5. Cap at 2000 URLs per domain to keep seed files manageable
    if len(filtered) > 2000:
        print(f"  [{domain}] Capping {len(filtered)} -> 2000 URLs")
        filtered = filtered[:2000]

    if not filtered:
        print(f"  [{domain}] Sitemap found but all URLs filtered, using homepage")
        return [f"{base}/"]

    print(f"  [{domain}] {len(filtered)} seed URLs after filtering")
    return filtered


def main():
    # Read CSV
    with open(CSV_PATH) as f:
        rows = list(csv.DictReader(f))

    print(f"Loaded {len(rows)} companies from {CSV_PATH}")

    sector_seeds: dict[str, list[str]] = defaultdict(list)
    all_seeds: list[str] = []

    for row in rows:
        company = row["company"]
        domain = row["domain"]
        sector = row["sector"]

        print(f"\n[{company}] ({domain}) — sector: {sector}")
        seeds = discover_seeds(domain)
        sector_seeds[sector].extend(seeds)
        all_seeds.extend(seeds)

        # Be polite between domains
        time.sleep(1)

    # Write per-sector seed files
    for sector, urls in sector_seeds.items():
        out = OUTPUT_DIR / f"commodities_seeds_{sector}.txt"
        with open(out, "w") as f:
            f.write("\n".join(urls) + "\n")
        print(f"\nWrote {len(urls)} URLs -> {out}")

    # Write combined
    combined = OUTPUT_DIR / "commodities_seeds_all.txt"
    with open(combined, "w") as f:
        f.write("\n".join(all_seeds) + "\n")
    print(f"\nWrote {len(all_seeds)} total URLs -> {combined}")

    # Summary
    print("\n=== Summary ===")
    for sector, urls in sorted(sector_seeds.items()):
        print(f"  {sector}: {len(urls)} URLs")
    print(f"  TOTAL: {len(all_seeds)} URLs")


if __name__ == "__main__":
    main()
