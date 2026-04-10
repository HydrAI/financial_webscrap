#!/usr/bin/env python3
"""Two-level simple crawl for domains that failed sitemap discovery.

Level 1: Fetch homepage -> extract all internal links
Level 2: Fetch each discovered link -> extract content

Uses crawl4ai simple mode (no BFS strategy) which is reliable on Windows.
"""

import asyncio
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse, urljoin

import pandas as pd
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# URL path fragments to skip
SKIP_FRAGMENTS = [
    "/login", "/signin", "/register", "/account",
    "/career", "/jobs", "/vacancies", "/applicant", "/talent",
    "/cookie", "/privacy", "/terms", "/legal", "/disclaimer",
    "/search", "/faq", "/video/", "/media-library", "/image-gallery",
    "javascript:", "mailto:", "#",
]

SEEDS_FILE = Path("financial_scraper/config/commodities_seeds_deep.txt")
OUTPUT_DIR = Path("commodities_crawl/deep")
MAX_PAGES_PER_DOMAIN = 500
MIN_WORDS = 50


def should_skip(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(frag in path or frag in url.lower() for frag in SKIP_FRAGMENTS)


def extract_internal_links(html: str, base_url: str) -> list[str]:
    """Extract all internal links from HTML."""
    base_domain = urlparse(base_url).netloc.lower().replace("www.", "")
    links = set()
    for match in re.finditer(r'href=["\']([^"\']+)["\']', html, re.IGNORECASE):
        href = match.group(1).strip()
        if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
            continue
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        domain = parsed.netloc.lower().replace("www.", "")
        if domain != base_domain:
            continue
        # Normalize
        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if clean.endswith("/"):
            clean = clean[:-1]
        if not should_skip(clean):
            links.add(clean)
    return sorted(links)


async def crawl_domain(crawler, config, seed_url: str) -> list[dict]:
    """Crawl a single domain: homepage + discovered links."""
    domain = urlparse(seed_url).netloc.lower().replace("www.", "")
    logger.info(f"[{domain}] Starting homepage fetch")

    records = []
    seen_urls = set()

    # Level 1: Fetch homepage
    try:
        result = await crawler.arun(url=seed_url, config=config)
        if isinstance(result, list):
            result = result[0]
    except Exception as e:
        logger.warning(f"[{domain}] Homepage fetch failed: {e}")
        return records

    if not result.success:
        logger.warning(f"[{domain}] Homepage not successful: {getattr(result, 'error_message', '?')}")
        return records

    html = result.html or ""
    text = result.markdown or ""
    word_count = len(text.split())

    if word_count >= MIN_WORDS:
        records.append({
            "company": domain,
            "title": getattr(result, "title", "") or "",
            "link": seed_url,
            "snippet": (text[:300] + "...") if len(text) > 300 else text,
            "date": "",
            "source": domain,
            "full_text": text,
            "source_file": f"crawl_deep/{domain}",
        })
    seen_urls.add(seed_url)

    # Extract internal links from homepage
    links = extract_internal_links(html, seed_url)
    logger.info(f"[{domain}] Homepage: {word_count} words, {len(links)} internal links discovered")

    if not links:
        return records

    # Level 2: Fetch discovered links (cap at MAX_PAGES_PER_DOMAIN)
    links = links[:MAX_PAGES_PER_DOMAIN]
    fetched = 0
    failed = 0

    for i, url in enumerate(links):
        if url in seen_urls:
            continue
        seen_urls.add(url)

        try:
            r = await crawler.arun(url=url, config=config)
            if isinstance(r, list):
                r = r[0]
        except Exception:
            failed += 1
            continue

        if not r.success:
            failed += 1
            continue

        text = r.markdown or ""
        wc = len(text.split())
        if wc < MIN_WORDS:
            failed += 1
            continue

        records.append({
            "company": domain,
            "title": getattr(r, "title", "") or "",
            "link": url,
            "snippet": (text[:300] + "...") if len(text) > 300 else text,
            "date": "",
            "source": domain,
            "full_text": text,
            "source_file": f"crawl_deep/{domain}",
        })
        fetched += 1

        if (i + 1) % 50 == 0:
            logger.info(f"[{domain}] Progress: {i+1}/{len(links)}, {fetched} extracted, {failed} failed")

        # Be polite
        await asyncio.sleep(1.0)

    logger.info(f"[{domain}] Done: {fetched} pages extracted, {failed} failed")
    return records


async def main():
    # Load seeds
    seeds = [line.strip() for line in open(SEEDS_FILE, encoding="utf-8") if line.strip()]
    logger.info(f"Loaded {len(seeds)} seeds for deep crawl")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    bc = BrowserConfig(verbose=False)
    rc = CrawlerRunConfig(
        exclude_all_images=True,
        exclude_external_images=True,
        exclude_social_media_links=True,
        mean_delay=2.0,
        max_range=1.0,
    )

    all_records = []

    async with AsyncWebCrawler(config=bc) as crawler:
        for i, seed in enumerate(seeds):
            domain = urlparse(seed).netloc.lower().replace("www.", "")
            logger.info(f"\n[{i+1}/{len(seeds)}] === {domain} ===")

            records = await crawl_domain(crawler, rc, seed)
            all_records.extend(records)

            # Save incrementally every 5 domains
            if (i + 1) % 5 == 0 and all_records:
                df = pd.DataFrame(all_records)
                out = OUTPUT_DIR / "deep_crawl_partial.parquet"
                df.to_parquet(out, index=False)
                logger.info(f"Checkpoint: {len(df)} rows saved to {out}")

    # Final save
    if all_records:
        df = pd.DataFrame(all_records)
        out = OUTPUT_DIR / "deep_crawl.parquet"
        df.to_parquet(out, index=False)
        logger.info(f"\nFinal: {len(df)} rows saved to {out}")

        # Stats
        logger.info(f"Total words: {df.full_text.str.split().str.len().sum():,}")
        logger.info(f"Domains: {df.company.nunique()}")
        for domain, count in df.company.value_counts().items():
            logger.info(f"  {domain}: {count} pages")
    else:
        logger.warning("No records extracted!")


if __name__ == "__main__":
    asyncio.run(main())
