"""Probe Rev.com as a potential earnings call transcript source.

Checks:
1. Does rev.com/blog/transcripts have earnings call transcripts?
2. What's the sitemap structure?
3. Can we extract content?
4. What's the coverage (years, companies)?
"""
import re
import time
import requests
from lxml import etree, html

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

s = requests.Session()
s.headers["User-Agent"] = UA


def check_robots():
    """Check robots.txt for rev.com."""
    print("=" * 60)
    print("1. Checking robots.txt")
    print("=" * 60)
    try:
        resp = s.get("https://www.rev.com/robots.txt", timeout=15)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            # Show relevant lines
            for line in resp.text.split("\n"):
                line = line.strip()
                if line and not line.startswith("#"):
                    if "blog" in line.lower() or "transcript" in line.lower() or "sitemap" in line.lower() or "disallow: /" == line.lower() or line.startswith("User-agent"):
                        print(f"  {line}")
        print()
    except Exception as e:
        print(f"Error: {e}\n")


def check_sitemaps():
    """Look for sitemaps that might contain transcript URLs."""
    print("=" * 60)
    print("2. Checking sitemaps")
    print("=" * 60)

    sitemap_urls = [
        "https://www.rev.com/sitemap.xml",
        "https://www.rev.com/sitemap_index.xml",
        "https://www.rev.com/blog/sitemap.xml",
        "https://www.rev.com/blog/sitemap_index.xml",
    ]

    for url in sitemap_urls:
        try:
            resp = s.get(url, timeout=15)
            print(f"  {url}: HTTP {resp.status_code}")
            if resp.status_code == 200:
                # Check if it's XML
                if "xml" in resp.headers.get("content-type", "").lower() or resp.text.strip().startswith("<?xml"):
                    try:
                        root = etree.fromstring(resp.content)
                        ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                        locs = [loc.text for loc in root.findall(".//s:loc", ns)]
                        print(f"    Contains {len(locs)} URLs")
                        # Show transcript-related ones
                        transcript_locs = [u for u in locs if "transcript" in u.lower()]
                        if transcript_locs:
                            print(f"    Transcript-related: {len(transcript_locs)}")
                            for u in transcript_locs[:10]:
                                print(f"      {u}")
                            if len(transcript_locs) > 10:
                                print(f"      ... and {len(transcript_locs) - 10} more")
                        else:
                            # Show a sample
                            for u in locs[:5]:
                                print(f"      {u}")
                    except Exception as e:
                        print(f"    XML parse error: {e}")
                        print(f"    First 500 chars: {resp.text[:500]}")
                else:
                    print(f"    Not XML: {resp.text[:200]}")
        except Exception as e:
            print(f"  {url}: Error: {e}")
        time.sleep(1)
    print()


def check_transcript_listing():
    """Check if the transcript listing page works."""
    print("=" * 60)
    print("3. Checking transcript listing page")
    print("=" * 60)

    urls = [
        "https://www.rev.com/blog/transcripts",
        "https://www.rev.com/blog/transcript-category/earnings-call-transcripts",
        "https://www.rev.com/blog/category/transcripts",
    ]

    for url in urls:
        try:
            resp = s.get(url, timeout=15, allow_redirects=True)
            print(f"  {url}")
            print(f"    Status: {resp.status_code}, Final URL: {resp.url}")
            if resp.status_code == 200:
                tree = html.fromstring(resp.text)
                title = tree.xpath("//title/text()")
                print(f"    Title: {title[0].strip() if title else 'N/A'}")

                # Look for transcript links
                links = tree.xpath("//a/@href")
                transcript_links = [l for l in links if "transcript" in l.lower() and "earnings" in l.lower()]
                if transcript_links:
                    print(f"    Earnings transcript links: {len(transcript_links)}")
                    for l in transcript_links[:10]:
                        print(f"      {l}")
                else:
                    # Any transcript links at all?
                    any_transcript = [l for l in links if "transcript" in l.lower()]
                    print(f"    Any transcript links: {len(any_transcript)}")
                    for l in any_transcript[:10]:
                        print(f"      {l}")

                # Check for earnings-related content
                text = " ".join(tree.itertext())
                earnings_mentions = len(re.findall(r"earnings\s+call", text, re.IGNORECASE))
                print(f"    'earnings call' mentions: {earnings_mentions}")
        except Exception as e:
            print(f"  {url}: Error: {e}")
        time.sleep(1)
    print()


def check_sample_transcript():
    """Try to find and fetch a sample earnings call transcript."""
    print("=" * 60)
    print("4. Trying to find a sample earnings call transcript")
    print("=" * 60)

    # Google-style search patterns for Rev.com earnings transcripts
    test_urls = [
        "https://www.rev.com/blog/transcripts/apple-aapl-q4-2024-earnings-call-transcript",
        "https://www.rev.com/blog/transcripts/apple-aapl-q3-2024-earnings-call-transcript",
        "https://www.rev.com/blog/transcripts/microsoft-msft-q4-2024-earnings-call-transcript",
        "https://www.rev.com/blog/transcripts/nvidia-nvda-q3-2025-earnings-call-transcript",
        "https://www.rev.com/blog/transcripts/amazon-amzn-q4-2024-earnings-call-transcript",
    ]

    found_one = False
    for url in test_urls:
        try:
            resp = s.get(url, timeout=15, allow_redirects=True)
            print(f"  {url.split('/')[-1]}")
            print(f"    Status: {resp.status_code}")
            if resp.status_code == 200:
                found_one = True
                tree = html.fromstring(resp.text)
                title = tree.xpath("//title/text()")
                print(f"    Title: {title[0].strip() if title else 'N/A'}")

                # Try to extract transcript content
                # Common selectors
                for selector in [
                    "//div[contains(@class,'transcript')]",
                    "//div[contains(@class,'fl-rich-text')]",
                    "//article",
                    "//div[contains(@class,'post-content')]",
                    "//div[contains(@class,'entry-content')]",
                ]:
                    elems = tree.xpath(selector)
                    if elems:
                        text = " ".join(elems[0].itertext()).strip()
                        if len(text) > 500:
                            print(f"    Selector: {selector}")
                            print(f"    Content length: {len(text)} chars")
                            print(f"    First 300 chars: {text[:300]}...")
                            break

                # Check for paywall indicators
                page_text = resp.text.lower()
                if "paywall" in page_text or "subscribe" in page_text[:2000] or "premium" in page_text[:2000]:
                    print(f"    WARNING: Possible paywall detected")
                if "sign in" in page_text[:2000] or "log in" in page_text[:2000]:
                    print(f"    WARNING: Possible login wall")
                break  # Found a working page, stop
        except Exception as e:
            print(f"    Error: {e}")
        time.sleep(1)

    if not found_one:
        print("  No working transcript pages found")
    print()


def estimate_coverage():
    """Try to estimate coverage via sitemap or listing pages."""
    print("=" * 60)
    print("5. Estimating coverage")
    print("=" * 60)

    # Try paginated listing
    page_url = "https://www.rev.com/blog/transcripts"
    try:
        resp = s.get(page_url, timeout=15)
        if resp.status_code == 200:
            tree = html.fromstring(resp.text)
            # Look for pagination
            pagination = tree.xpath("//a[contains(@class,'page')]/@href")
            if pagination:
                print(f"  Pagination links: {len(pagination)}")
                # Try to find the last page number
                pages = []
                for p in pagination:
                    m = re.search(r"page/(\d+)", p)
                    if m:
                        pages.append(int(m.group(1)))
                if pages:
                    max_page = max(pages)
                    print(f"  Max page: {max_page}")
                    print(f"  Estimated total transcripts: ~{max_page * 10} (assuming 10/page)")

            # Count earnings-related links on first page
            links = tree.xpath("//a/@href")
            earnings = [l for l in links if "earnings" in l.lower() and "transcript" in l.lower()]
            print(f"  Earnings transcript links on page 1: {len(earnings)}")
    except Exception as e:
        print(f"  Error: {e}")
    print()


if __name__ == "__main__":
    check_robots()
    check_sitemaps()
    check_transcript_listing()
    check_sample_transcript()
    estimate_coverage()

    print("=" * 60)
    print("PROBE COMPLETE")
    print("=" * 60)
