# headless_search.py
import time
import json
import sys
from typing import List
from urllib.parse import urlparse, urlunparse

def _normalize(u: str) -> str:
    try:
        p = urlparse(u)
        # drop fragments and query tracking
        return urlunparse((p.scheme or "https", p.netloc, p.path, "", "", ""))
    except Exception:
        return u

def search_ddg(query: str, max_results: int = 100, pause: float = 0.6) -> List[str]:
    """
    Prefer the duckduckgo_search library. Fallback to HTML scraping if not installed.
    Returns up to max_results unique URLs.
    """
    try:
        from duckduckgo_search import DDGS  # pip install duckduckgo_search
        urls = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, region="wt-wt", safesearch="off", max_results=max_results):
                if "href" in r:
                    urls.append(_normalize(r["href"]))
                elif "url" in r:
                    urls.append(_normalize(r["url"]))
                if len(urls) >= max_results:
                    break
        # de-dup preserving order
        seen, out = set(), []
        for u in urls:
            if u and u not in seen:
                seen.add(u)
                out.append(u)
        return out
    except Exception:
        return _search_ddg_html(query, max_results=max_results, pause=pause)

def _search_ddg_html(query: str, max_results: int = 100, pause: float = 0.8) -> List[str]:
    """
    Scrape DuckDuckGo's HTML endpoint (no JS, headless).
    """
    import requests
    from bs4 import BeautifulSoup

    base = "https://duckduckgo.com/html/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    urls, seen = [], set()
    s = 0  # offset
    while len(urls) < max_results:
        params = {"q": query, "s": str(s)}
        r = requests.get(base, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Links live under .result__a on the HTML endpoint
        items = soup.select("a.result__a") or soup.select("a.result__url")
        if not items:
            break

        added_this_page = 0
        for a in items:
            href = a.get("href")
            if not href:
                continue
            u = _normalize(href)
            if u and u not in seen:
                seen.add(u)
                urls.append(u)
                added_this_page += 1
                if len(urls) >= max_results:
                    break

        if added_this_page == 0:
            break

        s += added_this_page
        time.sleep(pause)

    return urls[:max_results]

if __name__ == "__main__":
    q = "site:reuters.com gold prices"
    if len(sys.argv) > 1:
        q = " ".join(sys.argv[1:])
    results = search_ddg(q, max_results=100)
    print(json.dumps(results, indent=2))
