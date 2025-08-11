# -*- coding: utf-8 -*-
"""
Created on Mon Aug 11 10:19:17 2025

@author: TonyGuida
"""

# -*- coding: utf-8 -*-
"""
Google News scraper with DuckDuckGo fallback, Crawl4AI/Playwright robustness,
and final direct-website scrape if cache is empty.

Keeps your columns: company, title, link, snippet, date, source, full_text,
content_hash, uuid, language, thread_site. Quiet logs, 61-min first backoff.
"""

import logging
# Quiet noisy libs
logging.getLogger("charset_normalizer").setLevel(logging.WARNING)
logging.getLogger("chardet.charsetprober").setLevel(logging.WARNING)
logging.getLogger("charset_normalizer.md").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("scraper_debug.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

import json
import time
import random
import hashlib
import uuid
import threading
from datetime import datetime
from urllib.parse import quote, urlparse
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import ftfy
from bs4 import BeautifulSoup
import dateparser

# Optional providers (used if installed)
try:
    from playwright.sync_api import sync_playwright
    _HAS_PLAYWRIGHT = True
except Exception:
    _HAS_PLAYWRIGHT = False

try:
    import asyncio
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig  # pip install crawl4ai
    _HAS_CRAWL4AI = True
except Exception:
    _HAS_CRAWL4AI = False

# ----------------- CONFIG -----------------
MWORKERS = 4
FILENAME_STR_SAVE = 'macro2'
LSTNBR = 'gnews'
year_min = 2025
year_max = 2025

SOURCES = [
    "ScienceDirect.com", "nature.com", "arxiv.org", "papers.ssrn.com", "springer.com",
    "researchgate.net", "tandfonline.com", "udemy.com", "emerald.com", "mdpi.com",
    "onlinelibrary.wiley.com", "journals.sagepub.com",
]
SOURCE_MODE = "exclude"  # "include" or "exclude"
LANG = 'en'

modality = [""]  # keep as you had
df_names = [
    "GDP growth", "Inflation data", "Core inflation", "CPI report", "PPI release",
    "Tax cuts corporate earnings", "Fiscal policy private sector", "Government spending sectors",
    "Federal deficit inflation", "Student loan forgiveness retail",
    "Inflation and S&P 500", "Earnings and macro outlook", "Central bank and company guidance",
    "GDP forecast and equity returns", "Supply chain and interest rates and cost",
    "Stock market and macro trends"
]
df_names = [f'"{theme}" {keyword}' for theme in df_names for keyword in modality]

RESULTS_LIMIT = 120
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"
    )
}

# --------- GLOBAL RATE-LIMIT BACKOFF (GOOGLE) ----------
BACKOFF_LOCK = threading.Lock()
BACKOFF_UNTIL = 0             # next-allowed timestamp
BACKOFF_INCREMENT = 60 * 60   # 60 min (subsequent events)
BACKOFF_FIRST = 61 * 60       # 61 min for the first 409/429 (changed from 90)

def wait_if_in_backoff():
    now = time.time()
    with BACKOFF_LOCK:
        if BACKOFF_UNTIL > now:
            to_wait = BACKOFF_UNTIL - now
            logger.error(f"[BACKOFF] Sleeping for {to_wait/60:.1f} minutes due to rate limit.")
            time.sleep(to_wait)

def safe_request_global(session, url, **kwargs):
    """Use for Google: triggers global backoff on 409/429."""
    global BACKOFF_UNTIL
    while True:
        wait_if_in_backoff()
        resp = session.get(url, **kwargs)
        if resp.status_code not in (409, 429):
            return resp
        with BACKOFF_LOCK:
            now = time.time()
            increment = BACKOFF_FIRST if BACKOFF_UNTIL < now else BACKOFF_INCREMENT
            BACKOFF_UNTIL = max(BACKOFF_UNTIL, now) + increment
            logger.info(f"[RATE LIMIT] Hit {resp.status_code} for {url}. Backing off {int(increment/60)} min.")

def safe_request_nobackoff(session, url, **kwargs):
    """For articles/caches: skip on 409/429 without global backoff."""
    try:
        resp = session.get(url, **kwargs)
        if resp.status_code in (409, 429):
            logger.info(f"[ARTICLE RATE LIMIT] {resp.status_code} for {url} — skipping.")
            return None
        return resp
    except Exception as e:
        logger.error(f"[ARTICLE ERROR] {url}: {e}")
        return None

# ----------------- UTIL -----------------
def log(msg):
    logger.info(msg)

def generate_content_hash(text):
    base_str = f"{text[:600]}"
    return hashlib.sha1(base_str.encode("utf-8")).hexdigest()

def generate_uuid_from_content(title, link):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{title}-{link}"))

def extract_domain(url):
    try:
        return urlparse(url).netloc
    except Exception:
        return "unknown"

def convert_relative_date(date_str):
    try:
        if not date_str:
            return date_str
        if any(tok in date_str.lower() for tok in ["hour", "minute", "second"]):
            return datetime.today().strftime('%Y-%m-%d')
        parsed_date = dateparser.parse(date_str, settings={'TIMEZONE': 'UTC'})
        return parsed_date.strftime('%Y-%m-%d') if parsed_date else date_str
    except Exception:
        return date_str

def is_consent_page(text):
    return "consent.google.com" in text or "window['ppConfig']" in text

# ----------------- ARTICLE FETCHERS -----------------
def _extract_text_from_html(html: str, limit: int = 9000) -> str:
    soup = BeautifulSoup(html, "html.parser")
    paragraphs = soup.find_all("p")
    text = " ".join(p.get_text(strip=True) for p in paragraphs)
    if len(text) > limit:
        # cut at sentence boundary if possible
        cut = text[:limit].rsplit(".", 1)[0]
        return (cut + ".") if cut else text[:limit]
    return text

def get_full_article_requests(url: str, timeout: int = 20) -> str:
    session = requests.Session()
    session.headers.update(HEADERS)
    resp = safe_request_nobackoff(session, url, timeout=timeout)
    if resp is None or resp.status_code != 200:
        return ""
    return _extract_text_from_html(resp.text)

def get_cached_article(url: str, timeout: int = 20) -> str:
    session = requests.Session()
    session.headers.update(HEADERS)
    cache_url = f"https://webcache.googleusercontent.com/search?q=cache:{url}"
    resp = safe_request_nobackoff(session, cache_url, timeout=timeout)
    if resp is None or resp.status_code != 200:
        return ""
    return _extract_text_from_html(resp.text)

def get_full_article_playwright(url: str, timeout_ms: int = 60000) -> str:
    if not _HAS_PLAYWRIGHT:
        return ""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=HEADERS["User-Agent"])
            page = context.new_page()
            page.goto(url, timeout=timeout_ms)
            # crude consent handling
            try:
                page.wait_for_load_state("networkidle", timeout=timeout_ms)
            except Exception:
                pass
            html = page.content()
            page.close()
            browser.close()
        return _extract_text_from_html(html)
    except Exception as e:
        logger.info(f"[PLAYWRIGHT] Fallback failed for {url}: {e}")
        return ""

def get_full_article_crawl4ai(url: str, timeout: int = 30) -> str:
    if not _HAS_CRAWL4AI:
        return ""
    async def _run():
        cfg = CrawlerRunConfig()
        bcfg = BrowserConfig()
        async with AsyncWebCrawler(config=bcfg) as crawler:
            result = await crawler.arun(url=url, config=cfg)
            return result.cleaned_html or result.html or ""
    try:
        html = asyncio.run(_run())
        return _extract_text_from_html(html)
    except Exception as e:
        logger.info(f"[CRAWL4AI] Fallback failed for {url}: {e}")
        return ""

def get_full_article_parallel(url: str) -> str:
    """
    Robust chain:
      1) requests → (if empty) 2) Google cache → (if empty) 3) Crawl4AI → (if empty) 4) Playwright → (if empty) 5) requests again
    """
    # 1) direct
    txt = get_full_article_requests(url)
    if txt:
        return txt

    # 2) google cache
    txt = get_cached_article(url)
    if txt:
        return txt

    # 3) crawl4ai (if available)
    txt = get_full_article_crawl4ai(url)
    if txt:
        return txt

    # 4) playwright (if available)
    txt = get_full_article_playwright(url)
    if txt:
        return txt

    # 5) last resort: try direct once more (maybe transient)
    return get_full_article_requests(url)

# ----------------- SEARCH: GOOGLE NEWS -----------------
def fetch_google_news_results(query_str, year_min, year_max, sources_list=None, filter_mode="include", language="en"):
    session = requests.Session()
    session.headers.update(HEADERS)
    results = []

    q = query_str
    if sources_list and filter_mode == "include":
        sources_str = " OR ".join([f"site:{s}" for s in sources_list])
        q = f'{query_str} ({sources_str})'
    elif sources_list and filter_mode == "exclude":
        sources_str = " ".join([f"-site:{s}" for s in sources_list])
        q = f'{query_str} {sources_str}'

    logger.info(f"Fetching Google News results for {query_str} ({filter_mode}: {sources_list})...")
    url = (
        f"https://www.google.com/search?q={quote(q)}"
        f"&gl=us&hl={language}&tbm=nws&num={RESULTS_LIMIT}"
        f"&tbs=cdr:1,cd_min:{year_min},cd_max:{year_max}"
    )
    print(f"Requesting URL: {url}")
    resp = safe_request_global(session, url, timeout=20)
    logger.info(f"[GOOGLE] Status Code: {resp.status_code}")

    if is_consent_page(resp.text):
        logger.info("Consent page hit — resetting session with SOCS cookie.")
        session.cookies.clear()
        session.headers.update({"Cookie": "SOCS=CAISJQgDEhJnd3NfMjAyNDAyMTMtMF9SQzIaAmVuIAEaBgiA7bWZBg"})
        resp = safe_request_global(session, url, timeout=20)

    if resp.status_code != 200:
        return pd.DataFrame(results)

    soup = BeautifulSoup(resp.text, "html.parser")
    for el in soup.select("div.SoaBEf"):
        title = el.select_one("div.MBeuO")
        link_el = el.find("a")
        snippet = el.select_one(".GI74Re")
        date_el = el.select_one(".LfVVr")
        src_el = el.select_one(".NUnG9d span")
        title = title.get_text() if title else "No Title"
        link = link_el["href"] if link_el else "No Link"
        snippet = snippet.get_text() if snippet else "No Snippet"
        date = convert_relative_date(date_el.get_text() if date_el else "Unknown Date")
        src = src_el.get_text() if src_el else "Unknown Source"
        results.append({
            "company": query_str,
            "title": title,
            "link": link,
            "snippet": snippet,
            "date": date,
            "source": src
        })
    # polite pause (randomized)
    time.sleep(random.uniform(40, 80))
    return pd.DataFrame(results)

# ----------------- SEARCH: DUCKDUCKGO NEWS (FALLBACK) -----------------
def fetch_ddg_news_results(query_str, year_min, year_max, language="en"):
    """
    Unofficial DDG news scrape. Structure may change; built to be resilient.
    Filters by year range post-hoc using parsed date text where available.
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    # DDG news: q=...&iar=news&ia=news; region/language hint via kl
    url = f"https://duckduckgo.com/html/?q={quote(query_str)}&iar=news&ia=news&kl=us-en"
    logger.info(f"[DDG] Fetching: {url}")
    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            logger.info(f"[DDG] Non-200: {resp.status_code}")
            return pd.DataFrame([])
        soup = BeautifulSoup(resp.text, "html.parser")

        # News items often under .result--news or .result
        cards = soup.select(".result--news, .result")
        out = []
        for c in cards:
            a = c.select_one("a.result__a")
            if not a:
                a = c.find("a")
            title = a.get_text(strip=True) if a else "No Title"
            link = a["href"] if a and a.has_attr("href") else "No Link"

            # Snippet/date/source are loosely structured on DDG
            snippet_el = c.select_one(".result__snippet") or c.select_one(".result__snippet.js-result-snippet")
            snippet = snippet_el.get_text(" ", strip=True) if snippet_el else "No Snippet"
            source_el = c.select_one(".result__url__domain") or c.select_one(".result__extras__url")
            source = source_el.get_text(strip=True) if source_el else extract_domain(link)

            # Date heuristic (DDG sometimes embeds in snippet/extras)
            date_txt = ""
            meta = c.select_one(".result__extras__url")
            if meta and "|" in meta.get_text():
                # e.g., "reuters.com | 2018-06-15"
                parts = [t.strip() for t in meta.get_text().split("|")]
                if len(parts) > 1:
                    date_txt = parts[-1]
            date = convert_relative_date(date_txt) if date_txt else ""

            # Year filter (if date parsed)
            if date and date[:4].isdigit():
                y = int(date[:4])
                if not (year_min <= y <= year_max):
                    continue

            out.append({
                "company": query_str,
                "title": title,
                "link": link,
                "snippet": snippet,
                "date": date if date else "",  # may be empty if not available
                "source": source or "Unknown Source",
            })
        time.sleep(random.uniform(3, 6))
        return pd.DataFrame(out)
    except Exception as e:
        logger.info(f"[DDG] Error: {e}")
        return pd.DataFrame([])

# ----------------- MAIN LOOP -----------------
def run():
    all_dfs = []
    query_counter = 0

    for year in range(year_max, year_min - 1, -1):
        for query in df_names:
            # 1) Try Google
            df = fetch_google_news_results(
                query, year, year,
                sources_list=SOURCES, filter_mode=SOURCE_MODE, language=LANG
            )

            # 2) If Google empty, try DuckDuckGo
            if df.empty:
                logger.info(f"[FALLBACK] No Google results for '{query}' {year}. Trying DuckDuckGo...")
                df = fetch_ddg_news_results(query, year, year, language=LANG)

            # 3) If still empty, skip this query
            if df.empty:
                logger.info(f"[SKIP] No results for '{query}' in {year}.")
            else:
                urls = df["link"].tolist()
                with ThreadPoolExecutor(max_workers=MWORKERS) as ex:
                    texts = list(ex.map(get_full_article_parallel, urls))
                df["full_text"] = [ftfy.fix_text(t or "") for t in texts]
                df["content_hash"] = df.apply(lambda r: generate_content_hash(f"{r['title']} || {r['snippet']}"), axis=1)
                df["uuid"] = df.apply(lambda r: generate_uuid_from_content(r["title"], r["link"]), axis=1)
                df["language"] = LANG
                df["thread_site"] = df["link"].apply(extract_domain)
                all_dfs.append(df)

            # 4) Throttle every 10 queries
            query_counter += 1
            if query_counter % 10 == 0:
                log("[INFO] Pausing ~5 minutes after 10 queries to avoid blocks.")
                time.sleep(300)

        # ---- Save per-year ----
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)

            # Deduplicate (keep first)
            before = len(final_df)
            final_df = final_df.drop_duplicates(subset=["uuid"])
            final_df = final_df.drop_duplicates(subset=["content_hash"])
            after = len(final_df)
            logger.info(f"[DEDUP] Dropped {before - after} duplicates (uuid/content_hash).")

            out_path = f"{FILENAME_STR_SAVE}_{LSTNBR}_{year}.parquet"
            final_df.to_parquet(out_path, index=False)
            log(f"[SAVE] Data saved for {year} to {out_path}")
            all_dfs = []
        else:
            log(f"[INFO] No data collected for {year}.")

if __name__ == "__main__":
    run()
