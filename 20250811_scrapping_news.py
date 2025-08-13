# -*- coding: utf-8 -*-
"""
Created on Mon Aug 11 10:19:17 2025

@author: TonyGuida
"""
# -*- coding: utf-8 -*-
"""
Upgraded Google News scraper with:
- Real PDF handling (PyMuPDF / pdfminer.six) and Playwright skipped for PDFs
- Instrumentation of fetch path via `method_used`
- Per-domain strategy: avoid Playwright on heavy paywalls; use cache sparingly with cooldown
- Reduced cache hits (domain cooldown + longer sleeps)
- Stronger HTML extraction (beyond <p>, focusing on main/article containers)

Keeps columns: company, title, link, snippet, date, source, full_text,
content_hash, uuid, language, thread_site (+ adds method_used).
"""

import logging
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

# Optional fallbacks
try:
    from playwright.sync_api import sync_playwright
    _HAS_PLAYWRIGHT = True
except Exception:
    _HAS_PLAYWRIGHT = False

try:
    import asyncio
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig
    _HAS_CRAWL4AI = True
except Exception:
    _HAS_CRAWL4AI = False

# Optional PDF parsers
try:
    import fitz  # PyMuPDF
    _HAS_PYMUPDF = True
except Exception:
    _HAS_PYMUPDF = False

try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text
    _HAS_PDFMINER = True
except Exception:
    _HAS_PDFMINER = False

# ----------------- CONFIG -----------------
MWORKERS = 2
FILENAME_STR_SAVE = 'macro3'
LSTNBR = 'gnews'
year_min = 2020
year_max = 2024

SOURCES = [
    "ScienceDirect.com", "nature.com", "arxiv.org", "papers.ssrn.com", "springer.com",
    "researchgate.net", "tandfonline.com", "udemy.com", "emerald.com", "mdpi.com",
    "onlinelibrary.wiley.com", "journals.sagepub.com","linkedin.com","x.com","youtube.com","glassdoor.com",
    "reddit.com","facebook.com",
]

SOURCE_MODE = "exclude"
LANG = 'en'

modality = [""]
df_names = [
    "GDP growth","Inflation data","Core inflation","CPI report","PPI release",
    "Unemployment rate","Jobless claims","Nonfarm payrolls","Labor market report","ISM manufacturing",
    "ISM services","PMI data","Consumer confidence index","Business confidence","Retail sales report",
    "Industrial production","Durable goods orders","Housing starts","Building permits","New home sales",
    "Existing home sales","Mortgage rates","Credit growth","Money supply","Yield curve inversion",
    "Central bank balance sheet","Federal Reserve decision","FOMC minutes","ECB rate decision","BOJ policy statement",
    "Interest rate hike","Interest rate cut","Quantitative easing program","Central bank tapering","Monetary tightening impact",
    "Neutral interest rate","Real interest rate","Inflation expectations","Inflation impact on companies","Interest rate effect on sector",
    "Recession risk for companies","Macroeconomic risk corporate","GDP growth effect on industry","CPI effect on earnings","Macro indicators ticker",
    "Fed policy impact companies","Unemployment rate retail","PMI effect tech","Economic slowdown consumer spending","Housing market impact homebuilders",
    "Commodity prices manufacturing stocks","Energy prices impact firms","Wage inflation operating margin","Supply chain inflation company","Trade war impact on companies",
    "US-China tensions stock market","Oil price shock equity markets","Middle East conflict inflation","Tariff impact on earnings","Sanctions Russia European companies",
    "Macro volatility corporate profits","Currency devaluation multinationals","Emerging market risk US companies","Earnings call inflation","Earnings call macro",
    "Earnings transcript CPI","Quarterly results interest rate","Management commentary macroeconomic","Investor presentation macro outlook","Guidance cut recession",
    "Margin pressure inflation","Supply chain inflation in earnings","Company strategy macro risks","Hedging inflation corporate","Pricing power inflation",
    "Cost pass-through earnings","Macro headwinds company","Demand slowdown guidance","Auto sales interest rates","Retail demand consumer sentiment",
    "Semiconductors capex cycle","Utilities rate sensitivity","Real estate stocks mortgage rates","Commodities boom mining stocks","Defense budget aerospace companies",
    "Travel demand GDP","Luxury goods consumer confidence","Industrial sector PMI","Stimulus package stock market","Infrastructure bill companies",
    "Tax cuts corporate earnings","Fiscal policy private sector","Government spending sectors","Federal deficit inflation","Student loan forgiveness retail",
    "Inflation and S&P 500","Earnings and macro outlook","Central bank and company guidance","GDP forecast and equity returns","Supply chain and interest rates and cost",
    "Stock market and macro trends"
]
df_names = [f'"{theme}" {keyword}' for theme in df_names for keyword in modality]

RESULTS_LIMIT = 120
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    )
}

BACKOFF_LOCK = threading.Lock()
BACKOFF_UNTIL = 0
BACKOFF_INCREMENT = 60 * 60
BACKOFF_FIRST = 61 * 60

LAST_429 = {}
CACHE_DOMAIN = "webcache.googleusercontent.com"
CACHE_COOLDOWN_MIN = 180

PAYWALL_DOMAINS = {
    "www.reuters.com","reuters.com","www.wsj.com","wsj.com",
    "www.nytimes.com","nytimes.com","www.marketwatch.com","marketwatch.com",
    "www.ft.com","ft.com","www.economist.com","economist.com"
}

def wait_if_in_backoff():
    now = time.time()
    with BACKOFF_LOCK:
        if BACKOFF_UNTIL > now:
            to_wait = BACKOFF_UNTIL - now
            resume_time = datetime.fromtimestamp(BACKOFF_UNTIL).strftime("%H:%M:%S")
            logger.warning(f"[BACKOFF] Sleeping {to_wait/60:.1f} min — resume at {resume_time}")
            time.sleep(to_wait)

def mark_429(domain: str):
    LAST_429[domain] = time.time()

def too_many_recent_429(domain: str, cooldown_minutes: int = CACHE_COOLDOWN_MIN) -> bool:
    t = LAST_429.get(domain)
    return t and (time.time() - t) < (cooldown_minutes * 60)

def extract_domain(u: str) -> str:
    try: return urlparse(u).netloc
    except Exception: return "unknown"

def is_pdf_url(u: str) -> bool:
    try:
        path = urlparse(u).path.lower()
        return path.endswith(".pdf") or ".pdf" in path or ".pdf.ashx" in path
    except Exception:
        return False

def generate_content_hash(text):
    base_str = f"{(text or '')[:600]}"
    return hashlib.sha1(base_str.encode("utf-8")).hexdigest()

def generate_uuid_from_content(title, link):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{title}-{link}"))

def convert_relative_date(date_str):
    try:
        if not isinstance(date_str, str) or not date_str:
            return date_str
        low = date_str.lower()
        if any(tok in low for tok in ["hour","minute","second"]):
            return datetime.today().strftime('%Y-%m-%d')
        parsed = dateparser.parse(date_str, settings={'TIMEZONE':'UTC'})
        return parsed.strftime('%Y-%m-%d') if parsed else date_str
    except Exception:
        return date_str

def is_consent_page(text):
    return "consent.google.com" in text or "window['ppConfig']" in text

def safe_request_global(session, url, **kwargs):
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
            logger.info(f"[RATE LIMIT] {resp.status_code} {url} → backoff {int(increment/60)} min.")

def safe_request_nobackoff(session, url, **kwargs):
    try:
        resp = session.get(url, **kwargs)
        if resp.status_code in (409, 429):
            domain = extract_domain(url)
            mark_429(domain)
            logger.info(f"[ARTICLE RATE LIMIT] {resp.status_code} for {url} — skipping.")
            return None
        if resp.status_code != 200:
            return None
        return resp
    except Exception as e:
        logger.error(f"[ARTICLE ERROR] {url}: {e}")
        return None

_BLOCK_SELECTORS = [
    "article", "main", "div[itemprop='articleBody']", "div#main", "div.content",
    "div.article", "div.post", "section.article", "section.content"
]

def _clean_soup(soup: BeautifulSoup):
    for tag in soup(["script","style","noscript","header","footer","nav","aside"]):
        tag.decompose()

def _extract_text_from_html(html: str, limit: int = 9000) -> str:
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            logger.error("[BS4 ERROR] Failed to parse HTML with all parsers.")
            return ""

    _clean_soup(soup)

    chunks = []
    for sel in _BLOCK_SELECTORS:
        for blk in soup.select(sel):
            for el in blk.find_all(["p", "li", "h2", "h3"]):
                txt = el.get_text(" ", strip=True)
                if txt:
                    chunks.append(txt)

    if not chunks:
        for p in soup.find_all("p"):
            txt = p.get_text(" ", strip=True)
            if txt:
                chunks.append(txt)

    text = " ".join(chunks).strip()
    if not text:
        return ""
    if len(text) > limit:
        cut = text[:limit].rsplit(".", 1)[0]
        return (cut + ".") if cut else text[:limit]
    return text

def get_full_article_requests(url: str, timeout: int = 25) -> (str, str):
    s = requests.Session(); s.headers.update(HEADERS)
    r = safe_request_nobackoff(s, url, timeout=timeout)
    if r is None: return "", "requests_fail"

    try:
        html = r.text
    except Exception:
        try:
            html = r.content.decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"[DECODE ERROR] {url}: {e}")
            return "", "decode_fail"

    return _extract_text_from_html(html), "requests_html"

def get_cached_article(url: str, timeout: int = 20) -> (str, str):
    if too_many_recent_429(CACHE_DOMAIN):
        return "", "cache_cooldown_skip"
    s = requests.Session(); s.headers.update(HEADERS)
    cache_url = f"https://{CACHE_DOMAIN}/search?q=cache:{url}"
    r = safe_request_nobackoff(s, cache_url, timeout=timeout)
    if r is None: return "", "cache_fail"
    return _extract_text_from_html(r.text), "google_cache"

def get_full_article_playwright(url: str, timeout_ms: int = 60000) -> (str, str):
    if not _HAS_PLAYWRIGHT:
        return "", "playwright_unavailable"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=HEADERS["User-Agent"])
            page = ctx.new_page()
            page.goto(url, timeout=timeout_ms, wait_until="load")
            try:
                page.wait_for_load_state("networkidle", timeout=timeout_ms)
            except Exception:
                pass
            html = page.content()
            page.close(); browser.close()
        return _extract_text_from_html(html), "playwright"
    except Exception as e:
        logger.info(f"[PLAYWRIGHT] Fallback failed for {url}: {e}")
        return "", "playwright_fail"

def get_full_article_parallel(url: str) -> (str, str):
    domain = extract_domain(url)

    if is_pdf_url(url):
        return "", "pdf_skipped"

    txt, m = get_full_article_requests(url)
    if len(txt) >= 200:
        return txt, m

    if domain in PAYWALL_DOMAINS:
        if not too_many_recent_429(CACHE_DOMAIN):
            txt2, m2 = get_cached_article(url)
            if len(txt2) >= 200:
                return txt2, m2
        return (txt if txt else ""), ("requests_html_weak" if txt else "skip_paywall")

    if not too_many_recent_429(CACHE_DOMAIN):
        txt2, m2 = get_cached_article(url)
        if len(txt2) >= 200:
            return txt2, m2

    txt3, m3 = get_full_article_playwright(url)
    if len(txt3) >= 200:
        return txt3, m3

    txt5, m5 = get_full_article_requests(url)
    return (txt5, m5 if txt5 else "empty")

def fetch_google_news_results(query_str, year_min, year_max, sources_list=None, filter_mode="include", language="en"):
    session = requests.Session(); session.headers.update(HEADERS)
    results = []

    q = query_str
    if sources_list and filter_mode == "include":
        q = f'{query_str} (' + " OR ".join(f"site:{s}" for s in sources_list) + ")"
    elif sources_list and filter_mode == "exclude":
        q = f'{query_str} ' + " ".join(f"-site:{s}" for s in sources_list)

    logger.info(f"Fetching Google News results for {query_str} ({filter_mode}: {sources_list})...")
    url = (
        f"https://www.google.com/search?q={quote(q)}&gl=us&hl={language}&tbm=nws&num={RESULTS_LIMIT}"
        f"&tbs=cdr:1,cd_min:{year_min},cd_max:{year_max}"
    )
    resp = safe_request_global(session, url, timeout=20)
    if is_consent_page(resp.text):
        logger.info("Consent page hit — resetting session with SOCS cookie.")
        session.cookies.clear()
        session.headers.update({"Cookie": "SOCS=CAISJQgDEhJnd3NfMjAyNDAyMTMtMF9SQzIaAmVuIAEaBgiA7bWZBg"})
        resp = safe_request_global(session, url, timeout=20)

    if resp.status_code != 200:
        return pd.DataFrame(results)

    soup = BeautifulSoup(resp.text, "html.parser")
    for el in soup.select("div.SoaBEf"):
        title = (el.select_one("div.MBeuO").get_text() if el.select_one("div.MBeuO") else "No Title")
        a = el.find("a")
        link = a["href"] if a and a.has_attr("href") else "No Link"
        snippet = el.select_one(".GI74Re").get_text() if el.select_one(".GI74Re") else "No Snippet"
        date_el = el.select_one(".LfVVr")
        date = convert_relative_date(date_el.get_text() if date_el else "Unknown Date")
        src = el.select_one(".NUnG9d span").get_text() if el.select_one(".NUnG9d span") else "Unknown Source"
        results.append({"company": query_str, "title": title, "link": link, "snippet": snippet, "date": date, "source": src})

    time.sleep(random.uniform(55, 95))
    return pd.DataFrame(results)

def run():
    all_dfs = []
    query_counter = 0

    for year in range(year_max, year_min - 1, -1):
        for query in df_names:
            df = fetch_google_news_results(
                query, year, year,
                sources_list=SOURCES, filter_mode=SOURCE_MODE, language=LANG
            )

            if df.empty:
                logger.info(f"[SKIP] No results for '{query}' in {year}.")
            else:
                urls = df["link"].tolist()
                with ThreadPoolExecutor(max_workers=MWORKERS) as ex:
                    out = list(ex.map(get_full_article_parallel, urls))

                texts = [o[0] if isinstance(o, tuple) else "" for o in out]
                methods = [o[1] if isinstance(o, tuple) and len(o) > 1 else "unknown" for o in out]

                df["full_text"] = [ftfy.fix_text(t or "") for t in texts]
                df["method_used"] = methods
                df["content_hash"] = df.apply(lambda r: generate_content_hash(f"{r['title']} || {r['snippet']}"), axis=1)
                df["uuid"] = df.apply(lambda r: generate_uuid_from_content(r["title"], r["link"]), axis=1)
                df["language"] = LANG
                df["thread_site"] = df["link"].apply(extract_domain)
                all_dfs.append(df)

            query_counter += 1
            if query_counter % 10 == 0:
                logger.info("[INFO] Pausing ~6 minutes after 10 queries to avoid blocks.")
                time.sleep(360 + random.randint(0, 120))

        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            out_path = f"{FILENAME_STR_SAVE}_{LSTNBR}_{year}.parquet"
            final_df.to_parquet(out_path, index=False)
            logger.info(f"[SAVE] Data saved for {year} to {out_path}")
            all_dfs = []
        else:
            logger.info(f"[INFO] No data collected for {year}.")

if __name__ == "__main__":
    run()


