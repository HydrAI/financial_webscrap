# search_then_scrape_multiquery.py
import os, sys, time, json, logging, hashlib, uuid, random, pathlib
from urllib.parse import urlencode, quote_plus, urlparse
import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
}
VALID_TBM = {None, "nws"}
PAYWALL_DOMAINS = {"ft.com","wsj.com","bloomberg.com","economist.com","nytimes.com"}

# ----- helpers -----
def is_pdf_url(u: str) -> bool:
    ul = u.lower()
    return ul.endswith(".pdf") or "/pdf" in ul or "contentType=pdf" in ul

def extract_domain(url: str) -> str:
    try: return urlparse(url).netloc.lower().lstrip("www.")
    except Exception: return ""

def _extract_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    main = soup.select_one("article") or soup.select_one("main") or soup.select_one("[itemprop='articleBody']")
    node = main if main else soup.body or soup
    for bad in node.select("script,style,noscript,header,footer,nav,aside"):
        bad.decompose()
    lines = [ln for ln in (node.get_text("\n", strip=True) or "").splitlines() if ln.strip()]
    return "\n".join(lines)

def safe_get(session: requests.Session, url: str, timeout=25):
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code >= 400: return None
        t = r.text.lower()
        if "captcha" in t or "recaptcha" in t or "unusual traffic" in t: return None
        return r
    except Exception:
        return None

# ----- Google search -----
CONSENT_BUTTONS = ['#L2AGLb','button:has-text("I agree")','button:has-text("Accept all")','button:has-text("Agree")','form[action*="consent"] button']
RESULT_SELECTORS = ['div#search a h3','div.MjjYud a h3','a[jsname="UWckNb"] h3','div#rso a h3','div.SoaBEf a.WlydOe']

def build_google_url(q, hl="en", lr=None, tbs=None, tbm=None, gl=None, start=0):
    p = {"q": q, "hl": hl, "num": 10, "start": start, "pws": 0, "safe": "off"}
    if lr:  p["lr"]  = lr
    if tbs: p["tbs"] = tbs
    if tbm in VALID_TBM and tbm is not None: p["tbm"] = tbm
    if gl:  p["gl"]  = gl
    return f"https://www.google.com/search?{urlencode(p, quote_via=quote_plus)}"

def maybe_accept_consent(page):
    for sel in CONSENT_BUTTONS:
        try:
            if page.locator(sel).count() > 0:
                logging.info(f"[CONSENT] {sel}")
                page.locator(sel).first.click(timeout=2000)
                page.wait_for_load_state("networkidle", timeout=8000)
                time.sleep(0.4)
                break
        except Exception:
            pass

def _is_google(u: str) -> bool:
    try: return "google." in urlparse(u).netloc.lower()
    except Exception: return True

def extract_urls_from_serp(page):
    urls, seen = [], set()
    for sel in RESULT_SELECTORS:
        nodes = page.locator(sel).element_handles()
        for h in nodes:
            try:
                href = page.evaluate("(el) => el.closest('a')?.href || ''", h)
                if href and href.startswith("http") and not _is_google(href) and href not in seen:
                    seen.add(href); urls.append(href)
            except Exception:
                continue
    return urls

def google_headless_search(query, max_results=100, hl="en", lr=None, tbs=None, tbm=None, gl=None,
                           min_pause=3.0, max_pause=5.0) -> (list, bool):
    """Returns (urls, blocked_flag)."""
    proxy_env = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"] + (["--proxy-server=" + proxy_env] if proxy_env else [])
        )
        ctx = browser.new_context(
            viewport={"width": 1366, "height": 2000},
            user_agent=HEADERS["User-Agent"],
            locale=hl,
            extra_http_headers={"Accept-Language": hl},
        )
        ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        page = ctx.new_page()

        out, seen = [], set()
        start = 0
        blocked = False

        while len(out) < max_results:
            url = build_google_url(query, hl=hl, lr=lr, tbs=tbs, tbm=tbm, gl=gl, start=start)
            logging.info(f"[NAVIGATE] {url}")
            try:
                page.goto(url, timeout=25000, wait_until="domcontentloaded")
                maybe_accept_consent(page)
                page.wait_for_load_state("networkidle", timeout=25000)
            except PWTimeout:
                logging.warning("[TIMEOUT] SERP load")

            body = (page.content()[:4000] or "").lower()
            if any(k in body for k in ["recaptcha","unusual traffic","our systems have detected"]):
                logging.error("[BLOCK] Google CAPTCHA")
                page.screenshot(path=f"debug_blocked_{start}.png", full_page=True)
                blocked = True
                break

            page.keyboard.press("End")
            page.wait_for_timeout(300)

            page_urls = extract_urls_from_serp(page)
            added = 0
            for u in page_urls:
                if u not in seen:
                    seen.add(u); out.append(u); added += 1
                    if len(out) >= max_results: break

            if added == 0: break
            start += 10
            time.sleep(random.uniform(min_pause, max_pause))

        browser.close()
        return out[:max_results], blocked

# ----- DuckDuckGo fallback -----
def ddg_html_search(query: str, max_results=100, pause=0.8):
    base = "https://duckduckgo.com/html/"
    sess = requests.Session(); sess.headers.update(HEADERS)
    urls, seen = [], set()
    s = 0
    while len(urls) < max_results:
        r = sess.get(base, params={"q": query, "s": str(s)}, timeout=25)
        if r.status_code >= 400: break
        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select("a.result__a") or soup.select("a.result__url")
        if not items: break
        for a in items:
            href = a.get("href")
            if href and href not in seen:
                seen.add(href); urls.append(href)
                if len(urls) >= max_results: break
        s += len(items)
        time.sleep(pause)
    return urls[:max_results]

# ----- multi-query orchestrator -----
def collect_urls_multiquery(base_query, subqueries, **search_kwargs):
    all_urls = []
    for sq in subqueries:
        q = f"{base_query} {sq}" if sq else base_query
        urls, blocked = google_headless_search(q, **search_kwargs)
        if blocked:
            logging.warning(f"[{sq}] Blocked in Google, falling back to DDG")
            urls += ddg_html_search(q, max_results=search_kwargs.get("max_results", 100))
        all_urls.extend(urls)
    # de-dup
    seen = set()
    uniq = []
    for u in all_urls:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

# ----- main -----
if __name__ == "__main__":
    # Example:
    # python search_then_scrape_multiquery.py "site:reuters.com commodities" en lang_en qdr:w us 100
    args = sys.argv[1:]
    base_query = args[0] if args else "site:reuters.com commodities"
    hl   = args[1] if len(args) > 1 else "en"
    lr   = args[2] if len(args) > 2 else "lang_en"
    tbs  = args[3] if len(args) > 3 else "qdr:w"
    gl   = args[4] if len(args) > 4 else "us"
    n    = int(args[5]) if len(args) > 5 else 100

    # subqueries to spread load
    subqueries = ["", "energy", "metals", "agriculture", "markets", "trading"]

    # 1) try tbm=nws first
    urls = collect_urls_multiquery(base_query, subqueries, hl=hl, lr=lr, tbs=tbs, tbm="nws", gl=gl,
                                   max_results=n, min_pause=3.0, max_pause=5.0)

    # if less than target, retry web tbm=None
    if len(urls) < n:
        extra = collect_urls_multiquery(base_query, subqueries, hl=hl, lr=lr, tbs=tbs, tbm=None, gl=gl,
                                        max_results=n, min_pause=3.0, max_pause=5.0)
        for u in extra:
            if u not in urls:
                urls.append(u)

    logging.info(f"[FINAL URL COUNT] {len(urls)}")
    pd.DataFrame({"link": urls}).to_csv("discovered_urls.csv", index=False)
