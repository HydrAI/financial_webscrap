# 20250813_headless_playwright_fix.py
import json, sys, time, logging, pathlib
from urllib.parse import urlencode, quote_plus, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

VALID_TBM = {None, "nws"}  # add more if needed: "vid", "shop", etc.

def build_google_url(q, hl="en", lr=None, tbs=None, tbm=None, gl=None, start=0):
    p = {"q": q, "hl": hl, "num": 10, "start": start, "pws": 0, "safe": "off"}
    if lr:  p["lr"]  = lr                # e.g., lang_en
    if tbs: p["tbs"] = tbs               # e.g., qdr:w or cdr:1,cd_min:08/01/2025,cd_max:08/13/2025
    if tbm in VALID_TBM and tbm is not None: p["tbm"] = tbm  # only if valid
    if gl:  p["gl"]  = gl                # e.g., us, gb, ch
    return f"https://www.google.com/search?{urlencode(p, quote_via=quote_plus)}"

def is_google(u: str) -> bool:
    try:
        host = urlparse(u).netloc.lower()
        return "google." in host or host in {"webcache.googleusercontent.com"}
    except Exception:
        return True

CONSENT_BUTTONS = ['#L2AGLb','button:has-text("I agree")','button:has-text("Accept all")','button:has-text("Agree")','form[action*="consent"] button']
RESULT_SELECTORS = ['div#search a h3','div.MjjYud a h3','a[jsname="UWckNb"] h3','div#rso a h3','div.SoaBEf a.WlydOe']  # includes News

def maybe_accept_consent(page):
    for sel in CONSENT_BUTTONS:
        try:
            if page.locator(sel).count() > 0:
                logging.info(f"[CONSENT] Clicking {sel}")
                page.locator(sel).first.click(timeout=2000)
                page.wait_for_load_state("networkidle", timeout=8000)
                time.sleep(0.4)
                break
        except Exception:
            pass

def extract_urls(page):
    urls, seen = [], set()
    any_sel = False
    for sel in RESULT_SELECTORS:
        nodes = page.locator(sel).element_handles()
        if nodes:
            any_sel = True
            logging.info(f"[EXTRACT] {sel} -> {len(nodes)}")
        for h in nodes:
            try:
                href = page.evaluate("(el) => el.closest('a')?.href || ''", h)
                if href and href.startswith("http") and not is_google(href) and href not in seen:
                    seen.add(href); urls.append(href)
            except Exception:
                continue
    if not any_sel:
        logging.warning("[EXTRACT] No known selectors matched")
    return urls

def debug_dump(page, tag):
    try:
        page.screenshot(path=f"debug_{tag}.png", full_page=True)
        pathlib.Path(f"debug_{tag}.html").write_text(page.content(), encoding="utf-8")
        logging.info(f"[DEBUG] Wrote debug_{tag}.png and debug_{tag}.html")
    except Exception:
        pass

def google_headless_search(query, max_results=100, hl="en", lr=None, tbs=None, tbm=None, gl=None, timeout_ms=20000):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled","--no-default-browser-check","--no-first-run"],
        )
        ctx = browser.new_context(
            viewport={"width": 1366, "height": 2000},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale=hl,
            extra_http_headers={"Accept-Language": hl},
        )
        ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        page = ctx.new_page()

        out, seen = [], set()
        start = 0
        while len(out) < max_results:
            url = build_google_url(query, hl=hl, lr=lr, tbs=tbs, tbm=tbm, gl=gl, start=start)
            logging.info(f"[NAVIGATE] {url}")
            try:
                page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
                maybe_accept_consent(page)
                page.wait_for_load_state("networkidle", timeout=timeout_ms)
            except PWTimeout:
                logging.warning("[TIMEOUT] load")
            logging.info(f"[TITLE] {page.title()} | [URL] {page.url}")

            if any(kw in page.title().lower() for kw in ["error 400", "bad request"]):
                debug_dump(page, f"badreq_{start}")
                logging.error("[STOP] Bad request. Check params (tbm, tbs).")
                break
            if "sorry" in page.url.lower():
                logging.error("[BLOCK] Rate limit page")
                debug_dump(page, f"blocked_{start}")
                break

            for _ in range(3):
                page.keyboard.press("End")
                page.wait_for_timeout(350)

            page_urls = extract_urls(page)
            logging.info(f"[PAGE {start//10+1}] +{len(page_urls)}")
            added = 0
            for u in page_urls:
                if u not in seen:
                    seen.add(u); out.append(u); added += 1
                    if len(out) >= max_results:
                        break

            logging.info(f"[TOTAL] {len(out)}")
            if added == 0:
                debug_dump(page, f"noresults_{start}")
                break

            start += 10
            time.sleep(1.2)

        browser.close()
        return out[:max_results]

if __name__ == "__main__":
    args = sys.argv[1:]
    query = args[0] if args else "site:reuters.com gold prices"
    hl   = args[1] if len(args) > 1 else "en"
    lr   = args[2] if len(args) > 2 else None                  # e.g., lang_en
    tbs  = args[3] if len(args) > 3 else "qdr:w"               # e.g., qdr:d / qdr:w / cdr:1,...
    tbm  = args[4] if len(args) > 4 else None                  # use "nws" for News or "None"
    gl   = args[5] if len(args) > 5 else "us"                  # geo bias
    n    = int(args[6]) if len(args) > 6 else 100

    if tbm == "None": tbm = None
    if tbm not in VALID_TBM:
        logging.warning(f"[PARAM] Invalid tbm='{tbm}'. Using None.")
        tbm = None

    res = google_headless_search(query, max_results=n, hl=hl, lr=lr, tbs=tbs, tbm=tbm, gl=gl)
    print(json.dumps(res, indent=2))
