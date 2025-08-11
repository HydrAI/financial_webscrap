# -*- coding: utf-8 -*-
"""
Created on Mon Aug 11 13:57:13 2025

@author: TonyGuida
"""

# --- prerequisites: reuse helpers from your upgraded script:
# is_pdf_url, extract_domain, _extract_text_from_html, get_pdf_text,
# safe_request_nobackoff, HEADERS, PAYWALL_DOMAINS

import time, random
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor

# ---------- PASS 1: FAST (requests only; no cache, no playwright) ----------
def get_fast_only(url, timeout=20):
    if is_pdf_url(url):
        return "", "queue_pdf"
    s = requests.Session(); s.headers.update(HEADERS)
    r = safe_request_nobackoff(s, url, timeout=timeout)
    if r is None: return "", "requests_fail"
    txt = _extract_text_from_html(r.text)
    return (txt, "requests_html") if len(txt) >= 200 else (txt, "requests_html_weak")

def pass1_fast(df_links: pd.DataFrame) -> pd.DataFrame:
    urls = df_links["link"].tolist()
    with ThreadPoolExecutor(max_workers=8) as ex:
        out = list(ex.map(get_fast_only, urls))
    df_links["full_text"] = [t for t,_m in out]
    df_links["method_used"] = [m for _t,m in out]
    df_links["ft_len"] = df_links["full_text"].str.len().fillna(0)
    df_links["is_pdf"] = df_links["link"].apply(is_pdf_url)
    return df_links

# ---------- PASS 2: SLOW (Playwright; reuse one browser; non-PDF weak only) ----------
def slow_playwright_pass(links):
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return {}

    results = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=HEADERS["User-Agent"])
        page = ctx.new_page()
        for url in links:
            dom = extract_domain(url)
            if dom in PAYWALL_DOMAINS:   # policy: skip hard paywalls
                results[url] = ("", "skip_paywall")
                continue
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=90000)
                # quick consent clicker
                for txt in ["Accept all","I agree","Consent","Continue"]:
                    btn = page.locator(f"button:has-text('{txt}')")
                    if btn.count(): 
                        btn.first.click(timeout=2000)
                        break
                page.wait_for_selector("article, main, [itemprop='articleBody']", timeout=60000)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                html = page.content()
                txt = _extract_text_from_html(html)
                if len(txt) >= 200:
                    results[url] = (txt, "playwright")
                else:
                    results[url] = (txt, "playwright_weak")
            except Exception as e:
                results[url] = ("", f"playwright_fail:{type(e).__name__}")
            time.sleep(random.uniform(5, 12))  # per-nav throttle
        page.close(); browser.close()
    return results

# ---------- PASS 3: PDF QUEUE (PyMuPDF -> pdfminer) ----------
def pdf_pass(links):
    out = {}
    for url in links:
        txt, m = get_pdf_text(url)    # returns (text, "pdf_pymupdf"/"pdf_pdfminer"/...)
        out[url] = (txt, m if txt else "pdf_all_failed")
        time.sleep(random.uniform(1.0, 2.5))
    return out

# ---------- DRIVER ----------
def run_three_pass(df_seed: pd.DataFrame) -> pd.DataFrame:
    # Expect df_seed with columns: company,title,link,snippet,date,source
    df1 = pass1_fast(df_seed.copy())

    # Queues
    q_pdf = df1.loc[df1["is_pdf"] | (df1["method_used"] == "queue_pdf"), "link"].unique().tolist()
    q_slow = df1.loc[
        (~df1["is_pdf"]) & (df1["ft_len"] < 200),
        "link"
    ].unique().tolist()

    # PASS 2
    rec2 = slow_playwright_pass(q_slow)
    if rec2:
        m = pd.DataFrame(
            [(u, rec2[u][0], rec2[u][1]) for u in rec2],
            columns=["link","full_text_p2","method_p2"]
        )
        df1 = df1.merge(m, on="link", how="left")
        # promote if better
        take = (df1["ft_len"] < 200) & (df1["full_text_p2"].fillna("").str.len() >= 200)
        df1.loc[take, "full_text"] = df1.loc[take, "full_text_p2"]
        df1.loc[take, "method_used"] = df1.loc[take, "method_p2"]
        df1["ft_len"] = df1["full_text"].str.len().fillna(0)

    # PASS 3 (PDFs)
    rec3 = pdf_pass(q_pdf)
    if rec3:
        m3 = pd.DataFrame(
            [(u, rec3[u][0], rec3[u][1]) for u in rec3],
            columns=["link","full_text_p3","method_p3"]
        )
        df1 = df1.merge(m3, on="link", how="left")
        take3 = (df1["is_pdf"]) & (df1["full_text_p3"].fillna("").str.len() >= 200)
        df1.loc[take3, "full_text"] = df1.loc[take3, "full_text_p3"]
        df1.loc[take3, "method_used"] = df1.loc[take3, "method_p3"]
        df1["ft_len"] = df1["full_text"].str.len().fillna(0)

    # Final housekeeping
    df1["content_hash"] = df1.apply(lambda r: hashlib.sha1(f"{(r['title'] or '')[:600]} || {(r['snippet'] or '')[:600]}".encode("utf-8")).hexdigest(), axis=1)
    df1["uuid"] = df1.apply(lambda r: str(uuid.uuid5(uuid.NAMESPACE_URL, f"{r['title']}-{r['link']}")), axis=1)
    df1["thread_site"] = df1["link"].apply(extract_domain)
    df1["language"] = df1.get("language", pd.Series(["en"]*len(df1)))

    # Optional: persist queues for audit
    df1.loc[df1["ft_len"] < 200].to_csv("failures_after_pass2.csv", index=False)
    pd.DataFrame({"link": q_pdf}).to_csv("pdf_queue.csv", index=False)

    return df1

# Example use:
# seed_df = <your Google/DDG results df>
# final_df = run_three_pass(seed_df)
# final_df.to_parquet("news_3pass.parquet", index=False)
