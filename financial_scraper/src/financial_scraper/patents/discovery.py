"""Multi-source patent ID discovery.

Sources (in priority order):
  1. File — text file with patent IDs (one per line, # comments)
  2. Inline — IDs passed directly via config
  3. Google Patents — XHR endpoint search by assignee/keywords/CPC
  4. DuckDuckGo — web search for patent pages (fallback)
  5. Justia — scrape assignee pages at patents.justia.com (best-effort)
"""

import asyncio
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

from .config import PatentConfig

# Windows asyncio compatibility for curl-cffi used by duckduckgo-search / ddgs
if sys.platform.lower().startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Regex to extract patent IDs from URLs or text
# Matches patterns like US9275645B2, US20150302858A1, WO2017139001A3, EP3012345B1
_PATENT_ID_RE = re.compile(
    r'\b([A-Z]{2}\d{5,}[A-Z]\d?)\b'
)

# More specific pattern for Google Patents URLs
_GPATENT_URL_RE = re.compile(
    r'patents\.google\.com/patent/([A-Z]{2}[\dA-Z]+)'
)

# Google Patents XHR result ID pattern: "patent/US12345678B2/en"
_XHR_ID_RE = re.compile(r'^patent/([A-Z]{2}[\dA-Z]+)')


# ---------------------------------------------------------------------------
# Source 1: File
# ---------------------------------------------------------------------------

def load_ids_from_file(path: Path) -> list[str]:
    """Load patent IDs from a text file (one per line, # comments)."""
    if not path.exists():
        logger.warning(f"Patent IDs file not found: {path}")
        return []

    ids = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            ids.append(line)

    logger.info(f"Loaded {len(ids)} patent ID(s) from {path}")
    return ids


# ---------------------------------------------------------------------------
# Source 3: Google Patents XHR
# ---------------------------------------------------------------------------

def _google_patents_xhr_request(
    url: str, headers: dict, max_retries: int = 3
) -> requests.Response | None:
    """Make a Google Patents XHR request with retry on 503."""
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=20)
        except Exception as e:
            logger.warning(f"  Google Patents XHR request failed: {e}")
            return None

        if resp.status_code == 200:
            return resp

        if resp.status_code == 503:
            wait = 5 * (2 ** attempt)  # 5s, 10s, 20s
            logger.info(
                f"  Google Patents 503 (rate limit) — "
                f"retry {attempt + 1}/{max_retries} in {wait}s"
            )
            time.sleep(wait)
            continue

        logger.warning(
            f"  Google Patents XHR HTTP {resp.status_code}"
        )
        return None

    logger.warning("  Google Patents XHR: max retries exceeded")
    return None


def _google_patents_xhr(
    query: str,
    max_results: int = 50,
    delay: float = 4.0,
) -> list[str]:
    """Search Google Patents via the XHR endpoint and return patent IDs.

    The Polymer SPA at patents.google.com loads search results via an
    XHR endpoint that returns structured JSON — no JS rendering needed.

    Supports query syntax:
      - assignee:"NVIDIA Corporation"
      - GPU computing patent
      - CPC:G06F
      - Combinations: assignee:"NVIDIA" CPC:G06F GPU
    """
    found_ids: list[str] = []
    seen: set[str] = set()

    # Google Patents returns 10 results per page, pages are 0-indexed
    per_page = 10
    max_pages = min((max_results + per_page - 1) // per_page, 10)  # cap at 10 pages

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://patents.google.com/",
    }

    for page in range(max_pages):
        if len(found_ids) >= max_results:
            break

        params = f"q={query}&num={per_page}&page={page}&oq={query}"
        encoded = quote(params)
        url = f"https://patents.google.com/xhr/query?url={encoded}"

        resp = _google_patents_xhr_request(url, headers)
        if resp is None:
            break

        try:
            data = resp.json()
        except Exception:
            logger.warning("  Google Patents XHR returned non-JSON response")
            break

        results_data = data.get("results", {})
        total = results_data.get("total_num_results", 0)

        if page == 0:
            logger.info(f"  Google Patents: {total} total results for query")

        clusters = results_data.get("cluster", [])
        page_count = 0
        for cluster in clusters:
            for r in cluster.get("result", []):
                raw_id = r.get("id", "")
                m = _XHR_ID_RE.match(raw_id)
                if m:
                    pid = m.group(1)
                    if pid not in seen:
                        seen.add(pid)
                        found_ids.append(pid)
                        page_count += 1

        if page_count == 0:
            break  # no more results

        if page < max_pages - 1 and len(found_ids) < max_results:
            time.sleep(delay)

    logger.info(f"  Google Patents found {len(found_ids)} patent ID(s)")
    return found_ids[:max_results]


def discover_via_google_patents(
    assignee: str,
    max_results: int = 50,
    cpc_codes: list[str] | None = None,
    granted_only: bool = False,
) -> list[str]:
    """Discover patent IDs by assignee via Google Patents XHR."""
    query_parts = [f'assignee:"{assignee}"']
    if cpc_codes:
        for code in cpc_codes[:3]:  # limit to avoid overly complex queries
            query_parts.append(f"CPC:{code}")
    if granted_only:
        query_parts.append("type:patent")
    query = " ".join(query_parts)
    logger.info(f"  Google Patents discovery: {query}")
    return _google_patents_xhr(query, max_results)


def discover_via_google_patents_keywords(
    search_queries: list[str],
    max_results: int = 50,
    cpc_codes: list[str] | None = None,
    granted_only: bool = False,
) -> list[str]:
    """Discover patent IDs by keyword/topic via Google Patents XHR."""
    found_ids: list[str] = []
    seen: set[str] = set()

    for q in search_queries:
        if len(found_ids) >= max_results:
            break

        query_parts = [q]
        if cpc_codes:
            for code in cpc_codes[:3]:
                query_parts.append(f"CPC:{code}")
        if granted_only:
            query_parts.append("type:patent")
        query = " ".join(query_parts)
        logger.info(f"  Google Patents keyword search: {query}")

        per_query = max(10, (max_results - len(found_ids)))
        ids = _google_patents_xhr(query, per_query, delay=3.0)
        for pid in ids:
            if pid not in seen:
                seen.add(pid)
                found_ids.append(pid)

    return found_ids[:max_results]


# ---------------------------------------------------------------------------
# Source 4: DuckDuckGo
# ---------------------------------------------------------------------------

def _get_ddgs_class():
    """Import DDGS from whichever package is installed."""
    try:
        from ddgs import DDGS
        return DDGS
    except ImportError:
        pass
    try:
        from duckduckgo_search import DDGS
        return DDGS
    except ImportError:
        return None


def _search_ddg(queries: list[str], max_results: int) -> list[str]:
    """Run DuckDuckGo searches and extract patent IDs from results."""
    DDGS = _get_ddgs_class()
    if DDGS is None:
        logger.warning(
            "Neither ddgs nor duckduckgo_search installed — "
            "skipping DDG discovery"
        )
        return []

    found_ids: list[str] = []
    seen: set[str] = set()

    for query in queries:
        if len(found_ids) >= max_results:
            break

        logger.info(f"  DDG discovery search: {query}")
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(
                    query,
                    region="wt-wt",
                    safesearch="off",
                    max_results=min(30, max_results),
                ))
        except Exception as e:
            logger.warning(f"  DDG search failed: {e}")
            continue

        for r in results:
            url = r.get("href", "") or r.get("link", "")
            for m in _GPATENT_URL_RE.finditer(url):
                pid = m.group(1)
                if pid not in seen:
                    seen.add(pid)
                    found_ids.append(pid)

            text = " ".join([
                r.get("title", ""),
                r.get("body", ""),
                r.get("snippet", ""),
                url,
            ])
            for m in _PATENT_ID_RE.finditer(text):
                pid = m.group(1)
                if pid not in seen:
                    seen.add(pid)
                    found_ids.append(pid)

        time.sleep(2.0)

    logger.info(f"  DDG search found {len(found_ids)} patent ID(s)")
    return found_ids[:max_results]


def discover_via_search(assignee: str, max_results: int = 50) -> list[str]:
    """Find patent IDs by assignee name via DuckDuckGo."""
    queries = [
        f'"{assignee}" patent US patent number',
        f'patents.google.com "{assignee}"',
        f'"{assignee}" patent assignee granted',
    ]
    return _search_ddg(queries, max_results)


def discover_via_queries(
    search_queries: list[str], max_results: int = 50
) -> list[str]:
    """Find patent IDs by topic/theme keywords via DuckDuckGo."""
    queries = []
    for q in search_queries:
        queries.append(f'patents.google.com {q}')
        queries.append(f'{q} US patent number')
    return _search_ddg(queries, max_results)


# ---------------------------------------------------------------------------
# Source 5: Justia
# ---------------------------------------------------------------------------

def discover_via_justia(assignee: str, max_results: int = 50) -> list[str]:
    """Discover patent IDs from Justia patent assignee pages.

    Note: Justia uses Cloudflare protection which may block automated
    requests. This source is best-effort.
    """
    slug = assignee.lower().strip()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'\s+', '-', slug)

    base_url = f"https://patents.justia.com/assignee/{slug}"
    logger.info(f"  Justia discovery: {base_url}")

    found_ids: list[str] = []
    seen: set[str] = set()

    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })

    for page in range(1, 6):
        if len(found_ids) >= max_results:
            break

        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            resp = session.get(url, timeout=30)
        except Exception as e:
            logger.warning(f"  Justia request failed: {e}")
            break

        if resp.status_code == 403:
            logger.warning(
                "  Justia blocked by Cloudflare — skipping "
                "(use Google Patents discovery instead)"
            )
            break
        if resp.status_code != 200:
            logger.warning(f"  Justia HTTP {resp.status_code} for page {page}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        patent_links = soup.find_all("a", href=re.compile(r'/patent/\d+'))
        if not patent_links:
            break

        for link in patent_links:
            href = link.get("href", "")
            num_match = re.search(r'/patent/(\d+)', href)
            if num_match:
                patent_num = num_match.group(1)
                text = link.get_text(strip=True)
                id_match = _PATENT_ID_RE.search(text)
                if id_match:
                    pid = id_match.group(1)
                else:
                    pid = f"US{patent_num}"

                if pid not in seen:
                    seen.add(pid)
                    found_ids.append(pid)

        time.sleep(3.0)

    logger.info(f"  Justia found {len(found_ids)} patent ID(s)")
    return found_ids[:max_results]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def discover_patent_ids(config: PatentConfig) -> list[str]:
    """Orchestrate all discovery sources, deduplicate, return unique IDs.

    Sources checked (in order):
      1. IDs from file (--ids-file)
      2. Inline IDs (--ids)
      3. Google Patents XHR (primary — assignee + keyword search)
      4. DuckDuckGo (auto-fallback if Google Patents fails, or explicit)
      5. Justia scraping (best-effort, --discover-justia)
    """
    all_ids: list[str] = []
    gp_found = 0  # track Google Patents results for fallback logic

    # 1. File
    if config.ids_file:
        all_ids.extend(load_ids_from_file(config.ids_file))

    # 2. Inline
    if config.ids:
        all_ids.extend(config.ids)
        logger.info(f"Added {len(config.ids)} inline patent ID(s)")

    # Resolve CPC codes from WIPO categories for use in queries
    cpc_codes = list(config.cpc_filter) if config.cpc_filter else None
    if config.wipo_categories and not cpc_codes:
        from .wipo import resolve_wipo_to_cpc
        cpc_codes = resolve_wipo_to_cpc(list(config.wipo_categories))

    # 3. Google Patents XHR (primary discovery source)
    if config.discover_via_google_patents:
        # Assignee search
        if config.assignee:
            gp_ids = discover_via_google_patents(
                config.assignee, config.max_discovery_results, cpc_codes,
                granted_only=config.granted_only,
            )
            gp_found += len(gp_ids)
            all_ids.extend(gp_ids)

        # Keyword/theme search
        if config.search_queries:
            gp_query_ids = discover_via_google_patents_keywords(
                list(config.search_queries),
                config.max_discovery_results, cpc_codes,
                granted_only=config.granted_only,
            )
            gp_found += len(gp_query_ids)
            all_ids.extend(gp_query_ids)

    # 4. DuckDuckGo — explicit or auto-fallback when Google Patents fails
    use_ddg = config.discover_via_search
    if config.discover_via_google_patents and gp_found == 0:
        # Google Patents returned nothing (rate-limited?) — auto-fallback
        needs_discovery = config.assignee or config.search_queries
        if needs_discovery:
            logger.info(
                "Google Patents returned 0 results — "
                "falling back to DuckDuckGo"
            )
            use_ddg = True

    if use_ddg and config.assignee:
        search_ids = discover_via_search(
            config.assignee, config.max_discovery_results
        )
        all_ids.extend(search_ids)

    if use_ddg and config.search_queries:
        query_ids = discover_via_queries(
            list(config.search_queries), config.max_discovery_results
        )
        all_ids.extend(query_ids)

    # 5. Justia (best-effort)
    if config.discover_via_justia:
        if not config.assignee:
            logger.warning("--discover-justia requires --assignee; skipping")
        else:
            justia_ids = discover_via_justia(
                config.assignee, config.max_discovery_results
            )
            all_ids.extend(justia_ids)

    # Deduplicate preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for pid in all_ids:
        if pid not in seen:
            seen.add(pid)
            unique.append(pid)

    logger.info(f"Total unique patent IDs: {len(unique)}")
    return unique
