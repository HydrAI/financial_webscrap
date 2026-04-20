"""Query expansion for deep historical research.

Given a topic like "copper futures" and a year range, generates a comprehensive
set of query variations that maximize DDG coverage:

  1. Core angles: price, market, supply/demand, forecast, review
  2. Site-targeted: high-value archival domains (USGS, World Bank, Reuters, etc.)
  3. Auto-jargon: mine domain-specific terms from first-pass scraped text
  4. Hardcoded jargon: known commodity-specific terms (optional supplement)
"""

import logging
import re
from collections import Counter
from pathlib import Path

logger = logging.getLogger(__name__)

# Core angles applied to every topic + year
# Mix of commodity-oriented and general research angles
_CORE_ANGLES = [
    "{topic} {year}",
    "{topic} review {year}",
    "{topic} survey {year}",
    "{topic} analysis {year}",
    "{topic} research {year}",
    "{topic} applications {year}",
    "{topic} tutorial {year}",
    "{topic} benchmark {year}",
    "{topic} trends {year}",
    "{topic} market {year}",
    "{topic} forecast {year}",
]

# High-value archival domains for site-targeted queries
# Mix of financial/commodity + research/tech domains
_SITE_TARGETS = [
    # Financial & commodity
    "pubs.usgs.gov",
    "worldbank.org",
    "imf.org",
    "reuters.com",
    "ft.com",
    "bloomberg.com",
    "mining.com",
    "kitco.com",
    "fastmarkets.com",
    "statista.com",
    "tradingeconomics.com",
    "investing.com",
    "spglobal.com",
    "iea.org",
    # Research & tech
    "arxiv.org",
    "paperswithcode.com",
    "distill.pub",
    "towardsdatascience.com",
    "neptune.ai",
    "huggingface.co",
    "mckinsey.com",
]

# Extra per-commodity jargon angles (commodity → list of extra patterns)
_COMMODITY_JARGON: dict[str, list[str]] = {
    "copper": [
        "copper cathode price LME {year}",
        "copper mining output Chile Peru {year}",
        "copper demand china {year}",
        "LME copper {year}",
        "COMEX copper futures {year}",
        "copper concentrate TC RC {year}",
        "copper warehouse stocks LME {year}",
        "refined copper consumption {year}",
        "copper scrap recycling {year}",
        "copper deficit surplus {year}",
    ],
    "oil": [
        "crude oil WTI Brent price {year}",
        "OPEC oil production {year}",
        "oil demand IEA {year}",
        "oil inventory stocks {year}",
        "oil refinery capacity {year}",
    ],
    "gold": [
        "gold price LBMA {year}",
        "gold ETF holdings {year}",
        "gold mining production {year}",
        "gold central bank reserves {year}",
        "COMEX gold futures {year}",
    ],
    "wheat": [
        "wheat futures CBOT {year}",
        "wheat production USDA {year}",
        "wheat export import {year}",
        "wheat stocks to use ratio {year}",
    ],
    "natural gas": [
        "natural gas Henry Hub price {year}",
        "LNG demand supply {year}",
        "natural gas storage {year}",
        "TTF natural gas Europe {year}",
    ],
}

# Common English stopwords + generic terms to exclude from jargon mining
_STOPWORDS = frozenset(
    "the a an and or but in on at to for of is it by as with from that this "
    "was were be been are have has had do does did will would shall should can "
    "could may might must not no nor so if then than also more most very much "
    "its their our your his her he she we they them us me my all any each both "
    "few many some such only just about after before between through during "
    "above below up down out off over under again further once here there when "
    "where why how what which who whom whose new said year years one two three "
    "four five six seven eight nine ten first last per cent percent total well "
    "even still however according report data based including figure table page "
    "source www http https com org net pdf html million billion trillion "
    "january february march april may june july august september october "
    "november december jan feb mar apr jun jul aug sep oct nov dec "
    "2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 "
    "2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025 2026".split()
)


def expand_queries(
    topic: str,
    year_start: int,
    year_end: int,
    *,
    include_sites: bool = True,
    include_jargon: bool = True,
) -> list[str]:
    """Generate expanded query list for a topic across a year range.

    Args:
        topic: Base topic (e.g. "copper futures", "oil market")
        year_start: First year (inclusive)
        year_end: Last year (inclusive)
        include_sites: Add site-targeted queries
        include_jargon: Add commodity-specific jargon queries

    Returns:
        List of query strings, deduplicated.
    """
    queries: list[str] = []
    seen: set[str] = set()

    def _add(q: str):
        q = q.strip()
        if q and q not in seen:
            seen.add(q)
            queries.append(q)

    # 1. Core angles × years
    for year in range(year_start, year_end + 1):
        for pattern in _CORE_ANGLES:
            _add(pattern.format(topic=topic, year=year))

    # 2. Site-targeted (no year — DDG returns mixed years)
    if include_sites:
        for site in _SITE_TARGETS:
            _add(f"site:{site} {topic}")

    # 3. Commodity jargon × years
    if include_jargon:
        # Match topic to known commodity jargon
        topic_lower = topic.lower()
        for commodity, patterns in _COMMODITY_JARGON.items():
            if commodity in topic_lower:
                for year in range(year_start, year_end + 1):
                    for pattern in patterns:
                        _add(pattern.format(year=year))
                break  # only match one commodity

    logger.info(
        "Query expansion: %d queries for topic=%r years=%d-%d",
        len(queries), topic, year_start, year_end,
    )
    return queries


def extract_jargon(
    texts: list[str],
    topic: str,
    *,
    max_terms: int = 20,
    min_freq: int = 3,
    ngram_range: tuple[int, int] = (1, 3),
) -> list[str]:
    """Extract domain-specific jargon from scraped texts.

    Finds terms that co-occur frequently with the topic but are not
    generic English words. Returns bigrams/trigrams first (more specific),
    then unigrams.

    Args:
        texts: List of scraped full_text strings
        topic: The base topic (used to filter out topic words themselves)
        max_terms: Maximum number of jargon terms to return
        min_freq: Minimum document frequency for a term
        ngram_range: (min_n, max_n) for n-gram extraction

    Returns:
        List of jargon phrases, most specific first.
    """
    topic_words = set(topic.lower().split())
    all_stopwords = _STOPWORDS | topic_words

    # Count n-gram document frequency (how many docs contain each n-gram)
    ngram_doc_freq: Counter = Counter()

    for text in texts:
        if not text:
            continue
        # Normalize: lowercase, keep only alphanumeric + spaces
        clean = re.sub(r"[^a-z0-9\s]", " ", text.lower())
        words = clean.split()

        # Extract n-grams from this document (deduplicated per doc)
        doc_ngrams: set[str] = set()

        for n in range(ngram_range[0], ngram_range[1] + 1):
            for i in range(len(words) - n + 1):
                gram_words = words[i : i + n]
                # Skip if all words are stopwords or topic words
                non_stop = [w for w in gram_words if w not in all_stopwords and len(w) > 2]
                if not non_stop:
                    continue
                # Skip if any word is just a number
                if all(w.isdigit() for w in gram_words):
                    continue
                # Skip n-grams starting/ending with function words
                _EDGE_WORDS = {"the", "a", "an", "of", "in", "on", "at", "to",
                               "for", "by", "as", "is", "it", "and", "or", "s"}
                if gram_words[0] in _EDGE_WORDS or gram_words[-1] in _EDGE_WORDS:
                    continue
                gram = " ".join(gram_words)
                doc_ngrams.add(gram)

        ngram_doc_freq.update(doc_ngrams)

    # Filter by minimum frequency
    frequent = {gram: freq for gram, freq in ngram_doc_freq.items() if freq >= min_freq}

    if not frequent:
        logger.info("Auto-jargon: no terms found above min_freq=%d", min_freq)
        return []

    # Score: prefer multi-word terms (more specific), then by frequency
    # Score = n_words * 10 + log(freq)
    import math
    scored = []
    for gram, freq in frequent.items():
        n_words = len(gram.split())
        # Skip single-char or very short grams
        if len(gram) < 4:
            continue
        # Skip if it's just stopwords that slipped through
        gram_words = gram.split()
        if all(w in all_stopwords for w in gram_words):
            continue
        score = n_words * 10 + math.log(freq)
        scored.append((gram, freq, score))

    scored.sort(key=lambda x: x[2], reverse=True)

    # Deduplicate: remove n-grams that are substrings of higher-ranked ones
    selected: list[str] = []
    for gram, freq, score in scored:
        if len(selected) >= max_terms:
            break
        # Skip if this gram is a substring of an already-selected term
        if any(gram in s for s in selected):
            continue
        selected.append(gram)

    logger.info(
        "Auto-jargon: extracted %d terms from %d documents (top: %s)",
        len(selected), len(texts),
        ", ".join(selected[:5]),
    )
    return selected


def expand_from_jargon(
    topic: str,
    jargon_terms: list[str],
    year_start: int,
    year_end: int,
    *,
    max_queries_per_term: int = 5,
    existing_queries: set[str] | None = None,
) -> list[str]:
    """Generate new queries by combining discovered jargon with years.

    For each jargon term, generates queries like:
      "{jargon_term} {year}"
      "{topic} {jargon_term} {year}"  (if jargon doesn't already contain topic)

    Skips queries that already exist in existing_queries.

    Args:
        topic: Base topic
        jargon_terms: Terms discovered by extract_jargon()
        year_start: First year
        year_end: Last year
        max_queries_per_term: Max years to query per jargon term
        existing_queries: Set of already-generated queries to avoid duplicates

    Returns:
        List of new query strings.
    """
    existing = existing_queries or set()
    queries: list[str] = []
    seen: set[str] = set()
    topic_lower = topic.lower()

    # Sample years evenly if range is large
    all_years = list(range(year_start, year_end + 1))
    if len(all_years) > max_queries_per_term:
        # Pick evenly spaced years
        step = max(1, len(all_years) // max_queries_per_term)
        sampled_years = all_years[::step][:max_queries_per_term]
        # Always include first and last
        if all_years[0] not in sampled_years:
            sampled_years[0] = all_years[0]
        if all_years[-1] not in sampled_years:
            sampled_years[-1] = all_years[-1]
    else:
        sampled_years = all_years

    for term in jargon_terms:
        for year in sampled_years:
            # If jargon already contains the topic, don't double it
            if topic_lower in term:
                q = f"{term} {year}"
            else:
                q = f"{topic} {term} {year}"

            q = q.strip()
            if q not in seen and q not in existing:
                seen.add(q)
                queries.append(q)

    logger.info(
        "Jargon expansion: %d new queries from %d terms × %d years",
        len(queries), len(jargon_terms), len(sampled_years),
    )
    return queries


def discover_top_domains(texts_and_sources: list[tuple[str, str]], min_docs: int = 3) -> list[str]:
    """Find the most productive domains from first-pass results.

    These become site-targeted queries for pass 2.

    Args:
        texts_and_sources: List of (full_text, source_domain) tuples
        min_docs: Minimum documents from a domain to include it

    Returns:
        List of domains sorted by document count.
    """
    domain_counts: Counter = Counter()
    for _, source in texts_and_sources:
        if source:
            # Normalize domain
            domain = source.lower().strip()
            if domain.startswith("www."):
                domain = domain[4:]
            domain_counts[domain] += 1

    # Filter out already-known site targets
    known = {s.lower().replace("www.", "") for s in _SITE_TARGETS}
    productive = [
        domain for domain, count in domain_counts.most_common(20)
        if count >= min_docs and domain not in known
    ]

    if productive:
        logger.info(
            "Discovered %d productive new domains: %s",
            len(productive), ", ".join(productive[:10]),
        )

    return productive


def write_queries_file(queries: list[str], path: Path) -> Path:
    """Write queries to a file (one per line) and return the path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(queries), encoding="utf-8")
    logger.info("Wrote %d queries to %s", len(queries), path)
    return path
