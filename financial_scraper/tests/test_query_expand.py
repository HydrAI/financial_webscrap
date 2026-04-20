"""Tests for query expansion."""

from financial_scraper.query_expand import (
    expand_queries,
    extract_jargon,
    expand_from_jargon,
    discover_top_domains,
)


class TestExpandQueries:
    def test_basic_expansion(self):
        q = expand_queries("gold price", 2023, 2024, include_sites=False, include_jargon=False)
        assert len(q) == 22  # 11 core angles × 2 years
        assert "gold price 2023" in q
        assert "gold price analysis 2024" in q

    def test_with_sites(self):
        q = expand_queries("copper", 2024, 2024, include_sites=True, include_jargon=False)
        core = 11  # 11 angles × 1 year
        assert len(q) > core  # should have site queries too
        assert any(s.startswith("site:") for s in q)

    def test_copper_jargon(self):
        q = expand_queries("copper futures", 2024, 2024, include_sites=False, include_jargon=True)
        assert any("LME copper" in s for s in q)
        assert any("COMEX copper" in s for s in q)
        assert any("cathode" in s for s in q)

    def test_no_duplicates(self):
        q = expand_queries("copper", 2020, 2025)
        assert len(q) == len(set(q))

    def test_unknown_commodity_no_jargon(self):
        q1 = expand_queries("lithium", 2024, 2024, include_sites=False, include_jargon=True)
        q2 = expand_queries("lithium", 2024, 2024, include_sites=False, include_jargon=False)
        # No jargon match for lithium, so both should be same
        assert q1 == q2

    def test_full_range(self):
        q = expand_queries("copper futures", 2000, 2025)
        # 11 core × 26 years + 15 sites + 10 jargon × 26 years = 286 + 15 + 260 = 561
        assert len(q) > 500


class TestExtractJargon:
    """Tests for auto-jargon extraction from scraped texts."""

    def test_finds_frequent_terms(self):
        texts = [
            "The LME copper cathode price rose sharply in Q3 due to supply constraints.",
            "LME copper cathode settlements reached record highs amid Chile disruptions.",
            "Copper cathode premiums widened as LME stocks declined significantly.",
            "Refined copper cathode demand from China exceeded all forecasts.",
        ]
        terms = extract_jargon(texts, "copper", min_freq=2)
        # "cathode" or "lme copper cathode" should appear
        assert any("cathode" in t for t in terms)

    def test_excludes_stopwords(self):
        texts = [
            "The price was very high and the market was strong.",
            "The price was very high and the market was strong.",
            "The price was very high and the market was strong.",
        ]
        terms = extract_jargon(texts, "test topic", min_freq=2)
        # Should not return pure stopword n-grams
        for t in terms:
            words = t.split()
            assert not all(w in {"the", "was", "and", "very"} for w in words)

    def test_excludes_edge_words(self):
        """N-grams starting/ending with articles/prepositions should be filtered."""
        texts = [
            "The global supply of rare earth elements is growing rapidly.",
            "The global supply of rare earth elements is growing rapidly.",
            "The global supply of rare earth elements is growing rapidly.",
            "The global supply of rare earth elements is growing rapidly.",
        ]
        terms = extract_jargon(texts, "copper", min_freq=3)
        edge_words = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "by", "as", "is", "it", "and", "or", "s"}
        for t in terms:
            words = t.split()
            assert words[0] not in edge_words, f"Term starts with edge word: {t}"
            assert words[-1] not in edge_words, f"Term ends with edge word: {t}"

    def test_excludes_topic_words(self):
        texts = [
            "copper futures trading copper futures contracts on the exchange.",
            "copper futures trading copper futures contracts on the exchange.",
            "copper futures trading copper futures contracts on the exchange.",
        ]
        terms = extract_jargon(texts, "copper futures", min_freq=2)
        # Pure topic words should not appear as jargon
        assert "copper futures" not in terms
        assert "copper" not in terms
        assert "futures" not in terms

    def test_prefers_multi_word(self):
        texts = [
            "The TC RC charges for copper concentrate processing rose significantly.",
            "Copper concentrate TC RC terms were renegotiated by smelters.",
            "Smelters demanded higher TC RC on copper concentrate shipments.",
            "Annual TC RC benchmark for copper concentrate settled lower.",
        ]
        terms = extract_jargon(texts, "copper", min_freq=3)
        if terms:
            # Multi-word terms should rank before single words
            first_multi = next((i for i, t in enumerate(terms) if len(t.split()) > 1), len(terms))
            first_single = next((i for i, t in enumerate(terms) if len(t.split()) == 1), len(terms))
            if first_multi < len(terms) and first_single < len(terms):
                assert first_multi < first_single

    def test_empty_input(self):
        assert extract_jargon([], "copper") == []
        assert extract_jargon(["", "", ""], "copper") == []

    def test_respects_max_terms(self):
        # Generate texts with many distinct frequent terms
        texts = [
            f"alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau upsilon"
            for _ in range(5)
        ]
        terms = extract_jargon(texts, "test", max_terms=5, min_freq=3)
        assert len(terms) <= 5


class TestExpandFromJargon:
    """Tests for generating queries from discovered jargon."""

    def test_basic_expansion(self):
        terms = ["cathode premium", "TC RC"]
        queries = expand_from_jargon("copper", terms, 2023, 2024)
        assert len(queries) > 0
        assert all(isinstance(q, str) for q in queries)
        # Should contain year references
        assert any("2023" in q for q in queries)
        assert any("2024" in q for q in queries)

    def test_avoids_existing(self):
        terms = ["cathode premium"]
        existing = {"copper cathode premium 2024"}
        queries = expand_from_jargon("copper", terms, 2024, 2024, existing_queries=existing)
        assert "copper cathode premium 2024" not in queries

    def test_no_topic_duplication(self):
        # If jargon already contains the topic, don't double it
        terms = ["copper cathode"]
        queries = expand_from_jargon("copper", terms, 2024, 2024)
        # Should be "copper cathode 2024", not "copper copper cathode 2024"
        assert all("copper copper" not in q for q in queries)

    def test_samples_years_for_large_range(self):
        terms = ["cathode"]
        queries = expand_from_jargon("copper", terms, 2000, 2025, max_queries_per_term=5)
        # Should limit to ~5 queries despite 26 years
        assert len(queries) <= 5
        # Should include first and last year
        assert any("2000" in q for q in queries)
        assert any("2025" in q for q in queries)

    def test_no_duplicates(self):
        terms = ["cathode", "TC RC", "smelter"]
        queries = expand_from_jargon("copper", terms, 2020, 2025)
        assert len(queries) == len(set(queries))


class TestDiscoverTopDomains:
    """Tests for productive domain discovery."""

    def test_finds_frequent_domains(self):
        data = [
            ("text1", "example.com"),
            ("text2", "example.com"),
            ("text3", "example.com"),
            ("text4", "other.org"),
        ]
        domains = discover_top_domains(data, min_docs=3)
        assert "example.com" in domains
        assert "other.org" not in domains

    def test_strips_www(self):
        data = [
            ("t", "www.example.com"),
            ("t", "www.example.com"),
            ("t", "www.example.com"),
        ]
        domains = discover_top_domains(data, min_docs=3)
        assert "example.com" in domains

    def test_excludes_known_sites(self):
        # reuters.com is in _SITE_TARGETS, should be excluded
        data = [("t", "reuters.com")] * 10
        domains = discover_top_domains(data, min_docs=3)
        assert "reuters.com" not in domains

    def test_empty_input(self):
        assert discover_top_domains([], min_docs=3) == []
