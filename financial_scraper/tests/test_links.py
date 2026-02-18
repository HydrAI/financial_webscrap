"""Tests for financial_scraper.extract.links."""

import pytest

from financial_scraper.extract.links import (
    _base_domain,
    extract_links,
    filter_links_same_domain,
)


class TestBaseDomain:
    def test_simple(self):
        assert _base_domain("reuters.com") == "reuters.com"

    def test_www(self):
        assert _base_domain("www.reuters.com") == "reuters.com"

    def test_subdomain(self):
        assert _base_domain("blog.reuters.com") == "reuters.com"

    def test_deep_subdomain(self):
        assert _base_domain("a.b.reuters.com") == "reuters.com"


class TestExtractLinks:
    def test_absolute_links(self):
        html = '<html><body><a href="https://example.com/page1">Link</a></body></html>'
        links = extract_links(html, "https://example.com/")
        assert links == ["https://example.com/page1"]

    def test_relative_links(self):
        html = '<html><body><a href="/page2">Link</a></body></html>'
        links = extract_links(html, "https://example.com/dir/")
        assert links == ["https://example.com/page2"]

    def test_fragment_stripping(self):
        html = '<html><body><a href="https://example.com/page#section">Link</a></body></html>'
        links = extract_links(html, "https://example.com/")
        assert links == ["https://example.com/page"]

    def test_javascript_skip(self):
        html = '<html><body><a href="javascript:void(0)">Bad</a></body></html>'
        links = extract_links(html, "https://example.com/")
        assert links == []

    def test_mailto_skip(self):
        html = '<html><body><a href="mailto:a@b.com">Mail</a></body></html>'
        links = extract_links(html, "https://example.com/")
        assert links == []

    def test_dedup(self):
        html = """<html><body>
            <a href="https://example.com/page">A</a>
            <a href="https://example.com/page">B</a>
        </body></html>"""
        links = extract_links(html, "https://example.com/")
        assert links == ["https://example.com/page"]

    def test_empty_html(self):
        links = extract_links("", "https://example.com/")
        assert links == []

    def test_asset_extensions_skipped(self):
        html = """<html><body>
            <a href="/image.jpg">Img</a>
            <a href="/style.css">CSS</a>
            <a href="/script.js">JS</a>
            <a href="/good-page">Page</a>
        </body></html>"""
        links = extract_links(html, "https://example.com/")
        assert links == ["https://example.com/good-page"]

    def test_hash_only_skip(self):
        html = '<html><body><a href="#">Top</a></body></html>'
        links = extract_links(html, "https://example.com/")
        assert links == []


class TestFilterLinksSameDomain:
    def test_same_domain_pass(self):
        links = ["https://reuters.com/article/1"]
        result = filter_links_same_domain(
            links, "reuters.com", set(), set(), {}, 50,
        )
        assert result == links

    def test_subdomain_allowed(self):
        links = ["https://blog.reuters.com/post"]
        result = filter_links_same_domain(
            links, "reuters.com", set(), set(), {}, 50,
        )
        assert result == links

    def test_different_domain_filtered(self):
        links = ["https://other.com/page"]
        result = filter_links_same_domain(
            links, "reuters.com", set(), set(), {}, 50,
        )
        assert result == []

    def test_excluded_filtered(self):
        links = ["https://reuters.com/page"]
        result = filter_links_same_domain(
            links, "reuters.com", {"reuters.com"}, set(), {}, 50,
        )
        assert result == []

    def test_already_seen_filtered(self):
        links = ["https://reuters.com/page"]
        result = filter_links_same_domain(
            links, "reuters.com", set(), {"https://reuters.com/page"}, {}, 50,
        )
        assert result == []

    def test_domain_cap_enforced(self):
        links = ["https://reuters.com/new-page"]
        result = filter_links_same_domain(
            links, "reuters.com", set(), set(),
            {"reuters.com": 50}, 50,
        )
        assert result == []

    def test_asset_extensions_filtered(self):
        links = ["https://reuters.com/logo.png", "https://reuters.com/article"]
        result = filter_links_same_domain(
            links, "reuters.com", set(), set(), {}, 50,
        )
        assert result == ["https://reuters.com/article"]
