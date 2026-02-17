"""Tests for financial_scraper.extract.clean."""

from financial_scraper.extract.clean import TextCleaner


class TestTextCleaner:
    def setup_method(self):
        self.cleaner = TextCleaner()

    def test_empty_string(self):
        assert self.cleaner.clean("") == ""

    def test_cookie_consent_removed(self):
        text = "Good content.\nCookie consent preferences here.\nMore content."
        result = self.cleaner.clean(text)
        assert "cookie" not in result.lower()
        assert "Good content" in result

    def test_newsletter_prompt_removed(self):
        text = "Article body.\nSubscribe to our newsletter for updates.\nEnd."
        result = self.cleaner.clean(text)
        assert "newsletter" not in result.lower()

    def test_social_share_removed(self):
        text = "Main text.\nShare this on Twitter for friends.\nFooter."
        result = self.cleaner.clean(text)
        assert "Share this on Twitter" not in result

    def test_copyright_notice_removed(self):
        text = "Content here.\n\u00a9 2024 Some Company Inc.\nMore text."
        result = self.cleaner.clean(text)
        assert "\u00a9 2024" not in result

    def test_all_rights_reserved_removed(self):
        text = "Article.\nAll rights reserved by Company.\nDone."
        result = self.cleaner.clean(text)
        assert "All rights reserved" not in result

    def test_bare_urls_removed(self):
        text = "Some text.\nhttps://example.com/page\nMore text."
        result = self.cleaner.clean(text)
        assert "https://example.com" not in result
        assert "Some text" in result

    def test_multiple_blank_lines_collapsed(self):
        text = "Line one.\n\n\n\n\nLine two."
        result = self.cleaner.clean(text)
        assert "\n\n\n" not in result
        assert "Line one." in result
        assert "Line two." in result

    def test_unicode_nfkc_normalization(self):
        # \ufb01 = fi ligature -> "fi" under NFKC
        text = "of\ufb01ce"
        result = self.cleaner.clean(text)
        assert "office" in result

    def test_compound_example(self):
        text = (
            "Important article content here.\n"
            "Cookie policy: we use cookies.\n"
            "Subscribe to our newsletter today.\n"
            "Share this on Facebook and LinkedIn.\n"
            "\u00a9 2024 MegaCorp.\n"
            "https://example.com/tracking\n"
            "\n\n\n\n"
            "Real conclusion text."
        )
        result = self.cleaner.clean(text)
        assert "Important article content" in result
        assert "Real conclusion text" in result
        assert "cookie" not in result.lower()
        assert "newsletter" not in result.lower()
        assert "Facebook" not in result
        assert "\u00a9 2024" not in result
        assert "https://example.com" not in result
        assert "\n\n\n" not in result
