"""Post-extraction text cleaning."""

import re
import unicodedata


class TextCleaner:
    """Clean extracted text after trafilatura processing."""

    BOILERPLATE_PATTERNS = [
        re.compile(r'(?i)cookie\s*(policy|consent|preferences|settings).*?(\n|$)'),
        re.compile(r'(?i)(subscribe|sign\s*up)\s*(to\s*(our|the))?\s*newsletter.*?(\n|$)'),
        re.compile(r'(?i)share\s*(this\s*)?(on|via)\s*(twitter|facebook|linkedin|x|email).*?(\n|$)'),
        re.compile(r'(?i)all\s*rights?\s*reserved.*?(\n|$)'),
        re.compile(r'(?i)(follow|connect\s*with)\s*us\s*(on)?.*?(\n|$)'),
        re.compile(r'(?i)\u00a9\s*\d{4}.*?(\n|$)'),
        re.compile(r'(?i)terms\s*(of\s*(use|service)|and\s*conditions).*?(\n|$)'),
        re.compile(r'(?i)privacy\s*policy.*?(\n|$)'),
        re.compile(r'^https?://\S+$', re.MULTILINE),
        re.compile(r'(?i)(advertisement|sponsored\s*content)'),
        # TipRanks promo blocks
        re.compile(r'(?i)claim\s+\d+%\s+off\s+TipRanks\s+Premium.*?(\n|$)'),
        re.compile(r'(?i)meet\s+your\s+.*?\s+analyst.*?(\n|$)'),
        re.compile(r'(?i)discover\s+how\s+TipRanks.*?(\n|$)'),
        re.compile(r'(?i)explore\s+ETFs\s+TipRanks.*?(\n|$)'),
        re.compile(r'(?i)learn\s+more\s+about\s+\S+\s+stock\s+on\s+TipRanks.*?(\n|$)'),
        re.compile(r'(?i)TipRanks.\s*Stock\s*Analysis\s*page.*?(\n|$)'),
        re.compile(r'(?i)unlock\s+hedge\s+fund.*?(\n|$)'),
        re.compile(r'(?i)stay\s+ahead\s+of\s+the\s+market\s+with\s+the\s+latest.*?(\n|$)'),
        re.compile(r'(?i)make\s+smarter\s+investments\s+with\s+weekly\s+expert.*?(\n|$)'),
        re.compile(r'(?i)Smart\s+Investor\s+Newsletter.*?(\n|$)'),
        # Trending / related article blocks
        re.compile(r'(?i)trending\s+articles.*?(\n|$)'),
        re.compile(r'(?i)related\s+stories.*?(\n|$)'),
        re.compile(r'(?i)recommended\s+stories.*?(\n|$)'),
        # PR wire disclaimer blocks (MENAFN-style multi-line disclaimers)
        re.compile(
            r'(?i)(?:MENAFN|Legal\s+Disclaimer)[^\n]*\n?'
            r'.*?we\s+do\s+not\s+accept\s+any\s+responsibility\s+or\s+liability'
            r'.*?kindly\s+contact\s+the\s+(?:provider|author)\s+above\.?',
            re.DOTALL,
        ),
        # Catch standalone disclaimer blocks without MENAFN header
        re.compile(
            r'(?i)we\s+do\s+not\s+accept\s*\n?\s*any\s+responsibility\s+or\s+liability'
            r'.*?kindly\s+contact\s+the\s+(?:provider|author)\s+above\.?',
            re.DOTALL,
        ),
    ]

    # Patterns that indicate a page is a stock ticker/profile, not an article
    TICKER_PAGE_PATTERNS = [
        re.compile(r'52\s+[Ww]eek\s+(range|high|low)', re.IGNORECASE),
        re.compile(r'EPS\s*\(TTM\)', re.IGNORECASE),
        re.compile(r'P/E\s*\(TTM\)', re.IGNORECASE),
        re.compile(r'Prev(ious)?\s*Close', re.IGNORECASE),
    ]
    TICKER_PAGE_THRESHOLD = 3  # must match at least 3 of 4 patterns

    # Patterns that indicate a Nature Index profile page
    NATURE_INDEX_PATTERNS = [
        re.compile(r'Nature\s+Index', re.IGNORECASE),
        re.compile(r'Collaboration\s+Score', re.IGNORECASE),
    ]

    def clean(self, text: str) -> str:
        if not text:
            return ""
        # 1. Normalize unicode
        text = unicodedata.normalize("NFKC", text)
        # 2. Apply boilerplate removal
        for pattern in self.BOILERPLATE_PATTERNS:
            text = pattern.sub("", text)
        # 3. Collapse multiple blank lines
        text = re.sub(r'\n{3,}', '\n\n', text)
        # 4. Collapse multiple spaces
        text = re.sub(r'[ \t]{2,}', ' ', text)
        # 5. Strip per-line whitespace
        lines = [line.strip() for line in text.splitlines()]
        text = '\n'.join(lines)
        # 6. Strip overall
        return text.strip()

    def is_ticker_page(self, text: str) -> bool:
        """Detect stock quote/profile pages (not real articles)."""
        if not text:
            return False
        matches = sum(1 for p in self.TICKER_PAGE_PATTERNS if p.search(text))
        return matches >= self.TICKER_PAGE_THRESHOLD

    def is_nature_index_page(self, text: str) -> bool:
        """Detect Nature Index profile pages (not financial content)."""
        if not text:
            return False
        return all(p.search(text) for p in self.NATURE_INDEX_PATTERNS)
