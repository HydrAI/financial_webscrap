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
