"""Content extraction using trafilatura."""

import logging
from dataclasses import dataclass

from ..config import ScraperConfig
from .clean import TextCleaner

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    text: str
    title: str | None
    author: str | None
    date: str | None
    word_count: int
    extraction_method: str
    language: str | None


class HTMLExtractor:
    """Extract clean text from HTML using trafilatura."""

    def __init__(self, config: ScraperConfig):
        self._config = config
        self._cleaner = TextCleaner()
        self._pages_extracted = 0

    def extract(self, html: str, url: str) -> ExtractionResult:
        import trafilatura
        from trafilatura import bare_extraction

        # Periodically reset caches
        self._pages_extracted += 1
        if self._pages_extracted % 500 == 0:
            try:
                trafilatura.meta.reset_caches()
            except Exception:
                pass

        # Primary extraction with favor_precision
        result = bare_extraction(
            html,
            url=url,
            favor_precision=self._config.favor_precision,
            include_tables=self._config.include_tables,
            include_comments=False,
            include_links=False,
            include_images=False,
            target_language=self._config.target_language,
            deduplicate=True,
            with_metadata=True,
        )

        text = None
        title = None
        author = None
        pub_date = None
        method = "trafilatura"

        if result is not None:
            text = getattr(result, "text", None) or (result.get("text") if isinstance(result, dict) else None)
            title = getattr(result, "title", None) or (result.get("title") if isinstance(result, dict) else None)
            author = getattr(result, "author", None) or (result.get("author") if isinstance(result, dict) else None)
            pub_date = getattr(result, "date", None) or (result.get("date") if isinstance(result, dict) else None)

        # Check if we got enough content
        word_count = len(text.split()) if text else 0
        if not text or word_count < self._config.min_word_count:
            # Fallback: try with precision off
            result2 = bare_extraction(
                html,
                url=url,
                favor_precision=False,
                include_tables=self._config.include_tables,
                include_comments=False,
                include_links=False,
                include_images=False,
                target_language=self._config.target_language,
                deduplicate=True,
                with_metadata=True,
            )
            if result2 is not None:
                text2 = getattr(result2, "text", None) or (result2.get("text") if isinstance(result2, dict) else None)
                if text2 and len(text2.split()) > word_count:
                    text = text2
                    method = "trafilatura_fallback"
                    if not title:
                        title = getattr(result2, "title", None) or (result2.get("title") if isinstance(result2, dict) else None)
                    if not author:
                        author = getattr(result2, "author", None) or (result2.get("author") if isinstance(result2, dict) else None)
                    if not pub_date:
                        pub_date = getattr(result2, "date", None) or (result2.get("date") if isinstance(result2, dict) else None)

        if not text:
            return ExtractionResult(
                text="", title=title, author=author, date=pub_date,
                word_count=0, extraction_method="failed", language=None,
            )

        # Post-clean
        text = self._cleaner.clean(text)
        word_count = len(text.split())

        return ExtractionResult(
            text=text,
            title=title,
            author=author,
            date=pub_date,
            word_count=word_count,
            extraction_method=method,
            language=self._config.target_language,
        )
