"""PDF text extraction using pdfplumber."""

import io
import logging

from .html import ExtractionResult

logger = logging.getLogger(__name__)


class PDFExtractor:
    """Extract text from PDF response bytes."""

    def extract(self, content_bytes: bytes, url: str) -> ExtractionResult:
        try:
            import pdfplumber

            text_parts = []
            title = ""
            with pdfplumber.open(io.BytesIO(content_bytes)) as pdf:
                if pdf.metadata:
                    title = pdf.metadata.get("Title", "") or pdf.metadata.get("title", "")
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)

            content = "\n\n".join(text_parts)
            if not content:
                return ExtractionResult(
                    text="", title=title, author=None, date=None,
                    word_count=0, extraction_method="failed", language=None,
                )
            return ExtractionResult(
                text=content,
                title=title or url.split("/")[-1].replace(".pdf", ""),
                author=None,
                date=None,
                word_count=len(content.split()),
                extraction_method="pdfplumber",
                language=None,
            )
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return ExtractionResult(
                text="", title="", author=None, date=None,
                word_count=0, extraction_method="failed", language=None,
            )
