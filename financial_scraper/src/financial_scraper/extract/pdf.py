"""PDF text extraction using pdfplumber or Docling."""

import io
import logging
import re
from datetime import datetime

from .html import ExtractionResult

logger = logging.getLogger(__name__)

# Date patterns for regex scan of PDF content (ordered by specificity)
_DATE_PATTERNS = [
    # 27 January 2026, 31 December 2024
    r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})",
    # January 2026, October 2025
    r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})",
    # 2026-01-27
    r"(\d{4}-\d{2}-\d{2})",
    # 31/12/2024
    r"(\d{1,2}/\d{1,2}/\d{4})",
]


def _parse_date_safe(text: str) -> datetime | None:
    """Try to parse a date string, return None on failure."""
    from dateutil import parser as dateparser

    try:
        return dateparser.parse(text, dayfirst=True)
    except Exception:
        return None


def _extract_content_date(text: str, max_chars: int = 500) -> datetime | None:
    """Regex-scan the first N characters for a date."""
    snippet = text[:max_chars]
    for pat in _DATE_PATTERNS:
        m = re.search(pat, snippet, re.IGNORECASE)
        if m:
            dt = _parse_date_safe(m.group(1))
            if dt:
                return dt
    return None


def _extract_metadata_date(content_bytes: bytes) -> datetime | None:
    """Read CreationDate/ModDate from PDF binary metadata via pdfplumber."""
    try:
        import pdfplumber

        with pdfplumber.open(io.BytesIO(content_bytes)) as pdf:
            meta = pdf.metadata or {}
            for key in ("CreationDate", "ModDate"):
                val = meta.get(key, "")
                if val:
                    m = re.search(r"(\d{8})", str(val))
                    if m:
                        try:
                            return datetime.strptime(m.group(1), "%Y%m%d")
                        except ValueError:
                            pass
    except Exception as e:
        logger.debug(f"PDF metadata date extraction failed: {e}")
    return None


def extract_pdf_date(content_bytes: bytes, text: str) -> str | None:
    """Extract the best date from a PDF: max(content_date, metadata_date).

    Takes the latest date to approximate the actual release/publication date,
    avoiding look-ahead bias (e.g. a report covering Q4 2024 published in
    Feb 2025 should use the Feb 2025 date, not Dec 2024).
    """
    content_date = _extract_content_date(text)
    metadata_date = _extract_metadata_date(content_bytes)

    dates = [d for d in (content_date, metadata_date) if d is not None]
    if not dates:
        return None

    best = max(dates)
    logger.debug(
        f"PDF date: content={content_date}, metadata={metadata_date}, "
        f"best(latest)={best}"
    )
    return best.strftime("%Y-%m-%d")

# Detect Docling availability at import time
try:
    from docling.document_converter import DocumentConverter, PdfFormatOption
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.datamodel.base_models import InputFormat

    DOCLING_AVAILABLE = True
except ImportError:
    DOCLING_AVAILABLE = False


class PDFExtractor:
    """Extract text from PDF response bytes using pdfplumber."""

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
            date = extract_pdf_date(content_bytes, content)
            return ExtractionResult(
                text=content,
                title=title or url.split("/")[-1].replace(".pdf", ""),
                author=None,
                date=date,
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


class DoclingExtractor:
    """Extract text from PDF response bytes using Docling (layout-aware)."""

    def __init__(self):
        if not DOCLING_AVAILABLE:
            raise ImportError(
                "docling is not installed. Install with: pip install financial-scraper[docling]"
            )
        pipeline_options = PdfPipelineOptions()
        self._converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
            }
        )

    def extract(self, content_bytes: bytes, url: str) -> ExtractionResult:
        try:
            from docling.datamodel.document import DocumentStream

            source = DocumentStream(name=url, stream=io.BytesIO(content_bytes))
            result = self._converter.convert(source)
            content = result.document.export_to_markdown()

            if not content or not content.strip():
                return ExtractionResult(
                    text="", title="", author=None, date=None,
                    word_count=0, extraction_method="failed", language=None,
                )

            title = url.split("/")[-1].replace(".pdf", "")
            date = extract_pdf_date(content_bytes, content)
            return ExtractionResult(
                text=content,
                title=title,
                author=None,
                date=date,
                word_count=len(content.split()),
                extraction_method="docling",
                language=None,
            )
        except Exception as e:
            logger.warning(f"Docling PDF extraction failed for {url}: {e}")
            return ExtractionResult(
                text="", title="", author=None, date=None,
                word_count=0, extraction_method="failed", language=None,
            )


def get_pdf_extractor(preference: str = "auto"):
    """Factory: return a PDF extractor based on user preference.

    Args:
        preference: "auto" (docling if available, else pdfplumber),
                    "docling" (raises ImportError if missing),
                    "pdfplumber" (always works).
    """
    if preference == "docling":
        return DoclingExtractor()
    elif preference == "pdfplumber":
        return PDFExtractor()
    else:  # "auto"
        if DOCLING_AVAILABLE:
            logger.info("Using Docling PDF extractor (auto-detected)")
            return DoclingExtractor()
        else:
            logger.info("Docling not available, using pdfplumber PDF extractor")
            return PDFExtractor()
