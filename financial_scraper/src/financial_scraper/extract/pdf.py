"""PDF text extraction using pdfplumber or Docling."""

import io
import logging

from .html import ExtractionResult

logger = logging.getLogger(__name__)

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
            return ExtractionResult(
                text=content,
                title=title,
                author=None,
                date=None,
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
