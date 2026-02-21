"""Earnings call transcript downloader (Motley Fool source)."""

__all__ = ["TranscriptPipeline", "TranscriptConfig"]

from financial_scraper.transcripts.config import TranscriptConfig


def __getattr__(name):
    """Lazy import for TranscriptPipeline."""
    if name == "TranscriptPipeline":
        from financial_scraper.transcripts.pipeline import TranscriptPipeline
        return TranscriptPipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
