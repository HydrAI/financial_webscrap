"""Configuration for the transcripts subcommand."""

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class TranscriptConfig:
    # Input — at least one of tickers or tickers_file must be set
    tickers: tuple[str, ...] = ()
    tickers_file: Path | None = None

    # Filters — single-year mode
    year: int | None = None  # None = current year (ignored when from_year is set)
    quarters: tuple[str, ...] = ()  # empty = all quarters

    # Filters — range mode (overrides year when from_year is set)
    from_year: int | None = None
    to_year: int | None = None  # defaults to current year when from_year is set

    # Fetch
    concurrent: int = 5

    # Store
    output_dir: Path = Path(".")
    output_path: Path = Path("output.parquet")
    jsonl_path: Path | None = None

    # Checkpoint
    checkpoint_file: Path = Path(".transcript_checkpoint.json")
    resume: bool = False

    # Fallback source
    fmp_api_key: str = ""  # Financial Modeling Prep key; also read from FMP_API_KEY env var
