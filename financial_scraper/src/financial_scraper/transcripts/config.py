"""Configuration for the transcripts subcommand."""

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class TranscriptConfig:
    # Input — at least one of tickers or tickers_file must be set
    tickers: tuple[str, ...] = ()
    tickers_file: Path | None = None

    # Filters
    year: int | None = None  # None = current year
    quarters: tuple[str, ...] = ()  # empty = all quarters

    # Fetch
    concurrent: int = 5

    # Store
    output_dir: Path = Path(".")
    output_path: Path = Path("output.parquet")
    jsonl_path: Path | None = None

    # Checkpoint
    checkpoint_file: Path = Path(".transcript_checkpoint.json")
    resume: bool = False
