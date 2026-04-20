"""Configuration for the futures subcommand."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FuturesConfig:
    # Exchange selection
    exchanges: tuple[str, ...] = ("lme", "cme", "ice")
    categories: tuple[str, ...] = ()  # empty = all

    # Fetch
    delay: float = 3.0
    max_delay: float = 60.0
    timeout: int = 30

    # Store
    output_dir: Path = Path(".")
    output_path: Path = Path("futures_output.parquet")
    jsonl_path: Path | None = None

    # Local HTML directory (skip live fetching, parse crawled files)
    local_html_dir: Path | None = None

    # Checkpoint
    checkpoint_file: Path = Path(".futures_checkpoint.json")
    resume: bool = False
