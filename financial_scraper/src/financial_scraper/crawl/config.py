"""Configuration for the crawl subcommand."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CrawlConfig:
    # Input
    urls_file: Path = Path("urls.txt")
    exclude_file: Path | None = None

    # Crawl (maps to crawl4ai strategy params)
    max_depth: int = 2
    max_pages: int = 50
    semaphore_count: int = 2

    # Extract (reused from ScraperConfig)
    min_word_count: int = 100
    target_language: str | None = None
    include_tables: bool = True
    favor_precision: bool = True
    date_from: str | None = None
    date_to: str | None = None

    # Store
    output_dir: Path = Path(".")
    output_path: Path = Path("output.parquet")
    jsonl_path: Path | None = None
    markdown_path: Path | None = None

    # Checkpoint
    checkpoint_file: Path = Path(".crawl_checkpoint.json")
    resume: bool = False

    # PDF
    pdf_extractor: str = "auto"  # "auto", "docling", or "pdfplumber"

    # Behavior
    check_robots_txt: bool = True
    stealth: bool = False


def apply_stealth(config: CrawlConfig) -> CrawlConfig:
    """Return a new config with stealth-mode overrides."""
    if not config.stealth:
        return config
    overrides = {
        "semaphore_count": 1,
    }
    fields = {f.name: getattr(config, f.name) for f in config.__dataclass_fields__.values()}
    fields.update(overrides)
    return CrawlConfig(**fields)
