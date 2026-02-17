"""Settings dataclass and configuration management."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ScraperConfig:
    # Search
    queries_file: Path = Path("queries.txt")
    max_results_per_query: int = 20
    search_delay_min: float = 3.0
    search_delay_max: float = 6.0
    ddg_region: str = "wt-wt"
    ddg_timelimit: str | None = None
    ddg_backend: str = "auto"
    search_type: str = "text"  # "text" or "news"
    proxy: str | None = None

    # Tor
    use_tor: bool = False
    tor_socks_port: int = 9150
    tor_control_port: int = 9051
    tor_password: str = ""
    tor_renew_every: int = 20
    tor_renew_on_ratelimit: bool = True

    # Fetch
    max_concurrent_total: int = 10
    max_concurrent_per_domain: int = 3
    fetch_timeout: int = 20
    stealth: bool = False
    respect_robots: bool = True
    wayback_fallback: bool = False

    # Crawl
    crawl: bool = False
    crawl_depth: int = 2
    max_pages_per_domain: int = 50

    # Extract
    min_word_count: int = 100
    target_language: str | None = None
    include_tables: bool = True
    favor_precision: bool = True
    date_from: str | None = None
    date_to: str | None = None

    # Store
    output_dir: Path = Path(".")  # directory for output files
    output_path: Path = Path("output.parquet")  # resolved parquet file path
    jsonl_path: Path | None = None
    exclude_file: Path | None = None

    # Checkpoint
    checkpoint_file: Path = Path(".scraper_checkpoint.json")
    resume: bool = False


def apply_stealth(config: ScraperConfig) -> ScraperConfig:
    """Return a new config with stealth-mode overrides."""
    if not config.stealth:
        return config
    overrides = {
        "max_concurrent_total": 4,
        "max_concurrent_per_domain": 2,
        "search_delay_min": 5.0,
        "search_delay_max": 8.0,
    }
    # frozen dataclass: rebuild with overrides
    fields = {f.name: getattr(config, f.name) for f in config.__dataclass_fields__.values()}
    fields.update(overrides)
    return ScraperConfig(**fields)
