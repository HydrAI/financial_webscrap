"""Configuration for the patents subcommand."""

import json
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PatentConfig:
    # Input — at least one source required
    company: str = ""
    ids_file: Path | None = None
    ids: tuple[str, ...] = ()
    assignee: str = ""
    search_queries: tuple[str, ...] = ()

    # Classification filter (post-fetch)
    cpc_filter: tuple[str, ...] = ()
    ipc_filter: tuple[str, ...] = ()
    wipo_categories: tuple[str, ...] = ()

    # Discovery
    discover_via_google_patents: bool = True  # Primary: Google Patents XHR
    discover_via_search: bool = False         # Fallback: DuckDuckGo
    discover_via_justia: bool = False         # Best-effort: Justia
    max_discovery_results: int = 50

    # Fetch
    delay: float = 4.0
    max_delay: float = 60.0
    timeout: int = 30

    # Store
    output_dir: Path = Path(".")
    output_path: Path = Path("output.parquet")
    jsonl_path: Path | None = None

    # Checkpoint
    checkpoint_file: Path = Path(".patent_checkpoint.json")
    resume: bool = False


def load_targets_file(path: Path) -> list[PatentConfig]:
    """Load patent targets from a JSON config file.

    The JSON file has two sections:
      - "targets": company-based searches (assignee + optional IDs)
      - "themes": topic-based searches (keyword queries + classification filters)

    Each entry becomes a PatentConfig. The pipeline processes them sequentially.
    """
    if not path.exists():
        logger.error(f"Targets file not found: {path}")
        return []

    data = json.loads(path.read_text(encoding="utf-8"))
    configs: list[PatentConfig] = []

    # Company-based targets
    for target in data.get("targets", []):
        configs.append(PatentConfig(
            company=target.get("company", ""),
            assignee=target.get("assignee", ""),
            ids=tuple(target.get("ids", [])),
            discover_via_google_patents=target.get("discover_google", True),
            discover_via_search=target.get("discover_search", False),
            discover_via_justia=target.get("discover_justia", False),
            cpc_filter=tuple(target.get("cpc_filter", [])),
            ipc_filter=tuple(target.get("ipc_filter", [])),
            wipo_categories=tuple(target.get("wipo_categories", [])),
            max_discovery_results=target.get("max_discovery", 50),
        ))

    # Theme-based targets (topic search, no assignee)
    for theme in data.get("themes", []):
        configs.append(PatentConfig(
            company=theme.get("name", ""),
            search_queries=tuple(theme.get("search_queries", [])),
            discover_via_google_patents=True,
            cpc_filter=tuple(theme.get("cpc_filter", [])),
            ipc_filter=tuple(theme.get("ipc_filter", [])),
            wipo_categories=tuple(theme.get("wipo_categories", [])),
            max_discovery_results=theme.get("max_discovery", 50),
        ))

    logger.info(
        f"Loaded {len(configs)} target(s) from {path} "
        f"({len(data.get('targets', []))} companies, "
        f"{len(data.get('themes', []))} themes)"
    )
    return configs
