#!/usr/bin/env python3
"""Parse commodity_data_sources.md into a structured CSV + bucketed crawl seeds.

The catalog (Downloads/commodity_data_sources.md) lists ~120 public commodity
data sources with per-source fields (url, format, frequency, access,
coverage, data_type, notes, tags). This script:

  1. Parses every ``### Source`` entry under each ``## N. Category`` heading.
  2. Classifies each source into a crawl bucket (deep / api_numeric /
     skip_binary / excluded_paid) from its format + access + data_type.
  3. Splits multi-URL entries (``url_a ; url_b``) into individual seeds.
  4. Writes:
       config/commodity_data_sources.csv          (full structured table)
       config/data_sources_seeds_deep.txt         (super-deep BFS crawl targets)
       config/data_sources_seeds_api_numeric.txt  (need bespoke API ingestion)
       config/data_sources_seeds_excluded.txt     (paid / pure binary geodata)

Usage:
    C:\\T\\python.exe financial_scraper/scripts/_parse_data_sources.py \\
        "C:\\Users\\TonyGuida\\Downloads\\commodity_data_sources.md"
"""

import csv
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]  # financial_webscrap/
CONFIG = ROOT / "financial_scraper" / "config"

FIELD_RE = re.compile(r"^- \*\*(?P<key>[\w_]+)\*\*:\s*(?P<val>.*)$")
CATEGORY_RE = re.compile(r"^##\s+\d+\.\s+(?P<cat>.+)$")
ENTRY_RE = re.compile(r"^###\s+(?P<name>.+)$")

# Markers that mean "binary geodata / imagery download portal" — no text to crawl.
BINARY_MARKERS = ["geotiff", "netcdf", "grib", "hdf", "kml"]


def parse(md_path: Path) -> list[dict]:
    """Parse the markdown catalog into a list of source dicts."""
    sources: list[dict] = []
    category = ""
    current: dict | None = None

    in_body = False
    for raw in md_path.read_text(encoding="utf-8").splitlines():
        line = raw.rstrip()

        # Skip the YAML front-matter block (between the first two '---').
        if line == "---" and not in_body:
            # Toggle until we've seen the closing fence; simplest: start body
            # once we hit the first top-level '# ' heading.
            continue

        cat_m = CATEGORY_RE.match(line)
        if cat_m:
            in_body = True
            category = cat_m.group("cat").strip()
            continue

        if line.startswith("# "):
            in_body = True
            continue

        if not in_body:
            continue

        entry_m = ENTRY_RE.match(line)
        if entry_m:
            if current and current.get("url"):
                sources.append(current)
            current = {
                "name": entry_m.group("name").strip(),
                "category": category,
                "url": "",
                "format": "",
                "frequency": "",
                "access": "",
                "coverage": "",
                "data_type": "",
                "notes": "",
                "tags": "",
            }
            continue

        if current is None:
            continue

        fld = FIELD_RE.match(line)
        if fld:
            key = fld.group("key").lower()
            val = fld.group("val").strip()
            if key in current:
                current[key] = val

    if current and current.get("url"):
        sources.append(current)
    return sources


def classify(fmt: str, access: str, data_type: str) -> str:
    """Bucket a source for crawl strategy from its catalog fields."""
    fmt_l = fmt.lower()
    access_l = access.lower()
    dt = data_type.lower()

    # Paid-only sources will just wall us off.
    if "paid" in access_l and "free" not in access_l and "mixed" not in access_l:
        return "excluded_paid"

    # Imagery / geodata download portals: binary rasters, nothing to text-crawl.
    if dt == "imagery" and any(m in fmt_l for m in BINARY_MARKERS):
        return "skip_binary"
    if any(m in fmt_l for m in BINARY_MARKERS) and "html" not in fmt_l and "pdf" not in fmt_l:
        return "skip_binary"

    # Pure numeric REST APIs with no HTML/PDF narrative around them: these need a
    # bespoke API client, not a page crawl.
    has_doc = ("pdf" in fmt_l) or ("html" in fmt_l) or ("web" in fmt_l)
    if "api" in fmt_l and dt == "numeric" and not has_doc:
        return "api_numeric"

    return "deep"


def split_urls(url_field: str) -> list[str]:
    """Split a possibly-multi-URL field into clean http(s) URLs."""
    out: list[str] = []
    for chunk in re.split(r"[;\s]+", url_field):
        chunk = chunk.strip().strip(",")
        if chunk.startswith("http://") or chunk.startswith("https://"):
            out.append(chunk)
    return out


def domain_of(url: str) -> str:
    netloc = urlparse(url).netloc.lower()
    return netloc[4:] if netloc.startswith("www.") else netloc


def main():
    if len(sys.argv) < 2:
        print("Usage: _parse_data_sources.py <commodity_data_sources.md>")
        sys.exit(1)

    md_path = Path(sys.argv[1])
    if not md_path.exists():
        print(f"Catalog not found: {md_path}")
        sys.exit(1)

    sources = parse(md_path)
    print(f"Parsed {len(sources)} sources from {md_path.name}")

    # Annotate + write CSV
    csv_path = CONFIG / "commodity_data_sources.csv"
    buckets: dict[str, list[str]] = {
        "deep": [],
        "api_numeric": [],
        "skip_binary": [],
        "excluded_paid": [],
    }
    seen_in_bucket: dict[str, set[str]] = {k: set() for k in buckets}

    fieldnames = [
        "name", "category", "url", "all_urls", "domain", "crawl_class",
        "format", "frequency", "access", "coverage", "data_type", "tags", "notes",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for s in sources:
            urls = split_urls(s["url"])
            if not urls:
                continue
            cls = classify(s["format"], s["access"], s["data_type"])
            writer.writerow({
                "name": s["name"],
                "category": s["category"],
                "url": urls[0],
                "all_urls": " ; ".join(urls),
                "domain": domain_of(urls[0]),
                "crawl_class": cls,
                "format": s["format"],
                "frequency": s["frequency"],
                "access": s["access"],
                "coverage": s["coverage"],
                "data_type": s["data_type"],
                "tags": s["tags"],
                "notes": s["notes"],
            })
            for u in urls:
                if u not in seen_in_bucket[cls]:
                    seen_in_bucket[cls].add(u)
                    buckets[cls].append(u)

    print(f"Wrote structured table -> {csv_path}")

    # Write seed files
    deep_path = CONFIG / "data_sources_seeds_deep.txt"
    api_path = CONFIG / "data_sources_seeds_api_numeric.txt"
    excl_path = CONFIG / "data_sources_seeds_excluded.txt"

    deep_path.write_text("\n".join(buckets["deep"]) + "\n", encoding="utf-8")
    api_path.write_text(
        "# Numeric REST APIs — ingest via bespoke API clients, NOT page crawl.\n"
        + "\n".join(buckets["api_numeric"]) + "\n",
        encoding="utf-8",
    )
    excl_path.write_text(
        "# Excluded from crawl: paid sources + binary/imagery geodata portals.\n"
        + "\n".join(buckets["skip_binary"] + buckets["excluded_paid"]) + "\n",
        encoding="utf-8",
    )

    print(f"\n=== Bucket summary ===")
    for k in ("deep", "api_numeric", "skip_binary", "excluded_paid"):
        print(f"  {k:14s}: {len(buckets[k])} URLs")
    print(f"\nDeep-crawl seeds -> {deep_path}")
    print(f"API/numeric list -> {api_path}")
    print(f"Excluded list    -> {excl_path}")


if __name__ == "__main__":
    main()
