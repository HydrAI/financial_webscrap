"""Assignee name normalization and disambiguation."""

import re
from difflib import SequenceMatcher

# Common corporate suffixes to strip before comparison
_SUFFIXES = re.compile(
    r",?\s*\b("
    r"LLC|L\.L\.C\.|Inc\.?|Incorporated|Corp\.?|Corporation|"
    r"Ltd\.?|Limited|GmbH|AG|S\.A\.|SA|S\.r\.l\.|"
    r"Co\.?|Company|PLC|Pty|NV|BV|SE|KG|OHG|"
    r"LP|L\.P\."
    r")\s*$",
    re.IGNORECASE,
)


def normalize_assignee(name: str) -> str:
    """Strip corporate suffixes and normalize whitespace/case.

    Examples:
        "Droneshield LLC"    -> "droneshield"
        "DroneShield Ltd."   -> "droneshield"
        "DRONESHIELD, Inc."  -> "droneshield"
    """
    if not name:
        return ""
    cleaned = name.strip()
    # Remove suffixes (may need multiple passes for "Corp. Ltd.")
    for _ in range(3):
        prev = cleaned
        cleaned = _SUFFIXES.sub("", cleaned).strip().rstrip(",").strip()
        if cleaned == prev:
            break
    return cleaned.lower()


def are_same_assignee(a: str, b: str, threshold: float = 0.15) -> bool:
    """Check if two assignee names refer to the same entity.

    Uses difflib.SequenceMatcher on normalized names. The threshold
    is the maximum allowed distance (1 - similarity_ratio). A threshold
    of 0.15 means names must be >= 85% similar.
    """
    norm_a = normalize_assignee(a)
    norm_b = normalize_assignee(b)
    if not norm_a or not norm_b:
        return False
    if norm_a == norm_b:
        return True
    ratio = SequenceMatcher(None, norm_a, norm_b).ratio()
    return (1.0 - ratio) <= threshold
