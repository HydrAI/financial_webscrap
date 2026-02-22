"""Extract structured transcript content from Motley Fool HTML."""

import json
import logging
import re
from dataclasses import dataclass, field

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# Speaker line: "Name -- Title" (live format, extracted text from <strong>/<em>)
_SPEAKER_DASH_RE = re.compile(r"^([A-Z][A-Za-z\s.\-']+?)\s*--\s*(.+)$")

# Speaker line: "Name:" or "Name, Title:" (older format)
_SPEAKER_COLON_RE = re.compile(r"^([A-Z][A-Za-z\s.\-']+?)(?:\s*,\s*[A-Za-z\s]+)?:")

# Common words that should not start a speaker name
_NON_NAME_WORDS = frozenset({
    "A", "About", "After", "All", "Also", "An", "And", "Are", "As", "At",
    "Be", "But", "By", "Can", "Do", "Did", "For", "From", "Get", "Had",
    "Has", "Have", "He", "Her", "Here", "His", "How", "I", "If", "In",
    "Into", "Is", "It", "Its", "Just", "Let", "Like", "May", "More", "My",
    "No", "Not", "Now", "Of", "On", "One", "Or", "Our", "Out", "Over",
    "So", "Some", "Such", "Than", "That", "The", "Their", "Then", "There",
    "These", "They", "This", "To", "Up", "Very", "Was", "We", "Were",
    "What", "When", "Which", "While", "Who", "Why", "Will", "With", "Would",
    "You", "Your",
})

# Heading normalization — strip colons, whitespace, lowercase for matching
def _norm_heading(text: str) -> str:
    return text.strip().rstrip(":").strip().upper()

# Known section heading patterns (normalized)
_PREPARED_REMARKS = {"PREPARED REMARKS"}
_QA_SECTION = {"QUESTIONS & ANSWERS", "QUESTIONS AND ANSWERS", "Q&A"}
_PARTICIPANTS = {"CALL PARTICIPANTS"}
_CONTENTS = {"CONTENTS"}
_FULL_TRANSCRIPT = {"FULL CONFERENCE CALL TRANSCRIPT"}


@dataclass
class TranscriptResult:
    """Structured earnings call transcript."""
    company: str = ""
    ticker: str = ""
    quarter: str = ""
    year: int = 0
    date: str = ""  # ISO date string
    full_text: str = ""
    speakers: list[str] = field(default_factory=list)
    participants: list[str] = field(default_factory=list)
    prepared_remarks: str = ""
    qa_section: str = ""


def _extract_json_ld(soup: BeautifulSoup) -> dict:
    """Extract JSON-LD structured data from the page."""
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if data.get("@type") == "NewsArticle":
                return data
        except (json.JSONDecodeError, TypeError):
            continue
    return {}


def _extract_ticker_from_jsonld(data: dict) -> str:
    """Extract ticker from JSON-LD about[].tickerSymbol field."""
    for about in data.get("about", []):
        symbol = about.get("tickerSymbol", "")
        if symbol:
            # Format is "NYSE EXR" or "NASDAQ AAPL" — take the last part
            parts = symbol.strip().split()
            return parts[-1] if parts else ""
    return ""


def _split_body_by_h2(body: Tag) -> dict[str, list[Tag]]:
    """Split article body into sections keyed by normalized h2 heading text.

    Returns a dict mapping normalized heading -> list of sibling elements
    between that h2 and the next h2.
    """
    sections: dict[str, list[Tag]] = {}
    current_key: str | None = None

    for child in body.children:
        if not isinstance(child, Tag):
            continue
        if child.name == "h2":
            current_key = _norm_heading(child.get_text(strip=True))
            sections[current_key] = []
        elif current_key is not None:
            sections[current_key].append(child)

    return sections


def _section_text(elements: list[Tag]) -> str:
    """Join text from paragraph/list elements."""
    paragraphs = []
    for el in elements:
        if el.name in ("p", "ul", "ol"):
            text = el.get_text(strip=True)
            if text:
                paragraphs.append(text)
    return "\n\n".join(paragraphs)


def _find_section(sections: dict[str, list[Tag]], headings: set[str]) -> list[Tag]:
    """Find a section by trying multiple heading variants."""
    for key, elements in sections.items():
        if key in headings:
            return elements
    return []


def _extract_participants_from_elements(elements: list[Tag]) -> list[str]:
    """Extract participant names from section elements.

    Handles two formats:
    - <ul><li>Name</li></ul> (older HTML)
    - <p><strong>Name</strong> -- <em>Title</em></p> (live HTML)
    """
    participants = []
    for el in elements:
        if el.name == "ul":
            for li in el.find_all("li"):
                text = li.get_text(strip=True)
                if text:
                    participants.append(text)
        elif el.name == "p":
            strong = el.find("strong")
            if strong and strong.get_text(strip=True):
                text = el.get_text(strip=True)
                if text and len(text) < 200:
                    participants.append(text)
    return participants


def _extract_speakers_from_elements(elements: list[Tag]) -> list[str]:
    """Extract unique speaker names from <strong> tags in section elements.

    In live Motley Fool HTML, speaker lines are:
      <p><strong>Name</strong> -- <em>Title</em></p>
    or just:
      <p><strong>Operator</strong></p>
    """
    speakers = set()
    for el in elements:
        if el.name != "p":
            continue
        strong = el.find("strong")
        if not strong:
            continue
        # A speaker <p> has <strong> as its first meaningful child and
        # the <strong> name is a large portion of the <p> text (not a bold
        # word inside a long paragraph of speech)
        text = el.get_text(strip=True)
        name = strong.get_text(strip=True)
        if not name or len(name) <= 2:
            continue
        # Speaker <p> tags are short: "Name -- Title" or just "Name"
        # Reject if <strong> text is less than 30% of <p> text (speech paragraph)
        if len(name) < len(text) * 0.3:
            continue
        # Speaker names never contain colons (filters "Duration: 0 minutes" etc.)
        if ":" in name:
            continue
        # Filter names starting with common non-name words
        first_word = name.split()[0] if name.split() else ""
        if first_word not in _NON_NAME_WORDS:
            speakers.add(name)
    return speakers


def _extract_speakers_from_text(text: str) -> list[str]:
    """Extract unique speaker names from plain transcript text (fallback).

    Uses "Name:" pattern for older format transcripts. The dash pattern
    is not used here because em-dashes in normal speech cause false positives;
    use _extract_speakers_from_elements for dash-format HTML instead.
    """
    speakers = set()
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue

        m = _SPEAKER_COLON_RE.match(line)
        if m:
            speaker = m.group(1).strip()
            words = speaker.split()
            # Must be 2+ words, not all-caps, and first word looks like a name
            if (len(words) >= 2
                    and not speaker.isupper()
                    and words[0] not in _NON_NAME_WORDS):
                speakers.add(speaker)

    return sorted(speakers)


def _split_prepared_qa(
    sections: dict[str, list[Tag]], full_text: str,
) -> tuple[str, str]:
    """Split transcript into prepared remarks and Q&A.

    First tries h2-based section split, then falls back to text markers.
    """
    # Strategy 1: Use h2 section headings
    prepared_els = _find_section(sections, _PREPARED_REMARKS)
    qa_els = _find_section(sections, _QA_SECTION)

    if prepared_els and qa_els:
        return _section_text(prepared_els), _section_text(qa_els)

    # Strategy 2: Text-based markers in the full text
    markers = [
        "questions & answers",
        "questions and answers",
        "we will now begin the question",
        "question-and-answer session",
        "q&a session",
        "open the line for questions",
    ]
    lower_text = full_text.lower()
    for marker in markers:
        idx = lower_text.find(marker)
        if idx != -1:
            return full_text[:idx].strip(), full_text[idx:].strip()

    return full_text, ""


def extract_transcript(html: str) -> TranscriptResult | None:
    """Parse Motley Fool transcript HTML into structured result.

    Returns None if the page doesn't contain a recognizable transcript.
    """
    soup = BeautifulSoup(html, "lxml")
    result = TranscriptResult()

    # 1. Extract metadata from JSON-LD
    jsonld = _extract_json_ld(soup)
    if jsonld:
        headline = jsonld.get("headline", "")
        result.company = headline
        result.ticker = _extract_ticker_from_jsonld(jsonld)
        result.date = jsonld.get("datePublished", "")[:10]  # YYYY-MM-DD

    # 2. Find the article body
    body = soup.find("div", class_="article-body")
    if not body:
        logger.warning("No article-body div found")
        return None

    # 3. Split body into h2-delimited sections
    sections = _split_body_by_h2(body)

    # 4. Extract participants
    participant_els = _find_section(sections, _PARTICIPANTS)
    if participant_els:
        result.participants = _extract_participants_from_elements(participant_els)

    # 5. Build full transcript text
    # Try "Prepared Remarks" + "Q&A" sections first (live format)
    prepared_els = _find_section(sections, _PREPARED_REMARKS)
    qa_els = _find_section(sections, _QA_SECTION)

    if prepared_els or qa_els:
        parts = []
        if prepared_els:
            parts.append(_section_text(prepared_els))
        if qa_els:
            parts.append(_section_text(qa_els))
        transcript_text = "\n\n".join(parts)
    else:
        # Try older "Full Conference Call Transcript" heading
        full_els = _find_section(sections, _FULL_TRANSCRIPT)
        if full_els:
            transcript_text = _section_text(full_els)
        else:
            # Fallback: grab all <p> text from article body
            paragraphs = []
            for p in body.find_all("p"):
                text = p.get_text(strip=True)
                if text and len(text) > 20:
                    paragraphs.append(text)
            transcript_text = "\n\n".join(paragraphs)

    result.full_text = transcript_text

    # 6. Extract speakers — prefer HTML <strong> tags, fall back to text regex
    if prepared_els or qa_els:
        html_speakers = set()
        if prepared_els:
            html_speakers |= _extract_speakers_from_elements(prepared_els)
        if qa_els:
            html_speakers |= _extract_speakers_from_elements(qa_els)
        result.speakers = sorted(html_speakers)
    else:
        result.speakers = _extract_speakers_from_text(transcript_text)

    # 7. Split into prepared remarks vs Q&A
    result.prepared_remarks, result.qa_section = _split_prepared_qa(
        sections, transcript_text,
    )

    # 8. Parse quarter/year from headline if not set
    if result.company and not result.quarter:
        q_match = re.search(r"Q(\d)", result.company)
        if q_match:
            result.quarter = f"Q{q_match.group(1)}"
        y_match = re.search(r"20\d{2}", result.company)
        if y_match:
            result.year = int(y_match.group())

    return result
