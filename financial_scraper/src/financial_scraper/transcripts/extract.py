"""Extract structured transcript content from Motley Fool HTML."""

import json
import logging
import re
from dataclasses import dataclass, field

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Speaker pattern: "Name:" or "Name, Title:" at the start of a paragraph
_SPEAKER_RE = re.compile(r"^([A-Z][A-Za-z\s.\-']+?)(?:\s*,\s*[A-Za-z\s]+)?:")


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


def _extract_section_text(body: BeautifulSoup, start_heading: str, stop_headings: set[str]) -> str:
    """Extract text between an h2 heading and the next h2."""
    paragraphs = []
    found = False
    for child in body.children:
        if not child.name:
            continue
        if child.name == "h2":
            heading_text = child.get_text(strip=True).upper()
            if start_heading.upper() in heading_text:
                found = True
                continue
            if found and any(h.upper() in heading_text for h in stop_headings):
                break
        if found and child.name in ("p", "ul"):
            text = child.get_text(strip=True)
            if text:
                paragraphs.append(text)
    return "\n\n".join(paragraphs)


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

    # 3. Extract DATE section
    date_text = _extract_section_text(body, "DATE", {"CALL PARTICIPANTS", "TAKEAWAYS"})
    if date_text and not result.date:
        result.date = date_text.strip()

    # 4. Extract CALL PARTICIPANTS
    for h2 in body.find_all("h2"):
        if "CALL PARTICIPANTS" in h2.get_text(strip=True).upper():
            ul = h2.find_next_sibling("ul")
            if ul:
                for li in ul.find_all("li"):
                    result.participants.append(li.get_text(strip=True))
            break

    # 5. Extract Full Conference Call Transcript
    all_sections = {"DATE", "CALL PARTICIPANTS", "TAKEAWAYS", "RISKS",
                    "SUMMARY", "INDUSTRY GLOSSARY", "FULL CONFERENCE CALL TRANSCRIPT",
                    "PREMIUM INVESTING"}

    transcript_text = _extract_section_text(
        body, "Full Conference Call Transcript", {"PREMIUM INVESTING", "Premium Investing"}
    )

    if not transcript_text:
        # Fallback: grab all <p> text from article body
        paragraphs = []
        for p in body.find_all("p"):
            text = p.get_text(strip=True)
            if text and len(text) > 20:
                paragraphs.append(text)
        transcript_text = "\n\n".join(paragraphs)

    result.full_text = transcript_text

    # 6. Extract speakers from transcript text
    speakers = set()
    for line in transcript_text.split("\n"):
        m = _SPEAKER_RE.match(line.strip())
        if m:
            speaker = m.group(1).strip()
            # Filter out very short or all-caps section headers
            if len(speaker) > 2 and not speaker.isupper():
                speakers.add(speaker)
    result.speakers = sorted(speakers)

    # 7. Split into prepared remarks vs Q&A
    operator_qa_markers = [
        "we will now begin the question",
        "question-and-answer session",
        "q&a session",
        "open the line for questions",
    ]
    lower_text = transcript_text.lower()
    split_idx = -1
    for marker in operator_qa_markers:
        idx = lower_text.find(marker)
        if idx != -1:
            split_idx = idx
            break

    if split_idx > 0:
        result.prepared_remarks = transcript_text[:split_idx].strip()
        result.qa_section = transcript_text[split_idx:].strip()
    else:
        result.prepared_remarks = transcript_text

    # 8. Parse quarter/year from headline if not set
    if result.company and not result.quarter:
        q_match = re.search(r"Q(\d)", result.company)
        if q_match:
            result.quarter = f"Q{q_match.group(1)}"
        y_match = re.search(r"20\d{2}", result.company)
        if y_match:
            result.year = int(y_match.group())

    return result
