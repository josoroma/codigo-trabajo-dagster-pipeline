"""Schema validation for generated entries (Stage 4)."""

from __future__ import annotations

import re
from typing import Any

from ..constants import SOURCE_URL

REQUIRED_FIELDS = (
    "instruction",
    "input",
    "output",
    "source_quote",
    "source_url",
    "law_code",
    "article",
    "chunk_id",
    "dataset_type",
)

MIN_QUOTE_LEN = 10
MIN_OUTPUT_LEN = 10


def _normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _strip_markdown(text: str) -> str:
    """Remove Markdown bold/italic markers so plain-text quotes match formatted source."""
    return re.sub(r"\*+", "", text)


def _normalize(text: str) -> str:
    t = _strip_markdown(text)
    # Remove footnote citation markers like () that appear in scraped HTML source
    t = re.sub(r"\(\)", "", t)
    # Normalize broken hyphenation from HTML line-wrapping artifacts (e.g. "médico- sanitaria")
    t = re.sub(r"(?<=\w)-\s+(?=\w)", "-", t)
    return _normalize_ws(t)


def _strip_list_markers(text: str) -> str:
    """Remove list-item markers (a), b), ch), -) that appear at the start or after colons/semicolons.

    This normalises structural formatting differences between the source (which may
    use dash bullets or lettered items) and LLM-generated quotes (which may use
    the other convention or omit markers entirely).
    """
    # Strip leading list marker
    text = re.sub(r"^(?:[a-záéíóúch]{1,3}\)\s+|-\s+)", "", text)
    # Strip list markers after colons or semicolons (list introductions and subsequent items).
    # Also handles "y" / "e" conjunctions before the last item (e.g. "; y e) texto" or "; y - texto").
    text = re.sub(r"([;:])\s*(?:(?:y|e)\s+)?(?:[a-záéíóúch]{1,3}\)\s+|-\s+)", r"\1 ", text)
    return text


def _quote_found_in_source(quote: str, source: str) -> bool:
    """Return True if quote (possibly with ellipsis) appears in source.

    Quotes containing ``...`` or ``[...]`` are split into segments; every
    non-trivial segment must appear verbatim in the normalised source.
    """
    norm_source = _normalize(source)
    norm_quote = _normalize(quote)

    # Fast path: exact substring match
    if norm_quote in norm_source:
        return True

    # Trailing-period fallback: LLM often adds "." to list items that end with ";" in source
    norm_quote_no_period = re.sub(r"\.\s*$", "", norm_quote)
    if len(norm_quote_no_period) >= MIN_QUOTE_LEN and norm_quote_no_period in norm_source:
        return True

    # Ellipsis path: split and check each segment independently
    segments = re.split(r"\[\.\.\.?\]|\[…\]|\.\.\.+|…", norm_quote)
    segments = [s.strip() for s in segments if len(s.strip()) >= MIN_QUOTE_LEN]
    if not segments:
        return False
    if all(seg in norm_source for seg in segments):
        return True

    # List-marker normalisation fallback: strip a)/b)/-  markers and compare again
    norm_source_lm = _strip_list_markers(norm_source)
    norm_quote_lm = _strip_list_markers(norm_quote)
    if norm_quote_lm in norm_source_lm:
        return True
    norm_quote_lm_no_period = re.sub(r"\.\s*$", "", norm_quote_lm)
    if len(norm_quote_lm_no_period) >= MIN_QUOTE_LEN and norm_quote_lm_no_period in norm_source_lm:
        return True
    segments_lm = [_strip_list_markers(s) for s in segments]
    return all(seg in norm_source_lm for seg in segments_lm if len(seg) >= MIN_QUOTE_LEN)


def validate_entry(entry: dict[str, Any], source_text: str | None = None) -> list[str]:
    """Return a list of validation errors (empty list = valid)."""
    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in entry:
            errors.append(f"missing field: {field}")

    quote = (entry.get("source_quote") or "").strip()
    if len(quote) < MIN_QUOTE_LEN:
        errors.append("source_quote too short or empty")
    elif source_text is not None and not _quote_found_in_source(quote, source_text):
        errors.append("source_quote not found in source article")

    if entry.get("source_url") != SOURCE_URL:
        errors.append(f"source_url mismatch: {entry.get('source_url')!r}")

    if len(entry.get("output", "")) < MIN_OUTPUT_LEN:
        errors.append("output too short")

    return errors


def filter_valid(
    entries: list[dict[str, Any]],
    source_text: str | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Split ``entries`` into (valid, rejection_reasons)."""
    valid: list[dict[str, Any]] = []
    rejections: list[str] = []
    for entry in entries:
        errs = validate_entry(entry, source_text=source_text)
        if errs:
            rejections.append(errs[0])
            continue
        valid.append(entry)
    return valid, rejections
