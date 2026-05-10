"""Tolerant JSON-array parser for LLM responses."""

from __future__ import annotations

import json
import re
from typing import Any


def parse_json_array(text: str | None) -> list[dict[str, Any]] | None:
    """Best-effort extraction of a JSON array of objects from ``text``."""
    if not text:
        return None

    cleaned = re.sub(r"^```json\s*", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"^```\s*", "", cleaned)
    cleaned = re.sub(r"```$", "", cleaned).strip()

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        data = None

    if isinstance(data, dict) and isinstance(data.get("entries"), list):
        return data["entries"]
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]

    arr_match = re.search(r"\[.*\]", cleaned, re.DOTALL)
    if arr_match:
        snippet = arr_match.group(0)
        for candidate in (snippet, re.sub(r",\s*([}\]])", r"\1", snippet)):
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                continue

    objects = []
    for obj_match in re.finditer(r"\{[^{}]*\}", cleaned):
        try:
            objects.append(json.loads(obj_match.group(0)))
        except json.JSONDecodeError:
            continue
    return objects or None
