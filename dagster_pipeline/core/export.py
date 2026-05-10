"""JSONL export (Stage 5)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

EXPORT_FIELDS = (
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


def write_jsonl(entries: Iterable[dict[str, Any]], dest: Path) -> int:
    """Write ``entries`` to ``dest`` as JSONL. Returns the record count."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with dest.open("w", encoding="utf-8") as fh:
        for entry in entries:
            record = {k: entry.get(k, "") for k in EXPORT_FIELDS}
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count
