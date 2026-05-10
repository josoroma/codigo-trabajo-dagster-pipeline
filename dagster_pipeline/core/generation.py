"""Normalize LLM generation responses into flat dataset entries."""

from __future__ import annotations

from typing import Any

from ..constants import LAW_CODE, SOURCE_URL


def _metadata(entry: dict[str, Any], chunk: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_url": entry.get("source_url") or SOURCE_URL,
        "law_code": entry.get("law_code") or LAW_CODE,
        "article": entry.get("article") or chunk["article"],
        "chunk_id": entry.get("chunk_id") or chunk["chunk_id"],
        "input": entry.get("input", ""),
    }


def _with_metadata(entry: dict[str, Any], chunk: dict[str, Any]) -> dict[str, Any]:
    hydrated = dict(entry)
    hydrated.update(_metadata(entry, chunk))
    return hydrated


def expand_generated_entries(
    entries: list[dict[str, Any]],
    chunk: dict[str, Any],
) -> list[dict[str, Any]]:
    """Flatten structured generation objects into one row per training example.

    The prompt asks for three conceptual objects: an explanation, a list of
    question-answer pairs, and a list of citations. Downstream validation and
    export expect flat instruction-style records, so this function expands the
    nested QA/citation lists while preserving backwards compatibility with the
    previous one-object-per-record schema.
    """
    expanded: list[dict[str, Any]] = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue

        dataset_type = entry.get("dataset_type")
        meta = _metadata(entry, chunk)

        if dataset_type == "qa" and isinstance(entry.get("qa_pairs"), list):
            for idx, pair in enumerate(entry["qa_pairs"], start=1):
                if not isinstance(pair, dict):
                    continue
                expanded.append({
                    **meta,
                    "dataset_type": "qa",
                    "instruction": pair.get("question") or entry.get("instruction", ""),
                    "output": pair.get("answer") or pair.get("output", ""),
                    "source_quote": pair.get("source_quote") or entry.get("source_quote", ""),
                    "item_index": idx,
                })
            continue

        if dataset_type == "cite_article" and isinstance(entry.get("citations"), list):
            for idx, citation in enumerate(entry["citations"], start=1):
                if not isinstance(citation, dict):
                    continue
                quote = citation.get("source_quote") or citation.get("quote", "")
                expanded.append({
                    **meta,
                    "dataset_type": "cite_article",
                    "instruction": (
                        citation.get("instruction")
                        or entry.get("instruction")
                        or f"Cita el artículo {meta['article']} y explica su propósito legal."
                    ),
                    "output": citation.get("purpose") or citation.get("output", ""),
                    "source_quote": quote,
                    "item_index": idx,
                })
            continue

        expanded.append(_with_metadata(entry, chunk))

    return expanded
