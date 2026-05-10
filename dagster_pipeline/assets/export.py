"""Stage 5 — fan-in across all article partitions to a single JSONL file."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import dagster as dg

from ..constants import DATASET_JSONL, UNSLOTH_JSONL, VALIDATED_DIR
from ..core.export import write_jsonl
from .validation import validated_entries


def _article_sort_key(path):
    """Sort articulo_N.json and articulo_N_bis.json numerically."""
    stem = path.stem  # e.g. "articulo_120_bis"
    parts = stem.split("_")  # ["articulo", "120", "bis"]
    try:
        num = int(parts[1])
    except (IndexError, ValueError):
        num = 0
    suffix = "_".join(parts[2:])  # "" or "bis", "ter", etc.
    return (num, suffix)


def _iter_validated_entries():
    for path in sorted(VALIDATED_DIR.glob("articulo_*.json"), key=_article_sort_key):
        try:
            entries = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        yield from entries


@dg.asset(deps=[validated_entries], group_name="export", compute_kind="python")
def dataset_jsonl(context) -> dg.MaterializeResult:
    """Emit ``data/datasets/codigo_trabajo.jsonl`` and report tallies."""
    by_type: Counter[str] = Counter()
    by_article: Counter[str] = Counter()

    def _entries():
        for entry in _iter_validated_entries():
            by_type[entry.get("dataset_type", "?")] += 1
            by_article[str(entry.get("article", "?"))] += 1
            yield entry

    records = write_jsonl(_entries(), DATASET_JSONL)

    context.log.info(
        f"dataset_jsonl: {records} records across {len(by_article)} articles"
    )
    return dg.MaterializeResult(
        metadata={
            "records": records,
            "articles": len(by_article),
            "by_type": dg.MetadataValue.json(dict(by_type)),
            "path": dg.MetadataValue.path(str(DATASET_JSONL)),
        }
    )


def _format_for_unsloth(entry: dict) -> dict:
    """Format validated entry for Unsloth fine-tuning.

    Unsloth expects instruction-input-output format. For legal Q&A:
    - instruction: the legal question or task
    - input: additional context (often article number/source)
    - output: the legal answer
    - text: the full conversation in Alpaca format (optional, for reference)
    """
    instruction = entry.get("instruction", "")
    input_text = entry.get("input", "")
    output = entry.get("output", "")
    article = entry.get("article", "")

    # Build full text in Alpaca chat format for reference
    full_text = f"<s>[INST] {instruction}"
    if input_text:
        full_text += f"\n{input_text}"
    full_text += f" [/INST] {output}</s>"

    return {
        "instruction": instruction,
        "input": input_text,
        "output": output,
        "article": article,
        "text": full_text,
    }


@dg.asset(deps=[dataset_jsonl], group_name="export", compute_kind="python")
def unsloth_training_dataset(context) -> dg.MaterializeResult:
    """Format dataset for Unsloth fine-tuning and emit to ``codigo_trabajo_unsloth.jsonl``.

    This asset transforms the full validated dataset into Unsloth's
    instruction-input-output format, suitable for LLM fine-tuning.
    """
    UNSLOTH_JSONL.parent.mkdir(parents=True, exist_ok=True)
    record_count = 0
    articles_seen: set[str] = set()

    with UNSLOTH_JSONL.open("w", encoding="utf-8") as fh:
        for entry in _iter_validated_entries():
            formatted = _format_for_unsloth(entry)
            fh.write(json.dumps(formatted, ensure_ascii=False) + "\n")
            record_count += 1
            articles_seen.add(str(entry.get("article", "?")))

    context.log.info(
        f"unsloth_training_dataset: {record_count} formatted records "
        f"for fine-tuning ({len(articles_seen)} articles)"
    )
    return dg.MaterializeResult(
        metadata={
            "records": record_count,
            "articles": len(articles_seen),
            "path": dg.MetadataValue.path(str(UNSLOTH_JSONL)),
            "format": "Unsloth instruction-input-output",
            "usage": "Upload to Colab for fine-tuning via Unsloth Studio",
        }
    )
