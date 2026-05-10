"""Rebuild validated files from existing synthetic outputs without OpenRouter."""

from __future__ import annotations

import json

from .constants import CHUNKS_DIR, SYNTHETIC_DIR, VALIDATED_DIR
from .core.validation import filter_valid
from .partitions import article_id_from_chunk_path, is_article_partition_key


def main() -> None:
    VALIDATED_DIR.mkdir(parents=True, exist_ok=True)
    articles = 0
    valid_total = 0
    rejected_total = 0
    missing_chunks = 0
    invalid_json = 0

    for synthetic_path in sorted(SYNTHETIC_DIR.glob("articulo_*.json")):
        article_id = article_id_from_chunk_path(synthetic_path.name)
        if not is_article_partition_key(article_id):
            continue

        chunk_path = CHUNKS_DIR / synthetic_path.name
        if not chunk_path.exists():
            missing_chunks += 1
            continue

        try:
            raw = json.loads(synthetic_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            invalid_json += 1
            continue

        chunk = json.loads(chunk_path.read_text(encoding="utf-8"))
        valid, rejections = filter_valid(raw, source_text=chunk.get("content", ""))
        out_path = VALIDATED_DIR / synthetic_path.name
        out_path.write_text(json.dumps(valid, ensure_ascii=False, indent=2), encoding="utf-8")

        articles += 1
        valid_total += len(valid)
        rejected_total += len(rejections)

    print(
        "Revalidated "
        f"{articles} synthetic files: {valid_total} valid entries, "
        f"{rejected_total} rejected, {missing_chunks} missing chunks, "
        f"{invalid_json} invalid JSON files."
    )


if __name__ == "__main__":
    main()
