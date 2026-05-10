"""Stage 2 — split the Markdown into one JSON per article and register
dynamic partitions for downstream assets.
"""

from __future__ import annotations

import json

import dagster as dg

from ..constants import CHUNKS_DIR, MARKDOWN_FILE
from ..core.chunking import (
    TOTAL_ARTICLES,
    chunk_articles,
    fill_missing_articles,
    normalize_markdown,
)
from ..partitions import sync_article_partitions
from .acquisition import markdown_text


def _clear_stale_chunks() -> None:
    if not CHUNKS_DIR.is_dir():
        return
    for pattern in ("articulo_*.json", "document_*.json"):
        for path in CHUNKS_DIR.glob(pattern):
            path.unlink()


@dg.asset(deps=[markdown_text], group_name="chunking", compute_kind="python")
def article_chunks(context) -> dg.MaterializeResult:
    """Chunk the Markdown article-by-article and register one partition per article."""
    md = MARKDOWN_FILE.read_text(encoding="utf-8")
    chunks = chunk_articles(normalize_markdown(md))
    chunks = fill_missing_articles(chunks, TOTAL_ARTICLES)

    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
    _clear_stale_chunks()
    for chunk in chunks:
        if section := chunk.get("document_section"):
            path = CHUNKS_DIR / f"document_{section}.json"
        else:
            path = CHUNKS_DIR / f"articulo_{chunk['article']}.json"
        path.write_text(json.dumps(chunk, ensure_ascii=False, indent=2), encoding="utf-8")

    article_count = sum(1 for chunk in chunks if not chunk.get("document_section"))
    document_count = len(chunks) - article_count
    new_keys, stale_keys, total_partitions = sync_article_partitions(context.instance)

    context.log.info(
        "article_chunks: wrote "
        f"{article_count} article files and {document_count} document files, "
        f"registered {len(new_keys)} new partitions and removed "
        f"{len(stale_keys)} stale partitions"
    )
    return dg.MaterializeResult(
        metadata={
            "total_chunks": len(chunks),
            "article_chunks": article_count,
            "document_chunks": document_count,
            "partitions_total": total_partitions,
            "partitions_added": len(new_keys),
            "partitions_removed": len(stale_keys),
        }
    )
