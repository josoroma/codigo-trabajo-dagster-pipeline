"""Partition definitions used across the asset graph."""

from __future__ import annotations

import re

import dagster as dg

from .constants import CHUNKS_DIR
from .core.chunking import article_sort_key


# One partition per article. Keys are the article id strings used in the
# chunk filenames: e.g. ``"94"`` for ``articulo_94.json`` and ``"94_bis"``
# for ``articulo_94_bis.json``. Partitions are registered at runtime by the
# ``article_chunks`` asset based on the contents of ``data/chunks/``.
articles_partitions = dg.DynamicPartitionsDefinition(name="articles")

ARTICLE_KEY_RE = re.compile(
    r"^\d+(?:_(?:bis|ter|quater|quinquies|sexies|septies|octies|novies|decies))?$"
)


def article_id_from_chunk_path(name: str) -> str:
    """``articulo_94.json`` → ``"94"``; ``articulo_94_bis.json`` → ``"94_bis"``."""
    return name.removesuffix(".json").removeprefix("articulo_")


def is_article_partition_key(article_id: str) -> bool:
    """Return true for article ids that map to generated chunk filenames."""
    return bool(ARTICLE_KEY_RE.fullmatch(article_id))


def article_chunk_path(article_id: str):
    """Return the expected chunk path for a valid article partition key."""
    if not is_article_partition_key(article_id):
        raise ValueError(f"Invalid article partition key: {article_id}")
    return CHUNKS_DIR / f"articulo_{article_id}.json"


def discover_article_keys() -> list[str]:
    """Return all article ids currently present on disk, naturally sorted."""
    if not CHUNKS_DIR.is_dir():
        return []

    return sorted(
        (
            article_id
            for p in CHUNKS_DIR.glob("articulo_*.json")
            if is_article_partition_key(article_id := article_id_from_chunk_path(p.name))
        ),
        key=article_sort_key,
    )


def sync_article_partitions(instance) -> tuple[list[str], list[str], int]:
    """Register on-disk article chunks and prune stale dynamic partition keys."""
    keys = discover_article_keys()
    existing = set(instance.get_dynamic_partitions("articles"))

    stale_keys = sorted(existing - set(keys), key=str)
    for key in stale_keys:
        instance.delete_dynamic_partition("articles", key)

    new_keys = [key for key in keys if key not in existing]
    if new_keys:
        instance.add_dynamic_partitions("articles", new_keys)

    return new_keys, stale_keys, len(keys)
