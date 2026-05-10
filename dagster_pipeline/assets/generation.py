"""Stage 3 — generate grounded examples per article via OpenRouter."""

from __future__ import annotations

import json

import dagster as dg

from ..constants import (
    OPENROUTER_POOL,
    SYNTHETIC_DIR,
)
from ..core.generation import expand_generated_entries
from ..core.parsing import parse_json_array
from ..core.prompts import SYSTEM_PROMPT, build_batch_prompt
from ..partitions import article_chunk_path, articles_partitions
from ..resources import OpenRouterResource
from .chunking import article_chunks


@dg.asset(
    deps=[article_chunks],
    partitions_def=articles_partitions,
    group_name="generation",
    compute_kind="openrouter",
    op_tags={"dagster/concurrency_key": OPENROUTER_POOL},
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=5,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
)
def synthetic_entries(
    context,
    openrouter: OpenRouterResource,
) -> dg.MaterializeResult:
    """Produce grounded explanation, QA, and citation entries for one article."""
    article_id = context.partition_key
    try:
        chunk_path = article_chunk_path(article_id)
    except ValueError as exc:
        raise dg.Failure(
            description=(
                f"{exc}. This is probably a stale dynamic partition or an old "
                "run/backfill. Run `make seed-partitions`, then launch a fresh "
                "materialization."
            ),
            metadata={"partition_key": article_id},
            allow_retries=False,
        ) from exc
    if not chunk_path.exists():
        raise dg.Failure(
            description=(
                f"Chunk file is missing for article partition {article_id}: "
                f"{chunk_path}. Run `make seed-partitions`, then launch a fresh "
                "materialization/backfill instead of retrying the old run."
            ),
            metadata={
                "partition_key": article_id,
                "expected_path": dg.MetadataValue.path(str(chunk_path)),
            },
            allow_retries=False,
        )
    out_path = SYNTHETIC_DIR / f"articulo_{article_id}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = None
        if isinstance(existing, list) and existing:
            context.log.info(
                f"synthetic_entries[{article_id}]: reusing existing {out_path}"
            )
            return dg.MaterializeResult(
                metadata={
                    "entries": len(existing),
                    "reused": True,
                    "model": openrouter.model_name,
                    "path": dg.MetadataValue.path(str(out_path)),
                }
            )

    chunk = json.loads(chunk_path.read_text(encoding="utf-8"))
    prompt = build_batch_prompt(
        article_num=chunk["article"],
        content=chunk["content"],
        chunk_id=chunk["chunk_id"],
    )

    context.log.info(f"synthetic_entries[{article_id}]: generating {out_path}")
    response = openrouter.call(
        prompt,
        system=SYSTEM_PROMPT,
        response_format_json=True,
        logger=context.log,
    )
    if not response:
        raise dg.Failure(description=f"OpenRouter returned no content for article {article_id}")

    parsed = parse_json_array(response) or []
    entries = expand_generated_entries(parsed, chunk)
    if not entries:
        raise dg.Failure(description=f"Could not parse generation for article {article_id}")

    out_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
    context.log.info(
        f"synthetic_entries[{article_id}]: wrote {len(entries)} entries to {out_path}"
    )
    return dg.MaterializeResult(
        metadata={
            "entries": len(entries),
            "dataset_types": [e.get("dataset_type", "?") for e in entries],
            "reused": False,
            "model": openrouter.model_name,
            "path": dg.MetadataValue.path(str(out_path)),
        }
    )
