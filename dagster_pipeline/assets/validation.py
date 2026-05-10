"""Stage 4 — drop entries that are missing grounding or required fields."""

from __future__ import annotations

import json

import dagster as dg

from ..constants import SYNTHETIC_DIR, VALIDATED_DIR
from ..core.validation import filter_valid
from ..partitions import article_chunk_path, articles_partitions
from .generation import synthetic_entries


@dg.asset(
    deps=[synthetic_entries],
    partitions_def=articles_partitions,
    group_name="validation",
    compute_kind="python",
)
def validated_entries(context) -> dg.MaterializeResult:
    """Validate one article's synthetic entries and persist the survivors."""
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
    in_path = SYNTHETIC_DIR / f"articulo_{article_id}.json"
    out_path = VALIDATED_DIR / f"articulo_{article_id}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
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
    if not in_path.exists():
        raise dg.Failure(
            description=(
                f"Synthetic output is missing for article partition {article_id}: "
                f"{in_path}. Materialize `synthetic_entries` for this partition first."
            ),
            metadata={
                "partition_key": article_id,
                "expected_path": dg.MetadataValue.path(str(in_path)),
            },
            allow_retries=False,
        )

    raw = json.loads(in_path.read_text(encoding="utf-8"))
    chunk = json.loads(chunk_path.read_text(encoding="utf-8"))
    valid, rejections = filter_valid(raw, source_text=chunk.get("content", ""))
    out_path.write_text(json.dumps(valid, ensure_ascii=False, indent=2), encoding="utf-8")
    context.log.info(
        f"validated_entries[{article_id}]: synthetic={in_path} "
        f"validated={out_path} valid={len(valid)} rejected={len(rejections)}"
    )

    if rejections:
        context.log.warning(
            f"validated_entries[{article_id}]: dropped {len(rejections)} "
            f"({rejections[0]}{' …' if len(rejections) > 1 else ''})"
        )
    return dg.MaterializeResult(
        metadata={
            "valid": len(valid),
            "rejected": len(rejections),
            "rejection_reasons": dg.MetadataValue.json(rejections[:5]),
            "path": dg.MetadataValue.path(str(out_path)),
        }
    )
