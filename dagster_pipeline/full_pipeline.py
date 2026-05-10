"""Single-run Dagster jobs for executing the full dataset pipeline.

The asset graph uses dynamic partitions for per-article generation, which is
great for targeted retries but awkward when someone wants one Jobs-page button.
These op jobs run the same core logic over real article chunk files in one run.
"""

from __future__ import annotations

import json
from collections import Counter

import dagster as dg

from .constants import (
    CHUNKS_DIR,
    DATASET_JSONL,
    MARKDOWN_FILE,
    OPENROUTER_POOL,
    RAW_HTML,
    SOURCE_URL,
    SYNTHETIC_DIR,
    VALIDATED_DIR,
)
from .core.chunking import (
    TOTAL_ARTICLES,
    chunk_articles,
    fill_missing_articles,
    normalize_markdown,
)
from .core.export import write_jsonl
from .core.fetch import fetch_html, html_to_markdown
from .core.generation import expand_generated_entries
from .core.parsing import parse_json_array
from .core.prompts import SYSTEM_PROMPT, build_batch_prompt
from .core.validation import filter_valid
from .partitions import article_chunk_path, discover_article_keys, sync_article_partitions


def _clear_stale_chunks() -> None:
    if not CHUNKS_DIR.is_dir():
        return
    for pattern in ("articulo_*.json", "document_*.json"):
        for path in CHUNKS_DIR.glob(pattern):
            path.unlink()


def _iter_validated_entries():
    for path in sorted(VALIDATED_DIR.glob("articulo_*.json")):
        try:
            entries = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        yield from entries


@dg.op
def prepare_all_chunks(context) -> dict[str, int]:
    """Fetch/extract/chunk the law and sync dynamic article partitions."""
    html = fetch_html(SOURCE_URL, RAW_HTML)
    md = html_to_markdown(html, MARKDOWN_FILE)
    chunks = fill_missing_articles(
        chunk_articles(normalize_markdown(md)),
        TOTAL_ARTICLES,
    )

    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
    _clear_stale_chunks()
    for chunk in chunks:
        if section := chunk.get("document_section"):
            path = CHUNKS_DIR / f"document_{section}.json"
        else:
            path = CHUNKS_DIR / f"articulo_{chunk['article']}.json"
        path.write_text(json.dumps(chunk, ensure_ascii=False, indent=2), encoding="utf-8")

    new_keys, stale_keys, total_partitions = sync_article_partitions(context.instance)
    article_count = sum(1 for chunk in chunks if not chunk.get("document_section"))
    document_count = len(chunks) - article_count
    context.log.info(
        f"Prepared {article_count} article chunks and {document_count} document chunks; "
        f"registered {len(new_keys)} new partitions and removed {len(stale_keys)} stale."
    )
    return {
        "article_chunks": article_count,
        "document_chunks": document_count,
        "partitions_total": total_partitions,
    }


@dg.op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": OPENROUTER_POOL},
    retry_policy=dg.RetryPolicy(max_retries=2, delay=5, backoff=dg.Backoff.EXPONENTIAL),
)
def generate_and_validate_all_articles(context, _prepared: dict[str, int]) -> dict[str, int]:
    """Generate and validate every registered article partition sequentially."""
    openrouter = context.resources.openrouter
    SYNTHETIC_DIR.mkdir(parents=True, exist_ok=True)
    VALIDATED_DIR.mkdir(parents=True, exist_ok=True)

    generated = 0
    reused = 0
    valid_total = 0
    rejected_total = 0
    skipped_generation = 0
    skipped_articles: list[str] = []
    max_skipped_before_fail = 5
    keys = discover_article_keys()

    for index, article_id in enumerate(keys, start=1):
        chunk_path = article_chunk_path(article_id)
        synthetic_path = SYNTHETIC_DIR / f"articulo_{article_id}.json"
        validated_path = VALIDATED_DIR / f"articulo_{article_id}.json"
        chunk = json.loads(chunk_path.read_text(encoding="utf-8"))

        validated_existing: list[dict] = []
        if validated_path.exists():
            try:
                loaded_validated = json.loads(validated_path.read_text(encoding="utf-8"))
                if isinstance(loaded_validated, list):
                    validated_existing = loaded_validated
            except json.JSONDecodeError:
                context.log.warning(
                    f"Ignoring invalid validated JSON for article {article_id}; rebuilding."
                )

        if synthetic_path.exists():
            try:
                loaded_synthetic = json.loads(synthetic_path.read_text(encoding="utf-8"))
                entries = loaded_synthetic if isinstance(loaded_synthetic, list) else []
            except json.JSONDecodeError:
                entries = []
                context.log.warning(
                    f"Ignoring invalid synthetic JSON for article {article_id}; regenerating."
                )
        else:
            entries = []

        synthetic_has_data = len(entries) > 0
        validated_has_data = len(validated_existing) > 0

        if synthetic_has_data and validated_has_data:
            reused += 1
            valid_total += len(validated_existing)
            context.log.info(
                f"Current article {index}/{len(keys)}: articulo_{article_id} "
                f"synthetic=reused entries={len(entries)} synthetic_path={synthetic_path} "
                f"validated=reused validated_entries={len(validated_existing)} "
                f"validated_path={validated_path}"
            )
            if index == 1 or index % 25 == 0 or index == len(keys):
                context.log.info(
                    f"Processed {index}/{len(keys)} article partitions "
                    f"(latest: {article_id}, valid={len(validated_existing)}, rejected=0)."
                )
            continue

        if synthetic_has_data and not validated_has_data:
            reused += 1
            synthetic_status = "reused"
            context.log.info(
                f"Current article {index}/{len(keys)}: articulo_{article_id} "
                f"synthetic=reused validated=regenerating validated_path={validated_path}"
            )
            valid, rejections = filter_valid(entries, source_text=chunk.get("content", ""))
            validated_path.write_text(
                json.dumps(valid, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            valid_total += len(valid)
            rejected_total += len(rejections)
            context.log.info(
                f"Current article {index}/{len(keys)}: articulo_{article_id} "
                f"synthetic={synthetic_status} entries={len(entries)} "
                f"synthetic_path={synthetic_path} "
                f"validated_entries={len(valid)} rejected={len(rejections)} "
                f"validated_path={validated_path}"
            )
            if index == 1 or index % 25 == 0 or index == len(keys):
                context.log.info(
                    f"Processed {index}/{len(keys)} article partitions "
                    f"(latest: {article_id}, valid={len(valid)}, rejected={len(rejections)})."
                )
            continue

        if not synthetic_has_data and validated_has_data:
            valid_total += len(validated_existing)
            context.log.warning(
                f"Current article {index}/{len(keys)}: articulo_{article_id} "
                f"synthetic=empty validated=reused validated_entries={len(validated_existing)} "
                f"validated_path={validated_path}."
            )
            if index == 1 or index % 25 == 0 or index == len(keys):
                context.log.info(
                    f"Processed {index}/{len(keys)} article partitions "
                    f"(latest: {article_id}, valid={len(validated_existing)}, rejected=0)."
                )
            continue

        context.log.info(
            f"Current article {index}/{len(keys)}: articulo_{article_id} "
            f"synthetic=generating path={synthetic_path}"
        )
        prompt = build_batch_prompt(
            article_num=chunk["article"],
            content=chunk["content"],
            chunk_id=chunk["chunk_id"],
        )
        response = openrouter.call(
            prompt,
            system=SYSTEM_PROMPT,
            response_format_json=True,
            logger=context.log,
        )
        if not response:
            skipped_generation += 1
            skipped_articles.append(article_id)
            context.log.error(
                f"Skipping article {article_id}: OpenRouter returned no content."
            )
            if skipped_generation >= max_skipped_before_fail:
                raise dg.Failure(
                    description=(
                        "Too many OpenRouter empty responses in one run. "
                        "Stopping to avoid exporting a heavily incomplete dataset."
                    ),
                    metadata={
                        "skipped_count": skipped_generation,
                        "sample_articles": skipped_articles[:5],
                    },
                )
            continue

        parsed = parse_json_array(response) or []
        entries = expand_generated_entries(parsed, chunk)
        if not entries:
            skipped_generation += 1
            skipped_articles.append(article_id)
            context.log.error(
                f"Skipping article {article_id}: could not parse generation response."
            )
            if skipped_generation >= max_skipped_before_fail:
                raise dg.Failure(
                    description=(
                        "Too many unparseable generations in one run. "
                        "Stopping to avoid exporting a heavily incomplete dataset."
                    ),
                    metadata={
                        "skipped_count": skipped_generation,
                        "sample_articles": skipped_articles[:5],
                    },
                )
            continue
        synthetic_path.write_text(
            json.dumps(entries, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        generated += 1
        synthetic_status = "generated"

        valid, rejections = filter_valid(entries, source_text=chunk.get("content", ""))
        validated_path.write_text(
            json.dumps(valid, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        valid_total += len(valid)
        rejected_total += len(rejections)

        context.log.info(
            f"Current article {index}/{len(keys)}: articulo_{article_id} "
            f"synthetic={synthetic_status} entries={len(entries)} "
            f"synthetic_path={synthetic_path} "
            f"validated_entries={len(valid)} rejected={len(rejections)} "
            f"validated_path={validated_path}"
        )

        if index == 1 or index % 25 == 0 or index == len(keys):
            context.log.info(
                f"Processed {index}/{len(keys)} article partitions "
                f"(latest: {article_id}, valid={len(valid)}, rejected={len(rejections)})."
            )

    if skipped_generation:
        context.log.warning(
            "Generation skipped for "
            f"{skipped_generation} articles due to transient or unparseable LLM responses. "
            f"Sample: {skipped_articles[:5]}"
        )

    return {
        "articles": len(keys),
        "generated": generated,
        "reused_synthetic": reused,
        "valid_entries": valid_total,
        "rejected_entries": rejected_total,
        "skipped_generation": skipped_generation,
    }


@dg.op
def export_full_dataset(context, _generation: dict[str, int]) -> dict[str, object]:
    """Export all validated records to JSONL."""
    by_type: Counter[str] = Counter()
    by_article: Counter[str] = Counter()

    def entries():
        for entry in _iter_validated_entries():
            by_type[entry.get("dataset_type", "?")] += 1
            by_article[str(entry.get("article", "?"))] += 1
            yield entry

    records = write_jsonl(entries(), DATASET_JSONL)
    context.log.info(
        f"Exported {records} records across {len(by_article)} articles to {DATASET_JSONL}."
    )
    return {
        "records": records,
        "articles": len(by_article),
        "by_type": dict(by_type),
    }


@dg.job(description="Run the whole pipeline in one Dagster job.")
def run_full_pipeline():
    export_full_dataset(generate_and_validate_all_articles(prepare_all_chunks()))


@dg.job(
    description=(
        "Compatibility full-pipeline job. Runs prepare, article generation/"
        "validation, and export in one Dagster run."
    )
)
def build_dataset():
    export_full_dataset(generate_and_validate_all_articles(prepare_all_chunks()))
