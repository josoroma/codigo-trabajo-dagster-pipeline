"""Reusable asset jobs for ad-hoc materialization."""

from __future__ import annotations

import dagster as dg

prepare_chunks_job = dg.define_asset_job(
    name="prepare_chunks",
    selection=dg.AssetSelection.assets("raw_html", "markdown_text", "article_chunks"),
    description=(
        "Fetch the source, extract Markdown, split it into article chunks, "
        "and register article partitions."
    ),
)

regenerate_one_article_job = dg.define_asset_job(
    name="regenerate_one_article",
    selection=dg.AssetSelection.assets("synthetic_entries", "validated_entries"),
    description="Re-run generation + validation for a single article partition.",
)

export_dataset_job = dg.define_asset_job(
    name="export_dataset",
    selection=dg.AssetSelection.assets("dataset_jsonl"),
    description="Export validated article files from disk into the final JSONL dataset.",
)
