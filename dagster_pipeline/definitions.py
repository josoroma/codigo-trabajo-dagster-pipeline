"""Top-level Dagster :class:`Definitions` discovered by ``dagster dev``."""

from __future__ import annotations

import dagster as dg

from .assets import acquisition, chunking, export, generation, validation
from .full_pipeline import build_dataset, run_full_pipeline
from .jobs import (
    export_dataset_job,
    prepare_chunks_job,
    regenerate_one_article_job,
)
from .resources import OpenRouterResource

defs = dg.Definitions(
    assets=dg.load_assets_from_modules(
        [acquisition, chunking, generation, validation, export]
    ),
    jobs=[
        prepare_chunks_job,
        regenerate_one_article_job,
        export_dataset_job,
        run_full_pipeline,
        build_dataset,
    ],
    resources={
        "openrouter": OpenRouterResource(),
    },
)
