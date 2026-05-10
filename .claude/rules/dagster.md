---
paths:
  - "dagster_pipeline/assets/**/*.py"
  - "dagster_pipeline/full_pipeline.py"
  - "dagster_pipeline/jobs.py"
  - "dagster_pipeline/definitions.py"
  - "dagster_pipeline/partitions.py"
  - "dagster.yaml"
  - "Makefile"
---

# Dagster Rules

- Dynamic partition keys must be actual article ids derived from
  `data/chunks/articulo_*.json`.
- Never use synthetic partition keys like `partition-articles`, `p001`, or law
  titles.
- Use `sync_article_partitions()` when chunk files may have changed.
- Full pipeline from the Jobs page should use `run_full_pipeline` or
  `build_dataset`, not a fake all-partitions asset run.
- Stop duplicate local Dagster processes with:

  ```bash
  make stop-dev
  ```

- Start the UI with:

  ```bash
  make dev
  ```

- `make dev` should seed partitions before launching UI.

