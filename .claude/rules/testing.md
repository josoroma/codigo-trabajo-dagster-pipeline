---
paths:
  - "dagster_pipeline_tests/**/*.py"
  - "dagster_pipeline/**/*.py"
---

# Testing Rules

- Use pytest through `make test`.
- Unit tests should avoid network calls and avoid requiring a live Dagster daemon.
- Test pure helpers in `dagster_pipeline/core/` and partition helpers directly.
- Add regression tests for every parser/chunker/validator bug.
- When touching Dagster definitions, run:

  ```bash
  DAGSTER_HOME="$PWD/.dagster_home" .venv/bin/dagster definitions validate -m dagster_pipeline
  ```

- When validating generated artifacts, prefer small targeted materializations:

  ```bash
  make article ARTICLE=94
  make revalidate
  make export
  ```

