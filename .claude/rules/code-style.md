---
paths:
  - "dagster_pipeline/**/*.py"
  - "dagster_pipeline_tests/**/*.py"
  - "Makefile"
---

# Code Style Rules

- Python 3.10-3.13 compatible; Python 3.12 is the recommended local runtime.
- Keep `core/` pure: no Dagster imports, no hidden global Dagster state.
- Keep asset modules thin and readable.
- Prefer `pathlib.Path` for filesystem paths.
- Use structured JSON APIs, not string concatenation/parsing, for artifacts.
- Use explicit, descriptive function names.
- Keep comments sparse and useful; explain non-obvious legal parsing or Dagster behavior.
- Preserve idempotency: reruns should reuse existing raw/markdown/synthetic artifacts when safe.
- Never hard-code secrets; read OpenRouter config from `.env` via `constants.py`.
- Do not introduce broad refactors while fixing a narrow pipeline issue.

