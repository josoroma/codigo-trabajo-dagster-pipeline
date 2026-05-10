---
paths:
  - "dagster_pipeline/core/prompts.py"
  - "dagster_pipeline/core/generation.py"
  - "dagster_pipeline/core/validation.py"
  - "data/synthetic/**/*.json"
  - "data/validated/**/*.json"
  - "data/datasets/**/*.jsonl"
---

# Data Quality Rules

- Every exported record must be grounded in one article.
- Every record must include:
  - `instruction`
  - `input`
  - `output`
  - `source_quote`
  - `source_url`
  - `law_code`
  - `article`
  - `chunk_id`
  - `dataset_type`
- `source_quote` should be a verbatim article slice modulo whitespace
  normalization.
- Reject hallucinated source quotes even if the answer sounds legally correct.
- Do not mix multiple articles in one generated entry.
- Preserve inserted article suffixes such as `bis`, `ter`, `quater`,
  `quinquies`.
- When validation drops records, inspect `data/synthetic/articulo_<id>.json`
  and `data/chunks/articulo_<id>.json` before changing prompts.

