---
name: run-pipeline
description: Choose and run the correct pipeline command for full, phased, or single-article generation.
---

# Run Pipeline Skill

Use this when the user asks how to run the project, run all pipeline stages, or
materialize a specific article.

## Decision Tree

- Full pipeline from terminal:
  - `make build`
- Full pipeline from Dagster UI:
  - Start with `make dev`
  - Jobs → `run_full_pipeline` → Launch Run
- Prepare chunks only:
  - `make prepare`
- Regenerate one article:
  - `make article ARTICLE=<id>`
- Revalidate existing synthetic files without OpenRouter:
  - `make revalidate`
- Export final JSONL:
  - `make export`

## Before Running

Check `.env` has `OPENROUTER_API_KEY` for generation. If only revalidating or
exporting existing synthetic/validated files, OpenRouter is not needed.

## Notes

Full generation may make hundreds of OpenRouter calls. Existing non-empty
synthetic files are reused.

