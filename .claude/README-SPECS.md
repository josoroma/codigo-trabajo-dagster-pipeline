# Spec → Code Mapping

> How implementation work maps to this pipeline.

This repo does not use a large product `SPECS.md` yet. Treat the README and
the current codebase as the living spec. When adding a new capability, map it
to the stage it changes and update README if user-facing commands, outputs, or
failure modes change.

## Feature → File Mapping

| Feature Area | Primary Files | Tests |
| --- | --- | --- |
| Official source acquisition | `core/fetch.py`, `assets/acquisition.py`, `constants.py` | mock-free pure tests where possible |
| Markdown/article chunking | `core/chunking.py`, `assets/chunking.py`, `partitions.py` | `dagster_pipeline_tests/test_pipeline.py` |
| Prompt contract | `core/prompts.py`, `core/generation.py` | generation normalization tests |
| OpenRouter behavior | `resources/openrouter.py`, `.env.example`, README config table | targeted unit tests if logic becomes pure |
| Validation | `core/validation.py`, `assets/validation.py` | validation tests, rejection cases |
| Export | `core/export.py`, `assets/export.py`, `full_pipeline.py` | JSONL shape tests when changed |
| Dagster operations | `jobs.py`, `full_pipeline.py`, `definitions.py`, `Makefile` | `dagster definitions validate` |
| Operator docs | `README.md`, `.claude/**` | manual verification of commands |

## Acceptance Checklist

Before calling a change done:

1. Pure logic is under `dagster_pipeline/core/` unless there is a strong reason.
2. Dagster definitions validate:

   ```bash
   DAGSTER_HOME="$PWD/.dagster_home" .venv/bin/dagster definitions validate -m dagster_pipeline
   ```

3. Unit tests pass:

   ```bash
   make test
   ```

4. README is updated for any new command, env var, output file, or known error.
5. Existing generated data is not deleted unless the user asked for reset.

