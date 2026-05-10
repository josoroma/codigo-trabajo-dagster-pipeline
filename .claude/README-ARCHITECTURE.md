# Architecture Contract

> Código de Trabajo de Costa Rica — Synthetic Dataset Pipeline

## Principle

Keep orchestration thin and domain logic reusable. Dagster modules should wire
assets/jobs/resources together; parsing, chunking, prompt normalization,
validation, and export logic should live in pure Python modules under
`dagster_pipeline/core/`.

---

## Repository Map

```text
dagster_pipeline/
├── assets/                 # Dagster asset wrappers around core logic
│   ├── acquisition.py      # raw_html, markdown_text
│   ├── chunking.py         # article_chunks + dynamic partition sync
│   ├── generation.py       # per-article OpenRouter generation
│   ├── validation.py       # per-article validation
│   └── export.py           # final JSONL fan-in
├── core/                   # Pure Python logic, no Dagster imports
│   ├── fetch.py            # HTTP + trafilatura extraction
│   ├── chunking.py         # markdown → article/document chunks
│   ├── prompts.py          # legal dataset prompt contract
│   ├── parsing.py          # tolerant JSON parsing
│   ├── generation.py       # LLM response normalization
│   ├── validation.py       # schema + source_quote validation
│   └── export.py           # JSONL writer
├── resources/
│   └── openrouter.py       # OpenRouter client, retry/backoff behavior
├── full_pipeline.py        # one-click Dagster jobs for full generation
├── jobs.py                 # asset jobs for phased UI workflows
├── partitions.py           # article dynamic partition keys
├── definitions.py          # top-level Dagster Definitions
└── constants.py            # paths, env loading, source URL, model config
```

```text
data/
├── raw/        # fetched official PGR HTML
├── markdown/   # extracted markdown
├── chunks/     # one JSON file per article plus document_* chunks
├── synthetic/  # raw normalized LLM entries per article
├── validated/  # validated entries per article
└── datasets/   # final JSONL export
```

---

## Stage Contract

| Stage | Owns | Must Preserve |
| --- | --- | --- |
| Acquisition | `raw_html`, `markdown_text` | Idempotent fetch/extract; no downstream assumptions in fetch code. |
| Chunking | `article_chunks`, `core/chunking.py` | One file per article, document chunks separate, suffix ids like `664_bis`. |
| Generation | `synthetic_entries`, `core/prompts.py`, `core/generation.py` | Grounded records only; reuse existing synthetic files when possible. |
| Validation | `validated_entries`, `core/validation.py` | Required schema, canonical source URL, source_quote grounded in article text. |
| Export | `dataset_jsonl`, `core/export.py` | JSONL with stable field projection and deterministic ordering. |

---

## Boundary Rules

- `dagster_pipeline/core/**` must not import Dagster.
- `dagster_pipeline/assets/**` should stay thin: path I/O, Dagster metadata,
  partition context, logging, and calls into `core/`.
- `dagster_pipeline/resources/openrouter.py` is the only module that should
  call OpenRouter directly.
- `data/chunks/articulo_<id>.json` filenames define valid article partition
  keys. Do not hand-register arbitrary partition keys.
- Document-level chunks (`document_intro.json`, `document_outro.json`,
  `document_transitory_articles.json`) are audit artifacts, not article
  partitions.

---

## Run Modes

### One-Click Full Pipeline

Use Dagster job `run_full_pipeline` or terminal:

```bash
make build
```

This runs prepare → generate/validate all real article ids → export.

### Phased Pipeline

Use these jobs/assets when debugging:

```bash
make prepare
make article ARTICLE=94
make revalidate
make export
```

### Reset Generated Outputs

```bash
make reset
```

Only deletes `data/synthetic`, `data/validated`, and `data/datasets` files.
It keeps fetched raw HTML, markdown, and chunks.

---

## Common Failure Modes

| Symptom | Architectural Cause | Preferred Fix |
| --- | --- | --- |
| `partition-articles`, `p001`, or title-like partition key | Fake/stale dynamic partition | `make seed-partitions`, then launch a fresh run. |
| `Another daemon is still sending heartbeats` | Multiple `dagster dev` processes share `.dagster_home` | `make stop-dev`, wait briefly, then `make dev`. |
| `HTTP 429` | OpenRouter/provider throttling | Let backoff run, lower concurrency, rely on synthetic cache. |
| Empty validated files | Validation rejected generated entries | Inspect `data/synthetic` and source chunk; fix validation/prompt if needed, then `make revalidate`. |

