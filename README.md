# Código de Trabajo de Costa Rica — Synthetic Dataset Pipeline

A grounded, reproducible pipeline that turns the official text of the
**Código de Trabajo de Costa Rica** (713 articles, published by the
Procuraduría General de la República) into a fine-tuning / RAG dataset of
instruction-style examples. Each generated example is anchored to a verbatim
quote from a single article so the dataset can be safely used to train or
evaluate legal assistants without hallucinating statute language.

The LLM step uses **OpenRouter** (default model
[`deepseek/deepseek-v4-pro`](https://openrouter.ai/deepseek/deepseek-v4-pro)).
There is no local model dependency.

---

## Table of contents

- [Quick answer](#quick-answer)
- [Architecture at a glance](#architecture-at-a-glance)
- [Repository layout](#repository-layout)
- [Hugging Face model](#hugging-face-model)
- [Kaggle workspace](#kaggle-workspace)
- [Configuration](#configuration)
- [Install & run](#install--run)
- [Jobs reference](#jobs-reference)
- [Asset graph](#asset-graph)
- [Stage-by-stage walkthrough](#stage-by-stage-walkthrough)
- [Validation rules](#validation-rules)
- [Common operations](#common-operations)
- [Design notes](#design-notes)
- [Output sample](#output-sample)
- [Troubleshooting](#troubleshooting)
- [Kaggle & model publishing workflow](#kaggle--model-publishing-workflow)
- [License & data provenance](#license--data-provenance)

---

## Quick answer

This repository is a Dagster pipeline for building a grounded synthetic legal
dataset from the official **Código de Trabajo de Costa Rica**. It:

1. Downloads the official PGR HTML for the law.
2. Extracts clean Markdown from the page via `trafilatura`.
3. Splits the law into one JSON chunk per article (plus optional document-level
   chunks for preamble/transitory/outro material).
4. Uses OpenRouter to generate grounded examples per article:
   `article_explanation`, one or more `qa` pairs, and one or more `cite_article`
   citations — all in a **single chat-completions call** per article.
5. Expands nested QA/citation lists into flat instruction-tuning records and
   validates each entry (required fields, verbatim `source_quote` presence,
   URL, output length).
6. Exports accepted records — sorted numerically by article number — to
   `data/datasets/codigo_trabajo.jsonl` for fine-tuning, evaluation, or RAG.

The code is organized so the reusable logic lives in
[`dagster_pipeline/core/`](dagster_pipeline/core/) and Dagster-specific asset
wrappers live in [`dagster_pipeline/assets/`](dagster_pipeline/assets/). The
`data/` directory is the working area for pipeline artifacts.

---

## Architecture at a glance

```
                     official PGR website
                               │
                  ┌────────────▼────────────┐
                  │  raw_html               │  HTTP fetch + cache
                  └────────────┬────────────┘
                               │
                  ┌────────────▼────────────┐
                  │  markdown_text          │  trafilatura → clean Markdown
                  └────────────┬────────────┘
                               │
                  ┌────────────▼────────────┐
                  │  article_chunks         │  Markdown → 1 JSON per article
                  └────────────┬────────────┘   (registers dynamic partitions)
                               │  fan-out per article id
                  ┌────────────▼────────────┐
                  │  synthetic_entries      │  single OpenRouter call per article
                  └────────────┬────────────┘   → explanation + QA pairs + citations
                               │
                  ┌────────────▼────────────┐
                  │  validated_entries      │  schema + grounding checks
                  └────────────┬────────────┘
                               │  fan-in (numerically sorted)
                  ┌────────────▼────────────┐
                  │  dataset_jsonl          │  → JSONL training file
                  └─────────────────────────┘
```

Every stage is **idempotent**: Stage 1–2 skip re-fetch when files exist;
`synthetic_entries` reuses a non-empty cached file; per-article assets are
partitioned so re-running touches only what changed.

---

## Repository layout

```
legal/
├── .env                            # OPENROUTER_API_KEY (gitignored)
├── .env.example                    # template
├── pyproject.toml                  # installable package; declares dagster_pipeline
├── workspace.yaml                  # tells `dagster dev` where the code lives
├── dagster.yaml                    # local Dagster instance config (concurrency pools)
├── Makefile                        # all common operations as make targets
├── dagster_pipeline/               # ── Dagster code location ──
│   ├── __init__.py                 #     exposes `defs` (lazy import)
│   ├── definitions.py              #     top-level Definitions(assets, jobs, resources)
│   ├── constants.py                #     filesystem layout + URLs + LAW_CODE + .env loading
│   ├── partitions.py               #     DynamicPartitionsDefinition + sync helpers
│   ├── jobs.py                     #     reusable AssetSelection jobs
│   ├── full_pipeline.py            #     op-based `run_full_pipeline` + `build_dataset` jobs
│   ├── register_partitions.py      #     CLI: seed dynamic partitions from data/chunks/
│   ├── revalidate_existing.py      #     CLI: rebuild validated/ without OpenRouter calls
│   ├── list_article_partitions.py  #     CLI: list registered partition keys
│   ├── core/                       # ── pure-Python domain logic (no Dagster imports) ──
│   │   ├── env.py                  #     stdlib-only .env loader
│   │   ├── fetch.py                #     HTTP fetch + trafilatura wrapper
│   │   ├── chunking.py             #     Markdown → article records (regex state machine)
│   │   ├── prompts.py              #     SYSTEM_PROMPT + build_batch_prompt
│   │   ├── generation.py           #     expand_generated_entries (flatten QA/citation lists)
│   │   ├── parsing.py              #     tolerant parse_json_array
│   │   ├── validation.py           #     REQUIRED_FIELDS + _quote_found_in_source + filter_valid
│   │   └── export.py               #     EXPORT_FIELDS + write_jsonl
│   ├── assets/                     # ── thin Dagster wrappers around core/ ──
│   │   ├── acquisition.py          #     raw_html, markdown_text
│   │   ├── chunking.py             #     article_chunks (registers partitions)
│   │   ├── generation.py           #     synthetic_entries (per article)
│   │   ├── validation.py           #     validated_entries (per article)
│   │   └── export.py               #     dataset_jsonl (fan-in, numeric sort)
│   └── resources/
│       └── openrouter.py           #     OpenRouterResource (ConfigurableResource)
├── dagster_pipeline_tests/
│   └── test_pipeline.py            # pytest unit tests (no network, no Dagster runtime)
├── kaggle/
│   ├── gemma4-31b-text-legal-codigo-de-trabajo.ipynb
│   ├── runpod/
│   │   └── gemma-4-codigo-trabajo-finetune/  # merged HF-format model artifacts
│   └── gemma4-31b-text-legal-codigo-de-trabajo/
│       ├── data/datasets/codigo_trabajo.jsonl
│       └── gemma_4_lora/           # LoRA adapter exported from training
└── data/
    ├── raw/         # raw HTML scraped from PGR
    ├── markdown/    # extracted Markdown
    ├── chunks/      # one JSON per article + optional document_*.json files
    ├── synthetic/   # raw LLM output (one JSON array per article)
    ├── validated/   # entries that passed quality checks
    └── datasets/    # final JSONL ready for training
```

The split is intentional:

- [`dagster_pipeline/core/`](dagster_pipeline/core/) holds **pure functions**
  with no Dagster imports — easy to unit-test, notebook-friendly, orchestrator-agnostic.
- [`dagster_pipeline/assets/`](dagster_pipeline/assets/) holds **thin wrappers**
  that read/write filesystem paths from `constants.py`, call into `core/`, and
  emit Dagster `MaterializeResult` metadata.
- [`dagster_pipeline/resources/openrouter.py`](dagster_pipeline/resources/openrouter.py)
  is the only module that talks to the OpenRouter API.

---

## Hugging Face model

The merged Hugging Face-format checkpoint is published at:

- <https://huggingface.co/josoroma/gemma-4-codigo-trabajo-finetune>

Direct link to edit the model README / model card in the browser:

- <https://huggingface.co/josoroma/gemma-4-codigo-trabajo-finetune/new/main?filename=README.md>

Local model-card draft used for publishing:

- Use your preferred local draft file for the model card content.

---

## Kaggle workspace

The `kaggle/` folder contains notebook and experiment artifacts related to model fine-tuning and publication:

- `kaggle/gemma4-31b-text-legal-codigo-de-trabajo.ipynb`: main notebook used for the training workflow.
- `kaggle/gemma4-31b-text-legal-codigo-de-trabajo/data/datasets/codigo_trabajo.jsonl`: dataset used for fine-tuning.
- `kaggle/gemma4-31b-text-legal-codigo-de-trabajo/gemma_4_lora/`: LoRA adapter artifacts.
- `kaggle/runpod/gemma-4-codigo-trabajo-finetune/`: merged Hugging Face-format model files.

These artifacts are complementary to the Dagster pipeline outputs in [`data/`](data/) and help document the fine-tune and model-sharing workflow.

---

## Configuration

1. Copy the env template and add your OpenRouter key:

   ```bash
   cp .env.example .env
   $EDITOR .env   # set OPENROUTER_API_KEY=sk-or-v1-...
   ```

2. Available environment variables (all loaded by
   [`constants.py`](dagster_pipeline/constants.py) at import time):

   | Variable | Default | Purpose |
   |---|---|---|
   | `OPENROUTER_API_KEY` | _(required)_ | Bearer token. |
   | `OPENROUTER_BASE_URL` | `https://openrouter.ai/api/v1` | Override for proxies / mocks. |
   | `OPENROUTER_MODEL` | `deepseek/deepseek-v4-pro` | Model id used by the resource. |
   | `OPENROUTER_REFERER` | `https://github.com/josoroma/legal` | Sent as `HTTP-Referer`. |
   | `OPENROUTER_TITLE` | `legal-synthetic-gen` | Sent as `X-Title`. |
   | `OPENROUTER_MAX_RETRIES` | `6` | Client retries per OpenRouter call. |
   | `OPENROUTER_RETRY_BACKOFF_SECONDS` | `5` | Base exponential retry delay. |
   | `OPENROUTER_RATE_LIMIT_BACKOFF_SECONDS` | `60` | Minimum wait after HTTP 429. |
   | `OPENROUTER_MAX_BACKOFF_SECONDS` | `300` | Maximum wait between retries. |

---

## Install & run

Requires Python `>=3.10,<3.14`; Python 3.12 is recommended (Dagster's gRPC server can fail to bind sockets under 3.14). `make install` creates `.venv/` using `python3.12` and installs the package in editable mode with `dev` extras.

```bash
make install   # create .venv, install dagster + project
make dev       # seed partitions, start Dagster UI at http://127.0.0.1:3000
```

`make dev` sets `DAGSTER_HOME="$PWD/.dagster_home"`, copies `dagster.yaml` there, registers existing article chunk files as dynamic partitions, and starts the UI. It refuses to start if another Dagster process is already using port 3000 or this repo's `.dagster_home`.

Without `make`:

```bash
python3.12 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -e ".[dev]"
mkdir -p .dagster_home
cp dagster.yaml .dagster_home/dagster.yaml
.venv/bin/python -m dagster_pipeline.register_partitions
(cd .dagster_home && DAGSTER_HOME="$PWD" ../.venv/bin/dagster dev -w ../workspace.yaml)
```

---

## Jobs reference

Five jobs are registered in [`definitions.py`](dagster_pipeline/definitions.py):

| Job | Make target | What it does |
|---|---|---|
| `prepare_chunks` | `make prepare` | Fetch HTML, extract Markdown, split into article chunks, seed partitions. |
| `regenerate_one_article` | `make article ARTICLE=N` | Re-run `synthetic_entries + validated_entries` for one partition. |
| `export_dataset` | `make export` | Write `data/datasets/codigo_trabajo.jsonl` from all validated files. |
| `run_full_pipeline` | `make build` | Op-based single run: prepare → generate → validate → export all articles. |
| `build_dataset` | _(also `make build`)_ | Alias for `run_full_pipeline` retained for backward compatibility. |

`run_full_pipeline` and `build_dataset` are implemented as op-based jobs in
[`full_pipeline.py`](dagster_pipeline/full_pipeline.py) — they bypass per-article
asset partitions and execute the complete flow in one run, which is more convenient
for a full rebuild but cannot be retried per-article. Use the asset graph backfill
for fine-grained retries.

---

## Asset graph

| Asset | Module | Stage | Partitioned? | Compute kind |
|---|---|---|---|---|
| `raw_html` | `assets/acquisition.py` | acquisition | no | http |
| `markdown_text` | `assets/acquisition.py` | acquisition | no | trafilatura |
| `article_chunks` | `assets/chunking.py` | chunking | no (registers partitions) | python |
| `synthetic_entries` | `assets/generation.py` | generation | **per article** | openrouter |
| `validated_entries` | `assets/validation.py` | validation | **per article** | python |
| `dataset_jsonl` | `assets/export.py` | export | no (fan-in) | python |
| `unsloth_training_dataset` | `assets/export.py` | export | no (derived from `dataset_jsonl`) | python |

Key behaviours:

- **Per-article partitions** — re-materialize a single article without re-running all 713 LLM calls.
- **Synthetic cache** — `synthetic_entries` reuses an existing non-empty `data/synthetic/articulo_<id>.json`, so re-runs validate and export without a new OpenRouter call.
- **Retry policy** — `synthetic_entries` has `max_retries=2, backoff=EXPONENTIAL`. HTTP 429 responses get a client-side wait governed by `OPENROUTER_RATE_LIMIT_BACKOFF_SECONDS`.
- **Concurrency cap** — `synthetic_entries` is tagged `dagster/concurrency_key: openrouter`. Set the pool limit in [`dagster.yaml`](dagster.yaml) to throttle parallel API calls.
- **Numeric sort on export** — `dataset_jsonl` reads validated files sorted by article number (not filename string), so the JSONL is in `1, 2, 3, …, 713` order with suffix variants (`120`, `120_bis`) collated correctly.
- **Single-run safety stop** — in `run_full_pipeline`, generation is skipped on empty/unparseable LLM responses, but the job fails after 5 skipped articles to avoid silently exporting a heavily incomplete dataset.

---

## Stage-by-stage walkthrough

### Stage 1 — Acquisition: HTML → Markdown

**Assets:** `raw_html`, `markdown_text`
([assets/acquisition.py](dagster_pipeline/assets/acquisition.py))  
**Core:** [core/fetch.py](dagster_pipeline/core/fetch.py)  
**Outputs:** `data/raw/codigo_trabajo.html`, `data/markdown/codigo_trabajo.md`

`trafilatura` strips navigation, scripts, and footers while keeping tables and
inline emphasis. Both artifacts are persisted so downstream stages are
deterministic and the source can be audited. Both assets skip re-fetch/re-extract
when the output file already exists.

---

### Stage 2 — Structural chunking

**Asset:** `article_chunks`
([assets/chunking.py](dagster_pipeline/assets/chunking.py))  
**Core:** [core/chunking.py](dagster_pipeline/core/chunking.py)  
**Output:** `data/chunks/articulo_<N>.json` (+ optional `document_*.json` files)

The chunker walks the Markdown line by line via a regex state machine, tracking
`LIBRO`, `TÍTULO`, and `CAPÍTULO` headings. Article headers are matched by a
permissive regex that handles the source's many formatting variants
(`**ARTICULO 1º.-**`, `Artículo 94 bis.-`, multi-line headers, etc.).

Spanish legal insertion suffixes (`bis`, `ter`, `quater`, `quinquies`, `sexies`,
`septies`, `octies`, `novies`, `decies`) are encoded in filenames with an
underscore: `Artículo 664 bis` → `articulo_664_bis.json`. The current source
uses `bis`, `ter`, `quater`, and `quinquies`.

Articles missing from the source (derogated or unpublished) receive a stub so
the 1–713 sequence stays complete. Document-level chunks (`document_intro.json`,
`document_transitory_articles.json`, `document_outro.json`) are written for
auditability but are **not** registered as article partitions.

After writing all chunk files, the asset calls `sync_article_partitions()` to
add new keys and prune stale ones from the `articles` `DynamicPartitionsDefinition`.

Each chunk JSON:

```json
{
  "article": "94",
  "chunk_id": "libro_None_titulo_SEGUNDO_capitulo_SETIMO_articulo_94",
  "libro": null,
  "titulo": "SEGUNDO",
  "capitulo": "SETIMO",
  "content": "Queda absolutamente prohibido a los patronos…",
  "source_url": "https://www.pgrweb.go.cr/scij/…",
  "law_code": "Código de Trabajo de Costa Rica"
}
```

---

### Stage 3 — Synthetic generation (the LLM step)

**Asset:** `synthetic_entries`
([assets/generation.py](dagster_pipeline/assets/generation.py))  
**Core:** [core/prompts.py](dagster_pipeline/core/prompts.py),
[core/generation.py](dagster_pipeline/core/generation.py),
[core/parsing.py](dagster_pipeline/core/parsing.py)  
**Resource:** [resources/openrouter.py](dagster_pipeline/resources/openrouter.py)

A **single chat-completions call** per article asks the LLM (via
`response_format: json_object`) to produce a JSON object with an `"entries"`
array of exactly 3 objects: an explanation, a QA object with a `qa_pairs` list,
and a citation object with a `citations` list.
`expand_generated_entries()` in `core/generation.py` then flattens these nested
lists into individual instruction-tuning rows.

| `dataset_type` | Source structure | Purpose |
|---|---|---|
| `article_explanation` | Single object | Plain-language explanation of what the article establishes. |
| `qa` | Expanded from `qa_pairs` list | Specific questions + grounded answers about the article. |
| `cite_article` | Expanded from `citations` list | Verbatim citation + explanation of its legal purpose. |

**Grounding contract.** Every entry must carry a `source_quote` that is a
verbatim slice of the article text. The system prompt explicitly forbids
invention. Stage 4 rejects any entry whose `source_quote` is not found in the
source chunk.

**Schema for each flat entry:**

```json
{
  "instruction": "…",
  "input": "",
  "output": "…",
  "source_quote": "verbatim slice of the article",
  "source_url": "https://www.pgrweb.go.cr/scij/…",
  "law_code": "Código de Trabajo de Costa Rica",
  "article": "94",
  "chunk_id": "libro_None_titulo_SEGUNDO_capitulo_SETIMO_articulo_94",
  "dataset_type": "article_explanation"
}
```

**Retry / cache:** if `data/synthetic/articulo_<id>.json` already exists and
is non-empty, the asset skips the OpenRouter call and returns immediately.
The asset-level `RetryPolicy(max_retries=2, backoff=EXPONENTIAL)` handles
transient HTTP errors.

---

### Stage 4 — Validation

**Asset:** `validated_entries`
([assets/validation.py](dagster_pipeline/assets/validation.py))  
**Core:** [core/validation.py](dagster_pipeline/core/validation.py)  
**Output:** `data/validated/articulo_<N>.json`

See [Validation rules](#validation-rules) for the full logic. Failures surface
as Dagster log warnings and asset metadata but do not abort the run.

---

### Stage 5 — Export

**Assets:** `dataset_jsonl`, `unsloth_training_dataset`
([assets/export.py](dagster_pipeline/assets/export.py))  
**Core:** [core/export.py](dagster_pipeline/core/export.py)  
**Outputs:** `data/datasets/codigo_trabajo.jsonl`, `data/datasets/codigo_trabajo_unsloth.jsonl`

Iterates validated files in **numeric article order** (using `_article_sort_key`
so `articulo_9.json` precedes `articulo_10.json`) and writes each entry as a
single-line JSON, projecting only the nine `EXPORT_FIELDS`:
`instruction`, `input`, `output`, `source_quote`, `source_url`, `law_code`,
`article`, `chunk_id`, `dataset_type`.

The `MaterializeResult` reports total record count, article count, and a
per-`dataset_type` breakdown.

`unsloth_training_dataset` is a downstream export that rewrites validated
entries into instruction/input/output format plus a convenience `text` field
for notebook fine-tuning workflows.

---

## Validation rules

`core/validation.py` rejects an entry when any of the following is true:

| Rule | Detail |
|---|---|
| **Missing required field** | Any of `instruction`, `input`, `output`, `source_quote`, `source_url`, `law_code`, `article`, `chunk_id`, `dataset_type` is absent. |
| **`source_quote` too short** | Fewer than 10 characters after stripping whitespace. |
| **`source_quote` not found in source** | See grounding check below. |
| **`output` too short** | Fewer than 10 characters (degenerate generation). |
| **`source_url` mismatch** | Does not exactly equal the canonical PGR URL in `constants.SOURCE_URL`. |

**Grounding check (`_quote_found_in_source`).**
Both the quote and the source text are normalised before comparison:

1. Strip Markdown `**`/`*` bold/italic markers (source stores list labels as `**a) **text`).
2. Remove empty footnote citation markers `()` from scraped HTML.
3. Normalise broken HTML hyphenation (`médico- sanitaria` → `médico-sanitaria`).
4. Collapse internal whitespace.

Then four matching strategies are tried in order:

1. **Exact substring** — normalised quote found in normalised source.
2. **Trailing-period fallback** — LLM often terminates list items with `.` where the source uses `;`; strip the trailing period and retry.
3. **Ellipsis splitting** — quotes containing `...`, `[...]`, or `…` are split into segments (≥ 10 chars each); every segment must appear independently in the source.
4. **List-marker normalisation** — `a)`, `b)`, `ch)` and `-` bullet prefixes are stripped from both quote and source after `:` and `;` separators (including `y`/`e` conjunctions before the last item), then strategies 1–2 are retried on the stripped versions.

---

## Common operations

### Regenerate one specific article

```bash
make article ARTICLE=94
```

Materialises `synthetic_entries+` (generation + validation) for partition `94`.
Also runs `make seed-partitions` first so a fresh `.dagster_home` knows about
existing chunk files.

### Switch model

Set `OPENROUTER_MODEL` in `.env`, or override the resource in
[`definitions.py`](dagster_pipeline/definitions.py):

```python
resources={"openrouter": OpenRouterResource(model="anthropic/claude-3.5-sonnet")}
```

### Rebuild validation from existing synthetic files (no OpenRouter calls)

```bash
make revalidate   # rebuilds all data/validated/ from data/synthetic/
make export       # rewrites data/datasets/codigo_trabajo.jsonl
```

`make revalidate` runs `dagster_pipeline/revalidate_existing.py` directly —
no Dagster runtime, no network. Useful after changing validation rules or
fixing chunk content.

### Export Unsloth-format JSONL

```bash
cd .dagster_home
DAGSTER_HOME="$PWD" ../.venv/bin/dagster asset materialize \
  --select unsloth_training_dataset \
  -m dagster_pipeline
```

This writes `data/datasets/codigo_trabajo_unsloth.jsonl`.

### Regenerate everything from scratch

```bash
make reset   # wipe data/synthetic, data/validated, data/datasets
make build   # run_full_pipeline: prepare → generate → validate → export
```

`make build` makes hundreds of OpenRouter calls. Lower
`concurrency.pools.default_limit` in `dagster.yaml` if you hit rate limits.

### Run the unit tests

```bash
make test
```

The test suite covers parsing, validation, chunking, and partition helpers
without touching the network or instantiating the Dagster runtime.

---

## Design notes

- **Single source of truth for credentials.** `.env` is loaded once by
  [`constants.py`](dagster_pipeline/constants.py) via a stdlib-only loader in
  [`core/env.py`](dagster_pipeline/core/env.py). Every module reads constants,
  never `os.environ` directly.
- **Pure core, thin assets.** All algorithmic logic lives under `core/`. Asset
  modules are short glue layers that handle I/O paths, logging, retries, and
  metadata emission.
- **Grounding by construction.** System prompt + schema both require a literal
  `source_quote`. Four-layer validation rejects anything not traceable to the
  source text.
- **Numeric JSONL ordering.** The export asset sorts validated files by
  article number (integer key + suffix), not by filename string, so the dataset
  is in logical statute order.
- **Resumability everywhere.** Stage 1 skips fetch when files exist; per-article
  assets are partitioned; `synthetic_entries` reuses cached LLM output. A
  partial run can always be resumed from the last successful article.
- **No hidden state.** Every intermediate artifact (raw HTML, Markdown, per-article
  chunks, raw LLM output, validated entries, final JSONL) lives on disk under
  `data/` and is human-readable / diff-able.

---

## Output sample

A line from `data/datasets/codigo_trabajo.jsonl`:

```json
{"instruction": "¿Qué establece el artículo 94 del Código de Trabajo respecto al despido de trabajadoras en estado de embarazo?", "input": "", "output": "El artículo 94 prohíbe a los patronos despedir a las trabajadoras embarazadas o en período de lactancia, salvo por causa justificada originada en falta grave a los deberes derivados del contrato, conforme a las causales del artículo 81. En tal caso, deben gestionarse el despido ante la Dirección Nacional e Inspección General de Trabajo…", "source_quote": "Queda prohibido a los patronos despedir a las trabajadoras que estuvieren en estado de embarazo o período de lactancia", "source_url": "https://www.pgrweb.go.cr/scij/Busqueda/Normativa/Normas/nrm_texto_completo.aspx?nValor1=1&nValor2=8045", "law_code": "Código de Trabajo de Costa Rica", "article": "94", "chunk_id": "libro_None_titulo_SEGUNDO_capitulo_SETIMO_articulo_94", "dataset_type": "qa"}
```

---

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `OPENROUTER_API_KEY is not set` | `.env` missing or empty — `cp .env.example .env` and add the key. |
| `[HTTP 401]` / `[HTTP 403]` | Bad / revoked key. The resource short-circuits retries on auth errors. |
| `[HTTP 429]` | Upstream provider is rate-limited. The resource waits before retrying; existing synthetic files are reused, so progress resumes from disk. Lower `concurrency.pools.default_limit` in `dagster.yaml` or raise `OPENROUTER_RATE_LIMIT_BACKOFF_SECONDS`. |
| Empty `data/chunks/` | Stage 2 needs `data/markdown/codigo_trabajo.md`. Re-run Stage 1 first, or run `make prepare`. |
| Dagster UI shows `0 partitions` | Run `make seed-partitions`, then refresh the browser. `make dev` does this automatically. |
| `No such file … articulo_<name>.json` or invalid partition key | Stale dynamic partition pointing to a deleted chunk. Run `make seed-partitions`, then launch a fresh materialization/backfill. |
| Invalid partition key `partition-articles` | Asset graph launched with a synthetic key. Use `run_full_pipeline` from the Jobs page, or `make build`. |
| Backfill warning mentions `DefaultRunLauncher` | Stop old Dagster processes and restart with `make dev` so `.dagster_home/dagster.yaml` uses `QueuedRunCoordinator`. |
| `Another … daemon is still sending heartbeats` | Two `dagster dev` processes sharing the same `.dagster_home`. Run `make stop-dev`, wait ~1 min for heartbeats to expire, then `make dev`. |
| Validation drops most entries | Inspect `data/synthetic/articulo_N.json` — usually `source_quote` is empty or the LLM returned the wrong schema (`question`/`answer` instead of `instruction`/`output`). Delete the file and run `make article ARTICLE=N`. |
| JSONL records out of order | Run `make export` again — the exporter now sorts by numeric article number. |

## Kaggle & model publishing workflow

This repo includes a practical Kaggle/RunPod workflow under `kaggle/` for
fine-tuning, adapter export, merge, and publishing.

### 1) Download Kaggle dataset artifacts

```bash
kaggle datasets download -d josoroma/codigo-trabajo-josoroma -p ./data/datasets --unzip
```

Useful files from that dataset include:

- `data/datasets/codigo_trabajo.jsonl`
- `gemma_4_lora/` (adapter-only output)

`gemma_4_lora/` is not a full base model; it is the LoRA adapter plus tokenizer/config files.

### 2) Publish the merged Hugging Face-format model

Merged model artifacts are expected in a folder like:

- `kaggle/runpod/gemma-4-codigo-trabajo-finetune/`

Published model page:

- <https://huggingface.co/josoroma/gemma-4-codigo-trabajo-finetune>

Create or reuse the target model repo, then upload:

```bash
huggingface-cli repo create josoroma/gemma-4-codigo-trabajo-finetune --type model
huggingface-cli upload-large-folder \
  josoroma/gemma-4-codigo-trabajo-finetune \
  kaggle/runpod/gemma-4-codigo-trabajo-finetune \
  --repo-type model
```

`upload-large-folder` is recommended for very large safetensors files.

### 3) Publish/update the model card

Prepared model-card content is in:

- your local model-card draft file.

Direct editor link on Hugging Face:

- <https://huggingface.co/josoroma/gemma-4-codigo-trabajo-finetune/new/main?filename=README.md>

---

## License & data provenance

Source text © Procuraduría General de la República de Costa Rica
(public-domain legal text). The synthetic dataset is generated from that
public source and is intended for research/educational fine-tuning of legal
assistants. Always re-verify generated outputs against the official source
before relying on them for any legal decision.


