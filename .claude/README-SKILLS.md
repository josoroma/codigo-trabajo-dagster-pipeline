# Skills Reference

> Project-local Claude workflows for the Código de Trabajo synthetic dataset.

## Available Skills

| Skill | Purpose | Modifies Files? |
| --- | --- | --- |
| `/run-pipeline` | Choose the right Dagster/Make command for full, phased, or single-article runs | No |
| `/revalidate-dataset` | Rebuild validation/export from existing synthetic files without OpenRouter calls | Yes |
| `/debug-dagster` | Diagnose Dagster UI, partition, daemon, and run failures | Maybe |
| `/audit-generated-data` | Inspect synthetic/validated/dataset quality and explain rejection patterns | No by default |

## Recommended Workflows

### Full Dataset Generation

```text
1. /run-pipeline full
2. Watch for OpenRouter 429s; let retries/backoff work
3. /audit-generated-data coverage
4. make export if validated files changed
```

### Fix Validation Logic

```text
1. /audit-generated-data article 94
2. Patch core/validation.py or prompts if needed
3. make test
4. /revalidate-dataset
5. make export
```

### Dagster UI Issue

```text
1. /debug-dagster <error text>
2. make seed-partitions
3. make stop-dev
4. make dev
```

## Ground Rules

- Prefer `make revalidate` when synthetic files already exist.
- Prefer `make article ARTICLE=<id>` for targeted regeneration.
- Use `make reset` only when intentionally discarding generated outputs.
- Do not retry old failed Dagster runs with stale partition keys; launch fresh.

