---
name: debug-dagster
description: Diagnose and recover local Dagster UI/job/partition failures.
---

# Debug Dagster Skill

Use when the user reports Dagster UI errors, failed runs, missing jobs,
duplicate daemon heartbeats, or bad partition keys.

## Fast Recovery

```bash
make stop-dev
make seed-partitions
make dev
```

## Validate Definitions

```bash
DAGSTER_HOME="$PWD/.dagster_home" .venv/bin/dagster definitions validate -m dagster_pipeline
```

## Bad Partition Keys

If logs mention `partition-articles`, `p001`, titles, or non-article strings:

1. Do not retry the old run.
2. Run `make seed-partitions`.
3. Launch a fresh `run_full_pipeline`, `build_dataset`, or real article
   materialization.

## Duplicate Daemon Heartbeats

Run:

```bash
make stop-dev
```

Wait about a minute if Dagster still reports old heartbeats.

