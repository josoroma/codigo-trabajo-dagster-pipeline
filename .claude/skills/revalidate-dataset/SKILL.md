---
name: revalidate-dataset
description: Rebuild validated files and final JSONL from existing synthetic files without making OpenRouter calls.
---

# Revalidate Dataset Skill

Use when validation rules changed, validated files are empty, or the user wants
to rebuild output from existing `data/synthetic`.

## Commands

```bash
make revalidate
make export
```

## Inspect A Single Article

```bash
sed -n '1,220p' data/synthetic/articulo_<id>.json
sed -n '1,220p' data/chunks/articulo_<id>.json
sed -n '1,220p' data/validated/articulo_<id>.json
```

## Expected Output

`make revalidate` prints counts for valid/rejected entries. If rejection counts
are high, inspect `core/validation.py` and `core/prompts.py`.

