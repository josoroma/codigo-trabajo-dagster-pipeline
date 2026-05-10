---
name: audit-generated-data
description: Inspect generated dataset artifacts for coverage, validation quality, and export consistency.
---

# Audit Generated Data Skill

Use when the user asks why validated files are empty, whether the dataset is
complete, or which articles need regeneration.

## Useful Checks

```bash
find data/chunks -name 'articulo_*.json' | wc -l
find data/synthetic -name 'articulo_*.json' | wc -l
find data/validated -name 'articulo_*.json' | wc -l
wc -l data/datasets/codigo_trabajo.jsonl
```

## Article-Level Inspection

```bash
sed -n '1,220p' data/chunks/articulo_<id>.json
sed -n '1,220p' data/synthetic/articulo_<id>.json
sed -n '1,220p' data/validated/articulo_<id>.json
```

## Recovery Commands

- Existing synthetic is good, validation changed:
  - `make revalidate`
  - `make export`
- Synthetic missing/bad for one article:
  - `make article ARTICLE=<id>`
- Start fresh generated outputs:
  - `make reset`
  - `make build`

