---
paths:
  - ".env*"
  - "dagster_pipeline/resources/openrouter.py"
  - "dagster_pipeline/constants.py"
---

# Security Rules

- Never commit real `OPENROUTER_API_KEY` values.
- `.env.example` may contain placeholder values only.
- Do not log bearer tokens or full request headers.
- API failures may log status codes and short response snippets, but not
  secrets.
- Legal dataset output is research/educational; do not present generated text
  as legal advice.

