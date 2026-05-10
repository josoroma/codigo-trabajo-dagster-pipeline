"""Project-wide constants and filesystem layout.

Importing this module also loads ``.env`` (from the repo root or CWD) so all
downstream code sees ``OPENROUTER_*`` variables without further setup.
"""

from __future__ import annotations

import os
from pathlib import Path

from .core.env import load_dotenv

REPO_ROOT: Path = Path(__file__).resolve().parent.parent

DATA_DIR: Path = REPO_ROOT / "data"
RAW_DIR: Path = DATA_DIR / "raw"
MARKDOWN_DIR: Path = DATA_DIR / "markdown"
CHUNKS_DIR: Path = DATA_DIR / "chunks"
SYNTHETIC_DIR: Path = DATA_DIR / "synthetic"
VALIDATED_DIR: Path = DATA_DIR / "validated"
DATASETS_DIR: Path = DATA_DIR / "datasets"

RAW_HTML: Path = RAW_DIR / "codigo_trabajo.html"
MARKDOWN_FILE: Path = MARKDOWN_DIR / "codigo_trabajo.md"
DATASET_JSONL: Path = DATASETS_DIR / "codigo_trabajo.jsonl"
UNSLOTH_JSONL: Path = DATASETS_DIR / "codigo_trabajo_unsloth.jsonl"

SOURCE_URL: str = (
    "https://www.pgrweb.go.cr/scij/Busqueda/Normativa/Normas/"
    "nrm_texto_completo.aspx?nValor1=1&nValor2=8045"
)
LAW_CODE: str = "Código de Trabajo de Costa Rica"

# Concurrency pool used by the LLM-facing asset. Configure the default pool
# limit in ``dagster.yaml`` under ``concurrency.pools.default_limit``.
OPENROUTER_POOL: str = "openrouter"


# Load .env files from the repo root and the current working directory.
load_dotenv([REPO_ROOT / ".env", Path.cwd() / ".env"])


# OpenRouter configuration (defaults can be overridden via env vars).
OPENROUTER_API_KEY: str = os.environ.get("OPENROUTER_API_KEY", "").strip()
OPENROUTER_BASE_URL: str = os.environ.get(
    "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"
).rstrip("/")
OPENROUTER_API: str = f"{OPENROUTER_BASE_URL}/chat/completions"
OPENROUTER_MODEL: str = os.environ.get("OPENROUTER_MODEL", "deepseek/deepseek-v4-pro")
OPENROUTER_REFERER: str = os.environ.get(
    "OPENROUTER_REFERER", "https://github.com/josoroma/legal"
)
OPENROUTER_TITLE: str = os.environ.get("OPENROUTER_TITLE", "legal-synthetic-gen")
OPENROUTER_MAX_RETRIES: int = int(os.environ.get("OPENROUTER_MAX_RETRIES", "6"))
OPENROUTER_RETRY_BACKOFF_SECONDS: int = int(
    os.environ.get("OPENROUTER_RETRY_BACKOFF_SECONDS", "5")
)
OPENROUTER_RATE_LIMIT_BACKOFF_SECONDS: int = int(
    os.environ.get("OPENROUTER_RATE_LIMIT_BACKOFF_SECONDS", "60")
)
OPENROUTER_MAX_BACKOFF_SECONDS: int = int(
    os.environ.get("OPENROUTER_MAX_BACKOFF_SECONDS", "300")
)
