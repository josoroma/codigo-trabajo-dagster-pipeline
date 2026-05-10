"""Minimal ``.env`` loader (no third-party deps).

Reads ``KEY=VALUE`` lines from one or more candidate files and inserts them
into :data:`os.environ` without overriding values that are already set.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable


def load_dotenv(paths: Iterable[Path]) -> None:
    for path in paths:
        if not path.is_file():
            continue
        try:
            for raw in path.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key.strip(), value)
        except OSError:
            continue
