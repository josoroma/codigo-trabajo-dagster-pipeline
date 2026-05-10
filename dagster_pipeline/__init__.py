"""Dagster code location for the Código de Trabajo synthetic-data pipeline.

The :data:`defs` symbol is what ``dagster dev`` (and any Dagster tool that
points at this module) discovers. Dagster's module scanner needs this symbol
to exist directly on the module, so we import it when Dagster is installed and
keep the package importable for pure-helper usage when Dagster is absent.
"""

from __future__ import annotations

try:
    from .definitions import defs
except ModuleNotFoundError as exc:
    if exc.name != "dagster":
        raise


__all__ = ["defs"]
