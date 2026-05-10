"""All software-defined assets for the pipeline, grouped by stage."""

from __future__ import annotations

from . import acquisition, chunking, export, generation, validation

__all__ = ["acquisition", "chunking", "generation", "validation", "export"]
