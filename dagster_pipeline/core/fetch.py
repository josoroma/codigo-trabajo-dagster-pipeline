"""HTML acquisition + Markdown extraction (Stage 1).

Wraps :mod:`trafilatura` so the asset layer doesn't have to know about it.
Both functions are idempotent at the filesystem level.
"""

from __future__ import annotations

from pathlib import Path

from trafilatura import extract, fetch_url


def fetch_html(url: str, dest: Path) -> str:
    """Download ``url`` to ``dest`` (skipped if the file already exists)."""
    if dest.exists():
        return dest.read_text(encoding="utf-8")

    html = fetch_url(url)
    if html is None:
        raise RuntimeError(f"Failed to fetch URL: {url}")

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(html, encoding="utf-8")
    return html


def html_to_markdown(html: str, dest: Path) -> str:
    """Extract clean Markdown from ``html`` and persist it at ``dest``."""
    if dest.exists():
        return dest.read_text(encoding="utf-8")

    md = extract(html, output_format="markdown", include_tables=True)
    if md is None:
        raise RuntimeError("trafilatura extraction returned None")

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(md, encoding="utf-8")
    return md
