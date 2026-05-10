"""Stage 1 — fetch HTML and convert it to clean Markdown."""

from __future__ import annotations

import dagster as dg

from ..constants import MARKDOWN_FILE, RAW_HTML, SOURCE_URL
from ..core.fetch import fetch_html, html_to_markdown


@dg.asset(group_name="acquisition", compute_kind="http")
def raw_html(context) -> dg.MaterializeResult:
    """Fetch the official HTML from the PGR site (idempotent)."""
    html = fetch_html(SOURCE_URL, RAW_HTML)
    context.log.info(f"raw_html: {len(html):,} bytes")
    return dg.MaterializeResult(
        metadata={
            "bytes": len(html),
            "path": dg.MetadataValue.path(str(RAW_HTML)),
            "source_url": dg.MetadataValue.url(SOURCE_URL),
        }
    )


@dg.asset(deps=[raw_html], group_name="acquisition", compute_kind="trafilatura")
def markdown_text(context) -> dg.MaterializeResult:
    """Convert the raw HTML to clean Markdown via trafilatura."""
    html = RAW_HTML.read_text(encoding="utf-8")
    md = html_to_markdown(html, MARKDOWN_FILE)
    context.log.info(f"markdown_text: {len(md):,} chars")
    return dg.MaterializeResult(
        metadata={
            "chars": len(md),
            "path": dg.MetadataValue.path(str(MARKDOWN_FILE)),
        }
    )
