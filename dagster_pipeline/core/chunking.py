"""Markdown → per-article chunks (Stage 2).

Pure functions over strings/lists. No I/O — the asset layer is responsible
for reading the input markdown and writing the resulting JSON files.
"""

from __future__ import annotations

import re
from typing import Any

from ..constants import LAW_CODE, SOURCE_URL

TOTAL_ARTICLES = 713

UNAVAILABLE_STUB = (
    "El documento para este artículo no está disponible en la fuente "
    "oficial. Artículo derogado o no publicado."
)

# Article header — handles many bold/spacing variants seen in the source.
ARTICLE_RE = re.compile(
    r"^\s*(?:[*_]+\s*)*"
    r"(?:ARTICULO|ARTÍCULO|Artículo|Articulo)"
    r"S?"
    r"\s*[*_]*"
    r"\s+(\d+)"
    r"(?:\s*(bis|ter|quater|quinquies|sexies|septies|octies|novies|decies))?"
    r"\s*[*_]*"
    r"\s*[ºo°]?"
    r"\s*[.\-–—]"
)
_ORDINAL_HEADING = (
    r"(?:"
    r"PRIMERO|SEGUNDO|TERCERO|CUARTO|QUINTO|SEXTO|S[EÉ]TIMO|OCTAVO|NOVENO|"
    r"D[EÉ]CIMO(?:\s+PRIMERO)?|UND[EÉ]CIMO|DUOD[EÉ]CIMO|DECIMOTERCERO|"
    r"DECIMOCUARTO|DECIMOQUINTO|UNICO|[IVXLCDM]+"
    r")"
)
LIBRO_RE = re.compile(
    rf"^\s*\*{{0,4}}\s*LIBRO\s+({_ORDINAL_HEADING})\b",
    re.IGNORECASE,
)
TITULO_RE = re.compile(
    rf"^\s*\*{{0,4}}\s*T[IÍ]TULO\s+({_ORDINAL_HEADING})\b",
    re.IGNORECASE,
)
CAPITULO_RE = re.compile(
    rf"^\s*\*{{0,4}}\s*CAP[IÍ]TULO\s+({_ORDINAL_HEADING})\b",
    re.IGNORECASE,
)
TRANSITORY_ARTICLE_RE = re.compile(
    r"^\s*(?:[*_]+\s*)*ART[IÍ]CULO\s+"
    r"(?:I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII|XIII|XIV|XV)"
    r"\s*[.\-–—]",
    re.IGNORECASE,
)
OUTRO_RE = re.compile(r"^\s*Fecha de generación:", re.IGNORECASE)

_SUFFIX_ORDER = {
    "": 0, "bis": 1, "ter": 2, "quater": 3, "quinquies": 4,
    "sexies": 5, "septies": 6, "octies": 7, "novies": 8, "decies": 9,
}


def normalize_markdown(md: str) -> str:
    """Strip table/HTML noise and rejoin article headers split across lines."""
    md = re.sub(
        r"([*_]{0,4}\s*(?:ARTICULO|ARTÍCULO|Artículo|Articulo)S?\s*[*_]{0,4})\s*\n\s*(\d+)",
        r"\1 \2",
        md,
    )
    out: list[str] = []
    for line in md.split("\n"):
        if re.match(r"^\s*\|", line):
            continue
        if re.match(r"^\s*</?[a-zA-Z]+[^>]*>\s*$", line):
            continue
        out.append(line)
    return "\n".join(out)


def _flush_article(chunks: list[dict[str, Any]], state: dict[str, Any]) -> None:
    if state["article"] is None:
        return
    chunks.append(_make_chunk(
        article=state["article"],
        libro=state["libro"],
        titulo=state["titulo"],
        capitulo=state["capitulo"],
        content="\n".join(state["body"]).strip(),
    ))
    state["last_article"] = state["article"]
    state["article"] = None
    state["body"] = []


def _flush_document_chunk(chunks: list[dict[str, Any]], state: dict[str, Any]) -> None:
    if state["document_section"] is None:
        return
    content = "\n".join(state["body"]).strip()
    if content:
        chunks.append(_make_document_chunk(
            section=state["document_section"],
            content=content,
            before_article=state.get("before_article"),
            after_article=state.get("after_article"),
        ))
    state["document_section"] = None
    state["before_article"] = None
    state["after_article"] = None
    state["body"] = []


def _make_chunk(
    *,
    article: str,
    libro: str | None,
    titulo: str | None,
    capitulo: str | None,
    content: str,
    unavailable: bool = False,
) -> dict[str, Any]:
    chunk_id = (
        f"libro_{libro}_titulo_{titulo}_capitulo_{capitulo}_articulo_{article}"
    )
    hierarchy = (
        f"libro/{libro}/titulo/{titulo}/capitulo/{capitulo}/articulo/{article}"
    )
    chunk: dict[str, Any] = {
        "chunk_id": chunk_id,
        "article": article,
        "libro": libro,
        "titulo": titulo,
        "capitulo": capitulo,
        "hierarchy_path": hierarchy,
        "source_url": SOURCE_URL,
        "law_code": LAW_CODE,
        "content": content,
    }
    if unavailable:
        chunk["unavailable"] = True
    return chunk


def _make_document_chunk(
    *,
    section: str,
    content: str,
    before_article: str | None = None,
    after_article: str | None = None,
) -> dict[str, Any]:
    chunk: dict[str, Any] = {
        "chunk_id": f"document_{section}",
        "document_section": section,
        "article": None,
        "libro": None,
        "titulo": None,
        "capitulo": None,
        "hierarchy_path": f"document/{section}",
        "source_url": SOURCE_URL,
        "law_code": LAW_CODE,
        "content": content,
    }
    if before_article is not None:
        chunk["before_article"] = before_article
    if after_article is not None:
        chunk["after_article"] = after_article
    return chunk


def chunk_articles(md: str) -> list[dict[str, Any]]:
    """Split normalized markdown into article and document-boundary chunks.

    Numeric article chunks keep ``article`` set to the article id. Non-article
    material, such as the document preamble, source footer, or transitional
    Roman-numeral articles, is emitted with ``document_section`` so downstream
    per-article assets can ignore it safely.
    """
    chunks: list[dict[str, Any]] = []
    state: dict[str, Any] = {
        "libro": None, "titulo": None, "capitulo": None,
        "article": None, "document_section": "intro", "body": [], "seen": set(),
        "before_article": None, "after_article": None, "last_article": None,
    }

    for raw in md.split("\n"):
        m_art = ARTICLE_RE.match(raw)
        m_transitory = TRANSITORY_ARTICLE_RE.match(raw)

        if OUTRO_RE.match(raw) and state["article"] is not None:
            previous = state["article"]
            _flush_article(chunks, state)
            state["document_section"] = "outro"
            state["after_article"] = previous
            state["body"] = [raw]
            continue

        if m_transitory and state["article"] is not None:
            previous = state["article"]
            _flush_article(chunks, state)
            state["document_section"] = "transitory_articles"
            state["after_article"] = previous
            state["body"] = [raw]
            continue

        if not m_art and state["document_section"] == "transitory_articles":
            state["body"].append(raw)
            continue

        if not m_art:
            if (m := LIBRO_RE.match(raw)):
                state["libro"] = m.group(1).strip().upper()
                state["titulo"] = state["capitulo"] = None
                if state["document_section"] is not None:
                    state["body"].append(raw)
                continue
            if (m := TITULO_RE.match(raw)):
                state["titulo"] = m.group(1).strip().upper()
                state["capitulo"] = None
                if state["document_section"] is not None:
                    state["body"].append(raw)
                continue
            if (m := CAPITULO_RE.match(raw)):
                state["capitulo"] = m.group(1).strip().upper()
                if state["document_section"] is not None:
                    state["body"].append(raw)
                continue

        if m_art:
            num = m_art.group(1)
            suffix = (m_art.group(2) or "").lower()
            article_id = f"{num}_{suffix}" if suffix else num
            if article_id in state["seen"]:
                if state["article"] is not None:
                    state["body"].append(raw)
                elif state["document_section"] is not None:
                    state["body"].append(raw)
                continue
            if state["document_section"] is not None:
                state["before_article"] = article_id
                _flush_document_chunk(chunks, state)
            _flush_article(chunks, state)
            state["seen"].add(article_id)
            state["article"] = article_id
            state["body"] = [raw]
            continue

        if state["article"] is not None:
            state["body"].append(raw)
        elif state["document_section"] is not None:
            state["body"].append(raw)

    _flush_article(chunks, state)
    _flush_document_chunk(chunks, state)
    return chunks


def article_sort_key(article_id: str) -> tuple[int, int]:
    if "_" in article_id:
        n, sfx = article_id.split("_", 1)
        return (int(n), _SUFFIX_ORDER.get(sfx, 99))
    return (int(article_id), 0)


def fill_missing_articles(
    chunks: list[dict[str, Any]], total: int = TOTAL_ARTICLES
) -> list[dict[str, Any]]:
    """Add stub chunks for articles 1..total that are missing from the source."""
    document_chunks = [c for c in chunks if c.get("document_section")]
    article_chunks = [c for c in chunks if not c.get("document_section")]
    by_num: dict[int, dict[str, Any]] = {}
    for c in article_chunks:
        base = int(c["article"].split("_", 1)[0])
        by_num.setdefault(base, c)
    if not by_num:
        return chunks

    ordered = sorted(by_num.keys())

    def context_for(n: int) -> tuple[str | None, str | None, str | None]:
        libro = titulo = capitulo = None
        for k in ordered:
            if k > n:
                break
            libro = by_num[k].get("libro")
            titulo = by_num[k].get("titulo")
            capitulo = by_num[k].get("capitulo")
        return libro, titulo, capitulo

    for n in range(1, total + 1):
        if n in by_num:
            continue
        libro, titulo, capitulo = context_for(n)
        article_chunks.append(_make_chunk(
            article=str(n),
            libro=libro,
            titulo=titulo,
            capitulo=capitulo,
            content=f"Artículo {n}.- {UNAVAILABLE_STUB}",
            unavailable=True,
        ))

    article_chunks.sort(key=lambda c: article_sort_key(c["article"]))
    before_docs: dict[str, list[dict[str, Any]]] = {}
    after_docs: dict[str, list[dict[str, Any]]] = {}
    unanchored_docs: list[dict[str, Any]] = []
    for chunk in document_chunks:
        if before := chunk.get("before_article"):
            before_docs.setdefault(str(before), []).append(chunk)
        elif after := chunk.get("after_article"):
            after_docs.setdefault(str(after), []).append(chunk)
        else:
            unanchored_docs.append(chunk)

    ordered: list[dict[str, Any]] = [*unanchored_docs]
    emitted_docs: set[int] = {id(chunk) for chunk in unanchored_docs}
    for chunk in article_chunks:
        article_id = str(chunk["article"])
        for doc in before_docs.get(article_id, []):
            ordered.append(doc)
            emitted_docs.add(id(doc))
        ordered.append(chunk)
        for doc in after_docs.get(article_id, []):
            ordered.append(doc)
            emitted_docs.add(id(doc))

    ordered.extend(c for c in document_chunks if id(c) not in emitted_docs)
    return ordered
