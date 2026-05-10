"""Unit tests for the pure-Python ``core`` modules.

Pure functions, no Dagster runtime, no network. Run with ``pytest``.
"""

from __future__ import annotations

import json
from pathlib import Path

import dagster as dg
import pytest

from dagster_pipeline.constants import SOURCE_URL
import dagster_pipeline.full_pipeline as full_pipeline_module
from dagster_pipeline.core.chunking import (
    article_sort_key,
    chunk_articles,
    fill_missing_articles,
    normalize_markdown,
)
from dagster_pipeline.core.generation import expand_generated_entries
from dagster_pipeline.core.parsing import parse_json_array
from dagster_pipeline.core.validation import filter_valid, validate_entry
import dagster_pipeline.partitions as partition_module
from dagster_pipeline.partitions import (
    article_chunk_path,
    article_id_from_chunk_path,
    discover_article_keys,
    is_article_partition_key,
    sync_article_partitions,
)


# ---------- parsing -------------------------------------------------------


def test_parse_json_array_plain() -> None:
    text = json.dumps([{"a": 1}, {"b": 2}])
    assert parse_json_array(text) == [{"a": 1}, {"b": 2}]


def test_parse_json_array_strips_markdown_fences() -> None:
    text = "```json\n[{\"a\": 1}]\n```"
    assert parse_json_array(text) == [{"a": 1}]


def test_parse_json_array_repairs_trailing_comma() -> None:
    text = '[{"a": 1}, {"b": 2},]'
    assert parse_json_array(text) == [{"a": 1}, {"b": 2}]


def test_parse_json_array_object_to_list() -> None:
    assert parse_json_array('{"a": 1}') == [{"a": 1}]


def test_parse_json_array_unwraps_entries_object() -> None:
    text = '{"entries": [{"a": 1}, {"b": 2}]}'
    assert parse_json_array(text) == [{"a": 1}, {"b": 2}]


def test_parse_json_array_returns_none_for_garbage() -> None:
    assert parse_json_array("nope") is None
    assert parse_json_array(None) is None


# ---------- validation ----------------------------------------------------


def _good_entry() -> dict:
    return {
        "instruction": "Explica el artículo 94.",
        "input": "",
        "output": "Establece protecciones para trabajadoras embarazadas.",
        "source_quote": "Queda prohibido a los patronos despedir a las trabajadoras embarazadas",
        "source_url": SOURCE_URL,
        "law_code": "Código de Trabajo de Costa Rica",
        "article": "94",
        "chunk_id": "libro_X_titulo_Y_capitulo_Z_articulo_94",
        "dataset_type": "article_explanation",
    }


def test_validate_entry_passes() -> None:
    assert validate_entry(_good_entry()) == []


def test_validate_entry_flags_missing_field() -> None:
    bad = _good_entry()
    del bad["source_quote"]
    errors = validate_entry(bad)
    assert any("source_quote" in e for e in errors)


def test_validate_entry_flags_short_quote() -> None:
    bad = _good_entry()
    bad["source_quote"] = "x"
    assert any("source_quote" in e for e in validate_entry(bad))


def test_validate_entry_flags_wrong_url() -> None:
    bad = _good_entry()
    bad["source_url"] = "https://example.com/"
    assert any("source_url" in e for e in validate_entry(bad))


def test_filter_valid_partitions_input() -> None:
    good = _good_entry()
    bad = _good_entry()
    bad["output"] = ""
    valid, rejections = filter_valid([good, bad])
    assert valid == [good]
    assert len(rejections) == 1


def test_validate_entry_can_check_source_quote_against_article_text() -> None:
    good = _good_entry()
    assert validate_entry(good, source_text=good["source_quote"]) == []
    errors = validate_entry(good, source_text="Texto distinto del artículo.")
    assert "source_quote not found in source article" in errors


def test_validate_entry_matches_source_quote_across_line_breaks() -> None:
    good = _good_entry()
    source_text = (
        "Queda prohibido a los patronos despedir a las trabajadoras\n"
        "embarazadas"
    )
    good["source_quote"] = (
        "Queda prohibido a los patronos despedir a las trabajadoras embarazadas"
    )

    assert validate_entry(good, source_text=source_text) == []


# ---------- generation normalization -------------------------------------


def test_expand_generated_entries_flattens_qa_pairs_and_citations() -> None:
    chunk = {"article": "94", "chunk_id": "chunk_94"}
    raw = [
        {
            "dataset_type": "article_explanation",
            "instruction": "Explica el artículo 94.",
            "input": "",
            "output": "Protege a ciertas trabajadoras.",
            "source_quote": "Queda prohibido despedir",
        },
        {
            "dataset_type": "qa",
            "qa_pairs": [
                {
                    "question": "¿Qué prohíbe?",
                    "answer": "Prohíbe el despido en los casos indicados.",
                    "source_quote": "Queda prohibido despedir",
                },
                {
                    "question": "¿A quién protege?",
                    "answer": "A las trabajadoras señaladas en el artículo.",
                    "source_quote": "trabajadoras que estuvieren",
                },
            ],
        },
        {
            "dataset_type": "cite_article",
            "citations": [
                {
                    "source_quote": "Queda prohibido despedir",
                    "purpose": "Establece la prohibición principal.",
                }
            ],
        },
    ]

    entries = expand_generated_entries(raw, chunk)
    assert [entry["dataset_type"] for entry in entries] == [
        "article_explanation",
        "qa",
        "qa",
        "cite_article",
    ]
    assert entries[1]["instruction"] == "¿Qué prohíbe?"
    assert entries[1]["output"] == "Prohíbe el despido en los casos indicados."
    assert entries[3]["output"] == "Establece la prohibición principal."
    assert all(entry["article"] == "94" for entry in entries)


# ---------- chunking ------------------------------------------------------


def test_normalize_markdown_rejoins_split_article_header() -> None:
    md = "**ARTICULO**\n1.- texto"
    out = normalize_markdown(md)
    assert "**ARTICULO** 1.- texto" in out


def test_chunk_articles_extracts_headers_and_body() -> None:
    md = (
        "LIBRO PRIMERO\nTITULO PRIMERO\nCAPITULO PRIMERO\n"
        "**ARTICULO 1.-** Primer artículo.\n"
        "**ARTICULO 2.-** Segundo artículo."
    )
    chunks = [
        c for c in chunk_articles(normalize_markdown(md))
        if not c.get("document_section")
    ]
    assert [c["article"] for c in chunks] == ["1", "2"]
    assert chunks[0]["libro"] == "PRIMERO"
    assert "Primer artículo" in chunks[0]["content"]


def test_chunk_articles_does_not_treat_body_libro_de_as_heading() -> None:
    md = (
        "TITULO PRIMERO\nCAPITULO PRIMERO\n"
        "**ARTICULO 1.-** El patrono llevará un libro de salarios.\n"
        "**ARTICULO 2.-** Segundo artículo."
    )
    chunks = [
        c for c in chunk_articles(normalize_markdown(md))
        if not c.get("document_section")
    ]
    assert [c["article"] for c in chunks] == ["1", "2"]
    assert chunks[0]["libro"] is None
    assert chunks[1]["libro"] is None
    assert "libro de salarios" in chunks[0]["content"]


def test_chunk_articles_preserves_intro_and_outro() -> None:
    md = (
        "Texto Completo acta: D4FE\n"
        "**CODIGO DE TRABAJO**\n"
        "**ARTICULO 1.-** Primer artículo.\n"
        "Ficha articulo\n"
        "Fecha de generación: 2/5/2026 17:01:39"
    )
    chunks = chunk_articles(normalize_markdown(md))
    assert [c.get("document_section") or c["article"] for c in chunks] == [
        "intro",
        "1",
        "outro",
    ]
    assert "CODIGO DE TRABAJO" in chunks[0]["content"]
    assert "Fecha de generación" not in chunks[1]["content"]
    assert "Fecha de generación" in chunks[2]["content"]


def test_chunk_articles_keeps_transitory_articles_out_of_numeric_article() -> None:
    md = (
        "**Artículo 624.- **Texto 624.\n"
        "ARTICULO I.- Transitorio uno.\n"
        "ARTICULO II.- Transitorio dos.\n"
        "**Artículo 625.- **Texto 625."
    )
    chunks = chunk_articles(normalize_markdown(md))
    assert [c.get("document_section") or c["article"] for c in chunks] == [
        "624",
        "transitory_articles",
        "625",
    ]
    assert "Transitorio uno" not in chunks[0]["content"]
    assert "Transitorio dos" in chunks[1]["content"]


def test_fill_missing_articles_adds_stubs() -> None:
    chunks = [
        {
            "article": "1",
            "chunk_id": "x",
            "libro": "A",
            "titulo": "B",
            "capitulo": "C",
            "hierarchy_path": "p",
            "source_url": SOURCE_URL,
            "law_code": "Código de Trabajo de Costa Rica",
            "content": "x",
        }
    ]
    filled = fill_missing_articles(chunks, total=3)
    assert [c["article"] for c in filled] == ["1", "2", "3"]
    assert filled[1].get("unavailable") is True


def test_fill_missing_articles_ignores_document_chunks() -> None:
    chunks = [
        {
            "chunk_id": "document_intro",
            "document_section": "intro",
            "article": None,
            "content": "Intro",
            "before_article": "1",
        },
        {
            "article": "1",
            "chunk_id": "x",
            "libro": "A",
            "titulo": "B",
            "capitulo": "C",
            "hierarchy_path": "p",
            "source_url": SOURCE_URL,
            "law_code": "Código de Trabajo de Costa Rica",
            "content": "x",
        },
    ]
    filled = fill_missing_articles(chunks, total=2)
    assert [c.get("document_section") or c["article"] for c in filled] == [
        "intro",
        "1",
        "2",
    ]


def test_article_sort_key_orders_suffixes() -> None:
    keys = ["10", "2_bis", "2", "10_ter"]
    assert sorted(keys, key=article_sort_key) == ["2", "2_bis", "10", "10_ter"]


# ---------- partitions ----------------------------------------------------


def test_article_id_from_chunk_path() -> None:
    assert article_id_from_chunk_path("articulo_94.json") == "94"
    assert article_id_from_chunk_path("articulo_94_bis.json") == "94_bis"


def test_is_article_partition_key_rejects_non_article_titles() -> None:
    assert is_article_partition_key("94")
    assert is_article_partition_key("376_quater")
    assert not is_article_partition_key("p001")
    assert not is_article_partition_key("CodigoDeTrabajoDeCostaRica")
    assert not is_article_partition_key("document_intro")


def test_article_chunk_path_rejects_invalid_keys() -> None:
    assert article_chunk_path("94").name == "articulo_94.json"
    try:
        article_chunk_path("p001")
    except ValueError as exc:
        assert "p001" in str(exc)
    else:
        raise AssertionError("Expected invalid partition key to raise")


def test_discover_article_keys_filters_invalid_chunk_names(tmp_path, monkeypatch) -> None:
    (tmp_path / "articulo_2.json").write_text("{}", encoding="utf-8")
    (tmp_path / "articulo_1_bis.json").write_text("{}", encoding="utf-8")
    (tmp_path / "articulo_p001.json").write_text("{}", encoding="utf-8")
    (tmp_path / "articulo_CodigoDeTrabajoDeCostaRica.json").write_text(
        "{}",
        encoding="utf-8",
    )
    monkeypatch.setattr(partition_module, "CHUNKS_DIR", tmp_path)

    assert discover_article_keys() == ["1_bis", "2"]


def test_sync_article_partitions_prunes_stale_keys(tmp_path, monkeypatch) -> None:
    class FakeInstance:
        def __init__(self) -> None:
            self.keys = {"1", "CodigoDeTrabajoDeCostaRica"}
            self.deleted: list[str] = []
            self.added: list[str] = []

        def get_dynamic_partitions(self, name: str) -> list[str]:
            assert name == "articles"
            return sorted(self.keys)

        def delete_dynamic_partition(self, name: str, key: str) -> None:
            assert name == "articles"
            self.keys.remove(key)
            self.deleted.append(key)

        def add_dynamic_partitions(self, name: str, keys: list[str]) -> None:
            assert name == "articles"
            self.keys.update(keys)
            self.added.extend(keys)

    (tmp_path / "articulo_1.json").write_text("{}", encoding="utf-8")
    (tmp_path / "articulo_2.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(partition_module, "CHUNKS_DIR", tmp_path)

    new_keys, stale_keys, total = sync_article_partitions(FakeInstance())

    assert new_keys == ["2"]
    assert stale_keys == ["CodigoDeTrabajoDeCostaRica"]
    assert total == 2


# ---------- full pipeline resilience -------------------------------------


class _FakeOpenRouter:
    def __init__(self, responses: list[str | None]) -> None:
        self._responses = iter(responses)
        self.calls = 0

    def call(self, *_args, **_kwargs) -> str | None:
        self.calls += 1
        return next(self._responses)


def _write_chunk(path: Path, article: str) -> None:
    content = f"Contenido del articulo {article} con texto suficiente para validar citas."
    payload = {
        "article": article,
        "chunk_id": f"chunk_{article}",
        "content": content,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_generate_and_validate_all_articles_skips_single_empty_generation(
    tmp_path,
    monkeypatch,
) -> None:
    chunks_dir = tmp_path / "chunks"
    synthetic_dir = tmp_path / "synthetic"
    validated_dir = tmp_path / "validated"
    chunks_dir.mkdir()
    synthetic_dir.mkdir()
    validated_dir.mkdir()

    _write_chunk(chunks_dir / "articulo_1.json", "1")
    _write_chunk(chunks_dir / "articulo_2.json", "2")

    monkeypatch.setattr(full_pipeline_module, "SYNTHETIC_DIR", synthetic_dir)
    monkeypatch.setattr(full_pipeline_module, "VALIDATED_DIR", validated_dir)
    monkeypatch.setattr(full_pipeline_module, "discover_article_keys", lambda: ["1", "2"])
    monkeypatch.setattr(
        full_pipeline_module,
        "article_chunk_path",
        lambda article_id: chunks_dir / f"articulo_{article_id}.json",
    )

    good_response = json.dumps([
        {
            "dataset_type": "article_explanation",
            "instruction": "Explica el articulo 2.",
            "output": "Respuesta.",
            "source_quote": "Contenido del articulo 2 con texto suficiente para validar citas.",
        }
    ])
    with dg.build_op_context(resources={"openrouter": _FakeOpenRouter([None, good_response])}) as context:
        result = full_pipeline_module.generate_and_validate_all_articles(
            context,
            {"article_chunks": 2},
        )

    assert result["generated"] == 1
    assert result["skipped_generation"] == 1
    assert not (synthetic_dir / "articulo_1.json").exists()
    assert (synthetic_dir / "articulo_2.json").exists()


def test_generate_and_validate_all_articles_fails_on_many_skips(
    tmp_path,
    monkeypatch,
) -> None:
    chunks_dir = tmp_path / "chunks"
    synthetic_dir = tmp_path / "synthetic"
    validated_dir = tmp_path / "validated"
    chunks_dir.mkdir()
    synthetic_dir.mkdir()
    validated_dir.mkdir()

    keys = [str(i) for i in range(1, 7)]
    for key in keys:
        _write_chunk(chunks_dir / f"articulo_{key}.json", key)

    monkeypatch.setattr(full_pipeline_module, "SYNTHETIC_DIR", synthetic_dir)
    monkeypatch.setattr(full_pipeline_module, "VALIDATED_DIR", validated_dir)
    monkeypatch.setattr(full_pipeline_module, "discover_article_keys", lambda: keys)
    monkeypatch.setattr(
        full_pipeline_module,
        "article_chunk_path",
        lambda article_id: chunks_dir / f"articulo_{article_id}.json",
    )

    with dg.build_op_context(
        resources={"openrouter": _FakeOpenRouter([None, None, None, None, None, None])}
    ) as context:
        with pytest.raises(dg.Failure):
            full_pipeline_module.generate_and_validate_all_articles(
                context,
                {"article_chunks": len(keys)},
            )


def test_generate_and_validate_all_articles_rebuilds_validated_when_synthetic_exists(
    tmp_path,
    monkeypatch,
) -> None:
    chunks_dir = tmp_path / "chunks"
    synthetic_dir = tmp_path / "synthetic"
    validated_dir = tmp_path / "validated"
    chunks_dir.mkdir()
    synthetic_dir.mkdir()
    validated_dir.mkdir()

    _write_chunk(chunks_dir / "articulo_3.json", "3")
    synthetic_entries = [
        {
            "dataset_type": "article_explanation",
            "instruction": "Explica el articulo 3.",
            "input": "",
            "output": "Respuesta.",
            "source_quote": "Contenido del articulo 3 con texto suficiente para validar citas.",
            "source_url": SOURCE_URL,
            "law_code": "Código de Trabajo de Costa Rica",
            "article": "3",
            "chunk_id": "chunk_3",
        }
    ]
    (synthetic_dir / "articulo_3.json").write_text(
        json.dumps(synthetic_entries, ensure_ascii=False),
        encoding="utf-8",
    )
    (validated_dir / "articulo_3.json").write_text("[]", encoding="utf-8")

    monkeypatch.setattr(full_pipeline_module, "SYNTHETIC_DIR", synthetic_dir)
    monkeypatch.setattr(full_pipeline_module, "VALIDATED_DIR", validated_dir)
    monkeypatch.setattr(full_pipeline_module, "discover_article_keys", lambda: ["3"])
    monkeypatch.setattr(
        full_pipeline_module,
        "article_chunk_path",
        lambda article_id: chunks_dir / f"articulo_{article_id}.json",
    )

    fake_openrouter = _FakeOpenRouter([])
    with dg.build_op_context(resources={"openrouter": fake_openrouter}) as context:
        result = full_pipeline_module.generate_and_validate_all_articles(
            context,
            {"article_chunks": 1},
        )

    rebuilt = json.loads((validated_dir / "articulo_3.json").read_text(encoding="utf-8"))
    assert fake_openrouter.calls == 0
    assert result["reused_synthetic"] == 1
    assert result["generated"] == 0
    assert len(rebuilt) == 1


def test_generate_and_validate_all_articles_regenerates_when_both_empty(
    tmp_path,
    monkeypatch,
) -> None:
    chunks_dir = tmp_path / "chunks"
    synthetic_dir = tmp_path / "synthetic"
    validated_dir = tmp_path / "validated"
    chunks_dir.mkdir()
    synthetic_dir.mkdir()
    validated_dir.mkdir()

    _write_chunk(chunks_dir / "articulo_22.json", "22")
    (synthetic_dir / "articulo_22.json").write_text("[]", encoding="utf-8")
    (validated_dir / "articulo_22.json").write_text("[]", encoding="utf-8")

    monkeypatch.setattr(full_pipeline_module, "SYNTHETIC_DIR", synthetic_dir)
    monkeypatch.setattr(full_pipeline_module, "VALIDATED_DIR", validated_dir)
    monkeypatch.setattr(full_pipeline_module, "discover_article_keys", lambda: ["22"])
    monkeypatch.setattr(
        full_pipeline_module,
        "article_chunk_path",
        lambda article_id: chunks_dir / f"articulo_{article_id}.json",
    )

    response = json.dumps([
        {
            "dataset_type": "article_explanation",
            "instruction": "Explica el articulo 22.",
            "output": "Respuesta.",
            "source_quote": "Contenido del articulo 22 con texto suficiente para validar citas.",
        }
    ])
    fake_openrouter = _FakeOpenRouter([response])
    with dg.build_op_context(resources={"openrouter": fake_openrouter}) as context:
        result = full_pipeline_module.generate_and_validate_all_articles(
            context,
            {"article_chunks": 1},
        )

    synthetic = json.loads((synthetic_dir / "articulo_22.json").read_text(encoding="utf-8"))
    validated = json.loads((validated_dir / "articulo_22.json").read_text(encoding="utf-8"))
    assert fake_openrouter.calls == 1
    assert result["generated"] == 1
    assert len(synthetic) == 1
    assert len(validated) == 1
