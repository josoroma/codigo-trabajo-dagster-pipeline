"""System + user prompts for the synthetic-generation step (Stage 3)."""

from __future__ import annotations

from ..constants import LAW_CODE, SOURCE_URL

SYSTEM_PROMPT = (
    "Eres un asistente legal experto en el Código de Trabajo de Costa Rica.\n"
    "Generarás datos sintéticos basados EXACTAMENTE en el texto proporcionado.\n"
    "Nunca inventes información, requisitos, excepciones ni consecuencias que "
    "no estén en el artículo.\n"
    "Cada respuesta debe estar anclada a una cita textual incluida en "
    "source_quote."
)

MAX_CONTENT_LEN = 3000


def build_batch_prompt(article_num: str, content: str, chunk_id: str) -> str:
    """Return the user prompt for the three grounded generation objects."""
    truncated = (
        content
        if len(content) <= MAX_CONTENT_LEN
        else content[:MAX_CONTENT_LEN] + "\n\n[contenido truncado]"
    )
    return f"""ARTÍCULO {article_num} DEL CÓDIGO DE TRABAJO DE COSTA RICA:
{truncated}

Genera un objeto JSON con la clave "entries". El valor de "entries" debe ser
un array con EXACTAMENTE 3 objetos, en este orden:

Objeto 1 (explicación): Explica qué trata este artículo en términos claros y simples.
Objeto 2 (preguntas-respuestas): Crea preguntas específicas sobre este artículo
y sus respuestas correctas.
Objeto 3 (citas): Cita textualmente las partes representativas del artículo y
explica cada uno de sus propósitos legales.

Reglas estrictas:
- Usa SOLO información presente en el artículo.
- Cada source_quote debe copiar una frase o fragmento literal del artículo.
- Si el artículo está derogado, anulado o no disponible, explica solo ese estado.
- No cites artículos distintos salvo que el texto de este artículo los mencione.
- No uses markdown code blocks ni texto fuera del JSON.
- Escribe en español claro y jurídico, sin adornos.

Formato del JSON:
{{
  "entries": [
    {{
      "dataset_type": "article_explanation",
      "instruction": "Explica en qué consiste el artículo {article_num} del Código de Trabajo.",
      "input": "",
      "output": "Explicación clara, breve y fiel del contenido del artículo.",
      "source_quote": "Frase literal del artículo que sustenta la explicación.",
      "source_url": "{SOURCE_URL}",
      "law_code": "{LAW_CODE}",
      "article": "{article_num}",
      "chunk_id": "{chunk_id}"
    }},
    {{
      "dataset_type": "qa",
      "instruction": "Responde preguntas específicas sobre el artículo {article_num}.",
      "input": "",
      "qa_pairs": [
        {{
          "question": "Pregunta específica cuya respuesta salga directamente del artículo.",
          "answer": "Respuesta correcta basada únicamente en el artículo.",
          "source_quote": "Frase literal del artículo que sustenta esta respuesta."
        }},
        {{
          "question": "Otra pregunta específica sobre un aspecto distinto del artículo.",
          "answer": "Respuesta correcta basada únicamente en el artículo.",
          "source_quote": "Otra frase literal del artículo que sustenta esta respuesta."
        }}
      ],
      "source_url": "{SOURCE_URL}",
      "law_code": "{LAW_CODE}",
      "article": "{article_num}",
      "chunk_id": "{chunk_id}"
    }},
    {{
      "dataset_type": "cite_article",
      "instruction": "Cita partes representativas y explica su propósito legal.",
      "input": "",
      "citations": [
        {{
          "source_quote": "Primera parte literal representativa del artículo.",
          "purpose": "Explicación del propósito legal de esta cita."
        }},
        {{
          "source_quote": "Segunda parte literal representativa del artículo, si existe.",
          "purpose": "Explicación del propósito legal de esta cita."
        }}
      ],
      "source_url": "{SOURCE_URL}",
      "law_code": "{LAW_CODE}",
      "article": "{article_num}",
      "chunk_id": "{chunk_id}"
    }}
  ]
}}

Devuelve SOLO el objeto JSON."""
