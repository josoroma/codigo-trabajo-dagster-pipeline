"""Standalone OpenRouter chat-completion resource.

Self-contained — does not depend on any code under ``scripts/``. Auth is
sourced from the ``OPENROUTER_API_KEY`` env var (loaded from ``.env`` by
:mod:`dagster_pipeline.constants`).
"""

from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.request

import dagster as dg

from ..constants import (
    OPENROUTER_API,
    OPENROUTER_API_KEY,
    OPENROUTER_MAX_BACKOFF_SECONDS,
    OPENROUTER_MAX_RETRIES,
    OPENROUTER_MODEL,
    OPENROUTER_RATE_LIMIT_BACKOFF_SECONDS,
    OPENROUTER_REFERER,
    OPENROUTER_RETRY_BACKOFF_SECONDS,
    OPENROUTER_TITLE,
)


class OpenRouterError(RuntimeError):
    """Raised when the OpenRouter client cannot recover (e.g. auth failure)."""


class OpenRouterResource(dg.ConfigurableResource):
    """Thin OpenRouter chat-completions client with retry + JSON-mode."""

    model: str | None = None
    temperature: float = 0.2
    max_tokens: int = 4096
    timeout: int = 180
    max_retries: int = OPENROUTER_MAX_RETRIES
    retry_backoff_seconds: int = OPENROUTER_RETRY_BACKOFF_SECONDS
    rate_limit_backoff_seconds: int = OPENROUTER_RATE_LIMIT_BACKOFF_SECONDS
    max_backoff_seconds: int = OPENROUTER_MAX_BACKOFF_SECONDS

    @property
    def model_name(self) -> str:
        return self.model or OPENROUTER_MODEL

    def call(
        self,
        prompt: str,
        *,
        system: str | None = None,
        response_format_json: bool = False,
        logger: "dg.DagsterLogManager | None" = None,
    ) -> str | None:
        """POST chat/completions and return the assistant content (or ``None``)."""
        if not OPENROUTER_API_KEY:
            raise OpenRouterError(
                "OPENROUTER_API_KEY is not set. Copy .env.example to .env and add your key."
            )

        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        body: dict[str, object] = {
            "model": self.model_name,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }
        if response_format_json:
            body["response_format"] = {"type": "json_object"}

        payload = json.dumps(body).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "HTTP-Referer": OPENROUTER_REFERER,
            "X-Title": OPENROUTER_TITLE,
        }

        warn = logger.warning if logger else (lambda msg: None)

        def retry_after_seconds(exc: urllib.error.HTTPError) -> float | None:
            retry_after = exc.headers.get("Retry-After")
            if retry_after is None:
                return None
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                return None

        def sleep_before_next_attempt(attempt: int, *, rate_limited: bool = False) -> None:
            if attempt >= self.max_retries:
                return
            exponential = self.retry_backoff_seconds * (2 ** (attempt - 1))
            delay = max(self.rate_limit_backoff_seconds, exponential) if rate_limited else exponential
            delay = min(delay, self.max_backoff_seconds)
            jitter = random.uniform(0, min(3.0, delay * 0.1))
            wait = delay + jitter
            warn(
                f"Waiting {wait:.1f}s before OpenRouter retry "
                f"{attempt + 1}/{self.max_retries}"
            )
            time.sleep(wait)

        for attempt in range(1, self.max_retries + 1):
            req = urllib.request.Request(
                OPENROUTER_API, data=payload, headers=headers, method="POST"
            )
            rate_limited = False
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    result = json.loads(resp.read().decode("utf-8"))
                    choices = result.get("choices") or []
                    if choices:
                        return choices[0].get("message", {}).get("content", "")
                    warn(f"OpenRouter returned empty choices (attempt {attempt}/{self.max_retries})")
            except urllib.error.HTTPError as exc:
                snippet = exc.read().decode("utf-8", errors="replace")[:500]
                warn(f"OpenRouter HTTP {exc.code} (attempt {attempt}/{self.max_retries}): {snippet}")
                if exc.code in (401, 403):
                    return None
                if exc.code == 429:
                    rate_limited = True
                    if attempt < self.max_retries and (retry_after := retry_after_seconds(exc)):
                        wait = min(retry_after, self.max_backoff_seconds)
                        warn(
                            f"OpenRouter asked us to retry after {retry_after:.1f}s; "
                            f"waiting {wait:.1f}s"
                        )
                        time.sleep(wait)
                        continue
            except urllib.error.URLError as exc:
                warn(f"OpenRouter URL error (attempt {attempt}/{self.max_retries}): {exc}")
            except json.JSONDecodeError as exc:
                warn(f"OpenRouter JSON decode error (attempt {attempt}/{self.max_retries}): {exc}")
            except Exception as exc:  # noqa: BLE001
                warn(f"OpenRouter error (attempt {attempt}/{self.max_retries}): {exc}")

            sleep_before_next_attempt(attempt, rate_limited=rate_limited)

        return None
