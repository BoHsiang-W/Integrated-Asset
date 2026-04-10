"""Gemini API client with automatic rate-limit retry and model fallback."""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

from google import genai
from google.genai import types

# Models ordered from best quality to worst (only those with free-tier quota).
_MODEL_FALLBACK: list[str] = [
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
    "gemini-3.1-flash-lite-preview",
    "gemini-2.5-flash-lite",
]


class GeminiClient:
    """Google Gemini API client with automatic rate-limit retry and model fallback."""

    def __init__(self, model: str | None = None) -> None:
        self._client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
        self.models = [model] if model else list(_MODEL_FALLBACK)

    def analyze_pdf(
        self, prompt: str, pdf_path: Path, max_retries: int = 5
    ) -> str | None:
        """Send *pdf_path* and *prompt* to Gemini; return the text response or ``None``.

        Tries each model in the fallback list. Within each model, retries
        on rate-limit / 503 errors with exponential backoff.
        """
        contents = [
            types.Part.from_bytes(
                data=pdf_path.read_bytes(),
                mime_type="application/pdf",
            ),
            prompt,
        ]

        for model in self.models:
            for attempt in range(1, max_retries + 1):
                try:
                    response = self._client.models.generate_content(
                        model=model,
                        contents=contents,
                    )
                    if attempt > 1 or model != self.models[0]:
                        print(f"  (using {model})")
                    return response.text
                except Exception as exc:
                    if _is_model_unavailable(exc):
                        print(f"  [{model}] not available, trying next model...")
                        break
                    if not _is_retryable_error(exc):
                        print(f"  Gemini error [{model}]: {exc}")
                        return None
                    wait = _parse_retry_delay(str(exc), attempt)
                    print(
                        f"  [{model}] attempt {attempt}/{max_retries}, retrying in {wait:.0f}s..."
                    )
                    time.sleep(wait)

            print(
                f"  {model} exhausted after {max_retries} retries, trying next model..."
            )

        print("  All models exhausted. Giving up.")
        return None


def _is_model_unavailable(exc: Exception) -> bool:
    msg = str(exc)
    return "404" in msg or "NOT_FOUND" in msg


def _is_retryable_error(exc: Exception) -> bool:
    msg = str(exc)
    return (
        "429" in msg
        or "RESOURCE_EXHAUSTED" in msg
        or "503" in msg
        or "UNAVAILABLE" in msg
    )


def _parse_retry_delay(error_msg: str, attempt: int) -> float:
    """Extract suggested wait time from the error message or fall back to exponential backoff."""
    match = re.search(r"retry in ([\d.]+)s", error_msg)
    return float(match.group(1)) + 1 if match else min(30 * attempt, 120)
