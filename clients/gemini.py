"""Gemini API client with automatic rate-limit retry."""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

from google import genai
from google.genai import types


class GeminiClient:
    """Google Gemini API client with automatic rate-limit retry."""

    def __init__(self, model: str = "gemini-2.5-flash") -> None:
        self._client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
        self.model = model

    def analyze_pdf(
        self, prompt: str, pdf_path: Path, max_retries: int = 5
    ) -> str | None:
        """Send *pdf_path* and *prompt* to Gemini; return the text response or ``None``."""
        for attempt in range(1, max_retries + 1):
            try:
                response = self._client.models.generate_content(
                    model=self.model,
                    contents=[
                        types.Part.from_bytes(
                            data=pdf_path.read_bytes(),
                            mime_type="application/pdf",
                        ),
                        prompt,
                    ],
                )
                return response.text
            except Exception as exc:
                if not _is_rate_limit_error(exc):
                    print(f"  Gemini error: {exc}")
                    return None
                wait = _parse_retry_delay(str(exc), attempt)
                print(
                    f"  Rate limited (attempt {attempt}/{max_retries}), retrying in {wait:.0f}s..."
                )
                time.sleep(wait)

        print(f"  Failed after {max_retries} retries.")
        return None


def _is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc)
    return "429" in msg or "RESOURCE_EXHAUSTED" in msg


def _parse_retry_delay(error_msg: str, attempt: int) -> float:
    """Extract suggested wait time from the error message or fall back to exponential backoff."""
    match = re.search(r"retry in ([\d.]+)s", error_msg)
    return float(match.group(1)) + 1 if match else min(30 * attempt, 120)
