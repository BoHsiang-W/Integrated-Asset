"""Select the PDF-analysis LLM client from the ``LLM_PROVIDER`` environment variable."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol


class PdfAnalyzer(Protocol):
    label: str

    def analyze_pdf(self, prompt: str, pdf_path: Path) -> str | None: ...


def get_llm_client() -> PdfAnalyzer:
    """Return the client for ``LLM_PROVIDER`` (``gemini`` by default, or ``claude``)."""
    provider = (os.getenv("LLM_PROVIDER") or "gemini").strip().lower()
    if provider == "gemini":
        from clients.gemini import GeminiClient

        client: PdfAnalyzer = GeminiClient()
    elif provider == "claude":
        from clients.claude import ClaudeClient

        client = ClaudeClient()
    else:
        raise ValueError(
            f"Unknown LLM_PROVIDER {provider!r}; expected 'gemini' or 'claude'."
        )
    print(f"LLM: {client.label}")
    return client
