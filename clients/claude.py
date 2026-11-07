"""Claude client that shells out to the local Claude Code CLI.

Uses the CLI's own login (e.g. a Claude Pro subscription), so no API key is needed.
Exposes the same ``analyze_pdf`` interface as GeminiClient.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

DEFAULT_MODEL = "claude-sonnet-5-5"
_TIMEOUT_SECONDS = 600


class ClaudeClient:
    def __init__(self, model: str | None = None) -> None:
        exe = shutil.which("claude")
        if exe is None:
            raise RuntimeError(
                "`claude` CLI not found on PATH. Install Claude Code and log in first."
            )
        self._exe = exe
        self.model = model or os.getenv("CLAUDE_MODEL") or DEFAULT_MODEL
        self.label = f"claude via Claude Code CLI ({self.model})"

    def analyze_pdf(self, prompt: str, pdf_path: Path) -> str | None:
        """Have Claude Code read *pdf_path* and follow *prompt*; return its text or ``None``."""
        pdf = pdf_path.resolve()
        request = (
            f"Use the Read tool to read the PDF at {pdf}. "
            "Then follow the instructions below. "
            "Reply with the final answer only, with no preamble or commentary.\n\n"
            f"{prompt}"
        )
        cmd = [
            self._exe,
            "-p",
            "--model", self.model,
            "--tools", "Read",
            "--allowedTools", "Read",
            "--permission-mode", "dontAsk",
            "--add-dir", str(pdf.parent),
            "--setting-sources", "",
            "--disable-slash-commands",
            "--no-session-persistence",
        ]
        try:
            result = subprocess.run(
                cmd,
                input=request,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=_TIMEOUT_SECONDS,
                cwd=tempfile.gettempdir(),
            )
        except subprocess.TimeoutExpired:
            print(f"  Claude timed out after {_TIMEOUT_SECONDS}s.")
            return None

        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()[:500]
            print(f"  Claude CLI failed (exit {result.returncode}): {detail}")
            return None
        return result.stdout.strip() or None
