"""BasePipeline — Template Method base class for all pipelines."""

from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path

from clients.gmail import GmailClient
from utils.patterns import match_pattern
from utils.pdf_helpers import decrypt_pdf, save_attachments


class BasePipeline(ABC):
    """Abstract base for fetch → decrypt → analyze pipelines.

    Subclasses must set class-level attributes and implement ``analyze()``.
    """

    # --- subclasses must override these ---
    config: dict[str, dict[str, str]]
    raw_dir: Path
    decrypted_dir: Path
    processed_file: Path
    csv_output: Path
    csv_fieldnames: list[str]
    prompt_dir: Path

    # ------------------------------------------------------------------
    # Concrete stages (shared by all pipelines)
    # ------------------------------------------------------------------

    def fetch(self, since: str | None = None) -> None:
        """Stage 1 — Download matching PDF attachments from Gmail."""
        if not since:
            since = (datetime.now() - timedelta(days=7)).strftime("%Y/%m/%d")

        patterns: list[str] = []
        for cfg in self.config.values():
            pattern = os.getenv(cfg["pattern_env"])
            if pattern:
                patterns.append(pattern)

        if not patterns:
            print("No broker patterns configured. Check .env file.")
            return

        gmail = GmailClient()
        attachments = gmail.fetch_attachments(query=f"has:attachment after:{since}")

        matching: list[dict] = []
        for att in attachments:
            fname = att.get("filename", "")
            if not fname.lower().endswith(".pdf"):
                continue
            subject = att.get("subject", "")
            matched_by_fname = any(re.search(p, fname) for p in patterns)
            matched_by_subject = not matched_by_fname and any(
                re.search(p, subject) for p in patterns
            )
            if matched_by_fname or matched_by_subject:
                if matched_by_subject:
                    safe_subject = re.sub(r'[\\/*?:"<>|]', '_', subject).strip()
                    att["filename"] = f"{safe_subject}.pdf"
                matching.append(att)

        if matching:
            save_attachments(matching, self.raw_dir)
            print(f"Saved {len(matching)} attachments to {self.raw_dir}")
        else:
            print("No matching statements found.")

    def decrypt(self) -> None:
        """Stage 2 — Decrypt each PDF using config-driven passwords."""
        password_map = self._build_pattern_map("password_env")
        if not self.raw_dir.exists():
            print(f"No raw PDFs found in {self.raw_dir}")
            return
        for file in self.raw_dir.iterdir():
            if not file.is_file():
                continue
            password = match_pattern(file.name, password_map)
            if password:
                decrypt_pdf(file, password, self.decrypted_dir)

    @abstractmethod
    def analyze(self, *, debug: bool = False) -> None:
        """Stage 3 — Analyze decrypted PDFs with Gemini (subclass-specific)."""

    def run_all(self, *, since: str | None = None, debug: bool = False) -> None:
        """Run all stages in sequence."""
        print(f"=== Stage 1: Fetching attachments ===")
        self.fetch(since=since)
        print(f"=== Stage 2: Decrypting PDFs ===")
        self.decrypt()
        print(f"=== Stage 3: Analyzing with Gemini ===")
        self.analyze(debug=debug)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _build_pattern_map(self, value_key: str) -> dict[str, object]:
        """Build a {filename_regex: value} mapping from config."""
        result: dict[str, object] = {}
        for name, cfg in self.config.items():
            pattern = os.getenv(cfg["pattern_env"])
            if not pattern:
                continue
            if value_key == "password_env":
                value = os.getenv(cfg[value_key])
                if not value:
                    raise ValueError(
                        f"Broker {name}: pattern matched but {cfg[value_key]} is not set"
                    )
            else:
                value = self.prompt_dir / cfg[value_key]
            result[pattern] = value
        return result
