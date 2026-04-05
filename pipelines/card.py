"""CardPipeline — Credit-card statement pipeline."""

from __future__ import annotations

import re
from pathlib import Path

from clients.gemini import GeminiClient
from config import (
    ATTACHMENTS_DIR,
    BANK_ORDER,
    CARD_CONFIG,
    PROMPT_DIR,
    SUMMARY_CSV_FIELDNAMES,
)
from models.transaction import CARD_CSV_FIELDNAMES
from pipelines.base import BasePipeline
from utils.csv_helpers import (
    dedup_and_sort,
    normalize_rows,
    parse_csv_response,
    read_existing_csv,
    write_csv,
)
from utils.patterns import load_processed, match_pattern, save_processed

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CARD_DIR = ATTACHMENTS_DIR / "card"
CARD_PROMPT_TEMPLATE = PROMPT_DIR / "Credit_Card.md"


class CardPipeline(BasePipeline):
    """Fetch → decrypt → analyze pipeline for credit-card statements."""

    config = CARD_CONFIG
    raw_dir = CARD_DIR / "raw"
    decrypted_dir = CARD_DIR / "decrypted"
    processed_file = CARD_DIR / ".processed.json"
    csv_output = CARD_DIR / "credit_card_all.csv"
    csv_fieldnames = CARD_CSV_FIELDNAMES
    prompt_dir = PROMPT_DIR

    # ------------------------------------------------------------------
    # Stage 3
    # ------------------------------------------------------------------

    def analyze(self, *, debug: bool = False) -> None:
        """Analyze credit-card PDFs with Gemini, producing transactions + summary."""
        bank_map = self._build_bank_map()
        template = CARD_PROMPT_TEMPLATE.read_text(encoding="utf-8")
        gemini = GeminiClient()
        processed = load_processed(self.processed_file)

        decrypted = sorted(
            f
            for f in self.decrypted_dir.iterdir()
            if f.name.startswith("decrypted_") and f.is_file()
        ) if self.decrypted_dir.exists() else []
        new_files = [
            f
            for f in decrypted
            if f.name not in processed and match_pattern(f.name, bank_map)
        ]

        if not new_files:
            print("All card files already processed. Nothing new to analyze.")
            return

        print(f"{len(new_files)} new card file(s) to process")

        new_rows: list[dict] = []
        bank_amounts: dict[str, str] = {}
        for idx, file in enumerate(new_files, start=1):
            print(f"[{idx}/{len(new_files)}] Processing: {file.name}")
            bank_name = match_pattern(file.name, bank_map)
            if not bank_name:
                continue
            prompt = template.replace("{BANK_NAME}", bank_name)

            raw = gemini.analyze_pdf(prompt, file)
            if not raw:
                print("  No response from Gemini.")
                continue

            if debug:
                print(f"  --- RAW GEMINI RESPONSE ---\n{raw}\n  --- END RAW RESPONSE ---")

            amount_due = _parse_amount_due(raw)
            if amount_due:
                bank_amounts[bank_name] = amount_due
                print(f"  應繳金額: {amount_due}")

            csv_text = raw.strip()
            if amount_due:
                csv_text = csv_text.split("\n", 1)[1] if "\n" in csv_text else ""

            rows = parse_csv_response(csv_text)

            if debug:
                for i, r in enumerate(rows, 1):
                    print(f"  [parsed row {i}] {dict(r)}")

            if rows:
                new_rows.extend(rows)
                processed.add(file.name)
                print(f"  Done. ({len(rows)} rows)")
            else:
                print("  No data rows parsed.")

        if not new_rows:
            print("No new card results to save.")
            return

        # --- credit_card_all.csv ---
        all_rows = read_existing_csv(self.csv_output) + new_rows
        all_rows = [r for r in all_rows if any(str(v).strip() for v in r.values())]
        all_rows = normalize_rows(all_rows)
        unique_rows = dedup_and_sort(
            all_rows,
            self.csv_fieldnames,
            sort_key=lambda r: (
                r.get("交易日期", ""),
                r.get("卡別", ""),
                r.get("商店名稱", ""),
            ),
        )

        write_csv(self.csv_output, unique_rows, self.csv_fieldnames)
        dupes = len(all_rows) - len(unique_rows)
        print(f"\nSaved {self.csv_output} ({len(unique_rows)} rows, {dupes} dupes removed)")

        # --- monthly_summary.csv ---
        if bank_amounts:
            summary_path = CARD_DIR / "monthly_summary.csv"
            existing_summary: dict[str, str] = {}
            if summary_path.exists():
                for row in read_existing_csv(summary_path):
                    bank = row.get("卡別", "").strip()
                    if bank:
                        existing_summary[bank] = row.get("應繳金額", "")
            existing_summary.update(bank_amounts)

            summary_rows = [
                {"卡別": bank, "應繳金額": existing_summary[bank]}
                for bank in BANK_ORDER
                if bank in existing_summary
            ]
            write_csv(summary_path, summary_rows, SUMMARY_CSV_FIELDNAMES)
            print(f"Saved {summary_path} ({len(summary_rows)} banks)")

        save_processed(processed, self.processed_file)

    # ------------------------------------------------------------------
    # Card-specific helpers
    # ------------------------------------------------------------------

    def _build_bank_map(self) -> dict[str, str]:
        """Build a {filename_regex: bank_name} mapping from CARD_CONFIG."""
        import os
        result: dict[str, str] = {}
        for cfg in self.config.values():
            pattern = os.getenv(cfg["pattern_env"])
            if pattern:
                result[pattern] = cfg["bank_name"]
        return result


def _parse_amount_due(text: str) -> str | None:
    """Extract 應繳金額 value from the first line of Gemini's response."""
    first_line = text.strip().split("\n", 1)[0]
    m = re.match(r"應繳金額[:：]\s*(.+)", first_line)
    if m:
        return m.group(1).strip()
    return None
