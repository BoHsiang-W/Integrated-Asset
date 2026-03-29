"""Credit-card statement pipeline stages."""

from __future__ import annotations

import os
import re
from pathlib import Path

from google_file_sync import (
    ATTACHMENTS_DIR,
    PROMPT_DIR,
    GeminiClient,
    decrypt_pdf,
    fetch_attachments_stage,
    load_processed,
    match_pattern,
    parse_csv_response,
    read_existing_csv,
    save_processed,
    write_csv,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CARD_DIR = ATTACHMENTS_DIR / "card"
RAW_DIR = CARD_DIR / "raw"
DECRYPTED_DIR = CARD_DIR / "decrypted"
CARD_PROCESSED_FILE = CARD_DIR / ".processed.json"
CARD_CSV_ALL = CARD_DIR / "credit_card_all.csv"
CARD_PROMPT_TEMPLATE = PROMPT_DIR / "Credit_Card.md"

CARD_CSV_FIELDNAMES = [
    "交易日期",
    "入帳日期",
    "卡別",
    "商店名稱",
    "金額",
    "幣別",
    "類別",
]

SUMMARY_CSV_FIELDNAMES = ["卡別", "應繳金額"]

BANK_ORDER = ["國泰", "富邦", "台新", "兆豐", "永豐", "樂天", "星展", "Line Bank", "上海", "聯邦"]

# pattern_env: env var holding the filename regex
# password_env: env var holding the PDF decryption password
# bank_name: display name injected into the shared prompt template
CARD_CONFIG: dict[str, dict[str, str]] = {
    "CATHAY_CARD":  {"pattern_env": "CATHAY_CARD",  "password_env": "CATHAY_CARD_PASSWORD",  "bank_name": "國泰"},
    "FUBON_CARD":   {"pattern_env": "FUBON_CARD",   "password_env": "FUBON_CARD_PASSWORD",   "bank_name": "富邦"},
    "TAISHIN_CARD": {"pattern_env": "TAISHIN_CARD", "password_env": "TAISHIN_CARD_PASSWORD", "bank_name": "台新"},
    "MEGA_CARD":    {"pattern_env": "MEGA_CARD",    "password_env": "MEGA_CARD_PASSWORD",    "bank_name": "兆豐"},
    "SINOPAC_CARD": {"pattern_env": "SINOPAC_CARD", "password_env": "SINOPAC_CARD_PASSWORD", "bank_name": "永豐"},
    "RAKUTEN_CARD": {"pattern_env": "RAKUTEN_CARD", "password_env": "RAKUTEN_CARD_PASSWORD", "bank_name": "樂天"},
    "DBS_CARD":     {"pattern_env": "DBS_CARD",     "password_env": "DBS_CARD_PASSWORD",     "bank_name": "星展"},
    "LINEBANK_CARD":{"pattern_env": "LINEBANK_CARD","password_env": "LINEBANK_CARD_PASSWORD","bank_name": "Line Bank"},
    "SCSB_CARD":    {"pattern_env": "SCSB_CARD",    "password_env": "SCSB_CARD_PASSWORD",    "bank_name": "上海"},
    "UBOT_CARD":    {"pattern_env": "UBOT_CARD",    "password_env": "UBOT_CARD_PASSWORD",    "bank_name": "聯邦"},
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_password_map() -> dict[str, str | None]:
    """Build a {filename_regex: password} mapping from CARD_CONFIG."""
    result: dict[str, str | None] = {}
    for cfg in CARD_CONFIG.values():
        pattern = os.getenv(cfg["pattern_env"])
        if pattern:
            result[pattern] = os.getenv(cfg["password_env"])
    return result


def _build_bank_map() -> dict[str, str]:
    """Build a {filename_regex: bank_name} mapping from CARD_CONFIG."""
    result: dict[str, str] = {}
    for cfg in CARD_CONFIG.values():
        pattern = os.getenv(cfg["pattern_env"])
        if pattern:
            result[pattern] = cfg["bank_name"]
    return result


def _normalize_card_rows(rows: list[dict]) -> list[dict]:
    """Sanitize card CSV rows: remove None keys and replace None values."""
    for row in rows:
        for key in [k for k in row if k is None]:
            row.pop(key)
        for key in row:
            if row[key] is None:
                row[key] = ""
    return rows


def _dedup_and_sort_cards(rows: list[dict]) -> list[dict]:
    """Remove exact duplicates and sort card rows by date -> merchant."""
    seen: set[tuple] = set()
    unique: list[dict] = []
    for row in rows:
        key = tuple(row.get(f, "") for f in CARD_CSV_FIELDNAMES)
        if key not in seen:
            seen.add(key)
            unique.append(row)
    unique.sort(
        key=lambda r: (
            r.get("交易日期", ""),
            r.get("卡別", ""),
            r.get("商店名稱", ""),
        )
    )
    return unique


def _parse_amount_due(text: str) -> str | None:
    """Extract 應繳金額 value from the first line of Gemini's response."""
    first_line = text.strip().split("\n", 1)[0]
    m = re.match(r"應繳金額[:：]\s*(.+)", first_line)
    if m:
        return m.group(1).strip()
    return None


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------


def card_fetch_stage(since: str | None = None) -> None:
    """Stage 1 — Download matching credit-card statement PDFs from Gmail."""
    fetch_attachments_stage(config=CARD_CONFIG, raw_dir=RAW_DIR, since=since)


def card_decrypt_stage() -> None:
    """Stage 2 — Decrypt credit-card PDFs using card-issuer-specific passwords."""
    password_map = _build_password_map()
    if not RAW_DIR.exists():
        print(f"No raw PDFs found in {RAW_DIR}")
        return
    for file in RAW_DIR.iterdir():
        if not file.is_file():
            continue
        password = match_pattern(file.name, password_map)
        if password:
            decrypt_pdf(file, password, DECRYPTED_DIR)


def card_analyze_stage(*, debug: bool = False) -> None:
    """Stage 3 — Analyze credit-card PDFs with Gemini and merge results into credit_card.csv."""
    bank_map = _build_bank_map()
    template = CARD_PROMPT_TEMPLATE.read_text(encoding="utf-8")
    gemini = GeminiClient()
    processed = load_processed(CARD_PROCESSED_FILE)

    decrypted = sorted(
        f
        for f in DECRYPTED_DIR.iterdir()
        if f.name.startswith("decrypted_") and f.is_file()
    ) if DECRYPTED_DIR.exists() else []
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
    bank_amounts: dict[str, str] = {}  # bank_name -> 應繳金額
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

        # Extract 應繳金額 from first line
        amount_due = _parse_amount_due(raw)
        if amount_due:
            bank_amounts[bank_name] = amount_due
            print(f"  應繳金額: {amount_due}")

        # Strip the 應繳金額 line so it doesn't pollute CSV parsing
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

    # --- 1. Write credit_card_all.csv (all detailed transactions) ---
    all_rows = read_existing_csv(CARD_CSV_ALL) + new_rows
    all_rows = [r for r in all_rows if any(str(v).strip() for v in r.values())]
    all_rows = _normalize_card_rows(all_rows)
    unique_rows = _dedup_and_sort_cards(all_rows)

    write_csv(CARD_CSV_ALL, unique_rows, CARD_CSV_FIELDNAMES)
    dupes = len(all_rows) - len(unique_rows)
    print(f"\nSaved {CARD_CSV_ALL} ({len(unique_rows)} rows, {dupes} dupes removed)")

    # --- 2. Write monthly summary from 應繳金額 extracted by Gemini ---
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

    save_processed(processed, CARD_PROCESSED_FILE)
