"""Stock / ETF / dividend pipeline stages."""

from __future__ import annotations

import os
import re
from pathlib import Path

from google_file_sync import (
    ATTACHMENTS_DIR,
    PROMPT_DIR,
    GeminiClient,
    SheetsClient,
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

STOCK_DIR = ATTACHMENTS_DIR / "stock"
RAW_DIR = STOCK_DIR / "raw"
DECRYPTED_DIR = STOCK_DIR / "decrypted"
PROCESSED_FILE = STOCK_DIR / ".processed.json"
CSV_OUTPUT = STOCK_DIR / "transactions.csv"

CSV_FIELDNAMES = [
    "交易日期",
    "買/賣/股利",
    "代號",
    "股票",
    "交易類別",
    "買入股數",
    "買入價格",
    "賣出股數",
    "賣出價格",
    "手續費",
    "收入",
]

# pattern_env: env var holding the filename regex to match broker PDFs
# password_env: env var holding the PDF decryption password
# prompt: filename inside PROMPT_DIR for the Gemini prompt
BROKER_CONFIG: dict[str, dict[str, str]] = {
    "CATHAY_US": {
        "pattern_env": "CATHAY_US",
        "password_env": "PDF_PASSWORD",
        "prompt": "Cathay_US.md",
    },
    "CATHAY_TW": {
        "pattern_env": "CATHAY_TW",
        "password_env": "PDF_PASSWORD",
        "prompt": "Cathay_TW.md",
    },
    "FUBON_US": {
        "pattern_env": "FUBON_US",
        "password_env": "FUBON_PDF_PASSWORD",
        "prompt": "Fubon_US.md",
    },
    "TW_dividend": {
        "pattern_env": "TW_DIVIDEND",
        "password_env": "PDF_PASSWORD",
        "prompt": "TW_Dividend.md",
    },
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_pattern_map(value_key: str) -> dict[str, object]:
    """Build a {filename_regex: value} mapping from BROKER_CONFIG."""
    result: dict[str, object] = {}
    for cfg in BROKER_CONFIG.values():
        pattern = os.getenv(cfg["pattern_env"])
        if not pattern:
            continue
        result[pattern] = (
            os.getenv(cfg[value_key])
            if value_key == "password_env"
            else PROMPT_DIR / cfg[value_key]
        )
    return result


def _normalize_rows(rows: list[dict]) -> list[dict]:
    """
    Sanitize rows produced by ``csv.DictReader``:

    - Gemini occasionally outputs an extra trailing comma, causing ``DictReader``
      to place the overflow value under a ``None`` key as a list.  If ``收入`` is
      empty, rescue that value into ``收入``.
    - Delete all remaining ``None`` keys.
    - Replace any remaining ``None`` values with empty strings.
    """
    for row in rows:
        for key in [k for k in row if k is None]:
            raw = row.pop(key)
            val = str(raw[0] if isinstance(raw, list) else raw or "").strip()
            if val:
                cur_income = row.get("收入", "").strip()
                if cur_income and not row.get("手續費", "").strip():
                    row["手續費"] = cur_income
                row["收入"] = val
        for key in row:
            if row[key] is None:
                row[key] = ""
    return rows


def _dedup_and_sort(rows: list[dict]) -> list[dict]:
    """Remove exact duplicates and sort by date -> ticker -> trade type."""
    seen: set[tuple] = set()
    unique: list[dict] = []
    for row in rows:
        key = tuple(row.get(f, "") for f in CSV_FIELDNAMES)
        if key not in seen:
            seen.add(key)
            unique.append(row)
    unique.sort(
        key=lambda r: (
            r.get("交易日期", ""),
            r.get("代號", ""),
            r.get("買/賣/股利", ""),
        )
    )
    return unique


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------


def stock_fetch_stage(since: str | None = None) -> None:
    """Stage 1 — Download matching broker PDF attachments from Gmail."""
    fetch_attachments_stage(config=BROKER_CONFIG, raw_dir=RAW_DIR, since=since)


def stock_decrypt_stage() -> None:
    """Stage 2 — Decrypt each unprocessed PDF using broker passwords."""
    password_map = _build_pattern_map("password_env")
    if not RAW_DIR.exists():
        print(f"No raw PDFs found in {RAW_DIR}")
        return
    for file in RAW_DIR.iterdir():
        if not file.is_file():
            continue
        password = match_pattern(file.name, password_map)
        if password:
            decrypt_pdf(file, password, DECRYPTED_DIR)


def stock_analyze_stage(*, debug: bool = False) -> None:
    """Stage 3 — Analyze decrypted PDFs with Gemini and merge results into transactions.csv."""
    prompt_map = _build_pattern_map("prompt")
    gemini = GeminiClient()
    processed = load_processed(PROCESSED_FILE)

    decrypted = sorted(
        f
        for f in DECRYPTED_DIR.iterdir()
        if f.name.startswith("decrypted_") and f.is_file()
    ) if DECRYPTED_DIR.exists() else []
    new_files = [f for f in decrypted if f.name not in processed]

    if not new_files:
        print("All files already processed. Nothing new to analyze.")
        return

    skipped = len(decrypted) - len(new_files)
    print(
        f"{len(new_files)} new file(s) to process (skipping {skipped} already processed)"
    )

    new_rows: list[dict] = []
    for idx, file in enumerate(new_files, start=1):
        print(f"[{idx}/{len(new_files)}] Processing: {file.name}")
        prompt_path = match_pattern(file.name, prompt_map)
        if not prompt_path:
            print(f"  No matching prompt for {file.name}, skipping.")
            continue

        raw = gemini.analyze_pdf(prompt_path.read_text(encoding="utf-8"), file)
        if not raw:
            print("  No response from Gemini.")
            continue

        if debug:
            print(f"  --- RAW GEMINI RESPONSE ---\n{raw}\n  --- END RAW RESPONSE ---")

        rows = parse_csv_response(raw)

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
        print("No new results to save.")
        return

    all_rows = read_existing_csv(CSV_OUTPUT) + new_rows
    all_rows = [r for r in all_rows if any(str(v).strip() for v in r.values())]
    all_rows = _normalize_rows(all_rows)
    unique_rows = _dedup_and_sort(all_rows)

    write_csv(CSV_OUTPUT, unique_rows, CSV_FIELDNAMES)
    dupes = len(all_rows) - len(unique_rows)
    print(f"\nSaved {CSV_OUTPUT} ({len(unique_rows)} rows, {dupes} duplicates removed)")
    save_processed(processed, PROCESSED_FILE)


# ---------------------------------------------------------------------------
# Stage 4 — Sync to Google Sheets
# ---------------------------------------------------------------------------

# Column mapping: CSV field → Google Sheet column letter
_SHEET_COL_MAP = {
    "交易日期": "A",
    "買/賣/股利": "B",
    "代號": "C",
    "股票": "D",
    "交易類別": "E",
    "買入股數": "F",
    "買入價格": "G",
    "賣出股數": "H",
    "賣出價格": "I",
    # J = 現價 (auto / leave blank)
    "手續費": "K",
    # L~P = auto-calculated in sheet
    "收入": "Q",
}

_COL_INDEX = {
    "A": 0, "B": 1, "C": 2, "D": 3, "E": 4,
    "F": 5, "G": 6, "H": 7, "I": 8, "K": 10, "Q": 16,
}

SHEET_NAME = "交易紀錄"

# Section header labels in column A (used to detect boundaries)
_SECTION_HEADERS = {"US", "Crypto"}


def _normalize_date(date_str: str) -> str:
    """Strip leading zeros: ``2026/01/05`` → ``2026/1/5``."""
    s = date_str.strip().replace("-", "/")
    parts = s.split("/")
    if len(parts) == 3:
        try:
            return f"{int(parts[0])}/{int(parts[1])}/{int(parts[2])}"
        except ValueError:
            pass
    return s


def _normalize_stock_name(name: str) -> str:
    """``BTCUSDT`` → ``BTCUSD`` to match existing sheet convention."""
    if name.endswith("USDT"):
        return name[:-1]
    return name


def _make_row_key(date: str, action: str, code: str) -> tuple[str, str, str]:
    """Normalize a (date, action, code) triple for dedup comparison."""
    return (_normalize_date(date), action.strip(), code.strip())


def _read_sheet_keys(sheets: SheetsClient, spreadsheet_id: str) -> set[tuple]:
    """Read existing (date, action, code) keys from the Google Sheet."""
    rows = sheets.read_rows(spreadsheet_id, f"{SHEET_NAME}!A3:C")
    keys: set[tuple] = set()
    for row in rows:
        if len(row) < 3:
            continue
        date_str = row[0].strip()
        if not date_str or not re.match(r"\d{4}", date_str):
            continue
        keys.add(_make_row_key(date_str, row[1], row[2]))
    return keys


def _categorize_csv_row(row: dict) -> str:
    """Return ``'TW'``, ``'US'``, or ``'Crypto'`` based on CSV category."""
    cat = row.get("交易類別", "").strip()
    if cat == "Crypto":
        return "Crypto"
    if cat == "美股":
        return "US"
    return "TW"


def _csv_row_to_sheet_row(row: dict) -> list[str]:
    """Convert a CSV dict row to a list of 17 values (columns A–Q)."""
    out = [""] * 17
    for field, col_letter in _SHEET_COL_MAP.items():
        val = row.get(field, "").strip()
        out[_COL_INDEX[col_letter]] = val
    # Normalize date (strip leading zeros)
    out[0] = _normalize_date(out[0])
    # Normalize Crypto stock names (BTCUSDT → BTCUSD)
    out[3] = _normalize_stock_name(out[3])
    return out


def _extract_sheet_id(raw: str) -> str:
    """Extract the spreadsheet ID from a full URL or return as-is."""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", raw)
    return m.group(1) if m else raw.strip()


def _find_section_headers(
    sheets: SheetsClient, spreadsheet_id: str
) -> dict[str, int]:
    """Return ``{'US': row, 'Crypto': row}`` (1-based) by scanning column A."""
    col_a = sheets.read_rows(spreadsheet_id, f"{SHEET_NAME}!A1:A2000")
    headers: dict[str, int] = {}
    for i, row in enumerate(col_a, 1):
        if row and row[0].strip() in _SECTION_HEADERS:
            headers[row[0].strip()] = i
    return headers


def _last_data_row(
    sheets: SheetsClient, spreadsheet_id: str, start: int, end: int
) -> int:
    """Return the last row (1-based) in [start, end] where columns A–C all have data."""
    rows = sheets.read_rows(
        spreadsheet_id, f"{SHEET_NAME}!A{start}:C{end}"
    )
    last = start - 1
    for i, row in enumerate(rows, start):
        if (
            len(row) >= 3
            and row[0].strip()
            and re.match(r"\d{4}", row[0])
            and row[2].strip()
        ):
            last = i
    return last


def _write_at(
    sheets: SheetsClient,
    spreadsheet_id: str,
    start_row: int,
    rows: list[list[str]],
) -> None:
    """Write *rows* starting at *start_row* (1-based), overwriting empty cells."""
    end_row = start_row + len(rows) - 1
    sheets.update_range(
        spreadsheet_id, f"{SHEET_NAME}!A{start_row}:Q{end_row}", rows
    )


def _ensure_space(
    sheets: SheetsClient,
    spreadsheet_id: str,
    sheet_id: int,
    write_at: int,
    count: int,
    boundary: int,
) -> int:
    """Insert blank rows before *boundary* if writing would overflow into the next section.

    Returns the number of rows inserted (0 if no insertion needed).
    """
    write_end = write_at + count - 1
    if write_end >= boundary:
        needed = write_end - boundary + 1
        # insert_rows_at uses 0-based index
        sheets.insert_rows_at(spreadsheet_id, sheet_id, boundary - 1, needed)
        return needed
    return 0


def stock_sync_stage() -> None:
    """Stage 4 — Sync local transactions.csv to Google Sheets (section-aware)."""
    raw_id = os.getenv("GOOGLE_SHEET_ID")
    if not raw_id:
        print("GOOGLE_SHEET_ID not set in .env — skipping sync.")
        return

    spreadsheet_id = _extract_sheet_id(raw_id)

    csv_rows = read_existing_csv(CSV_OUTPUT)
    if not csv_rows:
        print("No local CSV data to sync.")
        return
    # Normalize keys — CSV headers may contain padding spaces
    csv_rows = [{k.strip(): v for k, v in row.items()} for row in csv_rows]

    sheets = SheetsClient()

    # --- detect section boundaries ---
    headers = _find_section_headers(sheets, spreadsheet_id)
    us_header = headers.get("US")
    crypto_header = headers.get("Crypto")
    if not us_header or not crypto_header:
        print("Could not find US / Crypto section headers in the sheet. Aborting.")
        return

    # --- resolve numeric sheet ID (needed for row insertion) ---
    sheet_id = sheets.get_sheet_id(spreadsheet_id, SHEET_NAME)
    if sheet_id is None:
        print(f"Could not find sheet tab '{SHEET_NAME}'. Aborting.")
        return

    # --- dedup against existing sheet data ---
    existing_keys = _read_sheet_keys(sheets, spreadsheet_id)
    print(f"Google Sheet has {len(existing_keys)} existing records.")

    # --- bucket new rows by section ---
    new_by_section: dict[str, list[list[str]]] = {"TW": [], "US": [], "Crypto": []}
    for row in csv_rows:
        key = _make_row_key(
            row.get("交易日期", ""),
            row.get("買/賣/股利", ""),
            row.get("代號", ""),
        )
        if key not in existing_keys and key[0] and key[2]:
            section = _categorize_csv_row(row)
            new_by_section[section].append(_csv_row_to_sheet_row(row))

    total_new = sum(len(v) for v in new_by_section.values())
    if total_new == 0:
        print("All CSV records already exist in Google Sheet. Nothing to sync.")
        return

    for section, rows in new_by_section.items():
        if rows:
            print(f"  {section}: {len(rows)} new row(s)")

    # Write into the next empty row after existing data in each section.
    # Process order doesn't matter since we're not shifting rows.

    # 1. Crypto — write after last Crypto data row
    if new_by_section["Crypto"]:
        rows = sorted(new_by_section["Crypto"], key=lambda r: r[0])
        last = _last_data_row(sheets, spreadsheet_id, crypto_header + 1, crypto_header + 5000)
        write_at = last + 1
        _write_at(sheets, spreadsheet_id, write_at, rows)
        print(f"  Wrote {len(rows)} Crypto row(s) at row {write_at}")

    # 2. US — write after last US data row
    if new_by_section["US"]:
        rows = sorted(new_by_section["US"], key=lambda r: r[0])
        last = _last_data_row(sheets, spreadsheet_id, us_header + 1, crypto_header - 1)
        write_at = last + 1
        inserted = _ensure_space(sheets, spreadsheet_id, sheet_id, write_at, len(rows), crypto_header)
        if inserted:
            crypto_header += inserted
            print(f"  Inserted {inserted} blank row(s) before Crypto header to make space.")
        _write_at(sheets, spreadsheet_id, write_at, rows)
        print(f"  Wrote {len(rows)} US row(s) at row {write_at}")

    # 3. TW — write after last TW data row
    if new_by_section["TW"]:
        rows = sorted(new_by_section["TW"], key=lambda r: r[0])
        last = _last_data_row(sheets, spreadsheet_id, 3, us_header - 1)
        write_at = last + 1
        inserted = _ensure_space(sheets, spreadsheet_id, sheet_id, write_at, len(rows), us_header)
        if inserted:
            us_header += inserted
            crypto_header += inserted
            print(f"  Inserted {inserted} blank row(s) before US header to make space.")
        _write_at(sheets, spreadsheet_id, write_at, rows)
        print(f"  Wrote {len(rows)} TW row(s) at row {write_at}")

    print(f"\nSynced {total_new} new record(s) to Google Sheet.")
