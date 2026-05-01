"""StockPipeline — Stock / ETF / dividend pipeline with Google Sheets sync."""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

from clients.gemini import GeminiClient
from clients.sheets import (
    SheetsSyncWriter,
    SheetsClient,
    categorize_csv_row,
    csv_row_to_sheet_row,
    extract_sheet_id,
    make_row_key,
)
from config import ATTACHMENTS_DIR, BROKER_CONFIG, PROMPT_DIR
from models.transaction import STOCK_CSV_FIELDNAMES
from pipelines.base import BasePipeline
from utils.csv_helpers import (
    dedup_and_sort,
    normalize_rows,
    parse_csv_response,
    read_existing_csv,
    write_csv,
)
from utils.patterns import load_processed, match_pattern, save_processed

from brokers.etrade import ETradeBroker
from brokers.ibkr import IBKRBroker

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STOCK_DIR = ATTACHMENTS_DIR / "stock"


class StockPipeline(BasePipeline):
    """Fetch → decrypt → analyze → sync pipeline for stock/ETF/dividend."""

    config = BROKER_CONFIG
    raw_dir = STOCK_DIR / "raw"
    decrypted_dir = STOCK_DIR / "decrypted"
    processed_file = STOCK_DIR / ".processed.json"
    csv_output = STOCK_DIR / "transactions.csv"
    csv_fieldnames = STOCK_CSV_FIELDNAMES
    prompt_dir = PROMPT_DIR

    # ------------------------------------------------------------------
    # Stage 3
    # ------------------------------------------------------------------

    def analyze(self, *, debug: bool = False) -> None:
        """Analyze decrypted PDFs with Gemini and merge into transactions.csv."""
        prompt_map = self._build_pattern_map("prompt")
        gemini = GeminiClient()
        processed = load_processed(self.processed_file)

        decrypted = (
            sorted(
                f
                for f in self.decrypted_dir.iterdir()
                if f.name.startswith("decrypted_") and f.is_file()
            )
            if self.decrypted_dir.exists()
            else []
        )
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
                print(
                    f"  --- RAW GEMINI RESPONSE ---\n{raw}\n  --- END RAW RESPONSE ---"
                )

            rows = parse_csv_response(raw)

            if debug:
                for i, r in enumerate(rows, 1):
                    print(f"  [parsed row {i}] {dict(r)}")

            if rows:
                new_rows.extend(rows)
                print(f"  Done. ({len(rows)} rows)")
            else:
                print("  No data rows parsed.")
            processed.add(file.name)

        if not new_rows:
            print("No new results to save.")
            save_processed(processed, self.processed_file)
            return

        all_rows = read_existing_csv(self.csv_output) + new_rows
        all_rows = [r for r in all_rows if any(str(v).strip() for v in r.values())]
        all_rows = normalize_rows(all_rows, overflow_field="收入")
        unique_rows = dedup_and_sort(
            all_rows,
            self.csv_fieldnames,
            sort_key=lambda r: (
                r.get("交易日期", ""),
                r.get("代號", ""),
                r.get("買/賣/股利", ""),
            ),
        )

        write_csv(self.csv_output, unique_rows, self.csv_fieldnames)
        dupes = len(all_rows) - len(unique_rows)
        print(
            f"\nSaved {self.csv_output} ({len(unique_rows)} rows, {dupes} duplicates removed)"
        )
        save_processed(processed, self.processed_file)

    # ------------------------------------------------------------------
    # Stage 4
    # ------------------------------------------------------------------

    def sync(self) -> None:
        """Sync local transactions.csv to Google Sheets (section-aware)."""
        raw_id = os.getenv("GOOGLE_SHEET_ID")
        if not raw_id:
            print("GOOGLE_SHEET_ID not set in .env — skipping sync.")
            return

        spreadsheet_id = extract_sheet_id(raw_id)

        csv_rows = read_existing_csv(self.csv_output)
        if not csv_rows:
            print("No local CSV data to sync.")
            return
        csv_rows = [{k.strip(): v for k, v in row.items()} for row in csv_rows]

        sheets = SheetsClient()
        writer = SheetsSyncWriter(sheets, spreadsheet_id)

        # detect section boundaries
        headers = writer.find_section_headers()
        us_header = headers.get("US")
        crypto_header = headers.get("Crypto")
        if not us_header or not crypto_header:
            print("Could not find US / Crypto section headers in the sheet. Aborting.")
            return

        from clients.sheets import SHEET_NAME

        sheet_id = sheets.get_sheet_id(spreadsheet_id, SHEET_NAME)
        if sheet_id is None:
            print(f"Could not find sheet tab '{SHEET_NAME}'. Aborting.")
            return

        existing_keys = writer.read_existing_keys()
        print(f"Google Sheet has {len(existing_keys)} existing records.")

        new_by_section: dict[str, list[list[str]]] = {"TW": [], "US": [], "Crypto": []}
        for row in csv_rows:
            key = make_row_key(
                row.get("交易日期", ""),
                row.get("買/賣/股利", ""),
                row.get("代號", ""),
            )
            if key not in existing_keys and key[0] and key[2]:
                section = categorize_csv_row(row)
                new_by_section[section].append(csv_row_to_sheet_row(row))

        total_new = sum(len(v) for v in new_by_section.values())
        if total_new == 0:
            print("All CSV records already exist in Google Sheet. Nothing to sync.")
            return

        for section, rows in new_by_section.items():
            if rows:
                print(f"  {section}: {len(rows)} new row(s)")

        # Crypto
        if new_by_section["Crypto"]:
            rows = sorted(new_by_section["Crypto"], key=lambda r: r[0])
            last = writer.last_data_row(crypto_header + 1, crypto_header + 5000)
            write_at = last + 1
            writer.write_rows(write_at, rows)
            print(f"  Wrote {len(rows)} Crypto row(s) at row {write_at}")

        # US
        if new_by_section["US"]:
            rows = sorted(new_by_section["US"], key=lambda r: r[0])
            last = writer.last_data_row(us_header + 1, crypto_header - 1)
            write_at = last + 1
            inserted = writer.ensure_space(sheet_id, write_at, len(rows), crypto_header)
            if inserted:
                crypto_header += inserted
                print(
                    f"  Inserted {inserted} blank row(s) before Crypto header to make space."
                )
            writer.write_rows(write_at, rows)
            print(f"  Wrote {len(rows)} US row(s) at row {write_at}")

        # TW
        if new_by_section["TW"]:
            rows = sorted(new_by_section["TW"], key=lambda r: r[0])
            last = writer.last_data_row(3, us_header - 1)
            write_at = last + 1
            inserted = writer.ensure_space(sheet_id, write_at, len(rows), us_header)
            if inserted:
                us_header += inserted
                crypto_header += inserted
                print(
                    f"  Inserted {inserted} blank row(s) before US header to make space."
                )
            writer.write_rows(write_at, rows)
            print(f"  Wrote {len(rows)} TW row(s) at row {write_at}")

        print(f"\nSynced {total_new} new record(s) to Google Sheet.")

    # ------------------------------------------------------------------
    # Orchestrator override — include sync
    # ------------------------------------------------------------------

    def run_all(self, *, since: str | None = None, debug: bool = False) -> None:
        super().run_all(since=since, debug=debug)
        print("=== Stage 4: Syncing to Google Sheet ===")
        self.sync()

    # ------------------------------------------------------------------
    # Stage 5 — IBKR API fetch
    # ------------------------------------------------------------------

    def fetch_ibkr(self, since: int | None = None) -> None:
        """Fetch transactions from IBKR Client Portal API and merge into transactions.csv."""

        days_back = since if since is not None else 7
        since_date = date.today() - timedelta(days=days_back)

        print(f"Fetching IBKR transactions since {since_date} ...")
        broker = IBKRBroker()
        new_rows = broker.fetch_transactions(since=since_date)

        if not new_rows:
            print("No IBKR transactions returned.")
            return

        print(f"  {len(new_rows)} transaction(s) received.")
        all_rows = read_existing_csv(self.csv_output) + new_rows
        all_rows = [r for r in all_rows if any(str(v).strip() for v in r.values())]
        all_rows = normalize_rows(all_rows, overflow_field="收入")
        unique_rows = dedup_and_sort(
            all_rows,
            self.csv_fieldnames,
            sort_key=lambda r: (
                r.get("交易日期", ""),
                r.get("代號", ""),
                r.get("買/賣/股利", ""),
            ),
        )
        write_csv(self.csv_output, unique_rows, self.csv_fieldnames)
        dupes = len(all_rows) - len(unique_rows)
        print(
            f"Saved {self.csv_output} ({len(unique_rows)} rows, {dupes} dupes removed)"
        )

    # ------------------------------------------------------------------
    # Stage 6 — E*TRADE API fetch
    # ------------------------------------------------------------------

    def fetch_etrade(self, since: int | None = None) -> None:
        """Fetch transactions from E*TRADE REST API and merge into transactions.csv."""

        days_back = since if since is not None else 30
        since_date = date.today() - timedelta(days=days_back)

        print(f"Fetching E*TRADE transactions since {since_date} ...")
        broker = ETradeBroker()
        new_rows = broker.fetch_transactions(since=since_date)

        if not new_rows:
            print("No E*TRADE transactions returned.")
            return

        print(f"  {len(new_rows)} transaction(s) received.")
        all_rows = read_existing_csv(self.csv_output) + new_rows
        all_rows = [r for r in all_rows if any(str(v).strip() for v in r.values())]
        all_rows = normalize_rows(all_rows, overflow_field="收入")
        unique_rows = dedup_and_sort(
            all_rows,
            self.csv_fieldnames,
            sort_key=lambda r: (
                r.get("交易日期", ""),
                r.get("代號", ""),
                r.get("買/賣/股利", ""),
            ),
        )
        write_csv(self.csv_output, unique_rows, self.csv_fieldnames)
        dupes = len(all_rows) - len(unique_rows)
        print(
            f"Saved {self.csv_output} ({len(unique_rows)} rows, {dupes} dupes removed)"
        )
