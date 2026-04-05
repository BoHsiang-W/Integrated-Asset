"""
google_file_sync.py — Shared infrastructure & CLI entry point.

Shared:
  Gmail client, Gemini client, PDF helpers, CSV helpers,
  pattern-matching, processed-file tracking, fetch/decrypt primitives.

Pipelines (in separate modules):
  stock_pipeline.py — Stock / ETF / dividend stages
  card_pipeline.py  — Credit-card statement stages

Directory layout:
  attachments/stock/raw/        — downloaded stock PDFs
  attachments/stock/decrypted/  — decrypted stock PDFs
  attachments/stock/*.csv       — stock analysis output
  attachments/card/raw/         — downloaded card PDFs
  attachments/card/decrypted/   — decrypted card PDFs
  attachments/card/*.csv        — card analysis output

Usage:
  python google_file_sync.py              # run all stages (stock only, last 7 days)
  python google_file_sync.py --analyze    # only Stage 3
  python google_file_sync.py --since 2026/01/01  # fetch emails after a specific date
  python google_file_sync.py --card       # credit card pipeline (fetch+decrypt+analyze)
  python google_file_sync.py --card --analyze    # card analyze only (skip fetch/decrypt)
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import time
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google import genai
from google.auth.transport.requests import Request
from google.genai import types
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from PyPDF2 import PdfReader, PdfWriter
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ATTACHMENTS_DIR = Path("attachments")
PROMPT_DIR = Path("prompt")

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]
TOKEN_FILE = "token.json"
CREDENTIALS_FILE = "credentials.json"


# ---------------------------------------------------------------------------
# Gmail
# ---------------------------------------------------------------------------


class GmailClient:
    """Authenticated Gmail API client."""

    def __init__(self) -> None:
        self._service = _build_gmail_service()

    def fetch_attachments(
        self, query: str = "has:attachment"
    ) -> list[dict]:
        """Return all attachment metadata matching *query* from the inbox."""
        try:
            result = (
                self._service.users().messages().list(userId="me", q=query).execute()
            )
        except HttpError as exc:
            print(f"Gmail API error: {exc}")
            return []

        messages = result.get("messages", [])
        if not messages:
            print("No messages found.")
            return []

        attachments: list[dict] = []
        for message in tqdm(messages, desc="Fetching messages"):
            msg = (
                self._service.users()
                .messages()
                .get(userId="me", id=message["id"], format="full")
                .execute()
            )
            _extract_attachment_parts(
                msg.get("payload", {}).get("parts", []),
                self._service,
                msg,
                attachments,
            )

        print(f"Found {len(attachments)} attachments.")
        return attachments


def _get_credentials() -> Credentials:
    """Return valid OAuth2 credentials, refreshing or re-authorizing as needed."""
    creds: Credentials | None = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as fh:
            fh.write(creds.to_json())

    return creds


def _build_gmail_service():
    """Authenticate via OAuth2 and return a Gmail API service object."""
    return build("gmail", "v1", credentials=_get_credentials())


def _extract_attachment_parts(
    parts: list[dict],
    service: Any,
    message: dict,
    output: list[dict],
) -> None:
    """Recursively collect file attachments from message *parts* into *output*."""
    for part in parts:
        attachment_id = part.get("body", {}).get("attachmentId")
        if part.get("filename") and attachment_id:
            raw = (
                service.users()
                .messages()
                .attachments()
                .get(userId="me", messageId=message["id"], id=attachment_id)
                .execute()
            )
            subject = ""
            for header in message.get("payload", {}).get("headers", []):
                if header["name"].lower() == "subject":
                    subject = header["value"]
                    break
            output.append(
                {
                    "filename": part["filename"],
                    "subject": subject,
                    "data": raw.get("data"),
                    "mimeType": part.get("mimeType"),
                    "date": datetime.fromtimestamp(
                        int(message["internalDate"]) / 1000
                    ).strftime("%Y-%m-%d"),
                }
            )
        if "parts" in part:
            _extract_attachment_parts(part["parts"], service, message, output)


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------


class SheetsClient:
    """Authenticated Google Sheets API client."""

    def __init__(self) -> None:
        self._service = build("sheets", "v4", credentials=_get_credentials())

    def read_rows(
        self, spreadsheet_id: str, range_: str
    ) -> list[list[str]]:
        """Read values from a sheet range. Returns a list of rows."""
        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_)
            .execute()
        )
        return result.get("values", [])

    def get_sheet_id(self, spreadsheet_id: str, sheet_name: str) -> int | None:
        """Get the numeric sheet ID for a named sheet tab."""
        meta = (
            self._service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="sheets.properties")
            .execute()
        )
        for sheet in meta.get("sheets", []):
            props = sheet.get("properties", {})
            if props.get("title") == sheet_name:
                return props.get("sheetId")
        return None

    def insert_rows_at(
        self, spreadsheet_id: str, sheet_id: int, row_index: int, count: int
    ) -> None:
        """Insert *count* blank rows at *row_index* (0-based)."""
        body = {
            "requests": [
                {
                    "insertDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": row_index,
                            "endIndex": row_index + count,
                        },
                        "inheritFromBefore": True,
                    }
                }
            ]
        }
        self._service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()

    def update_range(
        self, spreadsheet_id: str, range_: str, rows: list[list]
    ) -> None:
        """Write *rows* to a specific range (overwrite existing values)."""
        body = {"values": rows}
        self._service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_,
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()

    def batch_update_ranges(
        self, spreadsheet_id: str, data: list[dict]
    ) -> None:
        """Write multiple ranges in a single API call.

        *data* is a list of ``{"range": "Sheet!A1:C3", "values": [[...], ...]}``.
        """
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": data,
        }
        self._service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# PDF helpers
# ---------------------------------------------------------------------------


def save_attachments(attachments: list[dict], folder: Path) -> None:
    """Decode and write *attachments* to *folder*, each prefixed with its date."""
    folder.mkdir(parents=True, exist_ok=True)
    for att in attachments:
        dest = folder / f"{att['date']}_{att['filename']}"
        dest.write_bytes(base64.urlsafe_b64decode(att["data"].encode("UTF-8")))


def decrypt_pdf(source: Path, password: str, dest_dir: Path) -> None:
    """Decrypt *source* with *password* and write ``decrypted_<name>`` into *dest_dir*."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"decrypted_{source.name}"
    try:
        reader = PdfReader(source)
        if reader.is_encrypted:
            reader.decrypt(password)
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)
        with open(dest, "wb") as fh:
            writer.write(fh)
    except Exception as exc:
        print(f"  Failed to decrypt {source.name}: {exc}")


# ---------------------------------------------------------------------------
# CSV / row helpers
# ---------------------------------------------------------------------------


def parse_csv_response(text: str) -> list[dict]:
    """Parse a (possibly markdown-fenced) CSV string from Gemini into row dicts."""
    cleaned = re.sub(r"```(?:csv)?\n?", "", text).strip()
    reader = csv.DictReader(StringIO(cleaned))
    return [row for row in reader if any(v and v.strip() for v in row.values())]


def read_existing_csv(path: Path) -> list[dict]:
    """Return rows from an existing CSV file, or an empty list if it does not exist."""
    if not path.exists() or path.stat().st_size == 0:
        return []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    """Write *rows* to *path* as UTF-8-BOM CSV using the given field order."""
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=fieldnames, extrasaction="ignore", restval=""
        )
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Pattern-matching helpers
# ---------------------------------------------------------------------------


def match_pattern(filename: str, mapping: dict[str, Any]) -> Any | None:
    """Return the value whose key (regex) matches *filename*, or ``None``."""
    for pattern, value in mapping.items():
        if pattern and re.search(pattern, filename):
            return value
    return None


# ---------------------------------------------------------------------------
# Processed-file tracking
# ---------------------------------------------------------------------------


def load_processed(path: Path) -> set[str]:
    """Load the set of already-analyzed filenames from disk."""
    if path.exists():
        return set(json.loads(path.read_text(encoding="utf-8")))
    return set()


def save_processed(processed: set[str], path: Path) -> None:
    """Persist the set of analyzed filenames to disk."""
    path.write_text(
        json.dumps(sorted(processed), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Shared fetch stage
# ---------------------------------------------------------------------------


def fetch_attachments_stage(
    config: dict[str, dict[str, str]],
    raw_dir: Path,
    since: str | None = None,
) -> None:
    """Download matching PDF attachments from Gmail.

    Fetches all emails with attachments since *since*, then keeps any
    attachment whose **filename** or **subject** matches a broker pattern.
    """
    if not since:
        since = (datetime.now() - timedelta(days=7)).strftime("%Y/%m/%d")

    patterns: list[str] = []
    for cfg in config.values():
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
        matched_by_subject = not matched_by_fname and any(re.search(p, subject) for p in patterns)
        if matched_by_fname or matched_by_subject:
            if matched_by_subject:
                safe_subject = re.sub(r'[\\/*?:"<>|]', '_', subject).strip()
                att["filename"] = f"{safe_subject}.pdf"
            matching.append(att)

    if matching:
        save_attachments(matching, raw_dir)
        print(f"Saved {len(matching)} attachments to {raw_dir}")
    else:
        print("No matching statements found.")




# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stock & credit-card statement processor.",
        epilog=(
            "Examples:\n"
            "  python google_file_sync.py                          # run all stock stages (last 7 days)\n"
            "  python google_file_sync.py --since 2026/01/01       # fetch since a specific date\n"
            "  python google_file_sync.py --analyze                # only Gemini analysis\n"
            "  python google_file_sync.py --card                   # credit card pipeline\n"
            "  python google_file_sync.py --card --analyze         # card analyze only\n"
            "  python google_file_sync.py --sync                    # sync CSV to Google Sheet\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--fetch", action="store_true", help="Stage 1: Fetch attachments from Gmail"
    )
    parser.add_argument(
        "--decrypt", action="store_true", help="Stage 2: Decrypt PDF attachments"
    )
    parser.add_argument(
        "--analyze", action="store_true", help="Stage 3: Analyze PDFs with Gemini"
    )
    parser.add_argument(
        "--card",
        action="store_true",
        help="Run credit-card pipeline instead of stock pipeline",
    )
    parser.add_argument(
        "--since",
        type=str,
        metavar="YYYY/MM/DD",
        help="Only fetch emails after this date (default: 7 days ago)",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Stage 4: Sync transactions.csv to Google Sheet",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print raw Gemini responses and parsed rows",
    )
    return parser.parse_args()


if __name__ == "__main__":
    load_dotenv()
    args = _parse_args()

    if args.card:
        from card_pipeline import card_analyze_stage, card_decrypt_stage, card_fetch_stage

        run_all_card = not (args.fetch or args.decrypt or args.analyze)

        if run_all_card or args.fetch:
            print("=== Card Stage 1: Fetching attachments ===")
            card_fetch_stage(since=args.since)

        if run_all_card or args.decrypt:
            print("=== Card Stage 2: Decrypting PDFs ===")
            card_decrypt_stage()

        if run_all_card or args.analyze:
            print("=== Card Stage 3: Analyzing with Gemini ===")
            card_analyze_stage(debug=args.debug)
    else:
        from stock_pipeline import (
            stock_analyze_stage,
            stock_decrypt_stage,
            stock_fetch_stage,
            stock_sync_stage,
        )

        run_all = not (args.fetch or args.decrypt or args.analyze or args.sync)

        if run_all or args.fetch:
            print("=== Stage 1: Fetching attachments ===")
            stock_fetch_stage(since=args.since)

        if run_all or args.decrypt:
            print("=== Stage 2: Decrypting PDFs ===")
            stock_decrypt_stage()

        if run_all or args.analyze:
            print("=== Stage 3: Analyzing with Gemini ===")
            stock_analyze_stage(debug=args.debug)

        if run_all or args.sync:
            print("=== Stage 4: Syncing to Google Sheet ===")
            stock_sync_stage()
