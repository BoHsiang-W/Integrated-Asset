"""
google_file_sync.py — Stock statement pipeline.

Stages:
  1. Fetch   — Download matching PDF attachments from Gmail.
  2. Decrypt — Decrypt password-protected PDFs per broker.
  3. Analyze — Send decrypted PDFs to Gemini and write transactions.csv.

Usage:
  python google_file_sync.py              # run all stages
  python google_file_sync.py --analyze    # only Stage 3
  python google_file_sync.py --decrypt --analyze
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import time
from datetime import datetime
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
PROCESSED_FILE = ATTACHMENTS_DIR / ".processed.json"
CSV_OUTPUT = ATTACHMENTS_DIR / "transactions.csv"

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
TOKEN_FILE = "token.json"
CREDENTIALS_FILE = "credentials.json"

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
# Gmail
# ---------------------------------------------------------------------------


class GmailClient:
    """Authenticated Gmail API client."""

    def __init__(self) -> None:
        self._service = _build_gmail_service()

    def fetch_attachments(
        self, query: str = "has:attachment newer_than:7d"
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


def _build_gmail_service():
    """Authenticate via OAuth2 and return a Gmail API service object."""
    creds: Credentials | None = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Only run the browser flow when no valid token exists at all
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, GMAIL_SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as fh:
            fh.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


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
            output.append(
                {
                    "filename": part["filename"],
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


def decrypt_pdf(source: Path, password: str) -> None:
    """Decrypt *source* with *password* and write ``decrypted_<name>`` alongside it."""
    dest = source.parent / f"decrypted_{source.name}"
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


def _parse_csv_response(text: str) -> list[dict]:
    """Parse a (possibly markdown-fenced) CSV string from Gemini into row dicts."""
    cleaned = re.sub(r"```(?:csv)?\n?", "", text).strip()
    reader = csv.DictReader(StringIO(cleaned))
    return [row for row in reader if any(v and v.strip() for v in row.values())]


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
                # Trailing-comma overflow: the overflow value is the intended 收入.
                # If 收入 already holds a shifted value, move it to 手續費.
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


def _read_existing_csv(path: Path) -> list[dict]:
    """Return rows from an existing CSV file, or an empty list if it does not exist."""
    if not path.exists() or path.stat().st_size == 0:
        return []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict]) -> None:
    """Write *rows* to *path* as UTF-8-BOM CSV using the canonical field order."""
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=CSV_FIELDNAMES, extrasaction="ignore", restval=""
        )
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Pattern-matching helpers
# ---------------------------------------------------------------------------


def _build_pattern_map(value_key: str) -> dict[str, Any]:
    """
    Build a {filename_regex: value} mapping from BROKER_CONFIG.

    value_key is "password_env" (resolves env var -> password string)
    or "prompt" (resolves to a Path).
    """
    result: dict[str, Any] = {}
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


def _match_pattern(filename: str, mapping: dict[str, Any]) -> Any | None:
    """Return the value whose key (regex) matches *filename*, or ``None``."""
    for pattern, value in mapping.items():
        if pattern and re.search(pattern, filename):
            return value
    return None


# ---------------------------------------------------------------------------
# Processed-file tracking
# ---------------------------------------------------------------------------


def _load_processed() -> set[str]:
    """Load the set of already-analyzed filenames from disk."""
    if PROCESSED_FILE.exists():
        return set(json.loads(PROCESSED_FILE.read_text(encoding="utf-8")))
    return set()


def _save_processed(processed: set[str]) -> None:
    """Persist the set of analyzed filenames to disk."""
    PROCESSED_FILE.write_text(
        json.dumps(sorted(processed), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------


def fetch_attachments_stage() -> None:
    """Stage 1 — Download matching statement PDFs from Gmail."""
    patterns = [os.getenv(cfg["pattern_env"]) for cfg in BROKER_CONFIG.values()]
    attachments = GmailClient().fetch_attachments()

    matching = [
        att
        for att in attachments
        if any(p and re.search(p, att["filename"]) for p in patterns)
    ]
    if matching:
        save_attachments(matching, ATTACHMENTS_DIR)
        print(f"Saved {len(matching)} attachments to {ATTACHMENTS_DIR}")
    else:
        print("No matching statements found.")


def decrypt_pdfs_stage() -> None:
    """Stage 2 — Decrypt each unprocessed PDF using its broker-specific password."""
    password_map = _build_pattern_map("password_env")
    for file in ATTACHMENTS_DIR.iterdir():
        if file.name.startswith("decrypted_") or not file.is_file():
            continue
        password = _match_pattern(file.name, password_map)
        if password:
            decrypt_pdf(file, password)
        else:
            print(f"  No password matched for {file.name}, skipping.")


def analyze_pdfs_stage(*, debug: bool = False) -> None:
    """Stage 3 — Analyze decrypted PDFs with Gemini and merge results into transactions.csv."""
    prompt_map = _build_pattern_map("prompt")
    gemini = GeminiClient()
    processed = _load_processed()

    decrypted = sorted(
        f
        for f in ATTACHMENTS_DIR.iterdir()
        if f.name.startswith("decrypted_") and f.is_file()
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
        prompt_path = _match_pattern(file.name, prompt_map)
        if not prompt_path:
            print(f"  No matching prompt for {file.name}, skipping.")
            continue

        raw = gemini.analyze_pdf(prompt_path.read_text(encoding="utf-8"), file)
        if not raw:
            print("  No response from Gemini.")
            continue

        if debug:
            print(f"  --- RAW GEMINI RESPONSE ---\n{raw}\n  --- END RAW RESPONSE ---")

        rows = _parse_csv_response(raw)

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

    all_rows = _read_existing_csv(CSV_OUTPUT) + new_rows
    all_rows = [r for r in all_rows if any(str(v).strip() for v in r.values())]
    all_rows = _normalize_rows(all_rows)
    unique_rows = _dedup_and_sort(all_rows)

    _write_csv(CSV_OUTPUT, unique_rows)
    dupes = len(all_rows) - len(unique_rows)
    print(f"\nSaved {CSV_OUTPUT} ({len(unique_rows)} rows, {dupes} duplicates removed)")
    _save_processed(processed)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stock statement processor — run stages independently.",
        epilog=(
            "Examples:\n"
            "  python google_file_sync.py                      # run all stages\n"
            "  python google_file_sync.py --analyze            # only Gemini analysis\n"
            "  python google_file_sync.py --decrypt --analyze  # decrypt then analyze\n"
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
        "--debug",
        action="store_true",
        help="Print raw Gemini responses and parsed rows",
    )
    return parser.parse_args()


if __name__ == "__main__":
    load_dotenv()
    args = _parse_args()
    run_all = not (args.fetch or args.decrypt or args.analyze)

    if run_all or args.fetch:
        print("=== Stage 1: Fetching attachments ===")
        fetch_attachments_stage()

    if run_all or args.decrypt:
        print("=== Stage 2: Decrypting PDFs ===")
        decrypt_pdfs_stage()

    if run_all or args.analyze:
        print("=== Stage 3: Analyzing with Gemini ===")
        analyze_pdfs_stage(debug=args.debug)
