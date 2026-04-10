"""Google Sheets API client + section-aware sync writer."""

from __future__ import annotations

import os
import re

from googleapiclient.discovery import build

from clients.gmail import get_credentials


# ---------------------------------------------------------------------------
# SheetsClient — generic Sheets API wrapper
# ---------------------------------------------------------------------------


class SheetsClient:
    """Authenticated Google Sheets API client."""

    def __init__(self) -> None:
        self._service = build("sheets", "v4", credentials=get_credentials())

    def read_rows(self, spreadsheet_id: str, range_: str) -> list[list[str]]:
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

    def update_range(self, spreadsheet_id: str, range_: str, rows: list[list]) -> None:
        """Write *rows* to a specific range (overwrite existing values)."""
        body = {"values": rows}
        self._service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_,
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()

    def batch_update_ranges(self, spreadsheet_id: str, data: list[dict]) -> None:
        """Write multiple ranges in a single API call."""
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": data,
        }
        self._service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()


# ---------------------------------------------------------------------------
# SheetsSyncWriter — section-aware sync logic for stock transactions
# ---------------------------------------------------------------------------

SHEET_NAME = "交易紀錄"
_SECTION_HEADERS = {"US", "Crypto"}

# Column mapping: CSV field → Google Sheet column letter
SHEET_COL_MAP = {
    "交易日期": "A",
    "買/賣/股利": "B",
    "代號": "C",
    "股票": "D",
    "交易類別": "E",
    "買入股數": "F",
    "買入價格": "G",
    "賣出股數": "H",
    "賣出價格": "I",
    "現價": "J",
    "手續費": "K",
    "折讓後手續費": "L",
    "交易稅": "M",
    "成交價金": "N",
    "交易成本": "O",
    "支出": "P",
    "收入": "Q",
    "決策原因": "R",
    "手續費折數": "S",
}

COL_INDEX = {
    "A": 0,
    "B": 1,
    "C": 2,
    "D": 3,
    "E": 4,
    "F": 5,
    "G": 6,
    "H": 7,
    "I": 8,
    "J": 9,
    "K": 10,
    "L": 11,
    "M": 12,
    "N": 13,
    "O": 14,
    "P": 15,
    "Q": 16,
    "R": 17,
    "S": 18,
}

# Contiguous column groups to write, skipping formula/manual columns.
_WRITE_GROUPS = [
    ("A", "C", 0, 3),  # A:C  交易日期, 買/賣/股利, 代號
    ("E", "I", 4, 9),  # E:I  交易類別 … 賣出價格
    ("L", "L", 11, 12),  # L    折讓後手續費
    ("Q", "Q", 16, 17),  # Q    收入
]


class SheetsSyncWriter:
    """Encapsulates all section-aware Google Sheet sync logic."""

    def __init__(self, sheets: SheetsClient, spreadsheet_id: str) -> None:
        self._sheets = sheets
        self._sid = spreadsheet_id

    # --- discovery ---

    def find_section_headers(self) -> dict[str, int]:
        """Return ``{'US': row, 'Crypto': row}`` (1-based) by scanning column A."""
        col_a = self._sheets.read_rows(self._sid, f"{SHEET_NAME}!A1:A2000")
        headers: dict[str, int] = {}
        for i, row in enumerate(col_a, 1):
            if row and row[0].strip() in _SECTION_HEADERS:
                headers[row[0].strip()] = i
        return headers

    def read_existing_keys(self) -> set[tuple]:
        """Read existing (date, action, code) keys from the Google Sheet."""
        rows = self._sheets.read_rows(self._sid, f"{SHEET_NAME}!A3:C")
        keys: set[tuple] = set()
        for row in rows:
            if len(row) < 3:
                continue
            date_str = row[0].strip()
            if not date_str or not re.match(r"\d{4}", date_str):
                continue
            keys.add(make_row_key(date_str, row[1], row[2]))
        return keys

    def last_data_row(self, start: int, end: int) -> int:
        """Return the last row (1-based) in [start, end] with data in A–C."""
        rows = self._sheets.read_rows(self._sid, f"{SHEET_NAME}!A{start}:C{end}")
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

    def ensure_space(
        self, sheet_id: int, write_at: int, count: int, boundary: int
    ) -> int:
        """Insert blank rows before *boundary* if needed. Returns rows inserted."""
        write_end = write_at + count - 1
        if write_end >= boundary:
            needed = write_end - boundary + 1
            self._sheets.insert_rows_at(self._sid, sheet_id, boundary - 1, needed)
            return needed
        return 0

    def write_rows(self, start_row: int, rows: list[list[str]]) -> None:
        """Write *rows* starting at *start_row*, skipping formula columns."""
        end_row = start_row + len(rows) - 1
        data = []
        for start_col, end_col, idx_start, idx_end in _WRITE_GROUPS:
            range_ = f"{SHEET_NAME}!{start_col}{start_row}:{end_col}{end_row}"
            values = [row[idx_start:idx_end] for row in rows]
            data.append({"range": range_, "values": values})
        self._sheets.batch_update_ranges(self._sid, data)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def normalize_date(date_str: str) -> str:
    """Strip leading zeros: ``2026/01/05`` → ``2026/1/5``."""
    s = date_str.strip().replace("-", "/")
    parts = s.split("/")
    if len(parts) == 3:
        try:
            return f"{int(parts[0])}/{int(parts[1])}/{int(parts[2])}"
        except ValueError:
            pass
    return s


def normalize_stock_name(name: str) -> str:
    """``BTCUSDT`` → ``BTCUSD`` to match existing sheet convention."""
    if name.endswith("USDT"):
        return name[:-1]
    return name


def make_row_key(date: str, action: str, code: str) -> tuple[str, str, str]:
    """Normalize a (date, action, code) triple for dedup comparison."""
    return (normalize_date(date), action.strip(), code.strip())


def extract_sheet_id(raw: str) -> str:
    """Extract the spreadsheet ID from a full URL or return as-is."""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", raw)
    return m.group(1) if m else raw.strip()


def categorize_csv_row(row: dict) -> str:
    """Return ``'TW'``, ``'US'``, or ``'Crypto'`` based on CSV category."""
    cat = row.get("交易類別", "").strip()
    symbol = row.get("代號", "").strip()
    if cat == "Crypto":
        return "Crypto"
    if (cat == "ETF" or cat == "一般") and symbol and not symbol.isdigit():
        return "US"
    return "TW"


def csv_row_to_sheet_row(row: dict) -> list[str]:
    """Convert a CSV dict row to a list of 19 values (columns A–S)."""
    out = [""] * 19
    for field, col_letter in SHEET_COL_MAP.items():
        val = row.get(field, "").strip()
        out[COL_INDEX[col_letter]] = val
    out[0] = normalize_date(out[0])
    out[3] = normalize_stock_name(out[3])
    return out
