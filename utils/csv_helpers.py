"""CSV parsing, reading, writing, normalization, and deduplication."""

from __future__ import annotations

import csv
import re
from io import StringIO
from pathlib import Path
from typing import Callable


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
        return [
            row
            for row in csv.DictReader(fh)
            if any(v and v.strip() for v in row.values())
        ]


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    """Write *rows* to *path* as UTF-8-BOM CSV using the given field order."""
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=fieldnames, extrasaction="ignore", restval=""
        )
        writer.writeheader()
        writer.writerows(rows)


def normalize_rows(rows: list[dict], overflow_field: str | None = None) -> list[dict]:
    """Sanitize rows from ``csv.DictReader``.

    - Remove ``None`` keys (caused by trailing commas in Gemini output).
    - If *overflow_field* is given and that field is empty, rescue the
      overflow value into it.
    - Replace remaining ``None`` values with empty strings.
    """
    for row in rows:
        for key in [k for k in row if k is None]:
            raw = row.pop(key)
            val = str(raw[0] if isinstance(raw, list) else raw or "").strip()
            if val and overflow_field:
                cur = row.get(overflow_field, "").strip()
                if cur and not row.get("手續費", "").strip():
                    row["手續費"] = cur
                row[overflow_field] = val
        for key in row:
            if row[key] is None:
                row[key] = ""
    return rows


def dedup_and_sort(
    rows: list[dict],
    fieldnames: list[str],
    sort_key: Callable[[dict], tuple],
) -> list[dict]:
    """Remove exact duplicates (by *fieldnames* tuple) and sort by *sort_key*."""
    seen: set[tuple] = set()
    unique: list[dict] = []
    for row in rows:
        key = tuple(row.get(f, "") for f in fieldnames)
        if key not in seen:
            seen.add(key)
            unique.append(row)
    unique.sort(key=sort_key)
    return unique
