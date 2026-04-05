"""Pattern matching and processed-file tracking."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def match_pattern(filename: str, mapping: dict[str, Any]) -> Any | None:
    """Return the value whose key (regex) matches *filename*, or ``None``."""
    for pattern, value in mapping.items():
        if pattern and re.search(pattern, filename):
            return value
    return None


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
