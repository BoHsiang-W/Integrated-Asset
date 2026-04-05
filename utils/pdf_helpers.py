"""PDF decryption and attachment saving."""

from __future__ import annotations

import base64
from pathlib import Path

from PyPDF2 import PdfReader, PdfWriter


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
