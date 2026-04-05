"""Domain models — typed dataclasses for attachments."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Attachment:
    """A single email attachment extracted from Gmail."""

    filename: str
    subject: str
    data: str  # base64-encoded content
    mime_type: str
    date: str  # YYYY-MM-DD

    def to_dict(self) -> dict[str, str]:
        return {
            "filename": self.filename,
            "subject": self.subject,
            "data": self.data,
            "mimeType": self.mime_type,
            "date": self.date,
        }
