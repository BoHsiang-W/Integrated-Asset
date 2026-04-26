"""Gmail API client — fetches email attachments via OAuth2."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm

from config import CREDENTIALS_FILE, SCOPES, TOKEN_FILE

# ---------------------------------------------------------------------------
# OAuth2 credentials
# ---------------------------------------------------------------------------


def get_credentials() -> Credentials:
    """Return valid OAuth2 credentials, refreshing or re-authorizing as needed."""
    creds: Credentials | None = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                print("Refresh token is invalid or revoked — re-authorising...")
                os.remove(TOKEN_FILE)
                creds = None

        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w", encoding="utf-8") as fh:
            fh.write(creds.to_json())

    return creds


# ---------------------------------------------------------------------------
# Gmail client
# ---------------------------------------------------------------------------


class GmailClient:
    """Authenticated Gmail API client."""

    def __init__(self) -> None:
        self._service = build("gmail", "v1", credentials=get_credentials())

    def fetch_attachments(self, query: str = "has:attachment") -> list[dict]:
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
