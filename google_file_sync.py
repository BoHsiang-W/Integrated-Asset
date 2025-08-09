import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
from tqdm import tqdm


class Mail:
    def __init__(self):
        self.SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
        self._authenticate()
        self.attachments = []

    def _authenticate(self):
        """Authenticate the user and create a service object."""
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", self.SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", self.SCOPES
                )
            creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(creds.to_json())
        self.service = build("gmail", "v1", credentials=creds)

    def get_attachments(self, user_id="me") -> list:
        """Get attachments from the user's Gmail."""
        self.attachments = []  # Reset attachments for each call
        try:
            results = (
                self.service.users()
                .messages()
                .list(userId=user_id, q="has:attachment newer_than:1d")
                .execute()
            )
            messages = results.get("messages", [])
            if not messages:
                print("No messages found.")
                return []
            for message in tqdm(messages):
                msg = (
                    self.service.users()
                    .messages()
                    .get(userId=user_id, id=message["id"], format="full")
                    .execute()
                )
                payload = msg.get("payload", {})
                parts = payload.get("parts", [])
                self._extract_attachments(parts, user_id, msg)
            print(f"Found {len(self.attachments)} attachments.")
            return self.attachments
        except HttpError as error:
            print(f"An error occurred: {error}")
            return []

    def _extract_attachments(self, parts, user_id, message):
        """Recursively extract attachments from message parts."""
        for part in parts:
            if part.get("filename") and part.get("body", {}).get("attachmentId"):
                filename = part["filename"]
                attachment_id = part["body"]["attachmentId"]
                attachment = (
                    self.service.users()
                    .messages()
                    .attachments()
                    .get(userId=user_id, messageId=message["id"], id=attachment_id)
                    .execute()
                )
                self.attachments.append(
                    {
                        "filename": filename,
                        "data": attachment.get("data"),
                        "mimeType": part.get("mimeType"),
                        "date": datetime.fromtimestamp(
                            int(message["internalDate"]) / 1000
                        ).strftime("%Y-%m-%d"),
                    }
                )
            # Recursively check for subparts
            if "parts" in part:
                self._extract_attachments(part["parts"], user_id, message)


# class Drive

# class Bank:
#     def __init__(self):
#         self.Shanghai = ""
#         self.newnewbank = ""


# class Stock


print(Mail().get_attachments())
