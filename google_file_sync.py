import os
import re
import base64
from datetime import datetime

from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv
from PyPDF2 import PdfReader, PdfWriter

from google import genai
from google.genai import types

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class Mail:
    def __init__(self):
        self.SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
        self._authenticate()
        self.attachments = []
        self.get_attachments()

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
                .list(userId=user_id, q="has:attachment newer_than:7d")
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


class Gemini:
    def __init__(self):
        self.client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
        self.model = "gemini-2.5-pro"

    def generate_response(self, prompt, filepath):
        """Generate a response using Gemini."""
        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=[
                    types.Part.from_bytes(
                        data=filepath.read_bytes(),
                        mime_type="application/pdf",
                    ),
                    prompt,
                ],
            )
            return response.text
        except Exception as e:
            print(f"Error generating response: {e}")
            return None


class Stock(Mail):
    def __init__(self):
        super().__init__()

    def get_statement_by_env(self, env_var):
        if self.attachments:
            pattern = os.getenv(env_var)
            return [
                att for att in self.attachments if re.search(pattern, att["filename"])
            ]
        return []

    def get_all_statements(self, patterns=None):
        if patterns is None:
            return
        print(f"Searching for attachments matching patterns: {patterns}")
        if self.attachments:
            return [
                att
                for att in self.attachments
                if any(re.search(pattern, att["filename"]) for pattern in patterns)
            ]

    @staticmethod
    def save_attachments(attachments, folder_path):
        """Save attachments to the specified folder."""
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        for att in attachments:
            file_path = os.path.join(folder_path, f"{att['date']}_{att['filename']}")
            with open(file_path, "wb") as f:
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                f.write(file_data)

    @staticmethod
    def decrypt_attachments(file, password=None):
        """Decrypt and save attachments to the specified folder."""

        file_path = os.path.join(file.parent, file.name)
        # Decrypt PDF with password
        if not password:
            print(
                f"PDF_PASSWORD environment variable not set. Skipping decryption for {file}."
            )
            return
        try:
            reader = PdfReader(file_path)
            if reader.is_encrypted:
                reader.decrypt(password)
            writer = PdfWriter()
            for page in reader.pages:
                writer.add_page(page)
            decrypted_path = os.path.join(file.parent, f"decrypted_{file.name}")
            with open(decrypted_path, "wb") as out_f:
                writer.write(out_f)
        except Exception as e:
            print(f"Failed to decrypt {file}: {e}")


load_dotenv()
pattern = [os.getenv("CATHAY_US"), os.getenv("CATHAY_TW"), os.getenv("FUBON_US")]
all_statements = Stock().get_all_statements(patterns=pattern)
Stock.save_attachments(all_statements, "attachments")
attachments_dir = Path("attachments")
for file in attachments_dir.iterdir():
    if file.name.startswith("decrypted_"):
        continue
    Stock.decrypt_attachments(file, os.getenv("PDF_PASSWORD"))

attachments_dir = Path("attachments")
gemini = Gemini()

PROMPT_DIR = Path("prompt")
prompt_file = PROMPT_DIR / "Cathay_US.md"
prompt = prompt_file.read_text(encoding="utf-8")

for file in attachments_dir.iterdir():
    if not file.name.startswith("decrypted_") or not file.is_file():
        continue
    res = gemini.generate_response(prompt, file)
    print(f"Processing {file.name}...{res}")
    if res:
        print(f"Results for {file.name}:\n{res}\n")
