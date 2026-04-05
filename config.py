"""Centralized configuration — constants, paths, broker/card configs."""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ATTACHMENTS_DIR = Path("attachments")
PROMPT_DIR = Path("prompt")

# ---------------------------------------------------------------------------
# OAuth2 / Google API
# ---------------------------------------------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]
TOKEN_FILE = "token.json"
CREDENTIALS_FILE = "credentials.json"

# ---------------------------------------------------------------------------
# Stock broker configuration
# ---------------------------------------------------------------------------

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
# Credit-card bank configuration
# ---------------------------------------------------------------------------

CARD_CONFIG: dict[str, dict[str, str]] = {
    "CATHAY_CARD":  {"pattern_env": "CATHAY_CARD",  "password_env": "CATHAY_CARD_PASSWORD",  "bank_name": "國泰"},
    "FUBON_CARD":   {"pattern_env": "FUBON_CARD",   "password_env": "FUBON_CARD_PASSWORD",   "bank_name": "富邦"},
    "TAISHIN_CARD": {"pattern_env": "TAISHIN_CARD", "password_env": "TAISHIN_CARD_PASSWORD", "bank_name": "台新"},
    "MEGA_CARD":    {"pattern_env": "MEGA_CARD",    "password_env": "MEGA_CARD_PASSWORD",    "bank_name": "兆豐"},
    "SINOPAC_CARD": {"pattern_env": "SINOPAC_CARD", "password_env": "SINOPAC_CARD_PASSWORD", "bank_name": "永豐"},
    "RAKUTEN_CARD": {"pattern_env": "RAKUTEN_CARD", "password_env": "RAKUTEN_CARD_PASSWORD", "bank_name": "樂天"},
    "DBS_CARD":     {"pattern_env": "DBS_CARD",     "password_env": "DBS_CARD_PASSWORD",     "bank_name": "星展"},
    "LINEBANK_CARD":{"pattern_env": "LINEBANK_CARD","password_env": "LINEBANK_CARD_PASSWORD","bank_name": "Line Bank"},
    "SCSB_CARD":    {"pattern_env": "SCSB_CARD",    "password_env": "SCSB_CARD_PASSWORD",    "bank_name": "上海"},
    "UBOT_CARD":    {"pattern_env": "UBOT_CARD",    "password_env": "UBOT_CARD_PASSWORD",    "bank_name": "聯邦"},
}

BANK_ORDER = ["國泰", "富邦", "台新", "兆豐", "永豐", "樂天", "星展", "Line Bank", "上海", "聯邦"]

SUMMARY_CSV_FIELDNAMES = ["卡別", "應繳金額"]
