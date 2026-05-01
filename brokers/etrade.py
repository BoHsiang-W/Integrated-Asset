"""ETradeBroker — E*TRADE adapter (OAuth 1.0a REST API).

Fetches account list and historical transactions via the E*TRADE v1 API.
Requires a one-time interactive OAuth handshake (browser + verifier code).

Environment variables:
  ETRADE_PROD_API_KEY    — Consumer key from E*TRADE developer portal
  ETRADE_PROD_SECRET_KEY — Consumer secret from E*TRADE developer portal

Usage:
  broker = ETradeBroker()
  transactions = broker.fetch_transactions(since=date(2026, 1, 1))

Reference:
  https://apisb.etrade.com/docs/api/account/api-transaction-v1.html
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from rauth import OAuth1Service

from brokers.base import BaseBroker

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONSUMER_KEY: str = os.getenv("ETRADE_PROD_API_KEY", "")
CONSUMER_SECRET: str = os.getenv("ETRADE_PROD_SECRET_KEY", "")
ACCOUNT_ID_KEY: str = os.getenv("ETRADE_ACCOUNT_ID", "")

BASE_URL = "https://api.etrade.com"

JSON_HEADERS = {"Accept": "application/json"}

ETRADE_TOKEN_FILE = Path("etrade_token.json")

_ACTION_MAP: dict[str, str] = {
    "Bought": "買",
    "Sold": "賣",
    "Dividend": "股利",
}

# ---------------------------------------------------------------------------
# OAuth helpers
# ---------------------------------------------------------------------------


def _build_oauth_service() -> OAuth1Service:
    """Create the rauth OAuth1Service for E*TRADE."""
    if not CONSUMER_KEY or not CONSUMER_SECRET:
        raise RuntimeError(
            "Missing ETRADE_PROD_API_KEY or ETRADE_PROD_SECRET_KEY in env."
        )
    return OAuth1Service(
        name="etrade",
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        request_token_url=f"{BASE_URL}/oauth/request_token",
        access_token_url=f"{BASE_URL}/oauth/access_token",
        authorize_url="https://us.etrade.com/e/t/etws/authorize",
    )


def _interactive_authorize(service: OAuth1Service):
    """Run the full OAuth 1.0a handshake (requires user interaction)."""
    request_token, request_token_secret = service.get_request_token(
        params={"oauth_callback": "oob"}
    )

    authorize_url = (
        f"https://us.etrade.com/e/t/etws/authorize"
        f"?key={CONSUMER_KEY}&token={request_token}"
    )
    print(f"\n  Open this URL to authorize E*TRADE:\n  {authorize_url}\n")

    verifier = input("  Enter verifier code: ").strip()

    session = service.get_auth_session(
        request_token,
        request_token_secret,
        method="POST",
        data={"oauth_verifier": verifier},
    )

    # Persist tokens for reuse
    _save_token(session.access_token, session.access_token_secret)
    return session


def _save_token(access_token: str, access_secret: str) -> None:
    """Save OAuth tokens to disk."""
    ETRADE_TOKEN_FILE.write_text(
        json.dumps(
            {"access_token": access_token, "access_secret": access_secret},
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"  Token saved to {ETRADE_TOKEN_FILE}")


def _load_saved_session(service: OAuth1Service):
    """Try to restore a session from saved tokens. Returns session or None."""
    if not ETRADE_TOKEN_FILE.exists():
        return None

    data = json.loads(ETRADE_TOKEN_FILE.read_text(encoding="utf-8"))
    access_token = data.get("access_token", "")
    access_secret = data.get("access_secret", "")
    if not access_token or not access_secret:
        return None

    session = service.get_session((access_token, access_secret))

    # Quick validation — if the token is expired E*TRADE returns 401
    res = session.get(f"{BASE_URL}/v1/accounts/list", headers=JSON_HEADERS)
    if res.ok:
        print("  Reusing saved E*TRADE token.")
        return session

    print("  Saved token expired or revoked — re-authorising...")
    ETRADE_TOKEN_FILE.unlink(missing_ok=True)
    return None


# ---------------------------------------------------------------------------
# Transaction mapper
# ---------------------------------------------------------------------------


def _map_transaction(txn: dict) -> dict | None:
    """Map a single E*TRADE transaction dict to CSV_FIELDNAMES dict.

    E*TRADE field    → CSV column
    --------------     ----------
    transactionDate  → 交易日期  (epoch int64 → YYYY/M/D)
    category         → 買/賣/股利
    brokerage.product.symbol → 代號
    description      → 股票
    amount           → 收入 / 支出
    brokerage.quantity → 買入股數 / 賣出股數
    brokerage.price    → 買入價格 / 賣出價格
    brokerage.commission → 手續費
    """
    raw_date = txn.get("transactionDate", 0)
    if isinstance(raw_date, (int, float)) and raw_date > 0:
        dt = datetime.fromtimestamp(raw_date / 1000)
        txn_date = dt.strftime("%Y/%m/%d")
    else:
        txn_date = str(raw_date)

    txn_type = txn.get("transactionType", "")
    description = txn.get("description", "")
    amount = txn.get("amount", 0)

    brokerage = txn.get("brokerage", {})
    product = brokerage.get("product", {})
    symbol = product.get("symbol", "")
    qty = str(abs(int(brokerage.get("quantity", 0) or 0)))
    price = str(brokerage.get("price", ""))
    fee = str(brokerage.get("fee", 0) or 0)

    action = _ACTION_MAP.get(txn_type)
    if action is None:
        return None
    is_buy = action == "買"
    is_sell = action == "賣"

    return {
        "交易日期": txn_date,
        "買/賣/股利": action,
        "代號": symbol,
        "股票": symbol,
        "交易類別": "US",
        "買入股數": qty if is_buy else "",
        "買入價格": price if is_buy else "",
        "賣出股數": qty if is_sell else "",
        "賣出價格": price if is_sell else "",
        "現價": "",
        "手續費": fee,
        "折讓後手續費": "",
        "交易稅": "",
        "成交價金": "",
        "交易成本": "",
        "支出": str(abs(amount)) if is_buy else "",
        "收入": str(abs(amount)) if (is_sell or action == "股利") else "",
        "決策原因": "",
        "手續費折數": "",
    }


# ---------------------------------------------------------------------------
# ETradeBroker
# ---------------------------------------------------------------------------


class ETradeBroker(BaseBroker):
    """E*TRADE data source via OAuth 1.0a REST API."""

    name = "ETRADE"
    source_type: Literal["api"] = "api"

    def __init__(self) -> None:
        self._service = _build_oauth_service()
        self._session = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_session(self) -> None:
        """Authenticate: reuse saved token if valid, otherwise interactive flow."""
        if self._session is None:
            self._session = _load_saved_session(self._service)
        if self._session is None:
            self._session = _interactive_authorize(self._service)

    def _get(self, path: str, params: dict | None = None):
        """Issue a GET to the E*TRADE v1 API with JSON accept header."""
        res = self._session.get(
            f"{BASE_URL}{path}", headers=JSON_HEADERS, params=params
        )
        if not res.ok:
            print(f"  E*TRADE {path} → HTTP {res.status_code}: {res.text}")
        return res

    def _list_accounts(self) -> list[dict]:
        """Return ACTIVE accounts from /v1/accounts/list."""
        res = self._get("/v1/accounts/list")
        if not res.ok or not res.text.strip():
            return []

        accounts = (
            res.json()
            .get("AccountListResponse", {})
            .get("Accounts", {})
            .get("Account", [])
        )
        active = [a for a in accounts if a.get("accountStatus") == "ACTIVE"]
        for a in active:
            print(f"  Account: {a.get('accountDesc')} ({a.get('accountIdKey')})")
        return active

    def _fetch_account_transactions(
        self, account_id_key: str, start: date
    ) -> list[dict]:
        """Fetch transactions for one account since *start*."""
        start_str = start.strftime("%m%d%Y")  # E*TRADE date format: MMDDYYYY
        res = self._get(
            f"/v1/accounts/{account_id_key}/transactions",
            params={"startDate": start_str},
        )
        if not res.ok or not res.text.strip():
            return []

        return res.json().get("TransactionListResponse", {}).get("Transaction", [])

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_transactions(self, since: date) -> list[dict]:
        """Fetch historical transactions for the configured E*TRADE account."""
        if not ACCOUNT_ID_KEY:
            raise RuntimeError("Missing ETRADE_ACCOUNT_ID. Set it in your .env file.")

        self._ensure_session()
        print(f"  Account: {ACCOUNT_ID_KEY}")
        raw = self._fetch_account_transactions(ACCOUNT_ID_KEY, since)
        print(f"  {len(raw)} transaction(s)")

        return [r for tx in raw if (r := _map_transaction(tx)) is not None]
