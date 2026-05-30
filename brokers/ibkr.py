"""IBKRBroker — Interactive Brokers adapter (Client Portal API).

Fetches historical transactions via the Client Portal Gateway REST API.
Gateway must be running locally before use (default: https://localhost:5000).

Environment variables:
  IBKR_HOST         — Gateway host (default: localhost)
  IBKR_PORT         — Gateway port (default: 5000)
  IBKR_ACCOUNT      — IBKR account ID (auto-resolved from gateway if not set)
  IBKR_WATCHLIST_ID — Watchlist ID to fetch holdings from (required)

Usage:
  broker = IBKRBroker()
  transactions = broker.fetch_transactions(since=date(2026, 1, 1))

References:
  https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/
  /pa/transactions          — historical transactions (per-conid, unlimited days)
  /iserver/account/trades   — executions w/ commission (account-wide, max 7 days)
"""

from __future__ import annotations

import os
import warnings
from contextlib import contextmanager
from datetime import date
from typing import Generator, Literal

import requests
from tqdm import tqdm

from brokers.base import BaseBroker

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WATCHLIST_ID: str = os.getenv("IBKR_WATCHLIST_ID")

_ETF_SYMBOLS: frozenset[str] = frozenset({"VOO", "QQQ", "QQQM", "VGT", "CSPX"})

_ACTION_MAP: dict[str, str] = {
    "Buy": "買",
    "Sell": "賣",
    "Dividend Payment": "股利",
}

# Maps /pa/transactions ``type`` → /iserver/account/trades ``side`` char.
_SIDE_MAP: dict[str, str] = {"Buy": "B", "Sell": "S"}


def _norm_num(value) -> str:
    """Normalize numeric value to a 4-decimal string for stable map keys."""
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return ""

# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------


@contextmanager
def _no_ssl_warnings() -> Generator[None, None, None]:
    """Suppress InsecureRequestWarning for self-signed IBKR gateway cert."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


# ---------------------------------------------------------------------------
# Watchlist helpers  (single network call → derive both conids and symbol map)
# ---------------------------------------------------------------------------


def _fetch_watchlist_instruments(
    base_url: str,
    session: requests.Session,
    watchlist_id: str = WATCHLIST_ID,
) -> list[dict]:
    """Return the raw instrument list from an IBKR watchlist (one API call)."""
    with _no_ssl_warnings():
        res = session.get(f"{base_url}/iserver/watchlist?id={watchlist_id}")
    return res.json().get("instruments", [])


def _conid_symbol_map(instruments: list[dict]) -> dict[int, str]:
    """Build a {conid: ticker} map from a pre-fetched instruments list."""
    return {
        item["conid"]: item["ticker"]
        for item in instruments
        if item.get("conid") and item.get("ticker")
    }


# ---------------------------------------------------------------------------
# Commission helpers  (/iserver/account/trades)
# ---------------------------------------------------------------------------


def _build_commission_map(trades: list[dict]) -> dict[tuple, list[dict]]:
    """Build a lookup of executed-trade commissions.

    Key: ``(conid, YYYYMMDD, side, qty)``
    Val: execution rows with normalized price and commission. Price is kept as
    metadata because /pa/transactions and /iserver/account/trades may report
    slightly different prices for the same execution.
    """
    out: dict[tuple, list[dict]] = {}
    for t in trades:
        conid = t.get("conid")
        trade_time = t.get("trade_time", "")
        ymd = trade_time.split("-", 1)[0] if "-" in trade_time else ""
        side = (t.get("side") or "").strip()
        qty = _norm_num(t.get("size"))
        price = _norm_num(t.get("price"))
        try:
            commission = float(t.get("commission") or 0)
        except (TypeError, ValueError):
            commission = 0.0
        if not (conid and ymd and side and qty and price):
            continue
        key = (conid, ymd, side, qty)
        out.setdefault(key, []).append({"price": price, "commission": commission})
    return out


def _match_commission(
    commission_map: dict[tuple, list[dict]],
    key: tuple,
    price,
) -> float | None:
    """Return commission for matching executions, tolerating IBKR price drift."""
    matches = commission_map.get(key, [])
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]["commission"]

    normalized_price = _norm_num(price)
    exact = [m for m in matches if m["price"] == normalized_price]
    if exact:
        return sum(m["commission"] for m in exact)
    return sum(m["commission"] for m in matches)


# ---------------------------------------------------------------------------
# Transaction mapper
# ---------------------------------------------------------------------------


def _parse_trade_date(raw_date: str) -> str:
    """Convert IBKR rawDate string ``YYYYMMDD`` → ``YYYY/M/D``."""
    if len(raw_date) == 8:
        return f"{raw_date[:4]}/{raw_date[4:6]}/{raw_date[6:]}"
    return raw_date


def _map_transaction(
    tx: dict,
    conid_map: dict[int, str],
    commission_map: dict[tuple, list[dict]] | None = None,
) -> dict:
    """Map a single IBKR /pa/transactions item to CSV_FIELDNAMES dict.

    IBKR field  → CSV column
    ----------    ----------
    rawDate      → 交易日期  (YYYY/M/D)
    type         → 買/賣/股利
    conid        → 代號  (resolved via conid_map)
    desc         → 股票
    qty          → 買入股數 / 賣出股數
    pr           → 買入價格 / 賣出價格
    amt          → 收入  (sell/dividend, absolute value)

    ``commission_map`` (from /iserver/account/trades, last ≤7 days) is used
    to populate 手續費 when a matching execution is found.
    """
    raw_date = tx.get("rawDate", "")
    trade_date = _parse_trade_date(raw_date)
    action_type = (tx.get("type") or "").strip()
    action = _ACTION_MAP.get(action_type, "")
    conid = tx.get("conid")
    symbol = conid_map.get(conid, str(conid) if conid else "")
    qty_raw = abs(float(tx.get("qty") or 0))
    qty = str(qty_raw)
    price = str(tx.get("pr") or "")
    proceeds = str(abs(float(tx.get("amt") or 0))) if action in ("賣", "股利") else ""

    commission = ""
    side_char = _SIDE_MAP.get(action_type, "")
    if commission_map and side_char and conid:
        key = (conid, raw_date, side_char, _norm_num(qty_raw))
        matched_commission = _match_commission(commission_map, key, price)
        if matched_commission is not None:
            commission = f"{matched_commission:.2f}"

    return {
        "交易日期": trade_date,
        "買/賣/股利": action,
        "代號": symbol,
        "股票": symbol,
        "交易類別": "ETF" if symbol in _ETF_SYMBOLS else "一般",
        "買入股數": qty if action == "買" else "",
        "買入價格": price if action == "買" else "",
        "賣出股數": qty if action == "賣" else "",
        "賣出價格": price if action == "賣" else "",
        "現價": "",
        "手續費": "",
        "折讓後手續費": commission,
        "交易稅": "",
        "成交價金": "",
        "交易成本": "",
        "支出": "",
        "收入": proceeds,
        "決策原因": "",
        "手續費折數": "",
    }


# ---------------------------------------------------------------------------
# IBKRBroker
# ---------------------------------------------------------------------------


class IBKRBroker(BaseBroker):
    """Interactive Brokers data source via Client Portal REST API."""

    name = "IBKR"
    source_type: Literal["api"] = "api"

    def __init__(self, *, debug: bool = False) -> None:
        self.host = os.getenv("IBKR_HOST", "localhost")
        self.port = os.getenv("IBKR_PORT", "5000")
        self.account = os.getenv("IBKR_ACCOUNT", "")
        self.base_url = f"https://{self.host}:{self.port}/v1/api"
        self.debug = debug
        self._session = requests.Session()
        self._session.verify = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, path: str) -> requests.Response:
        with _no_ssl_warnings():
            return self._session.get(f"{self.base_url}{path}")

    def _post(self, path: str, payload: dict) -> requests.Response:
        with _no_ssl_warnings():
            return self._session.post(f"{self.base_url}{path}", json=payload)

    def _switch_account(self) -> None:
        if not self.account:
            return
        res = self._post(f"/iserver/account/{self.account}/switch", {})
        if self.debug:
            print(f"  IBKR account switch HTTP {res.status_code}: {res.text[:500]}")
        if not res.ok:
            print(
                f"  IBKR account switch HTTP {res.status_code}; "
                "commission lookup may use the previously selected account."
            )

    def _authenticate(self) -> None:
        """Wake up the Client Portal session; auto-resolve account ID if unset."""
        res = self._get("/iserver/accounts")
        if not res.ok:
            raise SystemExit(
                f"❌ IBKR gateway unreachable (HTTP {res.status_code}). "
                f"Please log in at {self.base_url.replace('/v1/api', '')} first."
            )

        if not self.account:
            accounts = res.json().get("accounts", [])
            if accounts:
                self.account = accounts[0]
                print(f"  Resolved IBKR account: {self.account}")
            else:
                print("  ❌ No accounts returned. Check gateway credentials.")

        self._switch_account()

    def _fetch_account_trades(self, days: int) -> list[dict]:
        """Fetch executed trades (incl. commission) for the account.

        Endpoint ``/iserver/account/trades`` returns up to 7 days of trades
        for the currently selected account; values >7 are clamped.
        """
        clamped = max(1, min(days, 7))
        res = self._get(f"/iserver/account/trades?days={clamped}")
        if self.debug:
            print(f"  /iserver/account/trades HTTP {res.status_code}: {res.text[:1000]}")
        if not res.ok:
            print(
                f"  /iserver/account/trades HTTP {res.status_code}, "
                "skipping commission lookup."
            )
            return []
        data = res.json() or []
        print(f"  Fetched {len(data)} trade execution(s) for commission lookup.")
        return data

    def _fetch_conid_transactions(
        self, conid: int, days: int, symbol: str
    ) -> list[dict]:
        """Fetch raw transactions for a single conid."""
        payload = {
            "acctIds": [self.account],
            "conids": [conid],
            "currency": "USD",
            "days": days,
            "types": "TRADE",
        }
        res = self._post("/pa/transactions", payload)
        if not res.ok:
            print(f"  [{symbol}] HTTP {res.status_code}, skipping.")
            return []
        txns = res.json().get("transactions") or []
        if txns:
            print(f"  [{symbol}] {len(txns)} transaction(s)")
        return txns

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_transactions(self, since: date) -> list[dict]:
        """Fetch historical transactions for all watchlist holdings.

        Note: /pa/transactions accepts only ONE conid per request.
        Loops over each watchlist conid and aggregates the results.
        """
        self._authenticate()

        instruments = _fetch_watchlist_instruments(self.base_url, self._session)
        conids = [
            item["conid"]
            for item in instruments
            if item.get("ST") == "STK" and item.get("conid")
        ]
        conid_map = _conid_symbol_map(instruments)
        days = (date.today() - since).days

        commission_map = _build_commission_map(self._fetch_account_trades(days))

        raw: list[dict] = []
        for conid in tqdm(conids):
            symbol = conid_map.get(conid, str(conid))
            raw.extend(self._fetch_conid_transactions(conid, days, symbol))
        return [_map_transaction(tx, conid_map, commission_map) for tx in raw]
