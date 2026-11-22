"""IBKRBroker — Interactive Brokers adapter (Flex Web Service).

Downloads the Activity Flex Query report and maps its Trades (TRNT) and
Cash Transactions (CTRN) sections to CSV rows. No gateway or login is needed.

The Flex Query must use CSV format with section codes, include the Trades
section at Execution level and Cash Transactions with Dividends at Detail level.

Environment variables:
  IBKR_FLEX_TOKEN    — Flex Web Service token (Settings > Flex Web Service)
  IBKR_FLEX_QUERY_ID — ID of the Activity Flex Query

Reference:
  https://www.interactivebrokers.com/campus/ibkr-api-page/flex-web-service
"""

from __future__ import annotations

import csv
import os
import time
import xml.etree.ElementTree as ET
from datetime import date
from typing import Literal

import requests

from brokers.base import BaseBroker, SourceUnavailable

FLEX_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"

_SIDE_MAP: dict[str, str] = {"BUY": "買", "SELL": "賣"}
_DIVIDEND_TYPES = frozenset({"Dividends", "Payment In Lieu Of Dividends"})


def parse_flex_csv(text: str) -> dict[str, list[dict[str, str]]]:
    """Split a section-coded Flex CSV into ``{section_code: [row dicts]}``."""
    headers: dict[str, list[str]] = {}
    sections: dict[str, list[dict[str, str]]] = {}
    for record in csv.reader(text.splitlines()):
        if len(record) < 2:
            continue
        kind, code, values = record[0], record[1], record[2:]
        if kind == "HEADER":
            headers[code] = values
        elif kind == "DATA":
            sections.setdefault(code, []).append(dict(zip(headers[code], values)))
    return sections


def _ymd(raw: str) -> date:
    return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))


def _csv_date(d: date) -> str:
    return f"{d.year}/{d.month:02d}/{d.day:02d}"


def _row(trade_date: date, action: str, symbol: str, source_tag: str, **fields: str) -> dict:
    return {
        "交易日期": _csv_date(trade_date),
        "買/賣/股利": action,
        "代號": symbol,
        "股票": symbol,
        "交易類別": BaseBroker.classify_symbol_category(symbol),
        "買入股數": "",
        "買入價格": "",
        "賣出股數": "",
        "賣出價格": "",
        "現價": "",
        "手續費": "",
        "折讓後手續費": "",
        "交易稅": "0",
        "成交價金": "",
        "交易成本": "",
        "支出": "",
        "收入": "",
        "決策原因": source_tag,
        "手續費折數": "",
        **fields,
    }


def map_trade(trade: dict[str, str], source_tag: str) -> dict | None:
    """Map one TRNT execution to a CSV row; summary-level rows return None."""
    action = _SIDE_MAP.get(trade.get("Buy/Sell", ""))
    if action is None or trade.get("LevelOfDetail", "EXECUTION") != "EXECUTION":
        return None
    qty = str(abs(float(trade["Quantity"])))
    price = trade["TradePrice"]
    side = "買入" if action == "買" else "賣出"
    fields = {
        f"{side}股數": qty,
        f"{side}價格": price,
        "折讓後手續費": f"{abs(float(trade.get('IBCommission') or 0)):.2f}",
    }
    if action == "賣":
        fields["收入"] = str(abs(float(trade.get("Proceeds") or 0)))
    return _row(_ymd(trade["TradeDate"]), action, trade["Symbol"], source_tag, **fields)


def map_dividends(cash_rows: list[dict[str, str]], source_tag: str) -> list[dict]:
    """Map CTRN dividends to CSV rows net of the withholding tax booked the same day."""
    detail = [c for c in cash_rows if c.get("LevelOfDetail", "DETAIL") == "DETAIL"]
    withheld: dict[tuple[str, str], float] = {}
    for c in detail:
        if c.get("Type") == "Withholding Tax":
            key = (c["Date/Time"][:8], c["Symbol"])
            withheld[key] = withheld.get(key, 0.0) + float(c["Amount"])

    rows = []
    for c in detail:
        if c.get("Type") not in _DIVIDEND_TYPES:
            continue
        key = (c["Date/Time"][:8], c["Symbol"])
        net = float(c["Amount"]) + withheld.get(key, 0.0)
        rows.append(_row(_ymd(key[0]), "股利", key[1], source_tag, 收入=f"{net:.2f}"))
    return rows


class IBKRBroker(BaseBroker):
    """Interactive Brokers data source via the Flex Web Service."""

    name = "IBKR"
    source_type: Literal["api"] = "api"

    def __init__(self, *, debug: bool = False, poll_seconds: float = 5, max_polls: int = 12) -> None:
        self.token = os.getenv("IBKR_FLEX_TOKEN", "")
        self.query_id = os.getenv("IBKR_FLEX_QUERY_ID", "")
        self.debug = debug
        self.poll_seconds = poll_seconds
        self.max_polls = max_polls

    def _download_report(self) -> str:
        if not (self.token and self.query_id):
            raise SourceUnavailable("IBKR_FLEX_TOKEN or IBKR_FLEX_QUERY_ID is not set")

        res = requests.get(
            f"{FLEX_URL}/SendRequest",
            params={"t": self.token, "q": self.query_id, "v": 3},
            timeout=30,
        )
        reply = ET.fromstring(res.text)
        ref = reply.findtext("ReferenceCode")
        if reply.findtext("Status") != "Success" or not ref:
            raise SourceUnavailable(
                f"IBKR Flex SendRequest failed ({reply.findtext('ErrorCode')}: "
                f"{reply.findtext('ErrorMessage')})"
            )

        for _ in range(self.max_polls):
            time.sleep(self.poll_seconds)
            res = requests.get(
                f"{FLEX_URL}/GetStatement",
                params={"t": self.token, "q": ref, "v": 3},
                timeout=60,
            )
            if not res.text.lstrip().startswith("<"):
                return res.text
            if self.debug:
                print(f"  Flex statement not ready: {res.text[:200]}")
        raise SourceUnavailable(
            f"IBKR Flex statement not ready after {self.poll_seconds * self.max_polls:.0f} s"
        )

    def fetch_transactions(self, since: date) -> list[dict]:
        """Return trades and net dividends on or after *since* from the Flex report."""
        sections = parse_flex_csv(self._download_report())
        source_tag = BaseBroker.to_source_tag(self.name)
        trades = [map_trade(t, source_tag) for t in sections.get("TRNT", [])]
        rows = [r for r in trades if r is not None]
        rows += map_dividends(sections.get("CTRN", []), source_tag)
        since_str = _csv_date(since)
        return [r for r in rows if r["交易日期"] >= since_str]
