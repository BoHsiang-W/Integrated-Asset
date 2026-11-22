"""BitgetBroker — spot order history from the Bitget v2 REST API.

Replaces the trade-history half of ``sign.js``. Rows use the same strings
``sign.js`` wrote, so the exact-row dedup in ``StockPipeline`` recognises
orders that are already in ``transactions.csv``.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal
from urllib.parse import urlencode

from brokers.base import BaseBroker
from clients.exchanges import BITGET, signed_get

SYMBOLS = ("BTCUSDT", "ETHUSDT")

_ACTION = {"buy": "買", "sell": "賣"}


def order_to_row(order: dict) -> dict | None:
    """Map one ``history-orders`` entry to a transactions.csv row, or None if nothing filled."""
    quote_volume = float(order["quoteVolume"] or 0)
    price_avg = float(order["priceAvg"] or 0)
    if quote_volume <= 0 or price_avg <= 0:
        return None

    qty = f"{quote_volume / price_avg:.6f}"
    price = f"{price_avg:.6f}"
    is_buy = order["side"] == "buy"
    return {
        "交易日期": datetime.fromtimestamp(int(order["uTime"]) / 1000).strftime("%Y/%m/%d"),
        "買/賣/股利": _ACTION[order["side"]],
        "代號": order.get("baseCoin") or "",
        "股票": order["symbol"],
        "交易類別": "Crypto",
        "買入股數": qty if is_buy else "",
        "買入價格": price if is_buy else "",
        "賣出股數": "" if is_buy else qty,
        "賣出價格": "" if is_buy else price,
        "折讓後手續費": "0",
        "收入": "0",
    }


class BitgetBroker(BaseBroker):
    name = "Bitget"
    source_type: Literal["gmail_pdf", "api"] = "api"

    def fetch_transactions(self, since: date) -> list[dict]:
        rows: list[dict] = []
        for symbol in SYMBOLS:
            path = "/api/v2/spot/trade/history-orders?" + urlencode({"symbol": symbol, "limit": "100"})
            orders = signed_get(BITGET, path)["data"] or []
            mapped = [row for row in map(order_to_row, orders) if row]
            print(f"  [{symbol}] {len(orders)} order(s), {len(mapped)} filled")
            rows += mapped
        since_str = since.strftime("%Y/%m/%d")
        return [row for row in rows if row["交易日期"] >= since_str]
