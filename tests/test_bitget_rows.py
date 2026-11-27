"""Bitget orders must become the same CSV row sign.js wrote, so existing rows dedup.
Expected strings come from running sign.js's tradesToCsvRows logic in Node on this machine."""

import csv
import unittest
from io import StringIO

from brokers.bitget import order_to_row
from models.transaction import STOCK_CSV_FIELDNAMES

ORDER = {
    "symbol": "BTCUSDT",
    "priceAvg": "91234.56",
    "quoteVolume": "50",
    "baseCoin": "BTC",
    "side": "buy",
    "uTime": "1763341200123",
}


def filled(order: dict) -> dict:
    row = order_to_row(order)
    assert row is not None, "filled order maps to a row"
    return row


def as_csv_line(row: dict) -> str:
    out = StringIO()
    csv.DictWriter(
        out, fieldnames=STOCK_CSV_FIELDNAMES, restval="", lineterminator=""
    ).writerow(row)
    return out.getvalue()


class BitgetRows(unittest.TestCase):
    def test_buy_matches_sign_js(self):
        self.assertEqual(
            as_csv_line(filled(ORDER)),
            "2025/11/17,買,BTC,BTCUSDT,Crypto,0.000548,91234.560000,,,,,0,,,,,0,,",
        )

    def test_date_uses_local_time_like_sign_js(self):
        self.assertEqual(
            filled({**ORDER, "uTime": "1763312400000"})["交易日期"], "2025/11/17"
        )

    def test_sell_fills_sell_columns(self):
        row = filled({**ORDER, "side": "sell"})
        self.assertEqual(
            (row["買/賣/股利"], row["買入股數"], row["賣出股數"], row["賣出價格"]),
            ("賣", "", "0.000548", "91234.560000"),
        )

    def test_unfilled_order_is_skipped(self):
        self.assertIsNone(order_to_row({**ORDER, "quoteVolume": "0", "priceAvg": "0"}))


if __name__ == "__main__":
    unittest.main()
