"""E*TRADE dividends are booked net of the withholding tax posted on the same day; cash movements are skipped."""

import unittest
from datetime import date, datetime
from unittest import mock

import brokers.etrade as etrade

PAY_DAY = int(datetime(2025, 1, 15, 12).timestamp() * 1000)


def txn(kind: str, amount: float, symbol: str | None = None, **brokerage) -> dict:
    product = {"symbol": symbol} if symbol else {}
    return {"transactionType": kind, "transactionDate": PAY_DAY, "amount": amount, "brokerage": {"product": product, **brokerage}}


RAW = [
    txn("Funds Received", 500.0),
    txn("Transfer", -500.0),
    txn("Margin Interest", -0.5),
    txn("Qualified Dividend", 10.0, "XYZ"),
    txn("Tax Withholding", -3.0, "XYZ"),
]


class ETradeRowsTest(unittest.TestCase):
    def fetch(self, raw: list[dict]) -> list[dict]:
        with mock.patch.multiple(etrade, CONSUMER_KEY="k", CONSUMER_SECRET="s", ACCOUNT_ID_KEY="a"):
            broker = etrade.ETradeBroker()
            with mock.patch.object(broker, "_ensure_session"), mock.patch.object(
                broker, "_fetch_account_transactions", return_value=raw
            ):
                return broker.fetch_transactions(since=date(2025, 1, 1))

    def test_qualified_dividend_is_one_row_net_of_withholding(self):
        rows = self.fetch(RAW)
        self.assertEqual(
            [(r["交易日期"], r["買/賣/股利"], r["代號"], r["收入"]) for r in rows],
            [("2025/01/15", "股利", "XYZ", "7.00")],
        )

    def test_trade_rows_keep_the_csv_format_existing_rows_dedupe_against(self):
        bought = txn("Bought", -350.12, "SPY", quantity=0.5, price=700.24, fee=0)
        sold = txn("Sold", 501.0, "XYZ", quantity=-2, price=250.5, fee=0.01)
        common = {"交易日期": "2025/01/15", "現價": "", "手續費": "", "交易稅": "0", "成交價金": "", "交易成本": "", "決策原因": "source:etrade", "手續費折數": ""}
        self.assertEqual(
            self.fetch([bought, sold]),
            [
                {**common, "買/賣/股利": "買", "代號": "SPY", "股票": "SPY", "交易類別": "ETF",
                 "買入股數": "0.5", "買入價格": "700.24", "賣出股數": "", "賣出價格": "",
                 "折讓後手續費": "0", "支出": "350.12", "收入": ""},
                {**common, "買/賣/股利": "賣", "代號": "XYZ", "股票": "XYZ", "交易類別": "一般",
                 "買入股數": "", "買入價格": "", "賣出股數": "2.0", "賣出價格": "250.5",
                 "折讓後手續費": "0.01", "支出": "", "收入": "501.0"},
            ],
        )

    def test_history_request_sends_both_dates_and_follows_markers(self):
        pages = {None: ([txn("Transfer", 1.0)] * 50, "m1"), "m1": ([txn("Transfer", 1.0)] * 50, "m2"), "m2": ([txn("Transfer", 1.0)] * 3, None)}
        sent: list[dict] = []

        def fake_get(_path: str, params: dict):
            sent.append(params)
            txns, marker = pages[params.get("marker")]
            body = {"Transaction": txns, **({"marker": marker} if marker else {})}
            return mock.Mock(ok=True, text="x", json=lambda: {"TransactionListResponse": body})

        with mock.patch.multiple(etrade, CONSUMER_KEY="k", CONSUMER_SECRET="s"):
            broker = etrade.ETradeBroker()
        with mock.patch.object(broker, "_get", side_effect=fake_get), mock.patch.object(etrade, "date") as fake_date:
            fake_date.today.return_value = date(2025, 6, 30)
            raw = broker._fetch_account_transactions("a", date(2025, 1, 1))

        self.assertEqual(len(raw), 103)
        self.assertEqual([p.get("marker") for p in sent], [None, "m1", "m2"])
        self.assertTrue(all(p["startDate"] == "01012025" and p["endDate"] == "06302025" and p["count"] == 50 for p in sent))

    def test_cash_movements_and_interest_produce_no_rows(self):
        self.assertEqual(self.fetch([txn("Funds Received", 500.0), txn("Margin Interest", -0.02), txn("Interest Income", 0.1)]), [])


if __name__ == "__main__":
    unittest.main()
