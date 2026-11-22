"""IBKR rows come from the Flex Web Service report, with commissions and dividends net of withholding."""

import os
import threading
import unittest
from contextlib import contextmanager, redirect_stdout
from datetime import date
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import StringIO
from unittest import mock
from urllib.parse import parse_qs, urlparse

import brokers.ibkr as ibkr
from pipelines.base import BasePipeline

REPORT = "\n".join([
    '"BOF","U0000000","Trades","2","20261001","20261031"',
    '"HEADER","TRNT","ClientAccountID","Symbol","TradeDate","Buy/Sell","Quantity","TradePrice","Proceeds","IBCommission","LevelOfDetail"',
    '"DATA","TRNT","U0000000","CSPX","20261023","BUY","0.5152","700.12","-360.70","-1.7013","EXECUTION"',
    '"DATA","TRNT","U0000000","AAPL","20261024","SELL","-2","250.5","501","-1","EXECUTION"',
    '"DATA","TRNT","U0000000","CSPX","20260901","BUY","1","690","-690","-1","EXECUTION"',
    '"HEADER","CTRN","ClientAccountID","Symbol","Date/Time","Amount","Type","LevelOfDetail"',
    '"DATA","CTRN","U0000000","AAPL","20261016","10.00","Dividends","DETAIL"',
    '"DATA","CTRN","U0000000","AAPL","20261016","-3.00","Withholding Tax","DETAIL"',
    '"DATA","CTRN","U0000000","AAPL","20261016","10.00","Dividends","SUMMARY"',
    '"DATA","CTRN","U0000000","","20261017","500","Deposits/Withdrawals","DETAIL"',
    '"EOF"',
])
SEND_OK = "<FlexStatementResponse><Status>Success</Status><ReferenceCode>42</ReferenceCode><Url>x</Url></FlexStatementResponse>"
EXPIRED = "<FlexStatementResponse><Status>Fail</Status><ErrorCode>1012</ErrorCode><ErrorMessage>Token has expired.</ErrorMessage></FlexStatementResponse>"
NOT_READY = "<FlexStatementResponse><Status>Warn</Status><ErrorCode>1019</ErrorCode></FlexStatementResponse>"


@contextmanager
def fake_flex(send_reply: str, statements: list[str]):
    seen: list[dict] = []
    pending = list(statements)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlparse(self.path)
            seen.append({"path": url.path, **{k: v[0] for k, v in parse_qs(url.query).items()}})
            body = send_reply if url.path.endswith("/SendRequest") else pending.pop(0)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, format, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = {"IBKR_FLEX_TOKEN": "tok", "IBKR_FLEX_QUERY_ID": "123"}
    try:
        with mock.patch.object(ibkr, "FLEX_URL", f"http://127.0.0.1:{server.server_port}/fws"), \
                mock.patch.dict(os.environ, env):
            yield ibkr.IBKRBroker(poll_seconds=0), seen
    finally:
        server.shutdown()


def run_stage(func) -> tuple[bool, str]:
    out = StringIO()
    with redirect_stdout(out):
        ok = BasePipeline.run_stage(mock.Mock(spec=BasePipeline), "IBKR", func)
    return ok, out.getvalue()


class FlexRowsTest(unittest.TestCase):
    def setUp(self):
        with fake_flex(SEND_OK, [NOT_READY, REPORT]) as (broker, self.seen):
            self.rows = broker.fetch_transactions(since=date(2026, 10, 1))

    def test_requests_report_with_token_query_and_reference(self):
        send, _, poll2 = self.seen
        self.assertEqual((send["path"], send["t"], send["q"], send["v"]), ("/fws/SendRequest", "tok", "123", "3"))
        self.assertEqual((poll2["path"], poll2["q"]), ("/fws/GetStatement", "42"))

    def test_buy_keeps_fractional_quantity_and_commission(self):
        buy = self.rows[0]
        self.assertEqual(
            (buy["交易日期"], buy["買/賣/股利"], buy["代號"], buy["買入股數"], buy["買入價格"], buy["折讓後手續費"], buy["決策原因"]),
            ("2026/10/23", "買", "CSPX", "0.5152", "700.12", "1.70", "source:ibkr"),
        )

    def test_sell_records_positive_quantity_and_proceeds(self):
        sell = self.rows[1]
        self.assertEqual((sell["買/賣/股利"], sell["賣出股數"], sell["賣出價格"], sell["收入"]), ("賣", "2.0", "250.5", "501.0"))

    def test_dividend_is_net_of_withholding_and_counted_once(self):
        dividends = [r for r in self.rows if r["買/賣/股利"] == "股利"]
        self.assertEqual([(d["交易日期"], d["代號"], d["收入"]) for d in dividends], [("2026/10/16", "AAPL", "7.00")])

    def test_rows_before_since_and_deposits_are_dropped(self):
        self.assertEqual(len(self.rows), 3)


class FlexUnavailableTest(unittest.TestCase):
    def test_expired_token_is_skipped_with_ibkr_reason(self):
        with fake_flex(EXPIRED, []) as (broker, _):
            ok, log = run_stage(lambda: broker.fetch_transactions(date(2026, 1, 1)))
        self.assertFalse(ok)
        self.assertIn("Stage skipped: IBKR (IBKR Flex SendRequest failed (1012: Token has expired.))", log)
        self.assertNotIn("Traceback", log)

    def test_missing_settings_are_skipped(self):
        with mock.patch.dict(os.environ, {"IBKR_FLEX_TOKEN": "", "IBKR_FLEX_QUERY_ID": ""}):
            broker = ibkr.IBKRBroker()
        ok, log = run_stage(lambda: broker.fetch_transactions(date(2026, 1, 1)))
        self.assertFalse(ok)
        self.assertIn("IBKR_FLEX_TOKEN or IBKR_FLEX_QUERY_ID is not set", log)


if __name__ == "__main__":
    unittest.main()
