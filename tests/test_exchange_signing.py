"""Signatures must match what sign.js sends. Vectors come from Node's crypto HMAC
fed the same strings sign.js builds (crypto-js HmacSHA256 is standard HMAC-SHA256)."""

import unittest

from clients.exchanges import Credentials, sign_binance, sign_bitget, sign_okx

CREDS = Credentials(api_key="k", secret_key="test-secret", passphrase="p")
EPOCH_MS = "1763773323456"


class SignJsParity(unittest.TestCase):
    def test_okx(self):
        req = sign_okx(
            CREDS,
            "/api/v5/finance/savings/balance?ccy=USDT",
            "2026-11-22T01:02:03.456Z",
        )
        self.assertEqual(
            req.headers["OK-ACCESS-SIGN"],
            "kEIrzqjmFOhwQ9qA06obiXJ5c5brD6Ktw1osYC74g9A=",
        )
        self.assertEqual(
            req.url, "https://www.okx.com/api/v5/finance/savings/balance?ccy=USDT"
        )

    def test_bitget(self):
        req = sign_bitget(
            CREDS,
            "/api/v2/spot/trade/history-orders?symbol=BTCUSDT&limit=100",
            EPOCH_MS,
        )
        self.assertEqual(
            req.headers["ACCESS-SIGN"], "17KGYvwMSWRlfo/U1LE/rrZswyuwzroB1ZyRRtymMJY="
        )

    def test_binance(self):
        req = sign_binance(
            CREDS,
            "/sapi/v1/simple-earn/flexible/position?asset=&current=1&size=10",
            EPOCH_MS,
        )
        self.assertEqual(
            req.url,
            "https://api.binance.com/sapi/v1/simple-earn/flexible/position?timestamp=1763773323456&asset=&current=1&size=10"
            "&signature=ba2c1caea21a14c5cbddc3320072acb7a439f61ffa0eed13bb92466cd150959b",
        )


if __name__ == "__main__":
    unittest.main()
