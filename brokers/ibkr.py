"""IBKRBroker — Interactive Brokers adapter (Client Portal API).

This is a skeleton ready for implementation. IBKR Client Portal Gateway
must be running locally (default: https://localhost:5000).

Environment variables:
  IBKR_HOST     — Gateway host (default: localhost)
  IBKR_PORT     — Gateway port (default: 5000)
  IBKR_ACCOUNT  — IBKR account ID

Usage once implemented:
  broker = IBKRBroker()
  transactions = broker.fetch_transactions(since=date(2026, 1, 1))

Reference:
  https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/
"""

from __future__ import annotations

import os
from datetime import date
from typing import Literal

from brokers.base import BaseBroker


class IBKRBroker(BaseBroker):
    """Interactive Brokers data source via Client Portal REST API."""

    name = "IBKR"
    source_type: Literal["api"] = "api"

    def __init__(self) -> None:
        self.host = os.getenv("IBKR_HOST", "localhost")
        self.port = os.getenv("IBKR_PORT", "5000")
        self.account = os.getenv("IBKR_ACCOUNT", "")
        self.base_url = f"https://{self.host}:{self.port}/v1/api"

    @property
    def _headers(self) -> dict[str, str]:
        return {"Content-Type": "application/json"}

    def fetch_transactions(self, since: date) -> list[dict]:
        """Fetch trades from IBKR Client Portal API.

        TODO: Implement once IBKR gateway is available.

        Steps:
          1. GET /portfolio/{accountId}/ledger  — check connection
          2. GET /iserver/account/trades        — recent executions
             or POST /pa/transactions           — historical with date range
          3. Map IBKR fields to CSV_FIELDNAMES:
               - tradeTime    → 交易日期
               - side (BUY/SELL) → 買/賣/股利
               - symbol       → 代號
               - description  → 股票
               - "ETF"/"一般" → 交易類別
               - quantity     → 買入股數 / 賣出股數
               - price        → 買入價格 / 賣出價格
               - commission   → 手續費
               - netAmount    → 收入 (for sells)
        """
        raise NotImplementedError(
            "IBKRBroker.fetch_transactions() is not yet implemented. "
            "Set up IBKR Client Portal Gateway and complete the API integration."
        )
