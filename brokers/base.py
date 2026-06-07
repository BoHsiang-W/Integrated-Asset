"""BaseBroker — Abstract interface for all data source adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Literal


ETF_SYMBOLS: frozenset[str] = frozenset(
    {
        "VOO",
        "QQQ",
        "QQQM",
        "VGT",
        "CSPX",
        "SMH",
        "SPY",
        "IVV",
        "EFA",
        "EEM",
        "VEA",
        "VWO",
        "IEMG",
        "AGG",
        "BND",
        "LQD",
        "VNQ",
        "GLD",
        "SLV",
    }
)


class BaseBroker(ABC):
    """Common interface for all broker/exchange data sources.

    Two source types:
      - ``"gmail_pdf"`` — Statements arrive as encrypted PDF email attachments.
        Data flows through Gmail → decrypt → Gemini extraction.
      - ``"api"`` — Transactions are fetched directly via a broker REST API.
        No Gmail, no PDF, no Gemini involved.
    """

    name: str
    source_type: Literal["gmail_pdf", "api"]

    @staticmethod
    def classify_symbol_category(symbol: str) -> str:
        """Return ``ETF`` for known ETF tickers, otherwise ``一般``."""
        normalized = (symbol or "").strip().upper()
        return "ETF" if normalized in ETF_SYMBOLS else "一般"

    @abstractmethod
    def fetch_transactions(self, since: date) -> list[dict]:
        """Return transactions since *since* as a list of CSV-compatible dicts.

        Each dict should have keys matching ``STOCK_CSV_FIELDNAMES``.
        """
