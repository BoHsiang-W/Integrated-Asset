"""Broker/exchange adapters — Strategy pattern for data sources."""

from brokers.etrade import ETradeBroker
from brokers.ibkr import IBKRBroker

__all__ = ["IBKRBroker", "ETradeBroker"]
