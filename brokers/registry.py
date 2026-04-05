"""BrokerRegistry — discovers and manages broker adapters."""

from __future__ import annotations

from brokers.base import BaseBroker


class BrokerRegistry:
    """Central registry for broker adapters.

    Usage:
        registry = BrokerRegistry()
        registry.register(IBKRBroker())
        broker = registry.get("IBKR")
        transactions = broker.fetch_transactions(since=date.today())
    """

    def __init__(self) -> None:
        self._brokers: dict[str, BaseBroker] = {}

    def register(self, broker: BaseBroker) -> None:
        """Register a broker adapter by its name."""
        self._brokers[broker.name] = broker

    def get(self, name: str) -> BaseBroker | None:
        """Return broker by name, or ``None`` if not registered."""
        return self._brokers.get(name)

    def list_brokers(self) -> list[str]:
        """Return all registered broker names."""
        return list(self._brokers.keys())

    @property
    def api_brokers(self) -> list[BaseBroker]:
        """Return only API-based brokers (not gmail_pdf)."""
        return [b for b in self._brokers.values() if b.source_type == "api"]
