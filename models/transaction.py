"""Domain models — typed dataclasses for transactions."""

from __future__ import annotations

from dataclasses import dataclass, fields


@dataclass
class Transaction:
    """A single stock / ETF / dividend / crypto transaction row."""

    交易日期: str = ""
    買賣股利: str = ""  # 買/賣/股利 (field alias handled by CSV mapping)
    代號: str = ""
    股票: str = ""
    交易類別: str = ""
    買入股數: str = ""
    買入價格: str = ""
    賣出股數: str = ""
    賣出價格: str = ""
    現價: str = ""
    手續費: str = ""
    折讓後手續費: str = ""
    交易稅: str = ""
    成交價金: str = ""
    交易成本: str = ""
    支出: str = ""
    收入: str = ""
    決策原因: str = ""
    手續費折數: str = ""

    # CSV header uses "買/賣/股利" which isn't a valid Python identifier.
    # We map it explicitly in CSV_FIELD_MAP below.

    @property
    def dedup_key(self) -> tuple[str, ...]:
        return tuple(getattr(self, f.name) for f in fields(self))


@dataclass
class CardTransaction:
    """A single credit-card transaction row."""

    交易日期: str = ""
    入帳日期: str = ""
    卡別: str = ""
    商店名稱: str = ""
    金額: str = ""
    幣別: str = ""
    類別: str = ""

    @property
    def dedup_key(self) -> tuple[str, ...]:
        return tuple(getattr(self, f.name) for f in fields(self))


# Mapping from CSV header names to dataclass field names.
# Only entries that differ need to be listed.
STOCK_CSV_FIELD_MAP: dict[str, str] = {
    "買/賣/股利": "買賣股利",
}

STOCK_CSV_FIELDNAMES: list[str] = [
    "交易日期",
    "買/賣/股利",
    "代號",
    "股票",
    "交易類別",
    "買入股數",
    "買入價格",
    "賣出股數",
    "賣出價格",
    "現價",
    "手續費",
    "折讓後手續費",
    "交易稅",
    "成交價金",
    "交易成本",
    "支出",
    "收入",
    "決策原因",
    "手續費折數",
]

CARD_CSV_FIELDNAMES: list[str] = [
    "交易日期",
    "入帳日期",
    "卡別",
    "商店名稱",
    "金額",
    "幣別",
    "類別",
]
