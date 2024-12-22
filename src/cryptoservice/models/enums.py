# -*- coding: utf-8 -*-

from enum import Enum, IntEnum, auto


class SortBy(Enum):
    """排序方式枚举."""

    VOLUME = "volume"
    PRICE_CHANGE = "priceChange"
    PRICE_CHANGE_PERCENT = "priceChangePercent"
    QUOTE_VOLUME = "quoteVolume"


class InstType(IntEnum):
    UM = auto()
    Margin = auto()
    INDX = auto()
    ETF = auto()


class Market(IntEnum):
    CN = auto()
    CRYPTO = auto()


class Vendor(IntEnum):
    RQ = auto()


class IndustrySrc(IntEnum):
    CITICS = auto()


class Freq(str, Enum):
    """频率枚举."""

    m1 = "1m"
    m3 = "3m"
    m5 = "5m"
    m15 = "15m"
    m30 = "30m"
    h1 = "1h"
    h2 = "2h"
    h4 = "4h"
    h6 = "6h"
    h8 = "8h"
    h12 = "12h"
    d1 = "1d"
    d3 = "3d"
    w1 = "1w"
    M1 = "1M"

    def __str__(self) -> str:
        return self.value


class Status(IntEnum):
    NORMAL = auto()
    SUSPEND = auto()
    ST = auto()


class ReturnType(IntEnum):
    C2C = auto()
    V2V = auto()
    V2VM = auto()
