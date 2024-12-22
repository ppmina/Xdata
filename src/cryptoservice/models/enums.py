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


class Freq(IntEnum):
    D1 = auto()
    M1 = auto()
    M5 = auto()
    M15 = auto()
    M30 = auto()
    S1 = auto()
    S3 = auto()
    H1 = auto()
    H4 = auto()


class Status(IntEnum):
    NORMAL = auto()
    SUSPEND = auto()
    ST = auto()


class ReturnType(IntEnum):
    C2C = auto()
    V2V = auto()
    V2VM = auto()
