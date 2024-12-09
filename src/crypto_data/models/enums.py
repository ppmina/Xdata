# -*- coding: utf-8 -*-

from enum import IntEnum, auto


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
