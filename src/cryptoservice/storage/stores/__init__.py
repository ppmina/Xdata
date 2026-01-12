"""数据存储层模块.

提供各种数据类型的存储操作。

Stores:
-------
    KlineStore      K线(蜡烛图)数据存储 -> klines 表
    FundingStore    资金费率数据存储 -> funding_rates 表
    InterestStore   持仓量数据存储 -> open_interests 表
    RatioStore      多空比例数据存储 -> long_short_ratios 表

详细表结构请参阅各 Store 模块的头部注释。
"""

from .funding_store import FundingStore
from .interest_store import InterestStore
from .kline_store import KlineStore
from .ratio_store import RatioStore

__all__ = [
    "KlineStore",
    "FundingStore",
    "InterestStore",
    "RatioStore",
]
