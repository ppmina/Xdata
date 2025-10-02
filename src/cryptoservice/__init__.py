"""Cryptocurrency trading bot package."""

__version__ = "1.12.1"
__author__ = "Minnn"

# 可以在这里导出常用的模块，使得用户可以直接从包根导入
# 全局注册Decimal适配器
import decimal
import sqlite3

from .client import BinanceClientFactory
from .services import MarketDataService
from .storage import AsyncMarketDB


def adapt_decimal(d: decimal.Decimal) -> str:
    """Adapt decimal.Decimal to string for SQLite."""
    return str(d)


sqlite3.register_adapter(decimal.Decimal, adapt_decimal)

# 定义对外暴露的模块
__all__ = [
    "BinanceClientFactory",
    "MarketDataService",
    "AsyncMarketDB",
]
