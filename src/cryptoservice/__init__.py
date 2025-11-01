"""Cryptocurrency trading bot package."""

from importlib import metadata

try:
    __version__ = metadata.version("cryptoservice")
except metadata.PackageNotFoundError:
    # Package is not installed; fallback helps during local development.
    __version__ = "0.0.0"

__author__ = "Minnn"

# 可以在这里导出常用的模块，使得用户可以直接从包根导入
# 全局注册Decimal适配器
import decimal
import sqlite3

from .client import BinanceClientFactory
from .config import Environment, LogLevel, get_logger, setup_logging
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
    "setup_logging",
    "get_logger",
    "LogLevel",
    "Environment",
]
