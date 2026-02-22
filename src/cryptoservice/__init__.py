"""Cryptocurrency trading bot package."""

from importlib import metadata
from typing import Any

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


def adapt_decimal(d: decimal.Decimal) -> str:
    """Adapt decimal.Decimal to string for SQLite."""
    return str(d)


sqlite3.register_adapter(decimal.Decimal, adapt_decimal)


def __getattr__(name: str) -> Any:
    """Lazily import heavy modules so package import stays lightweight."""
    if name == "BinanceClientFactory":
        from .client import BinanceClientFactory

        return BinanceClientFactory
    if name == "MarketDataService":
        from .services import MarketDataService

        return MarketDataService
    if name == "AsyncMarketDB":
        from .storage import AsyncMarketDB

        return AsyncMarketDB
    if name in {"setup_logging", "get_logger", "LogLevel", "Environment"}:
        from .config import Environment, LogLevel, get_logger, setup_logging

        mapping = {
            "setup_logging": setup_logging,
            "get_logger": get_logger,
            "LogLevel": LogLevel,
            "Environment": Environment,
        }
        return mapping[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
