"""Cryptocurrency trading bot package."""

__version__ = "0.2.3"
__author__ = "Minnn"

# 可以在这里导出常用的模块，使得用户可以直接从包根导入
from .client import BinanceClientFactory
from .data import StorageUtils
from .interfaces import IMarketDataService
from .services import MarketDataService

# 定义对外暴露的模块
__all__ = [
    "BinanceClientFactory",
    "MarketDataService",
    "IMarketDataService",
    "StorageUtils",
]
