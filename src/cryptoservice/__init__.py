"""Public package API for cryptoservice."""

from importlib import metadata

__all__ = [
    "BinanceClientFactory",
    "MarketDataService",
    "AsyncMarketDB",
    "setup_logging",
    "get_logger",
    "LogLevel",
    "Environment",
    "__version__",
]

# Best-effort version discovery; falls back to 0.0.0 for editable installs.
try:
    __version__ = metadata.version("cryptoservice")
except metadata.PackageNotFoundError:
    __version__ = "0.0.0"

__author__ = "Minnn"

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
