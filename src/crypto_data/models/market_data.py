from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum


class SortBy(Enum):
    """排序方式枚举."""

    VOLUME = "volume"
    PRICE_CHANGE = "priceChange"
    PRICE_CHANGE_PERCENT = "priceChangePercent"
    QUOTE_VOLUME = "quoteVolume"


@dataclass
class MarketTicker:
    """市场数据模型."""

    symbol: str
    price_change: Decimal
    price_change_percent: Decimal
    weighted_avg_price: Decimal
    prev_close_price: Decimal
    last_price: Decimal
    last_qty: Decimal
    bid_price: Decimal
    ask_price: Decimal
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    volume: Decimal
    quote_volume: Decimal
    timestamp: datetime

    @classmethod
    def from_binance_ticker(cls, ticker: dict) -> "MarketTicker":
        """从Binance ticker数据创建MarketTicker实例."""
        return cls(
            symbol=ticker["symbol"],
            price_change=Decimal(str(ticker["priceChange"])),
            price_change_percent=Decimal(str(ticker["priceChangePercent"])),
            weighted_avg_price=Decimal(str(ticker["weightedAvgPrice"])),
            prev_close_price=Decimal(str(ticker["prevClosePrice"])),
            last_price=Decimal(str(ticker["lastPrice"])),
            last_qty=Decimal(str(ticker["lastQty"])),
            bid_price=Decimal(str(ticker["bidPrice"])),
            ask_price=Decimal(str(ticker["askPrice"])),
            open_price=Decimal(str(ticker["openPrice"])),
            high_price=Decimal(str(ticker["highPrice"])),
            low_price=Decimal(str(ticker["lowPrice"])),
            volume=Decimal(str(ticker["volume"])),
            quote_volume=Decimal(str(ticker["quoteVolume"])),
            timestamp=datetime.now(),
        )
