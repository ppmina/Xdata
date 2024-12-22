from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List


@dataclass
class BaseMarketTicker:
    """市场行情基础数据类.

    Attributes:
        symbol: 交易对
        last_price: 最新价格
    """

    symbol: str
    last_price: Decimal

    def to_dict(self) -> Dict[str, Any]:
        return {key: str(value) for key, value in self.__dict__.items() if not key.startswith("_")}

    def keys(self) -> List[str]:
        return [key for key in self.__dict__.keys() if not key.startswith("_")]

    def get(self, key: str) -> Any:
        return getattr(self, key)


@dataclass
class SymbolTicker(BaseMarketTicker):
    """单个交易币的行情数据类.

    Attributes:
        symbol: 交易对
        last_price: 最新价格
    """

    @classmethod
    def from_binance_ticker(cls, ticker: Dict[str, Any]) -> "SymbolTicker":
        return cls(
            symbol=ticker["symbol"],
            last_price=Decimal(str(ticker["price"])),
        )


@dataclass
class DailyMarketTicker(BaseMarketTicker):
    """24小时行情数据类.

    Attributes:
        symbol: 交易对
        last_price: 最新价格
        price_change: 价格变动
        price_change_percent: 价格变动百分比
        volume: 成交量
        quote_volume: 成交额
        weighted_avg_price: 加权平均价
        prev_close_price: 前收盘价
        bid_price: 买一价
        ask_price: 卖一价
        bid_qty: 买一量
        ask_qty: 卖一量
        open_price: 开盘价
        high_price: 最高价
        low_price: 最低价
        open_time: 开盘时间
        close_time: 收盘时间
        first_id: 第一个ID
        last_id: 最后一个ID
        count: 计数
    """

    price_change: Decimal
    price_change_percent: Decimal
    volume: Decimal
    quote_volume: Decimal
    weighted_avg_price: Decimal
    prev_close_price: Decimal
    bid_price: Decimal
    ask_price: Decimal
    bid_qty: Decimal
    ask_qty: Decimal
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    open_time: int
    close_time: int
    first_id: int
    last_id: int
    count: int

    @classmethod
    def from_binance_ticker(cls, ticker: Dict[str, Any]) -> "DailyMarketTicker":
        return cls(
            symbol=ticker["symbol"],
            last_price=Decimal(str(ticker["lastPrice"])),
            price_change=Decimal(str(ticker["priceChange"])),
            price_change_percent=Decimal(str(ticker["priceChangePercent"])),
            volume=Decimal(str(ticker["volume"])),
            quote_volume=Decimal(str(ticker["quoteVolume"])),
            weighted_avg_price=Decimal(str(ticker["weightedAvgPrice"])),
            prev_close_price=Decimal(str(ticker["prevClosePrice"])),
            bid_price=Decimal(str(ticker["bidPrice"])),
            ask_price=Decimal(str(ticker["askPrice"])),
            bid_qty=Decimal(str(ticker["bidQty"])),
            ask_qty=Decimal(str(ticker["askQty"])),
            open_price=Decimal(str(ticker["openPrice"])),
            high_price=Decimal(str(ticker["highPrice"])),
            low_price=Decimal(str(ticker["lowPrice"])),
            open_time=ticker["openTime"],
            close_time=ticker["closeTime"],
            first_id=ticker["firstId"],
            last_id=ticker["lastId"],
            count=ticker["count"],
        )


@dataclass
class KlineMarketTicker(BaseMarketTicker):
    """K线行情数据类.

    Attributes:
        symbol: 交易对
        last_price: 最新价格
        open_price: 开盘价
        high_price: 最高价
        low_price: 最低价
        volume: 成交量
        close_time: 收盘时间
    """

    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    volume: Decimal
    close_time: int

    @classmethod
    def from_binance_kline(cls, kline_data: list) -> "KlineMarketTicker":
        return cls(
            symbol=kline_data[0],
            last_price=Decimal(str(kline_data[4])),
            open_price=Decimal(str(kline_data[1])),
            high_price=Decimal(str(kline_data[2])),
            low_price=Decimal(str(kline_data[3])),
            volume=Decimal(str(kline_data[5])),
            close_time=kline_data[6],
        )


@dataclass
class PerpetualMarketTicker(BaseMarketTicker):
    """永续合约行情数据类.

    Attributes:
        symbol: 交易对
        last_price: 最新价格
        open_time: 开盘时间
        open_price: 开盘价
        high_price: 最高价
        low_price: 最低价
        volume: 成交量
        close_time: 收盘时间
        quote_volume: 成交额
        count: 计数
        taker_buy_volume: 买方成交量
        taker_buy_quote_volume: 买方成交额
    """

    open_time: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    volume: Decimal
    close_time: int
    quote_volume: Decimal
    count: int
    taker_buy_volume: Decimal
    taker_buy_quote_volume: Decimal

    @classmethod
    def from_binance_futures(cls, symbol: str, kline: list) -> "PerpetualMarketTicker":
        return cls(
            symbol=symbol,
            open_time=kline[0],
            open_price=Decimal(str(kline[1])),
            high_price=Decimal(str(kline[2])),
            low_price=Decimal(str(kline[3])),
            last_price=Decimal(str(kline[4])),
            volume=Decimal(str(kline[5])),
            close_time=kline[6],
            quote_volume=Decimal(str(kline[7])),
            count=kline[8],
            taker_buy_volume=Decimal(str(kline[9])),
            taker_buy_quote_volume=Decimal(str(kline[10])),
        )
