"""市场行情数据模型."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast


@dataclass
class BaseMarketTicker:
    """市场行情基础数据类.

    Attributes:
        symbol: 交易对
        last_price: 最新价格
    """

    symbol: str
    last_price: Decimal

    def to_dict(self) -> dict[str, Any]:
        """将数据类实例转换为字典."""
        return {key: str(value) for key, value in self.__dict__.items() if not key.startswith("_")}

    def keys(self) -> list[str]:
        """获取属性列表."""
        return [key for key in self.__dict__ if not key.startswith("_")]

    def get(self, key: str) -> Any:
        """获取属性值."""
        return getattr(self, key)


@dataclass
class SymbolTicker(BaseMarketTicker):
    """单个交易币的行情数据类.

    Attributes:
        symbol: 交易对
        last_price: 最新价格
    """

    @classmethod
    def from_binance_ticker(cls, ticker: dict[str, Any]) -> "SymbolTicker":
        """从Binance API响应创建SymbolTicker实例."""
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
    def from_binance_ticker(cls, ticker: dict[str, Any]) -> "DailyMarketTicker":
        """从Binance API响应创建DailyMarketTicker实例."""
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
    """K线行情数据基类（完整的Binance K线数据）.

    包含现货和期货K线的共同字段。

    Attributes:
        symbol: 交易对
        open_time: 开盘时间（毫秒时间戳）
        open_price: 开盘价
        high_price: 最高价
        low_price: 最低价
        close_price: 收盘价
        volume: 成交量
        close_time: 收盘时间（毫秒时间戳）
        quote_volume: 成交额（计价货币）
        trades_count: 成交笔数
        taker_buy_volume: 买方成交量（主动买入）
        taker_buy_quote_volume: 买方成交额（主动买入）
    """

    open_time: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    close_time: int
    quote_volume: Decimal
    trades_count: int
    taker_buy_volume: Decimal
    taker_buy_quote_volume: Decimal

    @classmethod
    def from_binance_kline(cls, symbol: str, kline_data: list) -> "KlineMarketTicker":
        """从Binance API响应创建KlineMarketTicker实例.

        Args:
            symbol: 交易对符号
            kline_data: Binance K线数据数组
                [0] open_time, [1] open, [2] high, [3] low, [4] close,
                [5] volume, [6] close_time, [7] quote_volume, [8] trades_count,
                [9] taker_buy_volume, [10] taker_buy_quote_volume, [11] ignore

        Returns:
            KlineMarketTicker 实例
        """
        close_price = Decimal(str(kline_data[4]))
        return cls(
            symbol=symbol,
            last_price=close_price,  # last_price 等同于 close_price
            open_time=int(kline_data[0]),
            open_price=Decimal(str(kline_data[1])),
            high_price=Decimal(str(kline_data[2])),
            low_price=Decimal(str(kline_data[3])),
            close_price=close_price,
            volume=Decimal(str(kline_data[5])),
            close_time=int(kline_data[6]),
            quote_volume=Decimal(str(kline_data[7])),
            trades_count=int(kline_data[8]),
            taker_buy_volume=Decimal(str(kline_data[9])),
            taker_buy_quote_volume=Decimal(str(kline_data[10])),
        )


@dataclass
class SpotKlineTicker(KlineMarketTicker):
    """现货K线行情数据类.

    专门用于现货市场的K线数据，继承基类的所有字段。
    未来可以添加现货特有的字段（如借贷利率等）。
    """

    @classmethod
    def from_binance_kline(cls, symbol: str, kline_data: list) -> "SpotKlineTicker":
        """从Binance API响应创建SpotKlineTicker实例."""
        return cast("SpotKlineTicker", super().from_binance_kline(symbol, kline_data))


@dataclass
class FuturesKlineTicker(KlineMarketTicker):
    """期货K线行情数据类.

    专门用于期货市场的K线数据，继承基类的所有字段。
    未来可以添加期货特有的字段（如标记价格、资金费率预测等）。
    """

    @classmethod
    def from_binance_kline(cls, symbol: str, kline_data: list) -> "FuturesKlineTicker":
        """从Binance API响应创建FuturesKlineTicker实例."""
        return cast("FuturesKlineTicker", super().from_binance_kline(symbol, kline_data))


class KlineIndex:
    """K线数据索引定义.

    Attributes:
        OPEN_TIME: 开盘时间
        OPEN: 开盘价
        HIGH: 最高价
        LOW: 最低价
        CLOSE: 收盘价
        VOLUME: 成交量
        CLOSE_TIME: 收盘时间
        QUOTE_VOLUME: 成交额
        TRADES_COUNT: 成交笔数
        TAKER_BUY_VOLUME: 买方成交量
        TAKER_BUY_QUOTE_VOLUME: 买方成交额
        IGNORE: 忽略
    """

    OPEN_TIME = 0
    OPEN = 1
    HIGH = 2
    LOW = 3
    CLOSE = 4
    VOLUME = 5
    CLOSE_TIME = 6
    QUOTE_VOLUME = 7
    TRADES_COUNT = 8
    TAKER_BUY_VOLUME = 9
    TAKER_BUY_QUOTE_VOLUME = 10
    IGNORE = 11


class PerpetualMarketTicker:
    """永续合约行情数据."""

    __slots__ = (
        "symbol",
        "open_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "close_time",
        "quote_volume",
        "trades_count",
        "taker_buy_volume",
        "taker_buy_quote_volume",
    )

    def __init__(
        self,
        symbol: str,
        open_time: int,
        open_price: Decimal,
        high_price: Decimal,
        low_price: Decimal,
        close_price: Decimal,
        volume: Decimal,
        close_time: int,
        quote_volume: Decimal,
        trades_count: int,
        taker_buy_volume: Decimal,
        taker_buy_quote_volume: Decimal,
    ):
        """Initialize the PerpetualMarketTicker."""
        self.symbol = symbol
        self.open_time = open_time
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.volume = volume
        self.close_time = close_time
        self.quote_volume = quote_volume
        self.trades_count = trades_count
        self.taker_buy_volume = taker_buy_volume
        self.taker_buy_quote_volume = taker_buy_quote_volume

    @classmethod
    def from_binance_kline(cls, symbol: str, kline: list[Any]) -> "PerpetualMarketTicker":
        """从 Binance K线数据创建实例."""
        return cls(
            symbol=symbol,
            open_time=int(kline[0]),
            open_price=Decimal(str(kline[1])),
            high_price=Decimal(str(kline[2])),
            low_price=Decimal(str(kline[3])),
            close_price=Decimal(str(kline[4])),
            volume=Decimal(str(kline[5])),
            close_time=int(kline[6]),
            quote_volume=Decimal(str(kline[7])),
            trades_count=int(kline[8]),
            taker_buy_volume=Decimal(str(kline[9])),
            taker_buy_quote_volume=Decimal(str(kline[10])),
        )
