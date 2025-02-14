# -*- coding: utf-8 -*-

from enum import Enum, IntEnum, auto

from binance import enums as binance_enums


class SortBy(Enum):
    """排序方式枚举"""

    VOLUME = "volume"
    PRICE_CHANGE = "price_change"
    PRICE_CHANGE_PERCENT = "price_change_percent"
    QUOTE_VOLUME = "quote_volume"


class InstType(IntEnum):
    """产品类型枚举"""

    UM = auto()
    Margin = auto()
    INDX = auto()
    ETF = auto()


class Market(IntEnum):
    """市场枚举"""

    CN = auto()
    CRYPTO = auto()


class Vendor(IntEnum):
    """供应商枚举"""

    RQ = auto()


class IndustrySrc(IntEnum):
    """行业来源枚举"""

    CITICS = auto()


class OrderStatus(str, Enum):
    """订单状态枚举，与 Binance SDK 保持一致"""

    NEW = binance_enums.ORDER_STATUS_NEW
    PARTIALLY_FILLED = binance_enums.ORDER_STATUS_PARTIALLY_FILLED
    FILLED = binance_enums.ORDER_STATUS_FILLED
    CANCELED = binance_enums.ORDER_STATUS_CANCELED
    PENDING_CANCEL = binance_enums.ORDER_STATUS_PENDING_CANCEL
    REJECTED = binance_enums.ORDER_STATUS_REJECTED
    EXPIRED = binance_enums.ORDER_STATUS_EXPIRED


class OrderType(str, Enum):
    """订单类型枚举"""

    LIMIT = binance_enums.ORDER_TYPE_LIMIT
    MARKET = binance_enums.ORDER_TYPE_MARKET
    STOP_LOSS = binance_enums.ORDER_TYPE_STOP_LOSS
    STOP_LOSS_LIMIT = binance_enums.ORDER_TYPE_STOP_LOSS_LIMIT
    TAKE_PROFIT = binance_enums.ORDER_TYPE_TAKE_PROFIT
    TAKE_PROFIT_LIMIT = binance_enums.ORDER_TYPE_TAKE_PROFIT_LIMIT
    LIMIT_MAKER = binance_enums.ORDER_TYPE_LIMIT_MAKER


class OrderSide(str, Enum):
    """订单方向枚举"""

    BUY = binance_enums.SIDE_BUY
    SELL = binance_enums.SIDE_SELL


class TimeInForce(str, Enum):
    """订单有效期枚举"""

    GTC = binance_enums.TIME_IN_FORCE_GTC
    IOC = binance_enums.TIME_IN_FORCE_IOC
    FOK = binance_enums.TIME_IN_FORCE_FOK
    GTX = binance_enums.TIME_IN_FORCE_GTX


class OrderResponseType(str, Enum):
    """订单响应类型枚举"""

    ACK = binance_enums.ORDER_RESP_TYPE_ACK
    RESULT = binance_enums.ORDER_RESP_TYPE_RESULT
    FULL = binance_enums.ORDER_RESP_TYPE_FULL


class Freq(str, Enum):
    """频率枚举
    s1: 1秒
    m1: 1分钟
    m3: 3分钟
    m5: 5分钟
    m15: 15分钟
    m30: 30分钟
    h1: 1小时
    h2: 2小时
    h4: 4小时
    h6: 6小时
    h8: 8小时
    h12: 12小时
    d1: 1天
    d3: 3天
    w1: 1周
    M1: 1个月
    """

    s1 = binance_enums.KLINE_INTERVAL_1SECOND
    m1 = binance_enums.KLINE_INTERVAL_1MINUTE
    m3 = binance_enums.KLINE_INTERVAL_3MINUTE
    m5 = binance_enums.KLINE_INTERVAL_5MINUTE
    m15 = binance_enums.KLINE_INTERVAL_15MINUTE
    m30 = binance_enums.KLINE_INTERVAL_30MINUTE
    h1 = binance_enums.KLINE_INTERVAL_1HOUR
    h2 = binance_enums.KLINE_INTERVAL_2HOUR
    h4 = binance_enums.KLINE_INTERVAL_4HOUR
    h6 = binance_enums.KLINE_INTERVAL_6HOUR
    h8 = binance_enums.KLINE_INTERVAL_8HOUR
    h12 = binance_enums.KLINE_INTERVAL_12HOUR
    d1 = binance_enums.KLINE_INTERVAL_1DAY
    d3 = binance_enums.KLINE_INTERVAL_3DAY
    w1 = binance_enums.KLINE_INTERVAL_1WEEK
    M1 = binance_enums.KLINE_INTERVAL_1MONTH

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def from_binance(cls, binance_interval: str) -> "Freq":
        """从 Binance 间隔转换为 Freq 枚举"""
        for freq in cls:
            if freq.value == binance_interval:
                return freq
        raise ValueError(f"Invalid Binance interval: {binance_interval}")


class Univ(str, Enum):
    """数据集枚举"""

    CL60 = "CL60"
    CALL = "CALL"


class Status(IntEnum):
    """状态枚举"""

    NORMAL = auto()
    SUSPEND = auto()
    ST = auto()


class ReturnType(IntEnum):
    """回报类型枚举"""

    C2C = auto()
    V2V = auto()
    V2VM = auto()


class HistoricalKlinesType(IntEnum):
    """K线历史数据类型枚举"""

    SPOT = binance_enums.HistoricalKlinesType.SPOT.value
    FUTURES = binance_enums.HistoricalKlinesType.FUTURES.value
    FUTURES_COIN = binance_enums.HistoricalKlinesType.FUTURES_COIN.value

    @classmethod
    def to_binance(cls, kline_type: "HistoricalKlinesType") -> binance_enums.HistoricalKlinesType:
        """转换为 Binance SDK 的 HistoricalKlinesType"""
        return binance_enums.HistoricalKlinesType(kline_type.value)
