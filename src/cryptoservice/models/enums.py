"""枚举类型定义."""

from enum import Enum, IntEnum, auto


class SortBy(Enum):
    """排序方式枚举."""

    VOLUME = "volume"
    PRICE_CHANGE = "price_change"
    PRICE_CHANGE_PERCENT = "price_change_percent"
    QUOTE_VOLUME = "quote_volume"


class InstType(IntEnum):
    """产品类型枚举."""

    UM = auto()
    Margin = auto()
    INDX = auto()
    ETF = auto()


class Market(IntEnum):
    """市场枚举."""

    CN = auto()
    CRYPTO = auto()


class Vendor(IntEnum):
    """供应商枚举."""

    RQ = auto()


class IndustrySrc(IntEnum):
    """行业来源枚举."""

    CITICS = auto()


class OrderStatus(str, Enum):
    """订单状态枚举，与 Binance SDK 保持一致."""

    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    PENDING_CANCEL = "PENDING_CANCEL"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class OrderType(str, Enum):
    """订单类型枚举."""

    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_LOSS = "STOP_LOSS"
    STOP_LOSS_LIMIT = "STOP_LOSS_LIMIT"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT"
    LIMIT_MAKER = "LIMIT_MAKER"


class OrderSide(str, Enum):
    """订单方向枚举."""

    BUY = "BUY"
    SELL = "SELL"


class TimeInForce(str, Enum):
    """订单有效期枚举."""

    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    GTX = "GTX"


class OrderResponseType(str, Enum):
    """订单响应类型枚举."""

    ACK = "ACK"
    RESULT = "RESULT"
    FULL = "FULL"


class Freq(str, Enum):
    """频率枚举.

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
    M1: 1个月.
    """

    s1 = "1s"
    m1 = "1m"
    m3 = "3m"
    m5 = "5m"
    m15 = "15m"
    m30 = "30m"
    h1 = "1h"
    h2 = "2h"
    h4 = "4h"
    h6 = "6h"
    h8 = "8h"
    h12 = "12h"
    d1 = "1d"
    d3 = "3d"
    w1 = "1w"
    M1 = "1M"

    def __str__(self) -> str:
        """返回枚举值的字符串表示形式."""
        return str(self.value)

    @classmethod
    def from_binance(cls, binance_interval: str) -> "Freq":
        """从 Binance 间隔转换为 Freq 枚举."""
        for freq in cls:
            if freq.value == binance_interval:
                return freq
        raise ValueError(f"Invalid Binance interval: {binance_interval}")

    @classmethod
    def from_string(cls, interval_str: str) -> "Freq":
        """从字符串间隔转换为 Freq 枚举.

        Args:
            interval_str: 间隔字符串，如 "1m", "5m", "1h", "1d" 等

        Returns:
            对应的 Freq 枚举值

        Raises:
            ValueError: 如果间隔字符串无效
        """
        # 保存原始字符串用于大小写敏感比较
        original_str = interval_str.strip()

        # 处理大小写敏感的月份
        if original_str == "1M":
            return cls.M1

        # 标准化输入字符串为小写
        interval_str = original_str.lower()

        # 映射字典
        string_to_freq = {
            "1s": cls.s1,
            "1m": cls.m1,
            "3m": cls.m3,
            "5m": cls.m5,
            "15m": cls.m15,
            "30m": cls.m30,
            "1h": cls.h1,
            "2h": cls.h2,
            "4h": cls.h4,
            "6h": cls.h6,
            "8h": cls.h8,
            "12h": cls.h12,
            "1d": cls.d1,
            "3d": cls.d3,
            "1w": cls.w1,
        }

        result = string_to_freq.get(interval_str)
        if result is None:
            raise ValueError(f"Invalid interval string: {original_str}")
        return result


class Univ(str, Enum):
    """数据集枚举."""

    CL60 = "CL60"
    CALL = "CALL"


class Status(IntEnum):
    """状态枚举."""

    NORMAL = auto()
    SUSPEND = auto()
    ST = auto()


class ReturnType(IntEnum):
    """回报类型枚举."""

    C2C = auto()
    V2V = auto()
    V2VM = auto()


class HistoricalKlinesType(IntEnum):
    """K线历史数据类型枚举."""

    SPOT = 1
    FUTURES = 2
    FUTURES_COIN = 3


class ErrorSeverity(Enum):
    """错误严重程度枚举."""

    LOW = "low"  # 可忽略的错误
    MEDIUM = "medium"  # 需要重试的错误
    HIGH = "high"  # 严重错误，需要立即处理
    CRITICAL = "critical"  # 致命错误，停止执行
