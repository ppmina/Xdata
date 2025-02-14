from typing import Optional


class MarketDataError(Exception):
    """市场数据相关错误的基类."""

    pass


class MarketDataFetchError(MarketDataError):
    """获取市场数据时的错误."""

    def __init__(self, message: str, cause: Optional[Exception] = None):
        super().__init__(message)
        self.cause = cause


class MarketDataParseError(MarketDataError):
    """解析市场数据时的错误."""

    pass


class InvalidSymbolError(MarketDataFetchError):
    """无效的交易对错误."""

    pass


class MarketDataStoreError(MarketDataError):
    """存储市场数据时的错误."""

    pass


class RateLimitError(MarketDataFetchError):
    """API请求速率限制错误."""

    pass
