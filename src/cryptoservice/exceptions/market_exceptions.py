class MarketDataError(Exception):
    """市场数据相关错误的基类."""

    pass


class MarketDataFetchError(MarketDataError):
    """获取市场数据时的错误."""

    pass


class MarketDataParseError(MarketDataError):
    """解析市场数据时的错误."""

    pass


class InvalidSymbolError(MarketDataError):
    """无效的交易对错误."""

    pass


class MarketDataStoreError(MarketDataError):
    """存储市场数据时的错误."""

    pass
