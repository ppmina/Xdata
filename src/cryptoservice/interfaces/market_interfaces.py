from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, overload

from cryptoservice.models import (
    DailyMarketTicker,
    Freq,
    KlineMarketTicker,
    PerpetualMarketTicker,
    SortBy,
    SymbolTicker,
)


class IMarketDataService(ABC):
    """市场数据服务接口."""

    @abstractmethod
    def get_top_coins(
        self,
        limit: int = 100,
        sort_by: SortBy = SortBy.QUOTE_VOLUME,
        quote_asset: Optional[str] = None,
    ) -> List[DailyMarketTicker]:
        """获取排名靠前的币种数据.

        Args:
            limit: 返回的币种数量
            sort_by: 排序依据
            quote_asset: 计价币种 (如 USDT)

        Returns:
            List[DailyMarketTicker]: 排序后的市场数据列表
        """
        pass

    @abstractmethod
    def get_market_summary(self, interval: Freq = Freq.d1) -> Dict[str, Any]:
        """获取市场概况.

        Args:
            symbols: 交易对列表
            interval: 数据间隔

        Returns:
            Dict[str, Any]: 市场概况数据
        """
        pass

    @overload
    def get_symbol_ticker(self, symbol: str) -> SymbolTicker:
        ...

    @overload
    def get_symbol_ticker(self) -> List[SymbolTicker]:
        ...

    @abstractmethod
    def get_symbol_ticker(self, symbol: str | None = None) -> SymbolTicker | List[SymbolTicker]:
        """获取单个或多个交易币的行情数据.

        Args:
            symbol: 交易对名称，如果为 None 则返回所有交易对数据

        Returns:
            - 当 symbol 指定时：返回单个 SymbolTicker
            - 当 symbol 为 None 时：返回 SymbolTicker 列表

        Raises:
            InvalidSymbolError: 当指定的交易对不存在时
        """
        pass

    @abstractmethod
    def get_historical_klines(
        self,
        symbol: str,
        start_time: Union[str, datetime],
        end_time: Optional[Union[str, datetime]] = None,
        interval: Freq = Freq.d1,
    ) -> List[KlineMarketTicker]:
        """获取历史行情数据.

        Args:
            symbol: 交易对名称
            start_time: 开始时间
            end_time: 结束时间，默认为当前时间
            interval: 数据间隔，如 1m, 5m, 1h, 1d

        Returns:
            List[KlineMarketTicker]: 历史行情数据列表
        """
        pass

    @abstractmethod
    def get_orderbook(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """获取订单簿数据.

        Args:
            symbol: 交易对名称
            limit: 订单簿深度

        Returns:
            Dict[str, Any]: 订单簿数据，包含 bids 和 asks
        """
        pass

    @abstractmethod
    def get_perpetual_data(
        self,
        symbols: List[str],
        start_time: str,  # YYYYMMDD
        end_time: str | None = None,  # YYYYMMDD
        freq: Freq = Freq.h1,
        store: bool = False,
        batch_size: int = 500,
    ) -> List[PerpetualMarketTicker]:
        """获取永续合约历史数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYYMMDD)
            end_time: 结束时间 (YYYYMMDD)
            freq: 数据频率 (1m, 1h, 4h, 1d等)
            store: 是否存储数据
            batch_size: 每次请求的数据量
            market: 市场类型 (如 'SWAP')
            features: 需要存储的特征列表 ['cls', 'hgh', 'low', 'opn', 'vwap', 'vol', 'amt', 'num']

        Returns:
            List[PerpetualMarketTicker]: 市场数据列表
        """
        pass
