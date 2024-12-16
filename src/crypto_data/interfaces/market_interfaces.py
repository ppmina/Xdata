from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from crypto_data.models import MarketTicker, SortBy


class IMarketDataService(ABC):
    """市场数据服务接口."""

    @abstractmethod
    def get_top_coins(
        self,
        limit: int = 100,
        sort_by: SortBy = SortBy.QUOTE_VOLUME,
        quote_asset: Optional[str] = None,
    ) -> List[MarketTicker]:
        """获取排名靠前的币种数据.

        Args:
            limit: 返回的币种数量
            sort_by: 排序依据
            quote_asset: 计价币种 (如 USDT)

        Returns:
            List[MarketTicker]: 排序后的市场数据列表
        """
        pass

    @abstractmethod
    def get_market_summary(self, symbols: List[str], interval: str = "1d") -> Dict[str, Any]:
        """获取市场概况.

        Args:
            symbols: 交易对列表
            interval: 数据间隔

        Returns:
            Dict[str, Any]: 市场概况数据
        """
        pass

    @abstractmethod
    def get_ticker(self, symbol: str) -> MarketTicker:
        """获取单个交易对的行情数据.

        Args:
            symbol: 交易对名称

        Returns:
            MarketTicker: 行情数据

        Raises:
            InvalidSymbolError: 当交易对不存在时
        """
        pass

    @abstractmethod
    def get_historical_data(
        self,
        symbol: str,
        start_time: Union[str, datetime],
        end_time: Optional[Union[str, datetime]] = None,
        interval: str = "1d",
    ) -> List[MarketTicker]:
        """获取历史行情数据.

        Args:
            symbol: 交易对名称
            start_time: 开始时间
            end_time: 结束时间，默认为当前时间
            interval: 数据间隔，如 1m, 5m, 1h, 1d

        Returns:
            List[MarketTicker]: 历史行情数据列表
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
