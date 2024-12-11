from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..models.market_data import MarketTicker, SortBy


class IMarketDataService(ABC):
    """市场数据服务接口."""

    @abstractmethod
    def get_top_coins(
        self, limit: int, sort_by: SortBy, quote_asset: Optional[str]
    ) -> List[MarketTicker]:
        """获取排名靠前的币种数据."""
        pass

    @abstractmethod
    def get_market_summary(self, symbols: List[str]) -> Dict[str, Any]:
        """获取市场概况."""
        pass

    @abstractmethod
    def get_ticker(self, symbol: str) -> MarketTicker:
        """获取单个交易对的行情数据."""
        pass
