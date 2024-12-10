from typing import List, Optional, Dict, Any
import logging
from datetime import datetime

from crypto_data.interfaces import IMarketDataService
from crypto_data.models import MarketTicker, SortBy
from crypto_data.exceptions import MarketDataFetchError, InvalidSymbolError
from crypto_data.utils.cache_manager import CacheManager
from crypto_data.utils.data_converter import DataConverter
from crypto_data.config.settings import settings

logger = logging.getLogger(__name__)


class MarketDataService(IMarketDataService):
    """市场数据服务实现类"""

    def __init__(self, client):
        self.client = client
        self.cache = CacheManager(ttl_seconds=settings.CACHE_TTL)
        self.converter = DataConverter()

    def get_ticker(self, symbol: str) -> MarketTicker:
        """获取单个交易对的行情数据"""
        try:
            cached_data = self.cache.get(f"ticker_{symbol}")
            if cached_data:
                return cached_data

            ticker = self.client.get_symbol_ticker(symbol=symbol)
            if not ticker:
                raise InvalidSymbolError(f"Invalid symbol: {symbol}")

            market_ticker = MarketTicker.from_binance_ticker(ticker)
            self.cache.set(f"ticker_{symbol}", market_ticker)
            return market_ticker

        except Exception as e:
            logger.error(f"Error fetching ticker for {symbol}: {e}")
            raise MarketDataFetchError(f"Failed to fetch ticker: {e}")

    def get_top_coins(
        self,
        limit: int = settings.DEFAULT_LIMIT,
        sort_by: SortBy = SortBy.QUOTE_VOLUME,
        quote_asset: Optional[str] = None,
    ) -> List[MarketTicker]:
        """获取排名靠前的币种数据"""
        try:
            cache_key = f"top_coins_{limit}_{sort_by.value}_{quote_asset}"
            cached_data = self.cache.get(cache_key)
            if cached_data:
                return cached_data

            tickers = self.client.get_ticker()
            market_tickers = [MarketTicker.from_binance_ticker(t) for t in tickers]

            if quote_asset:
                market_tickers = [
                    t for t in market_tickers if t.symbol.endswith(quote_asset)
                ]

            sorted_tickers = sorted(
                market_tickers, key=lambda x: getattr(x, "quote_volume"), reverse=True
            )[:limit]

            self.cache.set(cache_key, sorted_tickers)
            return sorted_tickers

        except Exception as e:
            logger.error(f"Error getting top coins: {e}")
            raise MarketDataFetchError(f"Failed to get top coins: {e}")

    def get_market_summary(self, symbols: List[str]) -> Dict[str, Any]:
        """获取市场概况"""
        try:
            cache_key = f"market_summary_{'_'.join(sorted(symbols))}"
            cached_data = self.cache.get(cache_key)
            if cached_data:
                return cached_data

            summary = {"timestamp": datetime.now(), "data": {}}

            for symbol in symbols:
                ticker = self.get_ticker(symbol)
                summary["data"][symbol] = self.converter.format_market_data(
                    {
                        "price": ticker.last_price,
                        "volume": ticker.quote_volume,
                        "priceChangePercent": ticker.price_change_percent,
                        "highPrice": ticker.high_price,
                        "lowPrice": ticker.low_price,
                    }
                )

            self.cache.set(cache_key, summary)
            return summary

        except Exception as e:
            logger.error(f"Error getting market summary: {e}")
            raise MarketDataFetchError(f"Failed to get market summary: {e}")
