import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, cast

from binance import Client

from crypto_data.config import settings
from crypto_data.exceptions import InvalidSymbolError, MarketDataFetchError
from crypto_data.interfaces import IMarketDataService
from crypto_data.models import MarketTicker, SortBy
from crypto_data.utils import CacheManager, DataConverter

logger = logging.getLogger(__name__)


class MarketDataService(IMarketDataService):
    """市场数据服务实现类."""

    def __init__(self, client: Client) -> None:
        self.client = client
        self.cache = CacheManager(ttl_seconds=settings.CACHE_TTL)
        self.converter = DataConverter()

    def get_ticker(self, symbol: str) -> MarketTicker:
        """获取单个交易对的行情数据."""
        try:
            cached_data = self.cache.get(f"ticker_{symbol}")
            if cached_data:
                return cast(MarketTicker, cached_data)

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
        """获取排名靠前的币种数据."""
        try:
            cache_key = f"top_coins_{limit}_{sort_by.value}_{quote_asset}"
            cached_data = self.cache.get(cache_key)
            if cached_data:
                return cast(List[MarketTicker], cached_data)

            tickers = self.client.get_ticker()
            market_tickers = [MarketTicker.from_binance_ticker(t) for t in tickers]

            if quote_asset:
                market_tickers = [t for t in market_tickers if t.symbol.endswith(quote_asset)]

            sorted_tickers = sorted(
                market_tickers,
                key=lambda x: getattr(x, "quote_volume"),
                reverse=True,
            )[:limit]

            self.cache.set(cache_key, sorted_tickers)
            return sorted_tickers

        except Exception as e:
            logger.error(f"Error getting top coins: {e}")
            raise MarketDataFetchError(f"Failed to get top coins: {e}")

    def get_market_summary(self, symbols: List[str], interval: str = "1d") -> Dict[str, Any]:
        """获取市场概况 实时快照."""
        try:
            cache_key = f"market_summary_{'_'.join(sorted(symbols))}"
            cached_data = self.cache.get(cache_key)
            if cached_data:
                return cast(Dict[str, Any], cached_data)

            summary: Dict[str, Any] = {"snapshot_time": datetime.now(), "data": {}}

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

    def get_historical_data(
        self,
        symbol: str,
        start_time: Union[str, datetime],
        end_time: Optional[Union[str, datetime]] = None,
        interval: str = "1d",
    ) -> List[MarketTicker]:
        """获取历史行情数据."""
        try:
            # 处理时间参数
            if isinstance(start_time, str):
                start_time = datetime.strptime(start_time, "%Y%m%d")
            if isinstance(end_time, str):
                end_time = datetime.strptime(end_time, "%Y%m%d")
            end_time = end_time or datetime.now()

            # 尝试从缓存获取
            cache_key = f"historical_{symbol}_{start_time}_{end_time}_{interval}"
            cached_data = self.cache.get(cache_key)
            if cached_data:
                return cast(List[MarketTicker], cached_data)

            # 从 Binance 获取历史数据
            klines = self.client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                start_str=start_time.strftime("%Y-%m-%d"),
                end_str=end_time.strftime("%Y-%m-%d"),
            )

            # 转换为 MarketTicker 对象
            tickers = [MarketTicker.from_binance_ticker(k) for k in klines]

            self.cache.set(cache_key, tickers)
            return tickers

        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {e}")
            raise MarketDataFetchError(f"Failed to get historical data: {e}")

    def get_orderbook(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """获取订单簿数据."""
        try:
            cache_key = f"orderbook_{symbol}_{limit}"
            cached_data = self.cache.get(cache_key)
            if cached_data:
                return cast(Dict[str, Any], cached_data)

            depth = self.client.get_order_book(symbol=symbol, limit=limit)
            orderbook = {
                "lastUpdateId": depth["lastUpdateId"],
                "bids": depth["bids"],
                "asks": depth["asks"],
                "timestamp": datetime.now(),
            }

            self.cache.set(cache_key, orderbook)
            return orderbook

        except Exception as e:
            logger.error(f"Error getting orderbook for {symbol}: {e}")
            raise MarketDataFetchError(f"Failed to get orderbook: {e}")
