import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, cast, overload

import pandas as pd

from cryptoservice.client import BinanceClientFactory
from cryptoservice.config import settings
from cryptoservice.data import StorageUtils
from cryptoservice.exceptions import InvalidSymbolError, MarketDataFetchError, MarketDataStoreError
from cryptoservice.interfaces import IMarketDataService
from cryptoservice.models import (
    DailyMarketTicker,
    Freq,
    HistoricalKlinesType,
    KlineMarketTicker,
    PerpetualMarketTicker,
    SortBy,
    SymbolTicker,
)
from cryptoservice.utils import CacheManager, DataConverter

logger = logging.getLogger(__name__)


class MarketDataService(IMarketDataService):
    """市场数据服务实现类."""

    def __init__(self, api_key: str, api_secret: str) -> None:
        """初始化市场数据服务.

        Args:
            api_key: 用户API密钥
            api_secret: 用户API密钥
        """
        self.client = BinanceClientFactory.create_client(api_key, api_secret)
        self.cache = CacheManager(ttl_seconds=settings.CACHE_TTL)
        self.converter = DataConverter()

    @overload
    def get_symbol_ticker(self, symbol: str) -> SymbolTicker:
        ...

    @overload
    def get_symbol_ticker(self) -> List[SymbolTicker]:
        ...

    def get_symbol_ticker(self, symbol: str | None = None) -> SymbolTicker | List[SymbolTicker]:
        try:
            cached_data = self.cache.get(f"ticker_{symbol}")
            if cached_data:
                return cast(Union[SymbolTicker, List[SymbolTicker]], cached_data)

            ticker = self.client.get_symbol_ticker(symbol=symbol)
            if not ticker:
                raise InvalidSymbolError(f"Invalid symbol: {symbol}")

            if isinstance(ticker, list):
                market_tickers: Union[SymbolTicker, List[SymbolTicker]] = [
                    SymbolTicker.from_binance_ticker(t) for t in ticker
                ]
            else:
                market_tickers = SymbolTicker.from_binance_ticker(ticker)
            self.cache.set(f"ticker_{symbol}", market_tickers)
            return market_tickers

        except Exception as e:
            logger.error(f"Error fetching ticker for {symbol}: {e}")
            raise MarketDataFetchError(f"Failed to fetch ticker: {e}")

    def get_top_coins(
        self,
        limit: int = settings.DEFAULT_LIMIT,
        sort_by: SortBy = SortBy.QUOTE_VOLUME,
        quote_asset: Optional[str] = None,
    ) -> List[DailyMarketTicker]:
        try:
            cache_key = f"top_coins_{limit}_{sort_by.value}_{quote_asset}"
            cached_data = self.cache.get(cache_key)
            if cached_data:
                return cast(List[DailyMarketTicker], cached_data)

            tickers = self.client.get_ticker()
            market_tickers = [DailyMarketTicker.from_binance_ticker(t) for t in tickers]

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

    def get_market_summary(self, interval: Freq = Freq.d1) -> Dict[str, Any]:
        try:
            cache_key = f"market_summary_{interval}"
            cached_data = self.cache.get(cache_key)
            if cached_data:
                return cast(Dict[str, Any], cached_data)

            summary: Dict[str, Any] = {"snapshot_time": datetime.now(), "data": {}}

            tickers = [ticker.to_dict() for ticker in self.get_symbol_ticker()]
            summary["data"] = tickers

            self.cache.set(cache_key, summary)
            return summary

        except Exception as e:
            logger.error(f"Error getting market summary: {e}")
            raise MarketDataFetchError(f"Failed to get market summary: {e}")

    def get_historical_klines(
        self,
        symbol: str,
        start_time: str | datetime,
        end_time: str | datetime | None = None,
        interval: Freq = Freq.h1,
        klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT,
    ) -> List[KlineMarketTicker]:
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
                return cast(List[KlineMarketTicker], cached_data)

            # 从 Binance 获取历史数据
            klines = self.client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                start_str=start_time.strftime("%Y-%m-%d"),
                end_str=end_time.strftime("%Y-%m-%d"),
                limit=1000,
                klines_type=HistoricalKlinesType.to_binance(klines_type),
            )

            # 转换为 MarketTicker 对象
            tickers = [KlineMarketTicker.from_binance_kline(k) for k in klines]

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

    def get_perpetual_data(
        self,
        symbols: List[str],
        start_time: str,
        end_time: str | None = None,
        freq: Freq = Freq.h1,
        store: bool = False,
        batch_size: int = 500,
        market: str = "SWAP",
        features: Optional[List[str]] = None,
    ) -> List[PerpetualMarketTicker]:
        try:
            all_data = []
            start_ts = int(pd.Timestamp(start_time).timestamp() * 1000)
            end_ts = int(pd.Timestamp(end_time).timestamp() * 1000)

            for symbol in symbols:
                data = []
                current_ts = start_ts

                while current_ts < end_ts:
                    # 获取一批数据
                    klines = self.client.futures_historical_klines(
                        symbol=symbol,
                        interval=freq,
                        start_str=current_ts,
                        end_str=end_ts,
                        limit=batch_size,
                    )

                    if not klines:
                        break

                    # 转换为 MarketTicker 对象
                    tickers = [
                        PerpetualMarketTicker.from_binance_futures(symbol, k) for k in klines
                    ]
                    data.extend(tickers)

                    # 更新时间戳
                    current_ts = klines[-1][6] + 1

                all_data.extend(data)

            if store:
                try:
                    # 按日期分组
                    grouped_data: Dict[str, List[PerpetualMarketTicker]] = {}
                    for ticker in all_data:
                        if ticker.open_time is None:
                            continue
                        date = datetime.fromtimestamp(ticker.open_time // 1000).strftime("%Y%m%d")
                        if date not in grouped_data:
                            grouped_data[date] = []
                        grouped_data[date].append(ticker)

                    # 存储交易对信息
                    StorageUtils.store_universe(symbols, market)

                    # 存储每个特征的数据
                    if features is None:
                        features = ["cls", "hgh", "low", "opn", "vwap", "vol", "amt", "num"]

                    for date, daily_data in grouped_data.items():
                        print([key for key in daily_data[0].__dict__.keys()], date)
                        for feature in features:
                            StorageUtils.store_feature_data(
                                daily_data, date, freq, market, feature, symbols
                            )

                    logger.info("数据存储完成")
                except Exception as e:
                    logger.error(f"Error storing perpetual data: {e}")
                    raise MarketDataStoreError(f"Failed to store perpetual data: {e}")

            return all_data

        except MarketDataStoreError:
            raise  # 直接重新抛出存储错误
        except Exception as e:
            logger.error(f"Error fetching perpetual data: {e}")
            raise MarketDataFetchError(f"Failed to fetch perpetual data: {e}")
