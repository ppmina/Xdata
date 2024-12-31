import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, overload

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from cryptoservice.client import BinanceClientFactory
from cryptoservice.config import settings
from cryptoservice.data import StorageUtils
from cryptoservice.exceptions import InvalidSymbolError, MarketDataFetchError
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
from cryptoservice.utils import DataConverter

# 配置 rich logger
logging.basicConfig(
    level=logging.INFO, format="%(message)s", handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)


class MarketDataService(IMarketDataService):
    """市场数据服务实现类."""

    def __init__(self, api_key: str, api_secret: str) -> None:
        """初始化市场数据服务."""
        self.client = BinanceClientFactory.create_client(api_key, api_secret)
        self.converter = DataConverter()
        self.console = Console()

    @overload
    def get_symbol_ticker(self, symbol: str) -> SymbolTicker: ...

    @overload
    def get_symbol_ticker(self) -> List[SymbolTicker]: ...

    def get_symbol_ticker(self, symbol: str | None = None) -> SymbolTicker | List[SymbolTicker]:
        try:
            ticker = self.client.get_symbol_ticker(symbol=symbol)
            if not ticker:
                raise InvalidSymbolError(f"Invalid symbol: {symbol}")

            if isinstance(ticker, list):
                return [SymbolTicker.from_binance_ticker(t) for t in ticker]
            return SymbolTicker.from_binance_ticker(ticker)

        except Exception as e:
            logger.error(f"[red]Error fetching ticker for {symbol}: {e}[/red]")
            raise MarketDataFetchError(f"Failed to fetch ticker: {e}")

    def get_top_coins(
        self,
        limit: int = settings.DEFAULT_LIMIT,
        sort_by: SortBy = SortBy.QUOTE_VOLUME,
        quote_asset: Optional[str] = None,
    ) -> List[DailyMarketTicker]:
        try:
            tickers = self.client.get_ticker()
            market_tickers = [DailyMarketTicker.from_binance_ticker(t) for t in tickers]

            if quote_asset:
                market_tickers = [t for t in market_tickers if t.symbol.endswith(quote_asset)]

            return sorted(
                market_tickers,
                key=lambda x: getattr(x, "quote_volume"),
                reverse=True,
            )[:limit]

        except Exception as e:
            logger.error(f"[red]Error getting top coins: {e}[/red]")
            raise MarketDataFetchError(f"Failed to get top coins: {e}")

    def get_market_summary(self, interval: Freq = Freq.d1) -> Dict[str, Any]:
        try:
            summary: Dict[str, Any] = {"snapshot_time": datetime.now(), "data": {}}
            tickers = [ticker.to_dict() for ticker in self.get_symbol_ticker()]
            summary["data"] = tickers
            return summary

        except Exception as e:
            logger.error(f"[red]Error getting market summary: {e}[/red]")
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
            if isinstance(start_time, str):
                start_time = datetime.strptime(start_time, "%Y%m%d")
            if isinstance(end_time, str):
                end_time = datetime.strptime(end_time, "%Y%m%d")
            end_time = end_time or datetime.now()

            klines = self.client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                start_str=start_time.strftime("%Y-%m-%d"),
                end_str=end_time.strftime("%Y-%m-%d"),
                limit=1000,
                klines_type=HistoricalKlinesType.to_binance(klines_type),
            )

            return [KlineMarketTicker.from_binance_kline(k) for k in klines]

        except Exception as e:
            logger.error(f"[red]Error getting historical data for {symbol}: {e}[/red]")
            raise MarketDataFetchError(f"Failed to get historical data: {e}")

    def get_orderbook(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """获取订单簿数据."""
        try:
            depth = self.client.get_order_book(symbol=symbol, limit=limit)
            return {
                "lastUpdateId": depth["lastUpdateId"],
                "bids": depth["bids"],
                "asks": depth["asks"],
                "timestamp": datetime.now(),
            }

        except Exception as e:
            logger.error(f"[red]Error getting orderbook for {symbol}: {e}[/red]")
            raise MarketDataFetchError(f"Failed to get orderbook: {e}")

    def _fetch_symbol_data(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        interval: Freq,
        batch_size: int,
        progress: Progress,
    ) -> List[PerpetualMarketTicker]:
        """单个交易对数据获取的工作函数"""
        data = []
        current_ts = start_ts

        # 创建进度任务
        batch_task = progress.add_task(f"[yellow]获取 {symbol} 数据", total=None, visible=True)

        while current_ts < end_ts:
            # 添加限流控制
            time.sleep(0.1)  # 简单的请求间隔

            progress.update(
                batch_task,
                description=f"[yellow]获取 {symbol} 数据 ({pd.Timestamp(current_ts, unit='ms')})",
            )

            klines = self.client.futures_historical_klines(
                symbol=symbol,
                interval=interval,
                start_str=current_ts,
                end_str=end_ts,
                limit=batch_size,
            )

            if not klines:
                break

            tickers = [PerpetualMarketTicker.from_binance_futures(symbol, k) for k in klines]
            data.extend(tickers)
            current_ts = klines[-1][6] + 1

        progress.remove_task(batch_task)
        return data

    def get_perpetual_data(
        self,
        symbols: List[str],
        start_time: str,
        end_time: str | None = None,
        interval: Freq = Freq.h1,
        batch_size: int = 500,
        data_path: Path | str = settings.DATA_STORAGE["PERPETUAL_DATA"],
        max_workers: int = 5,
    ) -> List[List[PerpetualMarketTicker]]:
        try:
            start_ts = int(pd.Timestamp(start_time).timestamp() * 1000)
            end_ts = int(pd.Timestamp(end_time).timestamp() * 1000)
            all_data: List[List[PerpetualMarketTicker]] = []  # 使用字典存储，键为symbol

            # 1. 先获取所有数据
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
            ) as progress:
                overall_task = progress.add_task("[cyan]处理所有交易对", total=len(symbols))

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_symbol = {
                        executor.submit(
                            self._fetch_symbol_data,
                            symbol,
                            start_ts,
                            end_ts,
                            interval,
                            batch_size,
                            progress,
                        ): symbol
                        for symbol in symbols
                    }

                    for future in as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            data = future.result()
                            all_data.append(data)
                            progress.advance(overall_task)
                        except Exception as e:
                            logger.error(f"[red]Error processing {symbol}: {e}[/red]")

            # 2. 数据全部获取完成后，统一进行存储
            StorageUtils.store_universe(symbols, data_path)
            try:
                StorageUtils.store_feature_data(all_data, interval, data_path)
            except Exception as e:
                logger.error(f"[red]Error storing data for {symbol}: {e}[/red]")
                raise MarketDataFetchError(f"Failed to store data: {e}")

            # 完成后显示汇总信息
            self.console.print(
                Panel(
                    f"✨ 数据获取完成\n"
                    f"📊 处理交易对: {len(symbols)}\n"
                    f"📅 时间范围: {datetime.strptime(start_time, '%Y%m%d').strftime('%Y-%m-%d')} 至 "
                    f"{datetime.strptime(end_time, '%Y%m%d').strftime('%Y-%m-%d') if end_time else datetime.now().strftime('%Y-%m-%d')}\n"
                    f"⏱️  数据间隔: {interval}",
                    title="处理完成",
                    border_style="green",
                )
            )

            return all_data

        except Exception as e:
            self.console.print(
                Panel(
                    f"❌ [red]Error: {str(e)}[/red]",
                    title="[red]Processing Failed[/red]",
                    border_style="red",
                )
            )
            logger.error(f"[red]Failed to fetch perpetual data: {e}[/red]")
            raise MarketDataFetchError(f"Failed to fetch perpetual data: {e}")
