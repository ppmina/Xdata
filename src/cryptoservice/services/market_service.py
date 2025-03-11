import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, overload

from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from cryptoservice.client import BinanceClientFactory
from cryptoservice.config import settings
from cryptoservice.data import MarketDB
from cryptoservice.exceptions import InvalidSymbolError, MarketDataFetchError, RateLimitError
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

cache_lock = Lock()


class MarketDataService(IMarketDataService):
    """市场数据服务实现类"""

    def __init__(self, api_key: str, api_secret: str) -> None:
        """初始化市场数据服务

        Args:
            api_key: 用户API密钥
            api_secret: 用户API密钥
        """
        self.client = BinanceClientFactory.create_client(api_key, api_secret)
        self.converter = DataConverter()
        self.db: Optional[MarketDB] = None

    @overload
    def get_symbol_ticker(self, symbol: str) -> SymbolTicker: ...

    @overload
    def get_symbol_ticker(self) -> List[SymbolTicker]: ...

    def get_symbol_ticker(self, symbol: str | None = None) -> SymbolTicker | List[SymbolTicker]:
        """获取单个或所有交易对的行情数据

        Args:
            symbol | List[symbol]: 交易对名称

        Returns:
            SymbolTicker | List[SymbolTicker]: 单个交易对的行情数据或所有交易对的行情数据
        """
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
        quote_asset: str | None = None,
    ) -> List[DailyMarketTicker]:
        """获取前N个交易对

        Args:
            limit: 数量
            sort_by: 排序方式
            quote_asset: 基准资产

        Returns:
            List[DailyMarketTicker]: 前N个交易对
        """
        try:
            tickers = self.client.get_ticker()
            market_tickers = [DailyMarketTicker.from_binance_ticker(t) for t in tickers]

            if quote_asset:
                market_tickers = [t for t in market_tickers if t.symbol.endswith(quote_asset)]

            return sorted(
                market_tickers,
                key=lambda x: getattr(x, sort_by.value),
                reverse=True,
            )[:limit]

        except Exception as e:
            logger.error(f"[red]Error getting top coins: {e}[/red]")
            raise MarketDataFetchError(f"Failed to get top coins: {e}")

    def get_market_summary(self, interval: Freq = Freq.d1) -> Dict[str, Any]:
        """获取市场概览

        Args:
            interval: 时间间隔

        Returns:
            Dict[str, Any]: 市场概览
        """
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
        """获取历史行情数据

        Args:
            symbol: 交易对名称
            start_time: 开始时间
            end_time: 结束时间
            interval: 时间间隔
            klines_type: 行情类型

        Returns:
            List[KlineMarketTicker]: 历史行情数据
        """
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

    def _fetch_symbol_data(
        self,
        symbol: str,
        start_ts: str,
        end_ts: str,
        interval: Freq,
        klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT,
    ) -> List[PerpetualMarketTicker]:
        """获取单个交易对的数据."""
        try:
            time.sleep(0.1)  # 简单的请求间隔

            klines = self.client.get_historical_klines_generator(
                symbol=symbol,
                interval=interval,
                start_str=start_ts,
                end_str=end_ts,
                klines_type=HistoricalKlinesType.to_binance(klines_type),
            )

            if not klines:
                logger.warning(f"No data available for {symbol} in specified time range")
                raise MarketDataFetchError(
                    f"No data available for {symbol} between {start_ts} and {end_ts}"
                )

            # 直接传递原始数据，不做类型转换
            return [
                PerpetualMarketTicker(
                    symbol=symbol, open_time=kline[0], raw_data=kline  # 保存原始数据
                )
                for kline in klines
            ]

        except Exception as e:
            logger.error(f"Failed to fetch data for {symbol}: {e}")
            raise MarketDataFetchError(f"Failed to fetch data for {symbol}: {e}")

    def get_perpetual_data(
        self,
        symbols: List[str],
        start_time: str,
        data_path: Path | str,
        end_time: str | None = None,
        interval: Freq = Freq.m1,
        max_workers: int = 1,
        max_retries: int = 3,
        progress: Progress | None = None,
    ) -> None:
        """获取永续合约数据并存储.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            data_path: 数据存储路径
            end_time: 结束时间 (YYYY-MM-DD)
            interval: 时间间隔
            max_workers: 最大线程数
            max_retries: 最大重试次数
            progress: 进度显示器
        """
        try:
            if not symbols:
                raise ValueError("Symbols list cannot be empty")

            data_path = Path(data_path)
            end_time = end_time or datetime.now().strftime("%Y-%m-%d")
            db_path = data_path / "market.db"

            # 初始化数据库连接池
            if self.db is None:
                self.db = MarketDB(db_path, use_pool=True, max_connections=max_workers)

            # 进度显示器设置
            should_close_progress = False
            if progress is None:
                progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TimeElapsedColumn(),
                )
                should_close_progress = True

            def process_symbol(symbol: str) -> None:
                """处理单个交易对的数据获取"""
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        data = self._fetch_symbol_data(
                            symbol=symbol,
                            start_ts=start_time,
                            end_ts=end_time,
                            interval=interval,
                            klines_type=HistoricalKlinesType.FUTURES,
                        )

                        if data:
                            # 确保 db_pool 不为 None
                            assert self.db is not None, "Database pool is not initialized"
                            self.db.store_data(data, interval)  # 直接传递 data，不需要包装成列表
                            return
                        else:
                            logger.warning(f"No data available for {symbol}")
                            return

                    except RateLimitError:
                        wait_time = min(2**retry_count + 1, 30)
                        time.sleep(wait_time)
                        retry_count += 1
                    except Exception as e:
                        if retry_count < max_retries - 1:
                            retry_count += 1
                            logger.warning(f"重试 {retry_count}/{max_retries} - {symbol}: {str(e)}")
                            time.sleep(1)
                        else:
                            logger.error(f"处理失败 - {symbol}: {str(e)}")
                            break

            with progress if should_close_progress else nullcontext():
                overall_task = progress.add_task("[cyan]处理所有交易对", total=len(symbols))

                # 使用线程池并行处理
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(process_symbol, symbol) for symbol in symbols]

                    # 跟踪完成进度
                    for future in as_completed(futures):
                        try:
                            future.result()
                            progress.update(overall_task, advance=1)
                        except Exception as e:
                            logger.error(f"处理失败: {e}")

        except Exception as e:
            logger.error(f"Failed to fetch perpetual data: {e}")
            raise MarketDataFetchError(f"Failed to fetch perpetual data: {e}")
        finally:
            if self.db:
                self.db.close()


# if __name__ == "__main__":
#     import os
#     from dotenv import load_dotenv

#     load_dotenv()

#     api_key = os.getenv("BINANCE_API_KEY")
#     api_secret = os.getenv("BINANCE_API_SECRET")
#     if not api_key or not api_secret:
#         raise ValueError(
#             "BINANCE_API_KEY and BINANCE_API_SECRET must be set in environment variables"
#         )

#     service = MarketDataService(api_key, api_secret)
#     service.get_perpetual_data(
#         symbols=["BTCUSDT"],
#         start_time="2024-01-08",
#         end_time="2024-01-09",
#         interval=Freq.m1,
#         data_path="./data",
#     )
