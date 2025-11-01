"""市场数据服务.

专注于核心API功能，使用组合模式整合各个专业模块。
"""

from datetime import datetime
from pathlib import Path
from typing import Any, cast

from binance import AsyncClient

from cryptoservice.client import BinanceClientFactory
from cryptoservice.config import RetryConfig, settings
from cryptoservice.config.logging import get_logger
from cryptoservice.exceptions import InvalidSymbolError, MarketDataFetchError
from cryptoservice.models import (
    DailyMarketTicker,
    Freq,
    FundingRate,
    FuturesKlineTicker,
    HistoricalKlinesType,
    IntegrityReport,
    LongShortRatio,
    OpenInterest,
    SortBy,
    SpotKlineTicker,
    SymbolTicker,
    UniverseDefinition,
)
from cryptoservice.storage.database import Database
from cryptoservice.utils import DataConverter
from cryptoservice.utils.run_id import generate_run_id

# 导入新的模块
from .downloaders import KlineDownloader, MetricsDownloader, VisionDownloader
from .processors import CategoryManager, DataValidator, TimeRangeProcessor, UniverseManager

logger = get_logger(__name__)


class MarketDataService:
    """市场数据服务实现类."""

    def __init__(self, client: AsyncClient) -> None:
        """初始化市场数据服务 (私有构造函数)."""
        self.client = client
        self.converter = DataConverter()
        self.db: Database | None = None

        # 初始化各种专业模块
        self.kline_downloader = KlineDownloader(self.client)
        self.metrics_downloader = MetricsDownloader(self.client)
        self.vision_downloader = VisionDownloader(self.client)
        self.data_validator = DataValidator()
        self.universe_manager = UniverseManager(self)
        self.category_manager = CategoryManager()

    @classmethod
    async def create(cls, api_key: str, api_secret: str) -> "MarketDataService":
        """异步创建MarketDataService实例.

        Args:
            api_key: Binance API Key.
            api_secret: Binance API Secret.

        Returns:
            已初始化的市场数据服务实例。

        Raises:
            MarketDataError: 当客户端初始化失败时抛出。
        """
        client = await BinanceClientFactory.create_async_client(api_key, api_secret)
        return cls(client)

    async def __aenter__(self) -> "MarketDataService":
        """异步上下文管理器入口."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """异步上下文管理器出口，确保客户端关闭."""
        await BinanceClientFactory.close_client()
        if self.db:
            await self.db.close()

    # ==================== 基础市场数据API ====================

    async def get_symbol_ticker(self, symbol: str | None = None) -> SymbolTicker | list[SymbolTicker]:
        """获取单个或所有交易对的行情数据.

        Args:
            symbol: 具体交易对，若为 ``None`` 则返回全部交易对 24 小时行情。

        Returns:
            单个 ``SymbolTicker`` 或交易对列表的 ``SymbolTicker`` 集合。

        Raises:
            MarketDataFetchError: 请求失败或交易对无效时抛出。
        """
        try:
            ticker = await self.client.get_symbol_ticker(symbol=symbol)
            if not ticker:
                raise InvalidSymbolError(f"Invalid symbol: {symbol}")

            if isinstance(ticker, list):
                return [SymbolTicker.from_binance_ticker(t) for t in ticker]
            return SymbolTicker.from_binance_ticker(ticker)

        except Exception as e:
            logger.error(f"Error fetching ticker for {symbol}: {e}")
            raise MarketDataFetchError(f"Failed to fetch ticker: {e}") from e

    async def get_perpetual_symbols(self, only_trading: bool = True, quote_asset: str = "USDT") -> list[str]:
        """获取当前市场上所有永续合约交易对.

        Args:
            only_trading: 仅返回当前处于 TRADING 状态的交易对。
            quote_asset: 过滤的计价货币（默认 ``USDT``）。

        Returns:
            满足条件的合约符号列表。

        Raises:
            MarketDataFetchError: 当请求 Binance 失败时抛出。
        """
        try:
            logger.debug("fetch_perpetual_symbols", quote_asset=quote_asset, only_trading=only_trading)
            futures_info = await self.client.futures_exchange_info()
            perpetual_symbols = [
                symbol["symbol"]
                for symbol in futures_info["symbols"]
                if symbol["contractType"] == "PERPETUAL" and (not only_trading or symbol["status"] == "TRADING") and symbol["symbol"].endswith(quote_asset)
            ]

            logger.debug("perpetual_symbols_fetched", count=len(perpetual_symbols))
            return perpetual_symbols

        except Exception as e:
            logger.error(f"获取永续合约交易对失败: {e}")
            raise MarketDataFetchError(f"获取永续合约交易对失败: {e}") from e

    async def get_top_coins(
        self,
        limit: int = settings.DEFAULT_LIMIT,
        sort_by: SortBy = SortBy.QUOTE_VOLUME,
        quote_asset: str | None = None,
    ) -> list[DailyMarketTicker]:
        """获取前 N 个交易对.

        Args:
            limit: 返回的交易对数量上限。
            sort_by: 排序字段。
            quote_asset: 指定计价货币过滤。

        Returns:
            排序后的 ``DailyMarketTicker`` 列表。

        Raises:
            MarketDataFetchError: 请求 Binance 失败时抛出。
        """
        try:
            tickers = await self.client.get_ticker()
            market_tickers = [DailyMarketTicker.from_binance_ticker(t) for t in tickers]

            if quote_asset:
                market_tickers = [t for t in market_tickers if t.symbol.endswith(quote_asset)]

            return sorted(
                market_tickers,
                key=lambda x: getattr(x, sort_by.value),
                reverse=True,
            )[:limit]

        except Exception as e:
            logger.error(f"Error getting top coins: {e}")
            raise MarketDataFetchError(f"Failed to get top coins: {e}") from e

    async def get_market_summary(self, interval: Freq = Freq.d1) -> dict[str, Any]:
        """获取市场概览.

        Args:
            interval: 聚合频率，默认按天。

        Returns:
            包含快照时间和行情数据的字典。

        Raises:
            MarketDataFetchError: 当行情请求失败时抛出。
        """
        try:
            summary: dict[str, Any] = {"snapshot_time": datetime.now(), "data": {}}
            tickers_result = await self.get_symbol_ticker()
            tickers = [ticker.to_dict() for ticker in tickers_result] if isinstance(tickers_result, list) else [tickers_result.to_dict()]
            summary["data"] = tickers

            return summary

        except Exception as e:
            logger.error(f"Error getting market summary: {e}")
            raise MarketDataFetchError(f"Failed to get market summary: {e}") from e

    async def get_historical_klines(
        self,
        symbol: str,
        start_time: str | datetime,
        interval: Freq,
        end_time: str | datetime | None = None,
        klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT,
    ) -> list["SpotKlineTicker"] | list["FuturesKlineTicker"]:
        """获取历史行情数据.

        Args:
            symbol: 交易对符号
            start_time: 开始时间
            end_time: 结束时间（默认为当前时间）
            interval: K线频率
            klines_type: K线类型（现货/期货）

        Returns:
            根据 klines_type 返回不同类型:
            - SPOT: list[SpotKlineTicker]
            - FUTURES: list[FuturesKlineTicker]
        """
        try:
            # 处理时间格式
            if isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time)
            if end_time is None:
                end_time = datetime.now()
            elif isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time)

            # 转换为时间戳
            start_ts = self._date_to_timestamp_start(start_time.strftime("%Y-%m-%d"))
            end_ts = self._date_to_timestamp_end(end_time.strftime("%Y-%m-%d"))

            market_type = "期货" if klines_type == HistoricalKlinesType.FUTURES else "现货"
            logger.debug("fetch_historical_klines", symbol=symbol, market_type=market_type, interval=interval.value)

            ticker_class: type[SpotKlineTicker] | type[FuturesKlineTicker]
            # 根据klines_type选择API和返回类型
            if klines_type == HistoricalKlinesType.FUTURES:
                klines = await self.client.futures_klines(
                    symbol=symbol,
                    interval=interval.value,
                    startTime=start_ts,
                    endTime=end_ts,
                    limit=1500,
                )
                ticker_class = FuturesKlineTicker
            else:  # SPOT
                klines = await self.client.get_klines(
                    symbol=symbol,
                    interval=interval.value,
                    startTime=start_ts,
                    endTime=end_ts,
                    limit=1500,
                )
                ticker_class = SpotKlineTicker

            data = list(klines)
            if not data:
                logger.warning(f"未找到交易对 {symbol} 在指定时间段内的数据")
                return []

            # 根据市场类型创建相应的Ticker对象
            result = [ticker_class.from_binance_kline(symbol, kline) for kline in data]
            return cast(list[FuturesKlineTicker] | list[SpotKlineTicker], result)

        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {e}")
            raise MarketDataFetchError(f"Failed to get historical data: {e}") from e

    # ==================== 市场指标API ====================

    async def get_funding_rate(
        self,
        symbol: str,
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 100,
    ) -> list[FundingRate]:
        """获取永续合约资金费率历史."""
        # 转换时间格式
        start_time_str = self._convert_time_to_string(start_time) if start_time else ""
        end_time_str = self._convert_time_to_string(end_time) if end_time else ""

        return await self.metrics_downloader.download_funding_rate(
            symbol=symbol,
            start_time=start_time_str,
            end_time=end_time_str,
            limit=limit,
        )

    async def get_open_interest(
        self,
        symbol: str,
        period: str = "5m",
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[OpenInterest]:
        """获取永续合约持仓量数据."""
        # 转换时间格式
        start_time_str = self._convert_time_to_string(start_time) if start_time else ""
        end_time_str = self._convert_time_to_string(end_time) if end_time else ""

        return await self.metrics_downloader.download_open_interest(
            symbol=symbol,
            period=period,
            start_time=start_time_str,
            end_time=end_time_str,
            limit=limit,
        )

    async def get_long_short_ratio(
        self,
        symbol: str,
        period: str = "5m",
        ratio_type: str = "account",
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[LongShortRatio]:
        """获取多空比例数据."""
        # 转换时间格式
        start_time_str = self._convert_time_to_string(start_time) if start_time else ""
        end_time_str = self._convert_time_to_string(end_time) if end_time else ""

        return await self.metrics_downloader.download_long_short_ratio(
            symbol=symbol,
            period=period,
            ratio_type=ratio_type,
            start_time=start_time_str,
            end_time=end_time_str,
            limit=limit,
        )

    # ==================== 批量数据下载 ====================

    async def get_perpetual_data(
        self,
        symbols: list[str],
        start_time: str,
        db_path: Path | str,
        end_time: str | None = None,
        interval: Freq = Freq.h1,
        max_workers: int = 1,
        max_retries: int = 3,
        retry_config: RetryConfig | None = None,
        incremental: bool = True,
        run_id: str | None = None,
    ) -> IntegrityReport:
        """获取永续合约数据并存储."""
        # 验证并准备数据库文件路径
        db_file_path = self._validate_and_prepare_path(db_path, is_file=True)
        end_time = end_time or datetime.now().strftime("%Y-%m-%d")

        # 使用K线下载器
        return await self.kline_downloader.download_multiple_symbols(
            symbols=symbols,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            db_path=db_file_path,
            max_workers=max_workers,
            retry_config=retry_config or RetryConfig(max_retries=max_retries),
            incremental=incremental,
            run_id=run_id,
        )

    async def download_universe_data(
        self,
        universe_file: Path | str,
        db_path: Path | str,
        retry_config: RetryConfig,
        api_request_delay: float,
        vision_request_delay: float,
        download_market_metrics: bool,
        incremental: bool,
        interval: Freq = Freq.m1,
        max_api_workers: int = 1,
        max_vision_workers: int = 50,
        max_retries: int = 3,
        custom_start_date: str | None = None,
        custom_end_date: str | None = None,
    ) -> None:
        """按周期分别下载universe数据.

        Args:
            universe_file: Path to the universe definition file
            db_path: Path to the database file where data will be stored
            retry_config: Custom retry configuration, overrides max_retries
            api_request_delay: Delay in seconds between API requests
            vision_request_delay: Delay in seconds between Vision requests
            download_market_metrics: Whether to download market metrics data
            incremental: Whether to download incremental data,
             if True, only download new data, if False, re-download all data
            interval: Time interval for K-line data (default: 1m)
            max_api_workers: Maximum number of concurrent API workers
            max_vision_workers: Maximum number of concurrent Vision workers
            max_retries: Maximum number of retry attempts for failed requests
            custom_start_date: Custom global start date, will override the start date
             in the universe but must be within the universe time range
            custom_end_date: Custom global end date, will override the end date
             in the universe but must be within the universe time range

        """
        run_id = generate_run_id("universe")
        try:
            universe_file_obj = self._validate_and_prepare_path(universe_file, is_file=True)
            db_file_path = self._validate_and_prepare_path(db_path, is_file=True)

            if not universe_file_obj.exists():
                raise FileNotFoundError(f"Universe文件不存在: {universe_file_obj}")

            universe_def = UniverseDefinition.load_from_file(universe_file_obj)

            if custom_start_date or custom_end_date:
                universe_def = TimeRangeProcessor.apply_custom_time_range(universe_def, custom_start_date, custom_end_date)

            logger.info(
                f"开始下载 Universe 数据：{len(universe_def.snapshots)} 个快照"
                f"（频率 {interval.value}，API 并发 {max_api_workers}，"
                f"Vision 并发 {max_vision_workers}，下载指标：{'是' if download_market_metrics else '否'}）。"
            )

            kline_download_results: list[IntegrityReport] = []

            for index, snapshot in enumerate(universe_def.snapshots, start=1):
                logger.debug(
                    "snapshot.start",
                    run=run_id,
                    index=index,
                    total=len(universe_def.snapshots),
                    snapshot=snapshot.effective_date,
                    start_date=snapshot.start_date,
                    end_date=snapshot.end_date,
                    symbols=len(snapshot.symbols),
                )

                kline_report = await self.get_perpetual_data(
                    symbols=snapshot.symbols,
                    start_time=snapshot.start_date,
                    end_time=snapshot.end_date,
                    db_path=db_file_path,
                    interval=interval,
                    max_workers=max_api_workers,
                    max_retries=max_retries,
                    retry_config=retry_config,
                    incremental=incremental,
                    run_id=run_id,
                )
                kline_download_results.append(kline_report)

                if download_market_metrics:
                    logger.debug(
                        "metrics.start",
                        run=run_id,
                        snapshot=snapshot.effective_date,
                    )
                    await self._download_market_metrics_for_snapshot(
                        snapshot=snapshot,
                        db_path=db_file_path,
                        api_request_delay=api_request_delay,
                        vision_request_delay=vision_request_delay,
                        max_api_workers=max_api_workers,
                        max_vision_workers=max_vision_workers,
                        incremental=incremental,
                        run_id=run_id,
                    )

                logger.debug(
                    "snapshot.done",
                    run=run_id,
                    snapshot=snapshot.effective_date,
                )

            total_symbols = sum(report.total_symbols for report in kline_download_results)
            total_success = sum(report.successful_symbols for report in kline_download_results)
            total_failures = sum(len(report.failed_symbols) for report in kline_download_results)

            logger.info(
                f"Universe 下载完成：{len(universe_def.snapshots)} 个快照，成功 {total_success}/{total_symbols}，失败 {total_failures}（写入 {db_file_path}）。"
            )
        except Exception as exc:
            logger.error(f"Universe 下载失败：{exc}")
            raise MarketDataFetchError(f"按周期下载universe数据失败: {exc}") from exc

    # ==================== Universe管理 ====================

    async def define_universe(
        self,
        start_date: str,
        end_date: str,
        t1_months: int,
        t2_months: int,
        t3_months: int,
        output_path: Path | str,
        top_k: int | None = None,
        top_ratio: float | None = None,
        description: str | None = None,
        delay_days: int = 7,
        api_delay_seconds: float = 1.0,
        batch_delay_seconds: float = 3.0,
        batch_size: int = 5,
        quote_asset: str = "USDT",
    ) -> UniverseDefinition:
        """定义universe并保存到文件."""
        return await self.universe_manager.define_universe(
            start_date=start_date,
            end_date=end_date,
            t1_months=t1_months,
            t2_months=t2_months,
            t3_months=t3_months,
            output_path=output_path,
            top_k=top_k,
            top_ratio=top_ratio,
            description=description,
            delay_days=delay_days,
            api_delay_seconds=api_delay_seconds,
            batch_delay_seconds=batch_delay_seconds,
            batch_size=batch_size,
            quote_asset=quote_asset,
        )

    # ==================== 分类管理 ====================

    def get_symbol_categories(self) -> dict[str, list[str]]:
        """获取当前所有交易对的分类信息."""
        return self.category_manager.get_symbol_categories()

    def get_all_categories(self) -> list[str]:
        """获取所有可能的分类标签."""
        return self.category_manager.get_all_categories()

    def create_category_matrix(self, symbols: list[str], categories: list[str] | None = None) -> tuple[list[str], list[str], list[list[int]]]:
        """创建 symbols 和 categories 的对应矩阵."""
        categories_list = categories if categories is not None else []
        return self.category_manager.create_category_matrix(symbols, categories_list)

    def save_category_matrix_csv(
        self,
        output_path: Path | str,
        symbols: list[str],
        date_str: str | None = None,
        categories: list[str] | None = None,
    ) -> None:
        """将分类矩阵保存为 CSV 文件."""
        date_str_value = date_str if date_str is not None else ""
        categories_list = categories if categories is not None else []
        self.category_manager.save_category_matrix_csv(
            output_path=output_path,
            symbols=symbols,
            date_str=date_str_value,
            categories=categories_list,
        )

    def download_and_save_categories_for_universe(
        self,
        universe_file: Path | str,
        output_path: Path | str,
    ) -> None:
        """为 universe 中的所有交易对下载并保存分类信息."""
        self.category_manager.download_and_save_categories_for_universe(
            universe_file=universe_file,
            output_path=output_path,
        )

    async def check_symbol_exists_on_date(self, symbol: str, date: str) -> bool:
        """检查指定日期是否存在该交易对."""
        try:
            # 将日期转换为时间戳范围
            start_time = self._date_to_timestamp_start(date)
            end_time = self._date_to_timestamp_end(date)

            # 尝试获取该时间范围内的K线数据
            klines = await self.client.futures_klines(
                symbol=symbol,
                interval="1d",
                startTime=start_time,
                endTime=end_time,
                limit=1,
            )

            # 如果有数据，说明该日期存在该交易对
            return bool(klines and len(klines) > 0)

        except Exception as e:
            logger.debug(f"检查交易对 {symbol} 在 {date} 是否存在时出错: {e}")
            return False

    # ==================== 私有辅助方法 ====================

    async def _download_market_metrics_for_snapshot(
        self,
        snapshot,
        db_path: Path,
        api_request_delay: float,
        vision_request_delay: float,
        max_api_workers: int,
        max_vision_workers: int,
        incremental: bool = True,
        run_id: str | None = None,
    ) -> None:
        """为单个快照下载市场指标数据.

        Args:
            snapshot: Universe快照定义
            db_path: 数据库路径
            api_request_delay: API请求延迟
            vision_request_delay: Vision请求延迟
            max_api_workers: 最大API并发数
            max_vision_workers: 最大Vision并发数
            incremental: 是否启用增量下载（默认True）
            run_id: 当前下载任务标识，用于日志跟踪
        """
        try:
            # 初始化数据库连接
            if self.db is None:
                self.db = Database(db_path)

            symbols = snapshot.symbols
            start_time = snapshot.start_date
            end_time = snapshot.end_date

            # 下载Vision数据（持仓量、多空比例）
            logger.debug(
                "vision.download.start",
                run=run_id,
                snapshot=snapshot.effective_date,
                dataset="vision-metrics",
                symbols=len(symbols),
                start=start_time,
                end=end_time,
                max_workers=max_vision_workers,
                incremental=incremental,
            )
            await self.vision_downloader.download_metrics_batch(
                symbols=symbols,
                start_date=start_time,
                end_date=end_time,
                db_path=str(db_path),
                request_delay=vision_request_delay,
                max_workers=max_vision_workers,
                incremental=incremental,
            )

            # 下载Metrics API数据（资金费率）
            logger.debug(
                "funding.download.start",
                run=run_id,
                snapshot=snapshot.effective_date,
                dataset="funding_rate",
                symbols=len(symbols),
                start=start_time,
                end=end_time,
                max_workers=max_api_workers,
                incremental=incremental,
            )
            await self.metrics_downloader.download_funding_rate_batch(
                symbols=symbols,
                start_time=start_time,
                end_time=end_time,
                db_path=str(db_path),
                request_delay=api_request_delay,
                max_workers=max_api_workers,  # 限制并发以避免API限制
                incremental=incremental,
            )

            logger.debug(
                "metrics.snapshot_done",
                run=run_id,
                snapshot=snapshot.effective_date,
                dataset="market_metrics",
            )

        except Exception as e:
            logger.error(
                "metrics.snapshot_error",
                run=run_id,
                snapshot=snapshot.effective_date,
                error=str(e),
            )
            raise MarketDataFetchError(f"下载市场指标数据失败: {e}") from e

    def _validate_and_prepare_path(self, path: Path | str, is_file: bool = False, file_name: str | None = None) -> Path:
        """验证并准备路径."""
        if not path:
            raise ValueError("路径不能为空，必须手动指定")

        path_obj = Path(path)

        # 如果是文件路径，确保父目录存在
        if is_file:
            if path_obj.is_dir():
                path_obj = path_obj.joinpath(file_name) if file_name else path_obj
            else:
                path_obj.parent.mkdir(parents=True, exist_ok=True)
        else:
            # 如果是目录路径，确保目录存在
            path_obj.mkdir(parents=True, exist_ok=True)

        return path_obj

    def _date_to_timestamp_start(self, date: str) -> str:
        """将日期字符串转换为当天开始的时间戳（UTC）."""
        from cryptoservice.utils import date_to_timestamp_start

        return str(date_to_timestamp_start(date))

    def _date_to_timestamp_end(self, date: str) -> str:
        """将日期字符串转换为次日开始的时间戳（UTC）."""
        from cryptoservice.utils import date_to_timestamp_end

        return str(date_to_timestamp_end(date))

    def _convert_time_to_string(self, time_value: str | datetime | None) -> str:
        """将时间值转换为字符串格式."""
        if time_value is None:
            return ""
        if isinstance(time_value, str):
            return time_value
        if isinstance(time_value, datetime):
            return time_value.strftime("%Y-%m-%d")
