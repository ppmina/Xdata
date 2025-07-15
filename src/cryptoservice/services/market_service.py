"""市场数据服务。

专注于核心API功能，使用组合模式整合各个专业模块。
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from cryptoservice.client import BinanceClientFactory
from cryptoservice.utils import DataConverter
from cryptoservice.storage import AsyncMarketDB
from cryptoservice.exceptions import InvalidSymbolError, MarketDataFetchError
from cryptoservice.models import (
    DailyMarketTicker,
    Freq,
    HistoricalKlinesType,
    KlineMarketTicker,
    SortBy,
    SymbolTicker,
    UniverseDefinition,
    IntegrityReport,
    FundingRate,
    OpenInterest,
    LongShortRatio,
)
from cryptoservice.config import settings, RetryConfig

# 导入新的模块
from .downloaders import KlineDownloader, MetricsDownloader, VisionDownloader
from .processors import DataValidator, UniverseManager, CategoryManager

logger = logging.getLogger(__name__)


class MarketDataService:
    """市场数据服务实现类（重构版）"""

    def __init__(self, api_key: str, api_secret: str) -> None:
        """初始化市场数据服务"""
        self.client = BinanceClientFactory.create_client(api_key, api_secret)
        self.converter = DataConverter()
        self.db: AsyncMarketDB | None = None

        # 初始化各种专业模块
        self.kline_downloader = KlineDownloader(self.client)
        self.metrics_downloader = MetricsDownloader(self.client)
        self.vision_downloader = VisionDownloader(self.client)
        self.data_validator = DataValidator()
        self.universe_manager = UniverseManager(self)
        self.category_manager = CategoryManager()

    # ==================== 基础市场数据API ====================

    def get_symbol_ticker(self, symbol: str | None = None) -> SymbolTicker | list[SymbolTicker]:
        """获取单个或所有交易对的行情数据"""
        try:
            ticker = self.client.get_symbol_ticker(symbol=symbol)
            if not ticker:
                raise InvalidSymbolError(f"Invalid symbol: {symbol}")

            if isinstance(ticker, list):
                return [SymbolTicker.from_binance_ticker(t) for t in ticker]
            return SymbolTicker.from_binance_ticker(ticker)

        except Exception as e:
            logger.error(f"Error fetching ticker for {symbol}: {e}")
            raise MarketDataFetchError(f"Failed to fetch ticker: {e}") from e

    def get_perpetual_symbols(self, only_trading: bool = True, quote_asset: str = "USDT") -> list[str]:
        """获取当前市场上所有永续合约交易对"""
        try:
            logger.info(f"获取当前永续合约交易对列表（筛选条件：{quote_asset}结尾）")
            futures_info = self.client.futures_exchange_info()
            perpetual_symbols = [
                symbol["symbol"]
                for symbol in futures_info["symbols"]
                if symbol["contractType"] == "PERPETUAL"
                and (not only_trading or symbol["status"] == "TRADING")
                and symbol["symbol"].endswith(quote_asset)
            ]

            logger.info(f"找到 {len(perpetual_symbols)} 个{quote_asset}永续合约交易对")
            return perpetual_symbols

        except Exception as e:
            logger.error(f"获取永续合约交易对失败: {e}")
            raise MarketDataFetchError(f"获取永续合约交易对失败: {e}") from e

    def get_top_coins(
        self,
        limit: int = settings.DEFAULT_LIMIT,
        sort_by: SortBy = SortBy.QUOTE_VOLUME,
        quote_asset: str | None = None,
    ) -> list[DailyMarketTicker]:
        """获取前N个交易对"""
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
            logger.error(f"Error getting top coins: {e}")
            raise MarketDataFetchError(f"Failed to get top coins: {e}") from e

    def get_market_summary(self, interval: Freq = Freq.d1) -> dict[str, Any]:
        """获取市场概览"""
        try:
            summary: dict[str, Any] = {"snapshot_time": datetime.now(), "data": {}}
            tickers_result = self.get_symbol_ticker()
            if isinstance(tickers_result, list):
                tickers = [ticker.to_dict() for ticker in tickers_result]
            else:
                tickers = [tickers_result.to_dict()]
            summary["data"] = tickers

            return summary

        except Exception as e:
            logger.error(f"Error getting market summary: {e}")
            raise MarketDataFetchError(f"Failed to get market summary: {e}") from e

    def get_historical_klines(
        self,
        symbol: str,
        start_time: str | datetime,
        end_time: str | datetime | None = None,
        interval: Freq = Freq.h1,
        klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT,
    ) -> list[KlineMarketTicker]:
        """获取历史行情数据"""
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

            logger.info(f"获取 {symbol} 的历史数据 ({interval.value})")

            # 根据klines_type选择API
            if klines_type == HistoricalKlinesType.FUTURES:
                klines = self.client.futures_klines(
                    symbol=symbol,
                    interval=interval.value,
                    startTime=start_ts,
                    endTime=end_ts,
                    limit=1500,
                )
            else:  # SPOT
                klines = self.client.get_klines(
                    symbol=symbol,
                    interval=interval.value,
                    startTime=start_ts,
                    endTime=end_ts,
                    limit=1500,
                )

            data = list(klines)
            if not data:
                logger.warning(f"未找到交易对 {symbol} 在指定时间段内的数据")
                return []

            # 转换为KlineMarketTicker对象
            from decimal import Decimal

            return [
                KlineMarketTicker(
                    symbol=symbol,
                    last_price=Decimal(str(kline[4])),  # 收盘价作为最新价格
                    open_price=Decimal(str(kline[1])),
                    high_price=Decimal(str(kline[2])),
                    low_price=Decimal(str(kline[3])),
                    volume=Decimal(str(kline[5])),
                    close_time=kline[6],
                )
                for kline in data
            ]

        except Exception as e:
            logger.error(f"Error getting historical data for {symbol}: {e}")
            raise MarketDataFetchError(f"Failed to get historical data: {e}") from e

    # ==================== 市场指标API ====================

    def get_funding_rate(
        self,
        symbol: str,
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 100,
    ) -> list[FundingRate]:
        """获取永续合约资金费率历史"""
        # 转换时间格式
        start_time_str = self._convert_time_to_string(start_time) if start_time else ""
        end_time_str = self._convert_time_to_string(end_time) if end_time else ""

        return self.metrics_downloader.download_funding_rate(
            symbol=symbol,
            start_time=start_time_str,
            end_time=end_time_str,
            limit=limit,
        )

    def get_open_interest(
        self,
        symbol: str,
        period: str = "5m",
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[OpenInterest]:
        """获取永续合约持仓量数据"""
        # 转换时间格式
        start_time_str = self._convert_time_to_string(start_time) if start_time else ""
        end_time_str = self._convert_time_to_string(end_time) if end_time else ""

        return self.metrics_downloader.download_open_interest(
            symbol=symbol,
            period=period,
            start_time=start_time_str,
            end_time=end_time_str,
            limit=limit,
        )

    def get_long_short_ratio(
        self,
        symbol: str,
        period: str = "5m",
        ratio_type: str = "account",
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[LongShortRatio]:
        """获取多空比例数据"""
        # 转换时间格式
        start_time_str = self._convert_time_to_string(start_time) if start_time else ""
        end_time_str = self._convert_time_to_string(end_time) if end_time else ""

        return self.metrics_downloader.download_long_short_ratio(
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
        max_workers: int = 5,
        max_retries: int = 3,
        progress=None,
        request_delay: float = 0.5,
        retry_config: Optional[RetryConfig] = None,
        enable_integrity_check: bool = True,
    ) -> IntegrityReport:
        """获取永续合约数据并存储"""
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
        )

    async def download_universe_data(
        self,
        universe_file: Path | str,
        db_path: Path | str,
        data_path: Path | str | None = None,
        interval: Freq = Freq.m1,
        max_workers: int = 4,
        max_retries: int = 3,
        include_buffer_days: int = 7,
        retry_config: RetryConfig | None = None,
        request_delay: float = 0.5,
        download_market_metrics: bool = True,
        metrics_interval: Freq = Freq.m5,
        long_short_ratio_period: Freq = Freq.m5,
        long_short_ratio_types: list[str] | None = None,
        use_binance_vision: bool = False,
    ) -> None:
        """按周期分别下载universe数据"""
        try:
            # 验证路径
            universe_file_obj = self._validate_and_prepare_path(universe_file, is_file=True)
            db_file_path = self._validate_and_prepare_path(db_path, is_file=True)

            # 检查universe文件是否存在
            if not universe_file_obj.exists():
                raise FileNotFoundError(f"Universe文件不存在: {universe_file_obj}")

            # 加载universe定义
            universe_def = UniverseDefinition.load_from_file(universe_file_obj)

            # 设置多空比例类型默认值
            if long_short_ratio_types is None:
                long_short_ratio_types = ["account", "position"]

            logger.info("📊 按周期下载数据:")
            logger.info(f"   - 总快照数: {len(universe_def.snapshots)}")
            logger.info(f"   - 数据频率: {interval.value}")
            logger.info(f"   - 并发线程: {max_workers}")
            logger.info(f"   - 请求间隔: {request_delay}秒")
            logger.info(f"   - 数据库路径: {db_file_path}")
            logger.info(f"   - 下载市场指标: {download_market_metrics}")

            # 为每个周期单独下载数据
            for i, snapshot in enumerate(universe_def.snapshots):
                logger.info(f"📅 处理快照 {i + 1}/{len(universe_def.snapshots)}: {snapshot.effective_date}")

                # 下载K线数据
                await self.get_perpetual_data(
                    symbols=snapshot.symbols,
                    start_time=snapshot.start_date,
                    end_time=snapshot.end_date,
                    db_path=db_file_path,
                    interval=interval,
                    max_workers=max_workers,
                    max_retries=max_retries,
                    retry_config=retry_config,
                    enable_integrity_check=True,
                    request_delay=request_delay,
                )

                # 下载市场指标数据
                if download_market_metrics:
                    logger.info("   📈 开始下载市场指标数据...")
                    await self._download_market_metrics_for_snapshot(
                        snapshot=snapshot,
                        db_path=db_file_path,
                        interval=metrics_interval,
                        period=long_short_ratio_period,
                        long_short_ratio_types=long_short_ratio_types,
                        request_delay=request_delay,
                        use_binance_vision=use_binance_vision,
                    )

                logger.info(f"   ✅ 快照 {snapshot.effective_date} 下载完成")

            logger.info("🎉 所有universe数据下载完成!")
            logger.info(f"📁 数据已保存到: {db_file_path}")

        except Exception as e:
            logger.error(f"按周期下载universe数据失败: {e}")
            raise MarketDataFetchError(f"按周期下载universe数据失败: {e}") from e

    # ==================== Universe管理 ====================

    def define_universe(
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
        """定义universe并保存到文件"""
        return self.universe_manager.define_universe(
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
        """获取当前所有交易对的分类信息"""
        return self.category_manager.get_symbol_categories()

    def get_all_categories(self) -> list[str]:
        """获取所有可能的分类标签"""
        return self.category_manager.get_all_categories()

    def create_category_matrix(
        self, symbols: list[str], categories: list[str] | None = None
    ) -> tuple[list[str], list[str], list[list[int]]]:
        """创建 symbols 和 categories 的对应矩阵"""
        categories_list = categories if categories is not None else []
        return self.category_manager.create_category_matrix(symbols, categories_list)

    def save_category_matrix_csv(
        self,
        output_path: Path | str,
        symbols: list[str],
        date_str: str | None = None,
        categories: list[str] | None = None,
    ) -> None:
        """将分类矩阵保存为 CSV 文件"""
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
        categories: list[str] | None = None,
    ) -> None:
        """为 universe 中的所有交易对下载并保存分类信息"""
        categories_list = categories if categories is not None else []
        self.category_manager.download_and_save_categories_for_universe(
            universe_file=universe_file,
            output_path=output_path,
            categories=categories_list,
        )

    # ==================== 私有辅助方法 ====================

    async def _download_market_metrics_for_snapshot(
        self,
        snapshot,
        db_path: Path,
        interval: Freq = Freq.m5,
        period: Freq = Freq.m5,
        long_short_ratio_types: list[str] | None = None,
        request_delay: float = 0.5,
        use_binance_vision: bool = False,
    ) -> None:
        """为单个快照下载市场指标数据"""
        try:
            # 初始化数据库连接
            if self.db is None:
                self.db = AsyncMarketDB(str(db_path))

            # 设置默认值
            if long_short_ratio_types is None:
                long_short_ratio_types = ["account"]

            symbols = snapshot.symbols
            start_time = snapshot.start_date
            end_time = snapshot.end_date

            if use_binance_vision:
                logger.info("      📊 使用 Binance Vision 下载市场指标数据...")
                await self.vision_downloader.download_metrics_batch(
                    symbols=symbols,
                    start_date=start_time,
                    end_date=end_time,
                    db_path=str(db_path),
                    request_delay=request_delay,
                )
            else:
                logger.info("      📊 使用 API 下载市场指标数据...")

                # 下载资金费率
                await self.metrics_downloader.download_funding_rate_batch(
                    symbols=symbols,
                    start_time=start_time,
                    end_time=end_time,
                    db_path=str(db_path),
                    request_delay=request_delay,
                )

                # 下载持仓量
                await self.metrics_downloader.download_open_interest_batch(
                    symbols=symbols,
                    start_time=start_time,
                    end_time=end_time,
                    db_path=str(db_path),
                    interval=interval,
                    request_delay=request_delay,
                )

                # 下载多空比例
                for ratio_type in long_short_ratio_types:
                    logger.info(f"        - 类型: {ratio_type}")
                    await self.metrics_downloader.download_long_short_ratio_batch(
                        symbols=symbols,
                        start_time=start_time,
                        end_time=end_time,
                        db_path=str(db_path),
                        period=period.value,
                        ratio_type=ratio_type,
                        request_delay=request_delay,
                    )

            logger.info("      ✅ 市场指标数据下载完成")

        except Exception as e:
            logger.error(f"下载市场指标数据失败: {e}")
            raise MarketDataFetchError(f"下载市场指标数据失败: {e}") from e

    def _validate_and_prepare_path(self, path: Path | str, is_file: bool = False, file_name: str | None = None) -> Path:
        """验证并准备路径"""
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
        """将日期字符串转换为当天开始的时间戳"""
        timestamp = int(datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return str(timestamp)

    def _date_to_timestamp_end(self, date: str) -> str:
        """将日期字符串转换为当天结束的时间戳"""
        timestamp = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return str(timestamp)

    def _convert_time_to_string(self, time_value: str | datetime | None) -> str:
        """将时间值转换为字符串格式"""
        if time_value is None:
            return ""
        if isinstance(time_value, str):
            return time_value
        if isinstance(time_value, datetime):
            return time_value.strftime("%Y-%m-%d")
        raise ValueError(f"Unsupported time type: {type(time_value)}")

    def check_symbol_exists_on_date(self, symbol: str, date: str) -> bool:
        """检查指定日期是否存在该交易对"""
        try:
            # 将日期转换为时间戳范围
            start_time = self._date_to_timestamp_start(date)
            end_time = self._date_to_timestamp_end(date)

            # 尝试获取该时间范围内的K线数据
            klines = self.client.futures_klines(
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

    # ==================== 支持旧版本的方法 ====================

    def _fetch_symbol_data(self, *args, **kwargs):
        """支持旧版本的方法，委托给K线下载器"""
        return self.kline_downloader.download_single_symbol(*args, **kwargs)

    @property
    def rate_limit_manager(self):
        """提供向后兼容的rate_limit_manager属性"""
        return self.kline_downloader.rate_limit_manager

    @rate_limit_manager.setter
    def rate_limit_manager(self, value):
        """设置rate_limit_manager"""
        self.kline_downloader.rate_limit_manager = value
        self.metrics_downloader.rate_limit_manager = value
        self.vision_downloader.rate_limit_manager = value
