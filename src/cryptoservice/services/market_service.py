"""市场数据服务.

专注于核心API功能，使用组合模式整合各个专业模块。
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd
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
    UniverseConfig,
    UniverseDefinition,
    UniverseSnapshot,
)
from cryptoservice.storage.database import Database
from cryptoservice.utils import DataConverter
from cryptoservice.utils.run_id import generate_run_id

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

        self.kline_downloader = KlineDownloader(self.client)
        self.metrics_downloader = MetricsDownloader(self.client)
        self.vision_downloader = VisionDownloader(self.client)
        self.data_validator = DataValidator()
        self.universe_manager = UniverseManager(self)
        self.category_manager = CategoryManager()

    @classmethod
    async def create(cls, api_key: str, api_secret: str) -> MarketDataService:
        """异步创建MarketDataService实例."""
        client = await BinanceClientFactory.create_async_client(api_key, api_secret)
        return cls(client)

    async def __aenter__(self) -> MarketDataService:
        """异步上下文管理器入口."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """异步上下文管理器出口，确保客户端关闭."""
        await BinanceClientFactory.close_client()
        if self.db:
            await self.db.close()

    # ==================== 基础市场数据API ====================

    async def get_symbol_ticker(self, symbol: str | None = None) -> SymbolTicker | list[SymbolTicker]:
        """获取单个或所有交易对的行情数据."""
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
        """获取当前市场上所有永续合约交易对."""
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
        """获取前 N 个交易对."""
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
        """获取市场概览."""
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
    ) -> list[SpotKlineTicker] | list[FuturesKlineTicker]:
        """获取历史行情数据."""
        try:
            if isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time)
            if end_time is None:
                end_time = datetime.now()
            elif isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time)

            start_ts = self._date_to_timestamp_start(start_time.strftime("%Y-%m-%d"))
            end_ts = self._date_to_timestamp_end(end_time.strftime("%Y-%m-%d"))

            market_type = "期货" if klines_type == HistoricalKlinesType.FUTURES else "现货"
            logger.debug("fetch_historical_klines", symbol=symbol, market_type=market_type, interval=interval.value)

            ticker_class: type[SpotKlineTicker] | type[FuturesKlineTicker]
            if klines_type == HistoricalKlinesType.FUTURES:
                klines = await self.client.futures_klines(
                    symbol=symbol,
                    interval=interval.value,
                    startTime=start_ts,
                    endTime=end_ts,
                    limit=1500,
                )
                ticker_class = FuturesKlineTicker
            else:
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
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[OpenInterest]:
        """获取永续合约持仓量数据."""
        start_time_str = self._convert_time_to_string(start_time) if start_time else ""
        end_time_str = self._convert_time_to_string(end_time) if end_time else ""

        return await self.metrics_downloader.download_open_interest(
            symbol=symbol,
            start_time=start_time_str,
            end_time=end_time_str,
            limit=limit,
        )

    async def get_long_short_ratio(
        self,
        symbol: str,
        ratio_type: str = "account",
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[LongShortRatio]:
        """获取多空比例数据."""
        start_time_str = self._convert_time_to_string(start_time) if start_time else ""
        end_time_str = self._convert_time_to_string(end_time) if end_time else ""

        return await self.metrics_downloader.download_long_short_ratio(
            symbol=symbol,
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
        db_file_path = self._validate_and_prepare_path(db_path, is_file=True)
        end_time = end_time or datetime.now().strftime("%Y-%m-%d")

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
        """按周期分别下载 universe 数据 (兼容旧行为，不返回值)."""
        run_id = generate_run_id("universe")
        try:
            universe_file_obj = self._validate_and_prepare_path(universe_file, is_file=True)
            if not universe_file_obj.exists():
                raise FileNotFoundError(f"Universe文件不存在: {universe_file_obj}")

            universe_def = UniverseDefinition.load_from_file(universe_file_obj)
            if custom_start_date or custom_end_date:
                universe_def = TimeRangeProcessor.apply_custom_time_range(universe_def, custom_start_date, custom_end_date)

            report = await self._download_universe_definition(
                universe_def=universe_def,
                db_path=db_path,
                retry_config=retry_config,
                api_request_delay=api_request_delay,
                vision_request_delay=vision_request_delay,
                download_market_metrics=download_market_metrics,
                incremental=incremental,
                interval=interval,
                max_api_workers=max_api_workers,
                max_vision_workers=max_vision_workers,
                max_retries=max_retries,
                run_id=run_id,
            )

            logger.info(
                "Universe 下载完成：%s 个快照，成功 %s/%s，失败 %s（写入 %s）。",
                report["total_snapshots"],
                report["total_successful_symbols"],
                report["total_symbols"],
                report["total_failed_symbols"],
                report["db_path"],
            )
        except Exception as exc:
            logger.error(f"Universe 下载失败：{exc}")
            raise MarketDataFetchError(f"按周期下载universe数据失败: {exc}") from exc

    async def download_custom_universe_data(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
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
        description: str | None = None,
        universe_output_path: Path | str | None = None,
        overwrite_universe_output: bool = False,
    ) -> dict[str, Any]:
        """下载自定义 universe（symbols + 时间区间）."""
        normalized_symbols = self._normalize_symbols(symbols)
        standardized_start = TimeRangeProcessor.standardize_date_format(start_date)
        standardized_end = TimeRangeProcessor.standardize_date_format(end_date)

        valid_symbols, skipped_symbols = await self._split_valid_symbols(normalized_symbols)

        if skipped_symbols:
            logger.warning("custom_universe_symbols_skipped", skipped=skipped_symbols)

        universe_def = self._create_custom_universe_definition(
            symbols=valid_symbols,
            start_date=standardized_start,
            end_date=standardized_end,
            description=description,
        )
        universe_file_path = self._build_custom_universe_file_path(
            universe_output_path=universe_output_path,
            start_date=standardized_start,
            end_date=standardized_end,
            symbol_count=len(valid_symbols),
        )
        universe_file_written = False
        if universe_file_path.exists() and not overwrite_universe_output:
            logger.info("custom_universe_file_preserved", path=str(universe_file_path))
        else:
            universe_def.save_to_file(universe_file_path)
            universe_file_written = True

        download_report = await self._download_universe_definition(
            universe_def=universe_def,
            db_path=db_path,
            retry_config=retry_config,
            api_request_delay=api_request_delay,
            vision_request_delay=vision_request_delay,
            download_market_metrics=download_market_metrics,
            incremental=incremental,
            interval=interval,
            max_api_workers=max_api_workers,
            max_vision_workers=max_vision_workers,
            max_retries=max_retries,
            run_id=generate_run_id("custom-universe"),
        )

        return {
            "requested_symbols": len(symbols),
            "normalized_symbols": normalized_symbols,
            "valid_symbols": valid_symbols,
            "skipped_symbols": skipped_symbols,
            "universe_file": str(universe_file_path),
            "universe_file_written": universe_file_written,
            "download_summary": download_report,
        }

    async def _download_universe_definition(
        self,
        universe_def: UniverseDefinition,
        db_path: Path | str,
        retry_config: RetryConfig,
        api_request_delay: float,
        vision_request_delay: float,
        download_market_metrics: bool,
        incremental: bool,
        interval: Freq,
        max_api_workers: int,
        max_vision_workers: int,
        max_retries: int,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """下载统一 universe 定义，返回汇总报告."""
        run = run_id or generate_run_id("universe")
        db_file_path = self._validate_and_prepare_path(db_path, is_file=True)

        logger.info(
            f"开始下载 Universe 数据：{len(universe_def.snapshots)} 个快照"
            f"（频率 {interval.value}，API 并发 {max_api_workers}，"
            f"Vision 并发 {max_vision_workers}，下载指标：{'是' if download_market_metrics else '否'}）。"
        )

        kline_download_results: list[IntegrityReport] = []
        snapshot_reports: list[dict[str, Any]] = []
        skipped_snapshots: list[dict[str, Any]] = []
        snapshot_starts = [snapshot.start_date for snapshot in universe_def.snapshots]
        snapshot_ends = [snapshot.end_date for snapshot in universe_def.snapshots]
        requested_start_date = min(snapshot_starts) if snapshot_starts else None
        requested_end_date = max(snapshot_ends) if snapshot_ends else None

        for index, snapshot in enumerate(universe_def.snapshots, start=1):
            if not snapshot.symbols:
                skipped_snapshots.append(
                    {
                        "index": index,
                        "effective_date": snapshot.effective_date,
                        "reason": "no_symbols",
                        "start_date": snapshot.start_date,
                        "end_date": snapshot.end_date,
                    }
                )
                logger.warning("snapshot_skipped_no_symbols", snapshot=snapshot.effective_date)
                continue

            logger.debug(
                "snapshot.start",
                run=run,
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
                run_id=run,
            )
            kline_download_results.append(kline_report)

            if download_market_metrics:
                logger.debug("metrics.start", run=run, snapshot=snapshot.effective_date)
                await self._download_market_metrics_for_snapshot(
                    snapshot=snapshot,
                    db_path=db_file_path,
                    api_request_delay=api_request_delay,
                    vision_request_delay=vision_request_delay,
                    max_api_workers=max_api_workers,
                    max_vision_workers=max_vision_workers,
                    incremental=incremental,
                    run_id=run,
                )

            snapshot_reports.append(
                {
                    "index": index,
                    "effective_date": snapshot.effective_date,
                    "start_date": snapshot.start_date,
                    "end_date": snapshot.end_date,
                    "total_symbols": kline_report.total_symbols,
                    "successful_symbols": kline_report.successful_symbols,
                    "failed_symbols": kline_report.failed_symbols,
                    "missing_periods": kline_report.missing_periods,
                }
            )

            logger.debug("snapshot.done", run=run, snapshot=snapshot.effective_date)

        total_symbols = sum(report.total_symbols for report in kline_download_results)
        total_success = sum(report.successful_symbols for report in kline_download_results)
        total_failures = sum(len(report.failed_symbols) for report in kline_download_results)

        failed_reason_summary: dict[str, int] = defaultdict(int)
        for report in kline_download_results:
            for missing in report.missing_periods:
                reason = missing.get("reason", "unknown")
                failed_reason_summary[reason] += 1

        return {
            "run_id": run,
            "db_path": str(db_file_path),
            "download_context": {
                "requested_start_date": requested_start_date,
                "requested_end_date": requested_end_date,
                "interval": interval.value,
                "incremental": incremental,
                "download_market_metrics": download_market_metrics,
                "max_api_workers": max_api_workers,
                "max_vision_workers": max_vision_workers,
                "max_retries": max_retries,
            },
            "total_snapshots": len(universe_def.snapshots),
            "processed_snapshots": len(snapshot_reports),
            "skipped_snapshots": skipped_snapshots,
            "total_symbols": total_symbols,
            "total_successful_symbols": total_success,
            "total_failed_symbols": total_failures,
            "failed_reason_summary": dict(failed_reason_summary),
            "snapshot_reports": snapshot_reports,
        }

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

    async def define_universe_with_daily_check(
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
        daily_check_workers: int = 5,
        daily_check_request_delay: float = 0.0,
    ) -> UniverseDefinition:
        """定义 universe 并执行使用期日级校验."""
        return await self.universe_manager.define_universe_with_daily_check(
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
            daily_check_workers=daily_check_workers,
            daily_check_request_delay=daily_check_request_delay,
        )

    async def define_custom_universe_with_daily_check(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        output_path: Path | str,
        description: str | None = None,
        daily_check_workers: int = 5,
        daily_check_request_delay: float = 0.0,
    ) -> UniverseDefinition:
        """基于 symbols + 时间区间定义 universe，并执行使用期日级校验."""
        normalized_symbols = self._normalize_symbols(symbols)
        standardized_start = TimeRangeProcessor.standardize_date_format(start_date)
        standardized_end = TimeRangeProcessor.standardize_date_format(end_date)

        valid_symbols, skipped_symbols = await self._split_valid_symbols(normalized_symbols)
        if skipped_symbols:
            logger.warning("custom_define_symbols_skipped", skipped=skipped_symbols)

        universe_def = self._create_custom_universe_definition(
            symbols=valid_symbols,
            start_date=standardized_start,
            end_date=standardized_end,
            description=description,
        )

        if universe_def.snapshots:
            metadata = universe_def.snapshots[0].metadata or {}
            metadata["requested_symbols"] = len(symbols)
            metadata["normalized_symbols"] = normalized_symbols
            metadata["valid_symbols"] = valid_symbols
            metadata["skipped_symbols"] = skipped_symbols
            universe_def.snapshots[0].metadata = metadata

        return await self.universe_manager.apply_daily_existence_check(
            universe_def=universe_def,
            output_path=output_path,
            daily_check_workers=daily_check_workers,
            daily_check_request_delay=daily_check_request_delay,
        )

    async def export_universe_data(
        self,
        universe_file: Path | str,
        db_path: Path | str,
        export_base_path: Path | str,
        source_freq: Freq,
        export_freq: Freq,
        include_klines: bool = True,
        include_metrics: bool = True,
        metrics_config: dict[str, Any] | None = None,
        field_mapping: dict[str, str] | None = None,
        custom_start_date: str | None = None,
        custom_end_date: str | None = None,
    ) -> dict[str, Any]:
        """按 universe 文件导出数据并生成 report.json."""
        universe_file_obj = self._validate_and_prepare_path(universe_file, is_file=True)
        if not universe_file_obj.exists():
            raise FileNotFoundError(f"Universe文件不存在: {universe_file_obj}")

        universe_def = UniverseDefinition.load_from_file(universe_file_obj)
        if custom_start_date or custom_end_date:
            universe_def = TimeRangeProcessor.apply_custom_time_range(universe_def, custom_start_date, custom_end_date)

        output_path = self._build_export_output_path(
            export_base_path=export_base_path,
            export_freq=export_freq,
            universe_def=universe_def,
            custom_start_date=custom_start_date,
            custom_end_date=custom_end_date,
        )

        return await self._export_universe_definition(
            universe_def=universe_def,
            db_path=db_path,
            output_path=output_path,
            source_freq=source_freq,
            export_freq=export_freq,
            include_klines=include_klines,
            include_metrics=include_metrics,
            metrics_config=metrics_config,
            field_mapping=field_mapping,
            extra_report_fields={
                "source": "universe_file",
                "universe_file": str(universe_file_obj),
            },
        )

    async def export_custom_universe_data(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        db_path: Path | str,
        export_base_path: Path | str,
        source_freq: Freq,
        export_freq: Freq,
        include_klines: bool = True,
        include_metrics: bool = True,
        metrics_config: dict[str, Any] | None = None,
        field_mapping: dict[str, str] | None = None,
        description: str | None = None,
        universe_output_path: Path | str | None = None,
    ) -> dict[str, Any]:
        """按自定义 symbols + 时间区间导出数据并生成 report.json."""
        normalized_symbols = self._normalize_symbols(symbols)
        standardized_start = TimeRangeProcessor.standardize_date_format(start_date)
        standardized_end = TimeRangeProcessor.standardize_date_format(end_date)

        valid_symbols, skipped_symbols = await self._split_valid_symbols(normalized_symbols)

        custom_universe = self._create_custom_universe_definition(
            symbols=valid_symbols,
            start_date=standardized_start,
            end_date=standardized_end,
            description=description,
        )

        output_path = self._build_export_output_path(
            export_base_path=export_base_path,
            export_freq=export_freq,
            universe_def=custom_universe,
            is_custom=True,
        )
        universe_file_path = self._build_custom_universe_file_path(
            universe_output_path=universe_output_path or output_path / "universe.json",
            start_date=standardized_start,
            end_date=standardized_end,
            symbol_count=len(valid_symbols),
        )
        custom_universe.save_to_file(universe_file_path)

        return await self._export_universe_definition(
            universe_def=custom_universe,
            db_path=db_path,
            output_path=output_path,
            source_freq=source_freq,
            export_freq=export_freq,
            include_klines=include_klines,
            include_metrics=include_metrics,
            metrics_config=metrics_config,
            field_mapping=field_mapping,
            extra_report_fields={
                "source": "custom_symbols",
                "requested_symbols": len(symbols),
                "normalized_symbols": normalized_symbols,
                "valid_symbols": valid_symbols,
                "skipped_symbols": skipped_symbols,
                "universe_file": str(universe_file_path),
            },
        )

    async def _export_universe_definition(
        self,
        universe_def: UniverseDefinition,
        db_path: Path | str,
        output_path: Path | str,
        source_freq: Freq,
        export_freq: Freq,
        include_klines: bool,
        include_metrics: bool,
        metrics_config: dict[str, Any] | None,
        field_mapping: dict[str, str] | None,
        extra_report_fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """导出统一 universe 定义并输出缺失汇总报告."""
        db_file_path = self._validate_and_prepare_path(db_path, is_file=True)
        output_dir = self._validate_and_prepare_path(output_path, is_file=False)

        define_missing_map: dict[str, set[str]] = defaultdict(set)
        export_missing_map: dict[str, set[str]] = defaultdict(set)
        merged_missing_map: dict[str, set[str]] = defaultdict(set)

        exported_snapshots: list[dict[str, Any]] = []
        skipped_snapshots: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []

        db = Database(db_file_path)
        await db.initialize()

        try:
            for index, snapshot in enumerate(universe_def.snapshots, start=1):
                define_missing = self._extract_define_missing(snapshot)
                self._merge_missing_sets(define_missing_map, define_missing)

                if not snapshot.symbols:
                    skipped_snapshots.append(
                        {
                            "index": index,
                            "effective_date": snapshot.effective_date,
                            "reason": "no_symbols",
                            "start_date": snapshot.start_date,
                            "end_date": snapshot.end_date,
                        }
                    )
                    continue

                try:
                    await db.numpy_exporter.export_combined_data(
                        symbols=snapshot.symbols,
                        start_time=snapshot.start_date,
                        end_time=snapshot.end_date,
                        source_freq=source_freq,
                        export_freq=export_freq,
                        output_path=output_dir,
                        include_klines=include_klines,
                        include_metrics=include_metrics,
                        metrics_config=metrics_config,
                        field_mapping=field_mapping,
                    )

                    symbol_payload = self._load_export_symbol_dict(output_dir / "univ_dct2.json")
                    export_missing = self._calculate_export_missing_for_snapshot(symbol_payload, snapshot)
                    self._merge_missing_sets(export_missing_map, export_missing)

                    expected_days = len(pd.date_range(start=snapshot.start_date, end=snapshot.end_date, freq="D", tz="UTC"))
                    missing_days = len(export_missing)

                    exported_snapshots.append(
                        {
                            "index": index,
                            "effective_date": snapshot.effective_date,
                            "start_date": snapshot.start_date,
                            "end_date": snapshot.end_date,
                            "symbols": len(snapshot.symbols),
                            "expected_days": expected_days,
                            "missing_days": missing_days,
                        }
                    )
                except Exception as exc:
                    errors.append(
                        {
                            "index": str(index),
                            "effective_date": snapshot.effective_date,
                            "error": str(exc),
                        }
                    )
                    logger.error("export_snapshot_failed", snapshot=snapshot.effective_date, error=str(exc))
        finally:
            await db.close()

        self._merge_missing_sets(merged_missing_map, self._serialize_missing_sets(define_missing_map))
        self._merge_missing_sets(merged_missing_map, self._serialize_missing_sets(export_missing_map))

        report: dict[str, Any] = {
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "output_path": str(output_dir),
            "db_path": str(db_file_path),
            "source_freq": source_freq.value,
            "export_freq": export_freq.value,
            "total_snapshots": len(universe_def.snapshots),
            "exported_snapshots": exported_snapshots,
            "skipped_snapshots": skipped_snapshots,
            "errors": errors,
            "define_missing": self._serialize_missing_sets(define_missing_map),
            "export_missing": self._serialize_missing_sets(export_missing_map),
            "merged_missing": self._serialize_missing_sets(merged_missing_map),
            "stats": {
                "exported_snapshot_count": len(exported_snapshots),
                "skipped_snapshot_count": len(skipped_snapshots),
                "error_count": len(errors),
                "define_missing_date_count": len(define_missing_map),
                "export_missing_date_count": len(export_missing_map),
                "merged_missing_date_count": len(merged_missing_map),
            },
        }

        if extra_report_fields:
            report.update(extra_report_fields)

        report_path = output_dir / "report.json"
        with open(report_path, "w", encoding="utf-8") as fp:
            json.dump(report, fp, ensure_ascii=False, indent=2)

        report["report_path"] = str(report_path)
        return report

    @staticmethod
    def _extract_define_missing(snapshot: UniverseSnapshot) -> dict[str, list[str]]:
        """提取定义阶段的缺失映射 (date -> symbols)."""
        metadata = snapshot.metadata or {}
        check_data = metadata.get("daily_existence_check", {})
        missing_by_date = check_data.get("missing_by_date", {})

        if not isinstance(missing_by_date, dict):
            return {}

        result: dict[str, list[str]] = {}
        for date_str, symbols in missing_by_date.items():
            if isinstance(symbols, list):
                result[date_str] = [str(symbol) for symbol in symbols]
        return result

    @staticmethod
    def _load_export_symbol_dict(symbol_dict_path: Path) -> dict[str, list[str]]:
        """读取导出的 symbol 字典，读取失败则返回空."""
        if not symbol_dict_path.exists():
            return {}

        try:
            with open(symbol_dict_path, encoding="utf-8") as fp:
                payload = json.load(fp)
        except (json.JSONDecodeError, OSError):
            return {}

        if not isinstance(payload, dict):
            return {}

        result: dict[str, list[str]] = {}
        for key, value in payload.items():
            if isinstance(value, list):
                result[key] = [str(item) for item in value]

        return result

    @staticmethod
    def _calculate_export_missing_for_snapshot(
        symbol_payload: dict[str, list[str]],
        snapshot: UniverseSnapshot,
    ) -> dict[str, list[str]]:
        """计算导出阶段缺失映射 (date -> missing symbols)."""
        expected_symbols = set(snapshot.symbols)
        if not expected_symbols:
            return {}

        missing_by_date: dict[str, list[str]] = {}
        for date in pd.date_range(start=snapshot.start_date, end=snapshot.end_date, freq="D", tz="UTC"):
            date_key = date.strftime("%Y%m%d")
            actual_symbols = set(symbol_payload.get(date_key, []))
            missing_symbols = sorted(expected_symbols - actual_symbols)
            if missing_symbols:
                missing_by_date[date.strftime("%Y-%m-%d")] = missing_symbols

        return missing_by_date

    @staticmethod
    def _merge_missing_sets(target: dict[str, set[str]], source: dict[str, list[str]]) -> None:
        """将 date->symbols(list) 合并进 date->symbols(set)."""
        for date_str, symbols in source.items():
            target[date_str].update(symbols)

    @staticmethod
    def _serialize_missing_sets(source: dict[str, set[str]]) -> dict[str, list[str]]:
        """将 date->symbols(set) 序列化为排序后的 list."""
        return {date_str: sorted(symbols) for date_str, symbols in sorted(source.items()) if symbols}

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
            start_time = self._date_to_timestamp_start(date)
            end_time = self._date_to_timestamp_end(date)

            klines = await self.client.futures_klines(
                symbol=symbol,
                interval="1d",
                startTime=start_time,
                endTime=end_time,
                limit=1,
            )

            return bool(klines and len(klines) > 0)

        except Exception as e:
            logger.debug(f"检查交易对 {symbol} 在 {date} 是否存在时出错: {e}")
            return False

    # ==================== 私有辅助方法 ====================

    async def _download_market_metrics_for_snapshot(
        self,
        snapshot: UniverseSnapshot,
        db_path: Path,
        api_request_delay: float,
        vision_request_delay: float,
        max_api_workers: int,
        max_vision_workers: int,
        incremental: bool = True,
        run_id: str | None = None,
    ) -> None:
        """为单个快照下载市场指标数据."""
        try:
            if self.db is None:
                self.db = Database(db_path)

            symbols = snapshot.symbols
            start_time = snapshot.start_date
            end_time = snapshot.end_date

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
                max_workers=max_api_workers,
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

    @staticmethod
    def _normalize_symbols(symbols: list[str]) -> list[str]:
        """标准化 symbol 列表（大写 + 去重 + 保序）."""
        normalized: list[str] = []
        seen: set[str] = set()

        for symbol in symbols:
            normalized_symbol = symbol.strip().upper()
            if not normalized_symbol or normalized_symbol in seen:
                continue
            seen.add(normalized_symbol)
            normalized.append(normalized_symbol)

        return normalized

    async def _split_valid_symbols(self, symbols: list[str]) -> tuple[list[str], list[str]]:
        """将 symbols 划分为有效和无效集合."""
        if not symbols:
            return [], []

        valid_set = set(await self.get_perpetual_symbols(only_trading=True))
        valid_symbols = [symbol for symbol in symbols if symbol in valid_set]
        skipped_symbols = [symbol for symbol in symbols if symbol not in valid_set]
        return valid_symbols, skipped_symbols

    @staticmethod
    def _create_custom_universe_definition(
        symbols: list[str],
        start_date: str,
        end_date: str,
        description: str | None = None,
    ) -> UniverseDefinition:
        """基于 symbols + 时间区间构造单快照 universe 定义."""
        custom_config = UniverseConfig(
            start_date=start_date,
            end_date=end_date,
            t1_months=1,
            t2_months=1,
            t3_months=0,
            delay_days=0,
            quote_asset="USDT",
            top_k=max(len(symbols), 1),
        )

        snapshot = UniverseSnapshot.create_with_dates_and_timestamps(
            usage_t1_start=start_date,
            usage_t1_end=end_date,
            calculated_t1_start=start_date,
            calculated_t1_end=end_date,
            symbols=symbols,
            mean_daily_amounts=dict.fromkeys(symbols, 0.0),
            metadata={
                "custom_universe": True,
                "selected_symbols_count": len(symbols),
            },
        )

        return UniverseDefinition(
            config=custom_config,
            snapshots=[snapshot],
            creation_time=datetime.now(tz=UTC),
            description=description or "Custom universe generated from symbols and date range",
        )

    def _build_custom_universe_file_path(
        self,
        universe_output_path: Path | str | None,
        start_date: str,
        end_date: str,
        symbol_count: int,
    ) -> Path:
        """构建自定义 universe 定义文件路径."""
        file_name = f"universe_custom_{start_date}_{end_date}_{symbol_count}.json"
        if universe_output_path is None:
            return self._validate_and_prepare_path(Path("./data") / file_name, is_file=True)
        return self._validate_and_prepare_path(universe_output_path, is_file=True, file_name=file_name)

    def _build_export_output_path(
        self,
        export_base_path: Path | str,
        export_freq: Freq,
        universe_def: UniverseDefinition,
        custom_start_date: str | None = None,
        custom_end_date: str | None = None,
        is_custom: bool = False,
    ) -> Path:
        """构建导出目录（自动子目录策略）."""
        base_path = self._validate_and_prepare_path(export_base_path, is_file=False)
        freq_dir = self._map_export_freq_dir(export_freq)

        if is_custom:
            snapshot = universe_def.snapshots[0] if universe_def.snapshots else None
            name = f"custom_{snapshot.start_date}_{snapshot.end_date}_{len(snapshot.symbols)}" if snapshot else "custom_empty"
        else:
            config = universe_def.config
            top_value = f"k{config.top_k}" if config.top_k is not None else f"r{config.top_ratio}"
            name = f"univ_{config.t1_months}_{config.t2_months}_{config.t3_months}_{top_value}"
            if custom_start_date or custom_end_date:
                suffix_start = custom_start_date or config.start_date
                suffix_end = custom_end_date or config.end_date
                name = f"{name}_custom_{suffix_start}_{suffix_end}"

        return base_path / freq_dir / name

    @staticmethod
    def _map_export_freq_dir(export_freq: Freq) -> str:
        """将导出频率映射到目录名."""
        mapping = {
            Freq.d1: "D1B",
            Freq.h1: "H1B",
            Freq.m1: "M1B",
            Freq.m5: "M5B",
        }
        return mapping.get(export_freq, f"{export_freq.value.upper()}B")

    def _validate_and_prepare_path(self, path: Path | str, is_file: bool = False, file_name: str | None = None) -> Path:
        """验证并准备路径."""
        if not path:
            raise ValueError("路径不能为空，必须手动指定")

        path_obj = Path(path)

        if is_file:
            if path_obj.is_dir():
                path_obj = path_obj.joinpath(file_name) if file_name else path_obj
            else:
                path_obj.parent.mkdir(parents=True, exist_ok=True)
        else:
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
        raise TypeError(f"不支持的时间类型: {type(time_value)}")
