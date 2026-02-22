"""市场数据服务.

专注于核心API功能，使用组合模式整合各个专业模块。
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from binance import AsyncClient

from cryptoservice.client import BinanceClientFactory
from cryptoservice.config import RetryConfig, settings
from cryptoservice.config.logging import get_logger
from cryptoservice.exceptions import InvalidSymbolError, MarketDataFetchError, RateLimitError
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
from cryptoservice.models.universe import MISSING_REASON_MISSING_IN_EXPORT, UniverseDailySnapshot
from cryptoservice.storage.database import Database
from cryptoservice.utils import DataConverter
from cryptoservice.utils.run_id import generate_run_id

from .downloaders import EndpointControlRegistry, KlineDownloader, MetricsDownloader, VisionDownloader
from .processors import CategoryManager, DataValidator, UniverseManager

logger = get_logger(__name__)
SYMBOL_CHECK_TIMEOUT_SECONDS = 20.0


class MarketDataService:
    """市场数据服务实现类."""

    def __init__(self, client: AsyncClient) -> None:
        """初始化市场数据服务 (私有构造函数)."""
        self.client = client
        self.converter = DataConverter()
        self.db: Database | None = None

        self._endpoint_controls = EndpointControlRegistry(base_delay=0.5)
        self.kline_downloader = KlineDownloader(self.client, endpoint_controls=self._endpoint_controls)
        self.metrics_downloader = MetricsDownloader(self.client, endpoint_controls=self._endpoint_controls)
        self.vision_downloader = VisionDownloader(self.client, endpoint_controls=self._endpoint_controls)
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
            logger.error(f"Failed to fetch perpetual symbols: {e}")
            raise MarketDataFetchError(f"Failed to fetch perpetual symbols: {e}") from e

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
                logger.warning(f"No data found for symbol {symbol} in the requested time range")
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
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        """Download data strictly from a v2 universe daily plan."""
        run_id = generate_run_id("universe")
        started_at = time.perf_counter()
        try:
            universe_file_obj = self._validate_and_prepare_path(universe_file, is_file=True)
            if not universe_file_obj.exists():
                raise FileNotFoundError(f"Universe file not found: {universe_file_obj}")

            universe_def = UniverseDefinition.load_from_file(universe_file_obj)
            (
                effective_start_date,
                effective_end_date,
                filtered_snapshots,
                override_applied,
                override_start_date,
                override_end_date,
            ) = self._resolve_universe_date_window(universe_def=universe_def, start_date=start_date, end_date=end_date)
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
                universe_file=str(universe_file_obj),
                snapshots=filtered_snapshots,
                effective_start_date=effective_start_date,
                effective_end_date=effective_end_date,
                override_applied=override_applied,
                override_start_date=override_start_date,
                override_end_date=override_end_date,
            )

            logger.info(
                "download.run_done",
                run=run_id,
                stage="universe",
                dataset="universe",
                status="complete",
                duration_ms=int((time.perf_counter() - started_at) * 1000),
                total_days=report["total_days"],
                total_successful_symbols=report["total_successful_symbols"],
                total_symbols=report["total_symbols"],
                total_failed_symbols=report["total_failed_symbols"],
                db_path=report["db_path"],
            )
            return report
        except Exception as exc:
            logger.error(
                "download.run_done",
                run=run_id,
                stage="universe",
                dataset="universe",
                status="error",
                duration_ms=int((time.perf_counter() - started_at) * 1000),
                terminal=False,
                error=str(exc),
            )
            raise MarketDataFetchError(f"download_universe_data failed: {exc}") from exc

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
        universe_file: str | None = None,
        snapshots: list[UniverseDailySnapshot] | None = None,
        effective_start_date: str | None = None,
        effective_end_date: str | None = None,
        override_applied: bool = False,
        override_start_date: str | None = None,
        override_end_date: str | None = None,
    ) -> dict[str, Any]:
        """Execute strict day-symbol download plan from universe daily snapshots."""
        run = run_id or generate_run_id("universe")
        db_file_path = self._validate_and_prepare_path(db_path, is_file=True)
        snapshots_to_process = snapshots if snapshots is not None else universe_def.daily_snapshots
        requested_start_date = universe_def.start_date
        requested_end_date = universe_def.end_date
        effective_start = effective_start_date or requested_start_date
        effective_end = effective_end_date or requested_end_date

        skipped_days: list[dict[str, Any]] = []
        active_days: list[tuple[int, UniverseDailySnapshot]] = []

        for index, snapshot in enumerate(snapshots_to_process, start=1):
            if not snapshot.active_symbols:
                skipped_days.append({"index": index, "date": snapshot.date, "reason": "no_active_symbols"})
                continue
            active_days.append((index, snapshot))

        kline_stage = await self._run_kline_stage(
            run_id=run,
            snapshots=active_days,
            db_path=db_file_path,
            retry_config=retry_config,
            incremental=incremental,
            interval=interval,
            max_api_workers=max_api_workers,
            max_retries=max_retries,
        )
        day_reports = kline_stage["day_reports"]
        stage_reports: dict[str, dict[str, Any]] = {"kline": kline_stage["stage_report"]}
        stage_errors: list[dict[str, Any]] = list(kline_stage["stage_errors"])

        if download_market_metrics:
            metrics_stage = await self._run_metrics_stage(
                run_id=run,
                snapshots=active_days,
                day_reports=day_reports,
                db_path=db_file_path,
                api_request_delay=api_request_delay,
                vision_request_delay=vision_request_delay,
                max_api_workers=max_api_workers,
                max_vision_workers=max_vision_workers,
                incremental=incremental,
            )
            day_reports = metrics_stage["day_reports"]
            stage_reports["metrics"] = metrics_stage["stage_report"]
            stage_errors.extend(metrics_stage["stage_errors"])
        else:
            stage_reports["metrics"] = {
                "stage": "metrics",
                "dataset": "market_metrics",
                "status": "skipped",
                "duration_ms": 0,
                "days_total": len(active_days),
                "days_complete": 0,
                "days_partial": 0,
                "days_error": 0,
                "days_aborted": 0,
                "sources": {
                    "vision": {
                        "status": "skipped",
                        "aborted": False,
                        "aborted_from_date": None,
                        "days_complete": 0,
                        "days_error": 0,
                        "days_aborted": 0,
                    },
                    "funding_rate": {
                        "status": "skipped",
                        "aborted": False,
                        "aborted_from_date": None,
                        "days_complete": 0,
                        "days_error": 0,
                        "days_aborted": 0,
                    },
                },
            }

        return self._compose_universe_report(
            run_id=run,
            db_file_path=db_file_path,
            universe_def=universe_def,
            universe_file=universe_file,
            snapshots_to_process=snapshots_to_process,
            day_reports=day_reports,
            skipped_days=skipped_days,
            stage_reports=stage_reports,
            stage_errors=stage_errors,
            requested_start_date=requested_start_date,
            requested_end_date=requested_end_date,
            effective_start=effective_start,
            effective_end=effective_end,
            override_applied=override_applied,
            override_start_date=override_start_date,
            override_end_date=override_end_date,
            interval=interval,
            incremental=incremental,
            download_market_metrics=download_market_metrics,
            max_api_workers=max_api_workers,
            max_vision_workers=max_vision_workers,
            max_retries=max_retries,
        )

    async def _run_kline_stage(
        self,
        *,
        run_id: str,
        snapshots: list[tuple[int, UniverseDailySnapshot]],
        db_path: Path,
        retry_config: RetryConfig,
        incremental: bool,
        interval: Freq,
        max_api_workers: int,
        max_retries: int,
    ) -> dict[str, Any]:
        """Run kline stage for all active universe days."""
        stage_started_at = time.perf_counter()
        day_reports: list[dict[str, Any]] = []
        stage_errors: list[dict[str, Any]] = []
        day_status_counts: dict[str, int] = defaultdict(int)
        terminal_aborted = False
        terminal_error_message: str | None = None

        logger.info(
            "download.stage_start",
            run=run_id,
            stage="kline",
            dataset="kline",
            status="start",
            duration_ms=0,
            days=len(snapshots),
        )

        for index, snapshot in snapshots:
            symbols = list(snapshot.active_symbols)
            day_started_at = time.perf_counter()

            if terminal_aborted:
                reason = terminal_error_message or "terminal_rate_limit_aborted"
                missing_periods = [{"symbol": symbol, "period": f"{snapshot.date} - {snapshot.date}", "reason": reason} for symbol in symbols]
                day_report = {
                    "index": index,
                    "date": snapshot.date,
                    "total_symbols": len(symbols),
                    "successful_symbols": 0,
                    "failed_symbols": symbols,
                    "missing_periods": missing_periods,
                }
                day_reports.append(day_report)
                day_status_counts["aborted"] += 1
                logger.warning(
                    "download.day_done",
                    run=run_id,
                    stage="kline",
                    dataset="kline",
                    status="aborted",
                    duration_ms=0,
                    date=snapshot.date,
                    total_symbols=len(symbols),
                    successful_symbols=0,
                    failed_symbols=len(symbols),
                    terminal=True,
                    error=reason,
                )
                continue

            logger.info(
                "download.day_start",
                run=run_id,
                stage="kline",
                dataset="kline",
                status="start",
                duration_ms=0,
                date=snapshot.date,
                total_symbols=len(symbols),
            )

            try:
                kline_report = await self.get_perpetual_data(
                    symbols=symbols,
                    start_time=snapshot.date,
                    end_time=snapshot.date,
                    db_path=db_path,
                    interval=interval,
                    max_workers=max_api_workers,
                    max_retries=max_retries,
                    retry_config=retry_config,
                    incremental=incremental,
                    run_id=run_id,
                )
                day_report = {
                    "index": index,
                    "date": snapshot.date,
                    "total_symbols": kline_report.total_symbols,
                    "successful_symbols": kline_report.successful_symbols,
                    "failed_symbols": list(kline_report.failed_symbols),
                    "missing_periods": list(kline_report.missing_periods),
                }
                day_reports.append(day_report)
                status = "complete" if not kline_report.failed_symbols and not kline_report.missing_periods else "partial"
                day_status_counts[status] += 1
                logger.info(
                    "download.day_done",
                    run=run_id,
                    stage="kline",
                    dataset="kline",
                    status=status,
                    duration_ms=int((time.perf_counter() - day_started_at) * 1000),
                    date=snapshot.date,
                    total_symbols=kline_report.total_symbols,
                    successful_symbols=kline_report.successful_symbols,
                    failed_symbols=len(kline_report.failed_symbols),
                )
            except RateLimitError as exc:
                terminal_aborted = True
                terminal_error_message = str(exc)
                stage_errors.append(
                    self._normalize_stage_error(
                        run_id=run_id,
                        stage="kline",
                        dataset="kline",
                        date=snapshot.date,
                        error=str(exc),
                        terminal=True,
                    )
                )
                missing_periods = [{"symbol": symbol, "period": f"{snapshot.date} - {snapshot.date}", "reason": str(exc)} for symbol in symbols]
                day_report = {
                    "index": index,
                    "date": snapshot.date,
                    "total_symbols": len(symbols),
                    "successful_symbols": 0,
                    "failed_symbols": symbols,
                    "missing_periods": missing_periods,
                }
                day_reports.append(day_report)
                day_status_counts["aborted"] += 1
                logger.error(
                    "download.day_done",
                    run=run_id,
                    stage="kline",
                    dataset="kline",
                    status="aborted",
                    duration_ms=int((time.perf_counter() - day_started_at) * 1000),
                    date=snapshot.date,
                    total_symbols=len(symbols),
                    successful_symbols=0,
                    failed_symbols=len(symbols),
                    terminal=True,
                    error=str(exc),
                )
            except Exception as exc:  # noqa: BLE001
                stage_errors.append(
                    self._normalize_stage_error(
                        run_id=run_id,
                        stage="kline",
                        dataset="kline",
                        date=snapshot.date,
                        error=str(exc),
                        terminal=False,
                    )
                )
                missing_periods = [{"symbol": symbol, "period": f"{snapshot.date} - {snapshot.date}", "reason": str(exc)} for symbol in symbols]
                day_report = {
                    "index": index,
                    "date": snapshot.date,
                    "total_symbols": len(symbols),
                    "successful_symbols": 0,
                    "failed_symbols": symbols,
                    "missing_periods": missing_periods,
                }
                day_reports.append(day_report)
                day_status_counts["error"] += 1
                logger.error(
                    "download.day_done",
                    run=run_id,
                    stage="kline",
                    dataset="kline",
                    status="error",
                    duration_ms=int((time.perf_counter() - day_started_at) * 1000),
                    date=snapshot.date,
                    total_symbols=len(symbols),
                    successful_symbols=0,
                    failed_symbols=len(symbols),
                    terminal=False,
                    error=str(exc),
                )

        total_symbols = sum(int(report.get("total_symbols", 0)) for report in day_reports)
        total_success = sum(int(report.get("successful_symbols", 0)) for report in day_reports)
        total_failures = sum(len(cast(list[str], report.get("failed_symbols", []))) for report in day_reports)

        if not day_reports:
            stage_status = "skipped"
        elif terminal_aborted and total_success == 0 and total_failures == total_symbols:
            stage_status = "aborted"
        elif day_status_counts.get("partial", 0) > 0 or total_failures > 0 or stage_errors:
            stage_status = "partial"
        else:
            stage_status = "complete"

        stage_report = {
            "stage": "kline",
            "dataset": "kline",
            "status": stage_status,
            "duration_ms": int((time.perf_counter() - stage_started_at) * 1000),
            "days_total": len(snapshots),
            "days_complete": day_status_counts.get("complete", 0),
            "days_partial": day_status_counts.get("partial", 0),
            "days_error": day_status_counts.get("error", 0),
            "days_aborted": day_status_counts.get("aborted", 0),
            "symbols_total": total_symbols,
            "symbols_successful": total_success,
            "symbols_failed": total_failures,
        }
        logger.info(
            "download.stage_done",
            run=run_id,
            stage="kline",
            dataset="kline",
            status=stage_status,
            duration_ms=stage_report["duration_ms"],
            days_total=stage_report["days_total"],
            symbols_total=stage_report["symbols_total"],
            symbols_successful=stage_report["symbols_successful"],
            symbols_failed=stage_report["symbols_failed"],
        )

        return {
            "day_reports": day_reports,
            "stage_report": stage_report,
            "stage_errors": stage_errors,
        }

    async def _run_metrics_stage(  # noqa: C901
        self,
        *,
        run_id: str,
        snapshots: list[tuple[int, UniverseDailySnapshot]],
        day_reports: list[dict[str, Any]],
        db_path: Path,
        api_request_delay: float,
        vision_request_delay: float,
        max_api_workers: int,
        max_vision_workers: int,
        incremental: bool,
    ) -> dict[str, Any]:
        """Run metrics stage with source-level isolation and terminal abort per source."""
        stage_started_at = time.perf_counter()
        day_status_counts: dict[str, int] = defaultdict(int)
        stage_errors: list[dict[str, Any]] = []
        day_report_by_date = {str(report["date"]): report for report in day_reports}

        source_state: dict[str, dict[str, Any]] = {
            "vision": {
                "aborted": False,
                "aborted_from_date": None,
                "days_complete": 0,
                "days_error": 0,
                "days_aborted": 0,
            },
            "funding_rate": {
                "aborted": False,
                "aborted_from_date": None,
                "days_complete": 0,
                "days_error": 0,
                "days_aborted": 0,
            },
        }

        logger.info(
            "download.stage_start",
            run=run_id,
            stage="metrics",
            dataset="market_metrics",
            status="start",
            duration_ms=0,
            days=len(snapshots),
            incremental=incremental,
        )

        for _, snapshot in snapshots:
            day_metrics, day_errors, source_aborts = await self._download_market_metrics_for_date(
                date=snapshot.date,
                symbols=snapshot.active_symbols,
                db_path=db_path,
                api_request_delay=api_request_delay,
                vision_request_delay=vision_request_delay,
                max_api_workers=max_api_workers,
                max_vision_workers=max_vision_workers,
                incremental=incremental,
                run_id=run_id,
                skip_vision=bool(source_state["vision"]["aborted"]),
                skip_funding_rate=bool(source_state["funding_rate"]["aborted"]),
            )

            report = day_report_by_date.get(snapshot.date)
            if report is not None:
                report["metrics"] = day_metrics

            day_status = str(day_metrics.get("status", "error"))
            day_status_counts[day_status] += 1
            stage_errors.extend(day_errors)

            for source_name in ("vision", "funding_rate"):
                source_diag = cast(dict[str, Any], day_metrics.get(source_name, {}))
                source_status = str(source_diag.get("status", "error"))
                if source_status == "complete":
                    source_state[source_name]["days_complete"] += 1
                elif source_status in {"error", "partial"}:
                    source_state[source_name]["days_error"] += 1
                elif source_status == "aborted":
                    source_state[source_name]["days_aborted"] += 1

                if source_aborts.get(source_name):
                    source_state[source_name]["aborted"] = True
                    if source_state[source_name]["aborted_from_date"] is None:
                        source_state[source_name]["aborted_from_date"] = snapshot.date

        if not snapshots:
            stage_status = "skipped"
        elif day_status_counts.get("error", 0) > 0:
            stage_status = "error"
        elif day_status_counts.get("partial", 0) > 0 or day_status_counts.get("aborted", 0) > 0 or stage_errors:
            stage_status = "partial"
        else:
            stage_status = "complete"

        sources_report = {
            "vision": {
                "status": ("aborted" if source_state["vision"]["aborted"] else ("partial" if source_state["vision"]["days_error"] > 0 else "complete")),
                "aborted": bool(source_state["vision"]["aborted"]),
                "aborted_from_date": source_state["vision"]["aborted_from_date"],
                "days_complete": int(source_state["vision"]["days_complete"]),
                "days_error": int(source_state["vision"]["days_error"]),
                "days_aborted": int(source_state["vision"]["days_aborted"]),
            },
            "funding_rate": {
                "status": (
                    "aborted" if source_state["funding_rate"]["aborted"] else ("partial" if source_state["funding_rate"]["days_error"] > 0 else "complete")
                ),
                "aborted": bool(source_state["funding_rate"]["aborted"]),
                "aborted_from_date": source_state["funding_rate"]["aborted_from_date"],
                "days_complete": int(source_state["funding_rate"]["days_complete"]),
                "days_error": int(source_state["funding_rate"]["days_error"]),
                "days_aborted": int(source_state["funding_rate"]["days_aborted"]),
            },
        }
        stage_report = {
            "stage": "metrics",
            "dataset": "market_metrics",
            "status": stage_status,
            "duration_ms": int((time.perf_counter() - stage_started_at) * 1000),
            "days_total": len(snapshots),
            "days_complete": day_status_counts.get("complete", 0),
            "days_partial": day_status_counts.get("partial", 0),
            "days_error": day_status_counts.get("error", 0),
            "days_aborted": day_status_counts.get("aborted", 0),
            "sources": sources_report,
        }

        logger.info(
            "download.stage_done",
            run=run_id,
            stage="metrics",
            dataset="market_metrics",
            status=stage_status,
            duration_ms=stage_report["duration_ms"],
            days_total=stage_report["days_total"],
            days_complete=stage_report["days_complete"],
            days_partial=stage_report["days_partial"],
            days_error=stage_report["days_error"],
            days_aborted=stage_report["days_aborted"],
        )

        return {
            "day_reports": day_reports,
            "stage_report": stage_report,
            "stage_errors": stage_errors,
        }

    @staticmethod
    def _normalize_stage_error(
        *,
        run_id: str,
        stage: str,
        dataset: str,
        error: str,
        terminal: bool,
        date: str | None = None,
        symbol: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        """Normalize stage error payload for additive report diagnostics."""
        payload: dict[str, Any] = {
            "run": run_id,
            "stage": stage,
            "dataset": dataset,
            "error": error,
            "terminal": terminal,
        }
        if date is not None:
            payload["date"] = date
        if symbol is not None:
            payload["symbol"] = symbol
        if source is not None:
            payload["source"] = source
        return payload

    def _compose_universe_report(
        self,
        *,
        run_id: str,
        db_file_path: Path,
        universe_def: UniverseDefinition,
        universe_file: str | None,
        snapshots_to_process: list[UniverseDailySnapshot],
        day_reports: list[dict[str, Any]],
        skipped_days: list[dict[str, Any]],
        stage_reports: dict[str, dict[str, Any]],
        stage_errors: list[dict[str, Any]],
        requested_start_date: str,
        requested_end_date: str,
        effective_start: str,
        effective_end: str,
        override_applied: bool,
        override_start_date: str | None,
        override_end_date: str | None,
        interval: Freq,
        incremental: bool,
        download_market_metrics: bool,
        max_api_workers: int,
        max_vision_workers: int,
        max_retries: int,
    ) -> dict[str, Any]:
        """Compose universe run report with additive stage diagnostics."""
        total_symbols = sum(int(report.get("total_symbols", 0)) for report in day_reports)
        total_success = sum(int(report.get("successful_symbols", 0)) for report in day_reports)
        total_failures = sum(len(cast(list[str], report.get("failed_symbols", []))) for report in day_reports)
        total_day_symbol_pairs = sum(len(snapshot.active_symbols) for snapshot in snapshots_to_process)

        failed_reason_summary: dict[str, int] = defaultdict(int)
        for report in day_reports:
            for missing in cast(list[dict[str, Any]], report.get("missing_periods", [])):
                reason = str(missing.get("reason", "unknown"))
                failed_reason_summary[reason] += 1

        return {
            "run_id": run_id,
            "db_path": str(db_file_path),
            "universe_file": universe_file,
            "date_range": {
                "start_date": requested_start_date,
                "end_date": requested_end_date,
                "requested_start_date": requested_start_date,
                "requested_end_date": requested_end_date,
                "effective_start_date": effective_start,
                "effective_end_date": effective_end,
            },
            "requested_symbols": universe_def.requested_symbols,
            "total_days": len(snapshots_to_process),
            "processed_days": len(day_reports),
            "skipped_days": skipped_days,
            "total_day_symbol_pairs": total_day_symbol_pairs,
            "total_symbols": total_symbols,
            "total_successful_symbols": total_success,
            "total_failed_symbols": total_failures,
            "failed_reason_summary": dict(failed_reason_summary),
            "day_reports": day_reports,
            "stage_reports": stage_reports,
            "stage_errors": stage_errors,
            "download_context": {
                "requested_start_date": requested_start_date,
                "requested_end_date": requested_end_date,
                "effective_start_date": effective_start,
                "effective_end_date": effective_end,
                "override_applied": override_applied,
                "override_start_date": override_start_date,
                "override_end_date": override_end_date,
                "interval": interval.value,
                "incremental": incremental,
                "download_market_metrics": download_market_metrics,
                "max_api_workers": max_api_workers,
                "max_vision_workers": max_vision_workers,
                "max_retries": max_retries,
            },
        }

    # ==================== Universe管理 ====================

    async def define_universe(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        output_path: Path | str,
        description: str | None = None,
        force: bool = False,
        daily_check_workers: int = 5,
        daily_check_request_delay: float = 0.0,
    ) -> UniverseDefinition:
        """Define immutable v2 universe from symbols and date range."""
        return await self.universe_manager.define_universe(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            output_path=output_path,
            description=description,
            force=force,
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
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        """Export data strictly from a v2 universe daily plan and write report.json."""
        universe_file_obj = self._validate_and_prepare_path(universe_file, is_file=True)
        if not universe_file_obj.exists():
            raise FileNotFoundError(f"Universe file not found: {universe_file_obj}")

        universe_def = UniverseDefinition.load_from_file(universe_file_obj)
        (
            effective_start_date,
            effective_end_date,
            filtered_snapshots,
            override_applied,
            override_start_date,
            override_end_date,
        ) = self._resolve_universe_date_window(universe_def=universe_def, start_date=start_date, end_date=end_date)
        output_path = self._build_export_output_path(
            export_base_path=export_base_path,
            export_freq=export_freq,
            universe_def=universe_def,
            start_date=effective_start_date,
            end_date=effective_end_date,
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
            universe_file=str(universe_file_obj),
            snapshots=filtered_snapshots,
            requested_start_date=universe_def.start_date,
            requested_end_date=universe_def.end_date,
            effective_start_date=effective_start_date,
            effective_end_date=effective_end_date,
            override_applied=override_applied,
            override_start_date=override_start_date,
            override_end_date=override_end_date,
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
        universe_file: str | None = None,
        snapshots: list[UniverseDailySnapshot] | None = None,
        requested_start_date: str | None = None,
        requested_end_date: str | None = None,
        effective_start_date: str | None = None,
        effective_end_date: str | None = None,
        override_applied: bool = False,
        override_start_date: str | None = None,
        override_end_date: str | None = None,
    ) -> dict[str, Any]:
        """Export from universe daily snapshots and build missing coverage report."""
        db_file_path = self._validate_and_prepare_path(db_path, is_file=True)
        output_dir = self._validate_and_prepare_path(output_path, is_file=False)
        snapshots_to_process = snapshots if snapshots is not None else universe_def.daily_snapshots
        requested_start = requested_start_date or universe_def.start_date
        requested_end = requested_end_date or universe_def.end_date
        effective_start = effective_start_date or requested_start
        effective_end = effective_end_date or requested_end

        define_missing_map: dict[str, dict[str, str]] = {}
        export_missing_map: dict[str, list[str]] = {}
        metrics_missing_coverage_map: dict[str, dict[str, Any]] = {}

        exported_days: list[dict[str, Any]] = []
        skipped_days: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []

        db = Database(db_file_path)
        await db.initialize()

        try:
            for index, snapshot in enumerate(snapshots_to_process, start=1):
                if snapshot.missing_symbols:
                    define_missing_map[snapshot.date] = dict(snapshot.missing_symbols)

                if not snapshot.active_symbols:
                    skipped_days.append(
                        {
                            "index": index,
                            "date": snapshot.date,
                            "reason": "no_active_symbols",
                        }
                    )
                    continue

                try:
                    export_summary = await db.numpy_exporter.export_combined_data(
                        symbols=snapshot.active_symbols,
                        start_time=snapshot.date,
                        end_time=snapshot.date,
                        source_freq=source_freq,
                        export_freq=export_freq,
                        output_path=output_dir,
                        include_klines=include_klines,
                        include_metrics=include_metrics,
                        metrics_config=metrics_config,
                        field_mapping=field_mapping,
                    )
                    if isinstance(export_summary, dict):
                        day_coverage = export_summary.get("metrics_missing_coverage")
                        if isinstance(day_coverage, dict) and day_coverage:
                            metrics_missing_coverage_map[snapshot.date] = day_coverage

                    symbol_payload = self._load_export_symbol_dict(output_dir / "univ_dct2.json")
                    export_missing_symbols = self._calculate_export_missing_for_day(
                        symbol_payload=symbol_payload,
                        date=snapshot.date,
                        expected_symbols=snapshot.active_symbols,
                    )
                    if export_missing_symbols:
                        export_missing_map[snapshot.date] = export_missing_symbols

                    exported_days.append(
                        {
                            "index": index,
                            "date": snapshot.date,
                            "symbols": len(snapshot.active_symbols),
                            "missing_in_export": len(export_missing_symbols),
                        }
                    )
                except Exception as exc:
                    errors.append(
                        {
                            "index": str(index),
                            "date": snapshot.date,
                            "error": str(exc),
                        }
                    )
                    logger.error("export_day_failed", date=snapshot.date, error=str(exc))
        finally:
            await db.close()

        merged_missing_map = self._merge_missing_reason_maps(define_missing_map, export_missing_map)

        report: dict[str, Any] = {
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "requested_symbols": universe_def.requested_symbols,
            "total_days": len(snapshots_to_process),
            "date_range": {
                "start_date": requested_start,
                "end_date": requested_end,
                "requested_start_date": requested_start,
                "requested_end_date": requested_end,
                "effective_start_date": effective_start,
                "effective_end_date": effective_end,
            },
            "exported_days": exported_days,
            "skipped_days": skipped_days,
            "errors": errors,
            "define_missing": self._serialize_reason_map(define_missing_map),
            "export_missing": {date: symbols for date, symbols in sorted(export_missing_map.items()) if symbols},
            "merged_missing": self._serialize_reason_map(merged_missing_map),
            "metrics_missing_coverage": {date: coverage for date, coverage in sorted(metrics_missing_coverage_map.items()) if coverage},
            "stats": {
                "exported_day_count": len(exported_days),
                "skipped_day_count": len(skipped_days),
                "error_count": len(errors),
                "define_missing_date_count": len([date for date, symbols in define_missing_map.items() if symbols]),
                "export_missing_date_count": len([date for date, symbols in export_missing_map.items() if symbols]),
                "merged_missing_date_count": len(merged_missing_map),
                "metrics_missing_coverage_date_count": len(metrics_missing_coverage_map),
            },
            "export_context": {
                "requested_start_date": requested_start,
                "requested_end_date": requested_end,
                "effective_start_date": effective_start,
                "effective_end_date": effective_end,
                "override_applied": override_applied,
                "override_start_date": override_start_date,
                "override_end_date": override_end_date,
                "source_freq": source_freq.value,
                "export_freq": export_freq.value,
                "include_klines": include_klines,
                "include_metrics": include_metrics,
            },
        }

        report_path = output_dir / "report.json"
        with open(report_path, "w", encoding="utf-8") as fp:
            json.dump(report, fp, ensure_ascii=False, indent=2)

        report["report_path"] = str(report_path)
        return report

    @staticmethod
    def _load_export_symbol_dict(symbol_dict_path: Path) -> dict[str, list[str]]:
        """Read exported symbol dictionary, returning empty payload on failure."""
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
    def _calculate_export_missing_for_day(
        symbol_payload: dict[str, list[str]],
        date: str,
        expected_symbols: list[str],
    ) -> list[str]:
        """Calculate missing symbols for one exported day."""
        expected_set = set(expected_symbols)
        if not expected_set:
            return []

        date_key = date.replace("-", "")
        actual_symbols = set(symbol_payload.get(date_key, []))
        return sorted(expected_set - actual_symbols)

    @staticmethod
    def _merge_missing_reason_maps(
        define_missing_map: dict[str, dict[str, str]],
        export_missing_map: dict[str, list[str]],
    ) -> dict[str, dict[str, str]]:
        """Merge define-time and export-time missing maps with reasons."""
        merged: dict[str, dict[str, str]] = {}

        for date, symbol_reason_map in define_missing_map.items():
            merged[date] = dict(symbol_reason_map)

        for date, symbols in export_missing_map.items():
            target = merged.setdefault(date, {})
            for symbol in symbols:
                target.setdefault(symbol, MISSING_REASON_MISSING_IN_EXPORT)

        return {date: reasons for date, reasons in sorted(merged.items()) if reasons}

    @staticmethod
    def _serialize_reason_map(source: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
        """Serialize date->symbol->reason map with deterministic order."""
        serialized: dict[str, dict[str, str]] = {}
        for date, symbol_reason_map in sorted(source.items()):
            if not symbol_reason_map:
                continue
            serialized[date] = {symbol: symbol_reason_map[symbol] for symbol in sorted(symbol_reason_map)}
        return serialized

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

    async def check_symbol_full_day_available_on_date(self, symbol: str, date: str, strict: bool = False) -> bool:
        """Check whether a symbol has complete day coverage on a specific date."""
        try:
            start_time = self._date_to_timestamp_start(date)
            end_time = self._date_to_timestamp_end(date)
            day_start_ts = int(start_time)

            klines = await asyncio.wait_for(
                self.client.futures_klines(
                    symbol=symbol,
                    interval="1m",
                    startTime=start_time,
                    endTime=end_time,
                    limit=1,
                ),
                timeout=SYMBOL_CHECK_TIMEOUT_SECONDS,
            )

            if not klines:
                return False

            first_open_time = int(klines[0][0])
            return first_open_time == day_start_ts

        except TimeoutError as exc:
            if strict:
                raise MarketDataFetchError(
                    f"Timed out checking full-day coverage for symbol {symbol} on {date} after {SYMBOL_CHECK_TIMEOUT_SECONDS:.0f}s"
                ) from exc
            logger.debug(
                "check_symbol_full_day_available_on_date_timeout symbol=%s date=%s timeout=%.0fs",
                symbol,
                date,
                SYMBOL_CHECK_TIMEOUT_SECONDS,
            )
            return False
        except Exception as exc:
            if strict:
                raise MarketDataFetchError(f"Failed to check full-day coverage for symbol {symbol} on {date}: {exc}") from exc
            logger.debug(f"check_symbol_full_day_available_on_date_failed symbol={symbol} date={date} error={exc}")
            return False

    async def check_symbol_exists_on_date(self, symbol: str, date: str, strict: bool = False) -> bool:
        """Check whether a symbol has data on a specific date."""
        try:
            start_time = self._date_to_timestamp_start(date)
            end_time = self._date_to_timestamp_end(date)

            klines = await asyncio.wait_for(
                self.client.futures_klines(
                    symbol=symbol,
                    interval="1d",
                    startTime=start_time,
                    endTime=end_time,
                    limit=1,
                ),
                timeout=SYMBOL_CHECK_TIMEOUT_SECONDS,
            )

            return bool(klines and len(klines) > 0)

        except TimeoutError as exc:
            if strict:
                raise MarketDataFetchError(f"Timed out checking symbol {symbol} on {date} after {SYMBOL_CHECK_TIMEOUT_SECONDS:.0f}s") from exc
            logger.debug(
                "check_symbol_exists_on_date_timeout symbol=%s date=%s timeout=%.0fs",
                symbol,
                date,
                SYMBOL_CHECK_TIMEOUT_SECONDS,
            )
            return False
        except Exception as exc:
            if strict:
                raise MarketDataFetchError(f"Failed to check symbol {symbol} on {date}: {exc}") from exc
            logger.debug(f"check_symbol_exists_on_date_failed symbol={symbol} date={date} error={exc}")
            return False

    # ==================== 私有辅助方法 ====================

    async def _download_market_metrics_for_date(
        self,
        date: str,
        symbols: list[str],
        db_path: Path,
        api_request_delay: float,
        vision_request_delay: float,
        max_api_workers: int,
        max_vision_workers: int,
        incremental: bool = True,
        run_id: str | None = None,
        skip_vision: bool = False,
        skip_funding_rate: bool = False,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, bool]]:
        """Download metrics for one date and symbol list."""
        if not symbols:
            metrics_payload = {
                "status": "skipped",
                "vision": {
                    "status": "skipped",
                    "dataset": "vision-metrics",
                    "duration_ms": 0,
                    "terminal": False,
                },
                "funding_rate": {
                    "status": "skipped",
                    "dataset": "funding_rate",
                    "duration_ms": 0,
                    "terminal": False,
                },
            }
            return metrics_payload, [], {"vision": False, "funding_rate": False}

        day_started_at = time.perf_counter()
        errors: list[dict[str, Any]] = []
        source_aborts = {"vision": False, "funding_rate": False}
        source_payloads: dict[str, dict[str, Any]] = {
            "vision": {
                "status": "skipped" if skip_vision else "pending",
                "dataset": "vision-metrics",
                "duration_ms": 0,
                "terminal": bool(skip_vision),
            },
            "funding_rate": {
                "status": "skipped" if skip_funding_rate else "pending",
                "dataset": "funding_rate",
                "duration_ms": 0,
                "terminal": bool(skip_funding_rate),
            },
        }

        logger.info(
            "download.day_start",
            run=run_id,
            stage="metrics",
            dataset="market_metrics",
            status="start",
            duration_ms=0,
            date=date,
            total_symbols=len(symbols),
        )
        logger.debug(
            "metrics.source_strategy",
            run=run_id,
            stage="metrics",
            dataset="market_metrics",
            status="ready",
            duration_ms=0,
            date=date,
            source_open_interest="vision",
            source_long_short_ratio="vision",
            source_funding_rate="api",
        )

        async def run_source(
            *,
            source: str,
            dataset: str,
            runner: Any,
        ) -> None:
            source_started_at = time.perf_counter()
            logger.info(
                "download.source_start",
                run=run_id,
                stage="metrics",
                dataset=dataset,
                status="start",
                duration_ms=0,
                date=date,
                total_symbols=len(symbols),
            )
            try:
                await runner
                source_payloads[source] = {
                    "status": "complete",
                    "dataset": dataset,
                    "duration_ms": int((time.perf_counter() - source_started_at) * 1000),
                    "terminal": False,
                }
                logger.info(
                    "download.source_done",
                    run=run_id,
                    stage="metrics",
                    dataset=dataset,
                    status="complete",
                    duration_ms=source_payloads[source]["duration_ms"],
                    date=date,
                    total_symbols=len(symbols),
                )
            except RateLimitError as exc:
                source_aborts[source] = True
                source_payloads[source] = {
                    "status": "aborted",
                    "dataset": dataset,
                    "duration_ms": int((time.perf_counter() - source_started_at) * 1000),
                    "terminal": True,
                    "error": str(exc),
                }
                errors.append(
                    self._normalize_stage_error(
                        run_id=str(run_id),
                        stage="metrics",
                        dataset=dataset,
                        date=date,
                        error=str(exc),
                        terminal=True,
                        source=source,
                    )
                )
                logger.error(
                    "download.source_done",
                    run=run_id,
                    stage="metrics",
                    dataset=dataset,
                    status="aborted",
                    duration_ms=source_payloads[source]["duration_ms"],
                    date=date,
                    total_symbols=len(symbols),
                    terminal=True,
                    error=str(exc),
                )
            except Exception as exc:  # noqa: BLE001
                source_payloads[source] = {
                    "status": "error",
                    "dataset": dataset,
                    "duration_ms": int((time.perf_counter() - source_started_at) * 1000),
                    "terminal": False,
                    "error": str(exc),
                }
                errors.append(
                    self._normalize_stage_error(
                        run_id=str(run_id),
                        stage="metrics",
                        dataset=dataset,
                        date=date,
                        error=str(exc),
                        terminal=False,
                        source=source,
                    )
                )
                logger.error(
                    "download.source_done",
                    run=run_id,
                    stage="metrics",
                    dataset=dataset,
                    status="error",
                    duration_ms=source_payloads[source]["duration_ms"],
                    date=date,
                    total_symbols=len(symbols),
                    terminal=False,
                    error=str(exc),
                )

        if skip_vision:
            source_payloads["vision"] = {
                "status": "aborted",
                "dataset": "vision-metrics",
                "duration_ms": 0,
                "terminal": True,
                "error": "source_aborted_after_terminal_rate_limit",
            }
            logger.warning(
                "download.source_done",
                run=run_id,
                stage="metrics",
                dataset="vision-metrics",
                status="aborted",
                duration_ms=0,
                date=date,
                total_symbols=len(symbols),
                terminal=True,
                error="source_aborted_after_terminal_rate_limit",
            )
        else:
            await run_source(
                source="vision",
                dataset="vision-metrics",
                runner=self.vision_downloader.download_metrics_batch(
                    symbols=symbols,
                    start_date=date,
                    end_date=date,
                    db_path=str(db_path),
                    request_delay=vision_request_delay,
                    max_workers=max_vision_workers,
                    incremental=incremental,
                    run_id=run_id,
                ),
            )

        if skip_funding_rate:
            source_payloads["funding_rate"] = {
                "status": "aborted",
                "dataset": "funding_rate",
                "duration_ms": 0,
                "terminal": True,
                "error": "source_aborted_after_terminal_rate_limit",
            }
            logger.warning(
                "download.source_done",
                run=run_id,
                stage="metrics",
                dataset="funding_rate",
                status="aborted",
                duration_ms=0,
                date=date,
                total_symbols=len(symbols),
                terminal=True,
                error="source_aborted_after_terminal_rate_limit",
            )
        else:
            await run_source(
                source="funding_rate",
                dataset="funding_rate",
                runner=self.metrics_downloader.download_funding_rate_batch(
                    symbols=symbols,
                    start_time=date,
                    end_time=date,
                    db_path=str(db_path),
                    request_delay=api_request_delay,
                    max_workers=max_api_workers,
                    incremental=incremental,
                    run_id=run_id,
                ),
            )

        source_statuses = {str(payload.get("status")) for payload in source_payloads.values()}
        if source_statuses.issubset({"complete", "skipped"}):
            day_status = "complete"
        elif "error" in source_statuses and "complete" not in source_statuses:
            day_status = "error"
        elif source_statuses.issubset({"aborted", "skipped"}):
            day_status = "aborted"
        else:
            day_status = "partial"

        metrics_payload = {
            "status": day_status,
            "vision": source_payloads["vision"],
            "funding_rate": source_payloads["funding_rate"],
        }
        logger.info(
            "download.day_done",
            run=run_id,
            stage="metrics",
            dataset="market_metrics",
            status=day_status,
            duration_ms=int((time.perf_counter() - day_started_at) * 1000),
            date=date,
            total_symbols=len(symbols),
        )

        return metrics_payload, errors, source_aborts

    @staticmethod
    def _normalize_symbols(symbols: list[str]) -> list[str]:
        """Normalize symbol list (uppercase + strip + stable dedupe)."""
        normalized: list[str] = []
        seen: set[str] = set()

        for symbol in symbols:
            normalized_symbol = symbol.strip().upper()
            if not normalized_symbol or normalized_symbol in seen:
                continue
            seen.add(normalized_symbol)
            normalized.append(normalized_symbol)

        return normalized

    def _build_export_output_path(
        self,
        export_base_path: Path | str,
        export_freq: Freq,
        universe_def: UniverseDefinition,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Path:
        """Build export directory path for v2 universe workflow."""
        base_path = self._validate_and_prepare_path(export_base_path, is_file=False)
        freq_dir = self._map_export_freq_dir(export_freq)
        start = start_date or universe_def.start_date
        end = end_date or universe_def.end_date
        name = f"univ_{start}_{end}_{len(universe_def.requested_symbols)}"
        return base_path / freq_dir / name

    @staticmethod
    def _map_export_freq_dir(export_freq: Freq) -> str:
        """Map export frequency to a directory suffix."""
        mapping = {
            Freq.d1: "D1B",
            Freq.h1: "H1B",
            Freq.m1: "M1B",
            Freq.m5: "M5B",
        }
        return mapping.get(export_freq, f"{export_freq.value.upper()}B")

    def _validate_and_prepare_path(self, path: Path | str, is_file: bool = False, file_name: str | None = None) -> Path:
        """Validate and prepare filesystem path."""
        if not path:
            raise ValueError("Path cannot be empty")

        path_obj = Path(path)

        if is_file:
            if path_obj.is_dir():
                path_obj = path_obj.joinpath(file_name) if file_name else path_obj
            else:
                path_obj.parent.mkdir(parents=True, exist_ok=True)
        else:
            path_obj.mkdir(parents=True, exist_ok=True)

        return path_obj

    @staticmethod
    def _parse_strict_date(value: str, field_name: str) -> str:
        """Parse CLI-style date as strict YYYY-MM-DD."""
        candidate = value.strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
            raise ValueError(f"Invalid {field_name} format: {value!r}. Expected YYYY-MM-DD.")

        try:
            return datetime.strptime(candidate, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"Invalid {field_name} format: {value!r}. Expected YYYY-MM-DD.") from exc

    def _resolve_universe_date_window(
        self,
        universe_def: UniverseDefinition,
        start_date: str | None,
        end_date: str | None,
    ) -> tuple[str, str, list[UniverseDailySnapshot], bool, str | None, str | None]:
        """Resolve effective date range and daily snapshots for universe runs."""
        requested_start = universe_def.start_date
        requested_end = universe_def.end_date
        override_start = self._parse_strict_date(start_date, "start_date") if start_date is not None else None
        override_end = self._parse_strict_date(end_date, "end_date") if end_date is not None else None
        effective_start = override_start or requested_start
        effective_end = override_end or requested_end

        if effective_start > effective_end:
            raise ValueError(f"Invalid override range: start_date {effective_start} must be <= end_date {effective_end}")

        if effective_start < requested_start or effective_end > requested_end:
            raise ValueError(f"Override date range {effective_start} ~ {effective_end} must be within universe range {requested_start} ~ {requested_end}.")

        snapshots = [snapshot for snapshot in universe_def.daily_snapshots if effective_start <= snapshot.date <= effective_end]
        override_applied = start_date is not None or end_date is not None
        return effective_start, effective_end, snapshots, override_applied, override_start, override_end

    def _date_to_timestamp_start(self, date: str) -> str:
        """Convert date string to UTC start timestamp (ms)."""
        from cryptoservice.utils import date_to_timestamp_start

        return str(date_to_timestamp_start(date))

    def _date_to_timestamp_end(self, date: str) -> str:
        """Convert date string to next-day UTC start timestamp (ms)."""
        from cryptoservice.utils import date_to_timestamp_end

        return str(date_to_timestamp_end(date))

    def _convert_time_to_string(self, time_value: str | datetime | None) -> str:
        """Convert supported time value to YYYY-MM-DD string."""
        if time_value is None:
            return ""
        if isinstance(time_value, str):
            return time_value
        if isinstance(time_value, datetime):
            return time_value.strftime("%Y-%m-%d")
        raise TypeError(f"Unsupported time type: {type(time_value)}")
