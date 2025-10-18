"""市场指标数据下载器.

专门处理资金费率、持仓量(当日)、多空比例（当日）等市场指标数据的下载。
"""

import asyncio
import time
from typing import Any

from binance import AsyncClient

from cryptoservice.config.logging import get_logger
from cryptoservice.exceptions import MarketDataFetchError
from cryptoservice.models import Freq, FundingRate, LongShortRatio, OpenInterest
from cryptoservice.storage.database import Database as AsyncMarketDB
from cryptoservice.utils.logger import generate_run_id
from cryptoservice.utils.time_utils import date_to_timestamp_end, date_to_timestamp_start, timestamp_to_datetime

from .base_downloader import BaseDownloader

logger = get_logger(__name__)


class MetricsDownloader(BaseDownloader):
    """市场指标数据下载器."""

    def __init__(self, client: AsyncClient, request_delay: float = 0.5):
        """初始化市场指标数据下载器.

        Args:
            client: API 客户端实例.
            request_delay: 请求之间的基础延迟（秒）.
        """
        super().__init__(client, request_delay)
        self.db: AsyncMarketDB | None = None
        self._run_id: str | None = None

    async def download_funding_rate_batch(  # noqa: C901
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        db_path: str,
        request_delay: float = 0.5,
        max_workers: int = 5,
        incremental: bool = True,
        run_id: str | None = None,
    ) -> None:
        """批量异步下载资金费率数据."""
        run = run_id or generate_run_id("funding")
        self._run_id = run
        started_at = time.perf_counter()

        try:
            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            logger.info(
                "download.start",
                run=run,
                dataset="funding_rate",
                symbols=len(symbols),
                start=start_time,
                end=end_time,
                max_workers=max_workers,
                incremental=incremental,
            )

            symbol_plans: dict[str, dict[str, Any]] = {}

            if incremental:
                logger.info(
                    "download.incremental_start",
                    run=run,
                    dataset="funding_rate",
                    symbols=len(symbols),
                    start=start_time,
                    end=end_time,
                )

                missing_plan_8h = await self.db.plan_metrics_download(
                    symbols=symbols,
                    start_date=start_time,
                    end_date=end_time,
                    data_type="funding_rate",
                    interval_hours=8,
                )

                missing_plan_4h = await self.db.plan_metrics_download(
                    symbols=symbols,
                    start_date=start_time,
                    end_date=end_time,
                    data_type="funding_rate",
                    interval_hours=4,
                )

                complete_8h = set(symbols) - set(missing_plan_8h.keys())
                complete_4h = set(symbols) - set(missing_plan_4h.keys())
                complete_symbols = complete_8h | complete_4h
                symbols_to_download = list(set(symbols) - complete_symbols)

                logger.info(
                    "download.incremental_summary",
                    run=run,
                    dataset="funding_rate",
                    complete_8h=len(complete_8h),
                    complete_4h=len(complete_4h),
                    needing=len(symbols_to_download),
                    total=len(symbols),
                )

                if not symbols_to_download:
                    logger.info(
                        "download.summary",
                        run=run,
                        dataset="funding_rate",
                        status="skipped",
                        reason="plan_empty",
                    )
                    return

                symbol_plans = {}
                for symbol in symbols_to_download:
                    plan_8h = missing_plan_8h.get(symbol)
                    plan_4h = missing_plan_4h.get(symbol)
                    selected_plan = plan_8h or plan_4h
                    if selected_plan is not None:
                        symbol_plans[symbol] = selected_plan
                symbols = symbols_to_download

            total_records = 0
            semaphore = asyncio.Semaphore(max_workers)
            lock = asyncio.Lock()

            default_range = [
                (
                    date_to_timestamp_start(start_time) if start_time else None,
                    date_to_timestamp_end(end_time) if end_time else None,
                )
            ]

            async def process_symbol(symbol: str) -> None:
                nonlocal total_records
                async with semaphore:
                    try:
                        logger.debug(
                            "download.symbol_start",
                            run=run,
                            dataset="funding_rate",
                            symbol=symbol,
                        )

                        plan = symbol_plans.get(symbol)
                        if plan and plan.get("start_ts") is not None and plan.get("end_ts") is not None:
                            ranges = [(int(plan["start_ts"]), int(plan["end_ts"]))]
                        else:
                            ranges = [
                                (start_ts, end_ts)
                                for start_ts, end_ts in default_range
                                if start_ts is not None and end_ts is not None
                            ]

                        total_inserted_symbol = 0

                        for range_start, range_end in ranges:
                            logger.debug(
                                "download.range_start",
                                run=run,
                                dataset="funding_rate",
                                symbol=symbol,
                                range=self._format_range(range_start, range_end),
                            )

                            funding_rates = await self.download_funding_rate(
                                symbol=symbol,
                                start_ts=range_start,
                                end_ts=range_end,
                                limit=1000,
                            )

                            if not funding_rates or not self.db:
                                logger.debug(
                                    "download.range_empty",
                                    run=run,
                                    dataset="funding_rate",
                                    symbol=symbol,
                                    range=self._format_range(range_start, range_end),
                                )
                                continue

                            inserted = await self.db.insert_funding_rates(funding_rates)
                            total_inserted_symbol += inserted
                            async with lock:
                                total_records += inserted

                            logger.info(
                                "download.range_done",
                                run=run,
                                dataset="funding_rate",
                                symbol=symbol,
                                range=self._format_range(range_start, range_end),
                                rows=inserted,
                            )

                            if request_delay > 0:
                                await asyncio.sleep(request_delay)

                        if total_inserted_symbol == 0:
                            logger.debug(
                                "download.symbol_empty",
                                run=run,
                                dataset="funding_rate",
                                symbol=symbol,
                            )
                        else:
                            logger.debug(
                                "download.symbol_done",
                                run=run,
                                dataset="funding_rate",
                                symbol=symbol,
                                rows=total_inserted_symbol,
                            )

                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "download.symbol_error",
                            run=run,
                            dataset="funding_rate",
                            symbol=symbol,
                            error=str(exc),
                        )
                        plan = symbol_plans.get(symbol)
                        metadata: dict[str, Any] = {
                            "data_type": "funding_rate",
                            "start_time": start_time,
                            "end_time": end_time,
                        }
                        if plan:
                            start_ts = plan.get("start_ts")
                            end_ts = plan.get("end_ts")
                            if start_ts is not None:
                                metadata["start_ts"] = start_ts
                            if end_ts is not None:
                                metadata["end_ts"] = end_ts
                        self._record_failed_download(symbol, str(exc), metadata)

            await asyncio.gather(*(process_symbol(symbol) for symbol in symbols))

            success_count = len(symbols) - len(self.failed_downloads)
            failed_count = len(self.failed_downloads)
            success_rate = (success_count / len(symbols) * 100) if symbols else 0
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)

            logger.info(
                "download.summary",
                run=run,
                dataset="funding_rate",
                symbols=len(symbols),
                succeeded=success_count,
                failed=failed_count,
                success_rate=f"{success_rate:.1f}",
                total_records=total_records,
                elapsed_ms=elapsed_ms,
            )

        except Exception as exc:  # noqa: BLE001
            logger.error(
                "download.error",
                run=run,
                dataset="funding_rate",
                error=str(exc),
            )
            raise MarketDataFetchError(f"批量下载资金费率失败: {exc}") from exc

    async def download_open_interest_batch(  # noqa: C901
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        db_path: str,
        interval: Freq = Freq.m5,
        request_delay: float = 0.5,
        max_workers: int = 5,
        incremental: bool = True,
    ) -> None:
        """批量异步下载持仓量数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            db_path: 数据库路径
            interval: 时间间隔
            request_delay: 请求延迟
            max_workers: 最大并发数
            incremental: 是否启用增量下载（默认True）
        """
        try:
            logger.info("download_batch_start", dataset="open_interest")

            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            # 如果启用增量下载模式，生成下载计划
            symbol_plans: dict[str, dict[str, Any]] = {}

            if incremental:
                logger.info("incremental_mode_enabled", dataset="open_interest", action="analyzing_data")
                # 根据interval计算时间间隔（小时）
                interval_hours_map = {
                    Freq.m5: 5 / 60,
                    Freq.m15: 15 / 60,
                    Freq.m30: 30 / 60,
                    Freq.h1: 1,
                    Freq.h2: 2,
                    Freq.h4: 4,
                    Freq.h6: 6,
                    Freq.h8: 8,
                    Freq.h12: 12,
                    Freq.d1: 24,
                }
                interval_hours = interval_hours_map.get(interval, 1)

                missing_plan = await self.db.plan_metrics_download(
                    symbols=symbols,
                    start_date=start_time,
                    end_date=end_time,
                    data_type="open_interest",
                    interval_hours=interval_hours,
                )

                # 过滤出需要下载的交易对
                symbols_to_download = list(missing_plan.keys())
                if not symbols_to_download:
                    logger.info("download_complete", dataset="open_interest", action="skipping")
                    return
                else:
                    logger.info(
                        "incremental_summary",
                        dataset="open_interest",
                        needed=len(symbols_to_download),
                        total=len(symbols),
                    )
                    symbol_plans = missing_plan
                    # 使用需要下载的交易对列表替换原始列表
                    symbols = symbols_to_download

            total_records = 0
            semaphore = asyncio.Semaphore(max_workers)
            lock = asyncio.Lock()

            default_range = [
                (
                    date_to_timestamp_start(start_time) if start_time else None,
                    date_to_timestamp_end(end_time) if end_time else None,
                )
            ]

            async def process_symbol(symbol: str):
                nonlocal total_records
                async with semaphore:
                    try:
                        logger.debug("download_symbol", dataset="open_interest", symbol=symbol)
                        plan = symbol_plans.get(symbol)
                        if plan and plan.get("start_ts") is not None and plan.get("end_ts") is not None:
                            ranges = [(int(plan["start_ts"]), int(plan["end_ts"]))]
                        else:
                            ranges = [
                                (start_ts, end_ts)
                                for start_ts, end_ts in default_range
                                if start_ts is not None and end_ts is not None
                            ]

                        inserted_symbol = 0

                        for range_start, range_end in ranges:
                            open_interests = await self.download_open_interest(
                                symbol=symbol,
                                period=interval.value,
                                start_ts=range_start,
                                end_ts=range_end,
                                limit=1000,
                            )

                            if not open_interests or not self.db:
                                logger.debug(
                                    "range_empty",
                                    dataset="open_interest",
                                    symbol=symbol,
                                    range=self._format_range(range_start, range_end),
                                )
                                continue

                            inserted = await self.db.insert_open_interests(open_interests)
                            inserted_symbol += inserted
                            async with lock:
                                total_records += inserted
                            logger.info(
                                "range_stored",
                                dataset="open_interest",
                                symbol=symbol,
                                records=inserted,
                                range=self._format_range(range_start, range_end),
                            )

                            if request_delay > 0:
                                await asyncio.sleep(request_delay)

                        if inserted_symbol == 0:
                            logger.debug("symbol_empty", dataset="open_interest", symbol=symbol)
                    except Exception as e:
                        logger.warning("download_symbol_error", dataset="open_interest", symbol=symbol, error=str(e))
                        plan = symbol_plans.get(symbol)
                        metadata: dict[str, Any] = {
                            "data_type": "open_interest",
                            "start_time": start_time,
                            "end_time": end_time,
                        }
                        if plan:
                            start_ts = plan.get("start_ts")
                            end_ts = plan.get("end_ts")
                            if start_ts is not None:
                                metadata["start_ts"] = start_ts
                            if end_ts is not None:
                                metadata["end_ts"] = end_ts
                        self._record_failed_download(symbol, str(e), metadata)

            tasks = [process_symbol(symbol) for symbol in symbols]
            await asyncio.gather(*tasks)

            # 完整性检查
            success_count = len(symbols) - len(self.failed_downloads)
            failed_count = len(self.failed_downloads)
            success_rate = (success_count / len(symbols) * 100) if len(symbols) > 0 else 0

            logger.info(
                "download_summary",
                dataset="open_interest",
                total_symbols=len(symbols),
                succeeded=success_count,
                failed=failed_count,
                success_rate=f"{success_rate:.1f}",
                total_records=total_records,
            )

            if failed_count > 0:
                logger.warning("download_failures", dataset="open_interest", hint="use get_failed_downloads()")

        except Exception as e:
            logger.error("download_batch_error", dataset="open_interest", error=str(e))
            raise MarketDataFetchError(f"批量下载持仓量失败: {e}") from e

    async def download_long_short_ratio_batch(  # noqa: C901
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        db_path: str,
        period: str = "5m",
        ratio_type: str = "account",
        request_delay: float = 0.5,
        max_workers: int = 5,
        incremental: bool = True,
    ) -> None:
        """批量异步下载多空比例数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            db_path: 数据库路径
            period: 时间周期
            ratio_type: 比例类型
            request_delay: 请求延迟
            max_workers: 最大并发数
            incremental: 是否启用增量下载（默认True）
        """
        try:
            logger.info("download_batch_start", dataset="long_short_ratio", ratio_type=ratio_type)

            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            # 如果启用增量下载模式，生成下载计划
            symbol_plans: dict[str, dict[str, Any]] = {}

            if incremental:
                logger.info("incremental_mode_enabled", dataset="long_short_ratio", action="analyzing_data")
                # 解析period转换为小时数
                period_hours_map = {
                    "5m": 5 / 60,
                    "15m": 15 / 60,
                    "30m": 30 / 60,
                    "1h": 1,
                    "2h": 2,
                    "4h": 4,
                    "6h": 6,
                    "12h": 12,
                    "1d": 24,
                }
                interval_hours = period_hours_map.get(period, 1)

                missing_plan = await self.db.plan_metrics_download(
                    symbols=symbols,
                    start_date=start_time,
                    end_date=end_time,
                    data_type="long_short_ratio",
                    interval_hours=interval_hours,
                )

                # 过滤出需要下载的交易对
                symbols_to_download = list(missing_plan.keys())
                if not symbols_to_download:
                    logger.info(
                        "download_complete", dataset="long_short_ratio", ratio_type=ratio_type, action="skipping"
                    )
                    return
                else:
                    logger.info(
                        "incremental_summary",
                        dataset="long_short_ratio",
                        ratio_type=ratio_type,
                        needed=len(symbols_to_download),
                        total=len(symbols),
                    )
                    symbol_plans = missing_plan
                    # 使用需要下载的交易对列表替换原始列表
                    symbols = symbols_to_download

            total_records = 0
            semaphore = asyncio.Semaphore(max_workers)
            lock = asyncio.Lock()

            default_range = [
                (
                    date_to_timestamp_start(start_time) if start_time else None,
                    date_to_timestamp_end(end_time) if end_time else None,
                )
            ]

            async def process_symbol(symbol: str):
                nonlocal total_records
                async with semaphore:
                    try:
                        logger.debug("download_symbol", dataset="long_short_ratio", symbol=symbol)
                        plan = symbol_plans.get(symbol)
                        if plan and plan.get("start_ts") is not None and plan.get("end_ts") is not None:
                            ranges = [(int(plan["start_ts"]), int(plan["end_ts"]))]
                        else:
                            ranges = [
                                (start_ts, end_ts)
                                for start_ts, end_ts in default_range
                                if start_ts is not None and end_ts is not None
                            ]

                        inserted_symbol = 0

                        for range_start, range_end in ranges:
                            long_short_ratios = await self.download_long_short_ratio(
                                symbol=symbol,
                                period=period,
                                ratio_type=ratio_type,
                                start_ts=range_start,
                                end_ts=range_end,
                                limit=500,
                            )

                            if not long_short_ratios or not self.db:
                                logger.debug(
                                    "range_empty",
                                    dataset="long_short_ratio",
                                    symbol=symbol,
                                    range=self._format_range(range_start, range_end),
                                )
                                continue

                            inserted = await self.db.insert_long_short_ratios(long_short_ratios)
                            inserted_symbol += inserted
                            async with lock:
                                total_records += inserted
                            logger.info(
                                "range_stored",
                                dataset="long_short_ratio",
                                symbol=symbol,
                                records=inserted,
                                range=self._format_range(range_start, range_end),
                            )

                            if request_delay > 0:
                                await asyncio.sleep(request_delay)

                        if inserted_symbol == 0:
                            logger.debug("symbol_empty", dataset="long_short_ratio", symbol=symbol)
                    except Exception as e:
                        logger.warning("download_symbol_error", dataset="long_short_ratio", symbol=symbol, error=str(e))
                        plan = symbol_plans.get(symbol)
                        metadata: dict[str, Any] = {
                            "data_type": "long_short_ratio",
                            "ratio_type": ratio_type,
                            "start_time": start_time,
                            "end_time": end_time,
                        }
                        if plan:
                            start_ts = plan.get("start_ts")
                            end_ts = plan.get("end_ts")
                            if start_ts is not None:
                                metadata["start_ts"] = start_ts
                            if end_ts is not None:
                                metadata["end_ts"] = end_ts
                        self._record_failed_download(symbol, str(e), metadata)

            tasks = [process_symbol(symbol) for symbol in symbols]
            await asyncio.gather(*tasks)

            # 完整性检查
            success_count = len(symbols) - len(self.failed_downloads)
            failed_count = len(self.failed_downloads)
            success_rate = (success_count / len(symbols) * 100) if len(symbols) > 0 else 0

            logger.info(
                "download_summary",
                dataset="long_short_ratio",
                ratio_type=ratio_type,
                total_symbols=len(symbols),
                succeeded=success_count,
                failed=failed_count,
                success_rate=f"{success_rate:.1f}",
                total_records=total_records,
            )

            if failed_count > 0:
                logger.warning("download_failures", dataset="long_short_ratio", hint="use get_failed_downloads()")

        except Exception as e:
            logger.error("download_batch_error", dataset="long_short_ratio", error=str(e))
            raise MarketDataFetchError(f"批量下载多空比例失败: {e}") from e

    async def download_funding_rate(
        self,
        symbol: str,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 100,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> list[FundingRate]:
        """异步下载单个交易对的资金费率数据."""
        try:
            logger.debug("download_start", dataset="funding_rate", symbol=symbol)

            async def request_func():
                params = {"symbol": symbol, "limit": limit}
                if start_ts is not None:
                    params["startTime"] = int(start_ts)
                elif start_time:
                    params["startTime"] = date_to_timestamp_start(start_time)
                if end_ts is not None:
                    params["endTime"] = int(end_ts)
                elif end_time:
                    params["endTime"] = date_to_timestamp_end(end_time)
                return await self.client.futures_funding_rate(**params)

            data = await self._handle_async_request_with_retry(request_func)

            if not data:
                logger.warning("download_empty", dataset="funding_rate", symbol=symbol)
                return []

            result = [FundingRate.from_binance_response(item) for item in data]
            logger.debug("download_success", dataset="funding_rate", symbol=symbol, records=len(result))
            return result

        except Exception as e:
            logger.error("download_error", dataset="funding_rate", symbol=symbol, error=str(e))
            raise MarketDataFetchError(f"获取资金费率失败: {e}") from e

    async def download_open_interest(
        self,
        symbol: str,
        period: str = "5m",
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 1000,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> list[OpenInterest]:
        """异步下载单个交易对的持仓量数据."""
        try:
            logger.debug("download_start", dataset="open_interest", symbol=symbol)

            async def request_func():
                params = {"symbol": symbol, "period": period, "limit": min(limit, 500)}
                if start_ts is not None:
                    params["startTime"] = int(start_ts)
                elif start_time:
                    params["startTime"] = date_to_timestamp_start(start_time)
                if end_ts is not None:
                    params["endTime"] = int(end_ts)
                elif end_time:
                    params["endTime"] = date_to_timestamp_end(end_time)
                return await self.client.futures_open_interest_hist(**params)

            data = await self._handle_async_request_with_retry(request_func)

            if not data:
                logger.warning("download_empty", dataset="open_interest", symbol=symbol)
                return []

            result = [OpenInterest.from_binance_response(item) for item in data]
            logger.debug("download_success", dataset="open_interest", symbol=symbol, records=len(result))
            return result

        except Exception as e:
            logger.error("download_error", dataset="open_interest", symbol=symbol, error=str(e))
            raise MarketDataFetchError(f"获取持仓量失败: {e}") from e

    async def download_long_short_ratio(  # noqa: C901
        self,
        symbol: str,
        period: str = "5m",
        ratio_type: str = "account",
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 500,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> list[LongShortRatio]:
        """异步下载单个交易对的多空比例数据."""
        try:
            logger.debug("download_start", dataset="long_short_ratio", symbol=symbol, ratio_type=ratio_type)

            async def request_func():
                params = {"symbol": symbol, "period": period, "limit": min(limit, 500)}
                if start_ts is not None:
                    params["startTime"] = int(start_ts)
                elif start_time:
                    params["startTime"] = date_to_timestamp_start(start_time)
                if end_ts is not None:
                    params["endTime"] = int(end_ts)
                elif end_time:
                    params["endTime"] = date_to_timestamp_end(end_time)

                # 根据ratio_type选择API端点
                if ratio_type == "account":
                    return await self.client.futures_top_longshort_account_ratio(**params)
                elif ratio_type == "position":
                    return await self.client.futures_top_longshort_position_ratio(**params)
                elif ratio_type == "global":
                    return await self.client.futures_global_longshort_ratio(**params)
                elif ratio_type == "taker":
                    return await self.client.futures_taker_longshort_ratio(**params)
                else:
                    raise ValueError(f"不支持的ratio_type: {ratio_type}")

            data = await self._handle_async_request_with_retry(request_func)

            if not data:
                logger.warning("download_empty", dataset="long_short_ratio", symbol=symbol, ratio_type=ratio_type)
                return []

            result = [LongShortRatio.from_binance_response(item, ratio_type) for item in data]
            logger.debug(
                "download_success",
                dataset="long_short_ratio",
                symbol=symbol,
                ratio_type=ratio_type,
                records=len(result),
            )
            return result

        except Exception as e:
            logger.error("download_error", dataset="long_short_ratio", symbol=symbol, error=str(e))
            raise MarketDataFetchError(f"获取多空比例失败: {e}") from e

    @staticmethod
    def _format_timestamp(ts: int | str | None) -> str:
        if ts is None:
            return "-"
        return timestamp_to_datetime(int(ts)).strftime("%Y-%m-%d %H:%M:%S")

    def _format_range(self, start_ts: int, end_ts: int) -> str:
        return f"{self._format_timestamp(start_ts)} -> {self._format_timestamp(end_ts)}"

    def download(self, *args, **kwargs):
        """实现基类的抽象方法."""
        # 这里可以根据参数决定调用哪个具体的下载方法
        if "funding_rate" in kwargs:
            return self.download_funding_rate_batch(*args, **kwargs)
        elif "open_interest" in kwargs:
            return self.download_open_interest_batch(*args, **kwargs)
        elif "long_short_ratio" in kwargs:
            return self.download_long_short_ratio_batch(*args, **kwargs)
        else:
            raise ValueError("请指定要下载的数据类型")
