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
from cryptoservice.utils.run_id import generate_run_id
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

    @staticmethod
    def _plan_examples(plan_dict: dict[str, dict[str, Any]]) -> str | None:
        if not plan_dict:
            return None
        ranked = sorted(
            ((symbol, int(info.get("missing_count", 0))) for symbol, info in plan_dict.items()),
            key=lambda item: item[1],
            reverse=True,
        )
        top = [f"{symbol}({count})" for symbol, count in ranked[:3] if count > 0]
        return ", ".join(top) if top else None

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

            logger.info(f"开始检查资金费率数据：{len(symbols)} 个交易对（{start_time} ~ {end_time}）。")

            symbol_plans: dict[str, dict[str, Any]] = {}

            if incremental:
                logger.debug(f"资金费率增量模式：准备分析缺失区间（{len(symbols)} 个交易对）。")

                # 使用新的增量计划方法，不依赖固定频率
                symbol_plans = await self.db.plan_metrics_download(
                    symbols=symbols,
                    start_date=start_time,
                    end_date=end_time,
                    data_type="funding_rate",
                    interval_hours=0,  # 对于 funding_rate，这个参数会被忽略
                )

                if not symbol_plans:
                    logger.info("资金费率数据已是最新状态，跳过下载。")
                    return

                symbols = list(symbol_plans.keys())
                logger.info(f"资金费率增量计划：{len(symbol_plans)} 个交易对需要补齐数据。")

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
                            ranges = [(s_ts, e_ts) for s_ts, e_ts in default_range if s_ts is not None and e_ts is not None]

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

                    except Exception as exc:
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
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)

            logger.info(
                f"资金费率下载完成：成功 {success_count}/{len(symbols)}，写入 {total_records} 条记录，失败 {failed_count} 个交易对（耗时 {elapsed_ms} ms）。"
            )

        except Exception as exc:  # noqa: BLE001
            logger.error(f"资金费率下载失败：{exc}")
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
            logger.info(f"开始下载持仓量数据：{len(symbols)} 个交易对（频率 {interval.value}）。")

            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            # 如果启用增量下载模式，生成下载计划
            symbol_plans: dict[str, dict[str, Any]] = {}

            if incremental:
                logger.debug("incremental_mode_enabled", dataset="open_interest", action="analyzing_data")
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
                    logger.info("持仓量数据已是最新状态，跳过下载。")
                    return
                else:
                    logger.debug(
                        "incremental_summary",
                        dataset="open_interest",
                        needed=len(symbols_to_download),
                        total=len(symbols),
                    )
                    symbol_plans = missing_plan
                    # 使用需要下载的交易对列表替换原始列表
                    symbols = symbols_to_download

                    examples = self._plan_examples(symbol_plans)
                    if examples:
                        logger.info(f"持仓量增量计划：{len(symbol_plans)} 个交易对需要补齐数据（示例：{examples}）。")
                    else:
                        logger.info(f"持仓量增量计划：{len(symbol_plans)} 个交易对需要补齐数据。")

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
                            ranges = [(start_ts, end_ts) for start_ts, end_ts in default_range if start_ts is not None and end_ts is not None]

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
                            logger.debug(
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

            logger.info(f"持仓量下载完成：成功 {success_count}/{len(symbols)}，写入 {total_records} 条记录，失败 {failed_count} 个交易对。")

            if failed_count > 0:
                logger.warning("部分持仓量数据下载失败，可调用 get_failed_downloads() 查看详情。")

        except Exception as e:
            logger.error(f"持仓量下载失败：{e}")
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
            logger.info(f"开始下载多空比例数据（{ratio_type} 类型）：{len(symbols)} 个交易对。")

            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            # 如果启用增量下载模式，生成下载计划
            symbol_plans: dict[str, dict[str, Any]] = {}

            if incremental:
                logger.debug("incremental_mode_enabled", dataset="long_short_ratio", action="analyzing_data")
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
                    logger.info("多空比例数据已是最新状态，跳过下载。")
                    return
                else:
                    logger.debug(
                        "incremental_summary",
                        dataset="long_short_ratio",
                        ratio_type=ratio_type,
                        needed=len(symbols_to_download),
                        total=len(symbols),
                    )
                    symbol_plans = missing_plan
                    # 使用需要下载的交易对列表替换原始列表
                    symbols = symbols_to_download

                    examples = self._plan_examples(symbol_plans)
                    if examples:
                        logger.info(f"多空比例增量计划：{len(symbol_plans)} 个交易对需要补齐数据（示例：{examples}）。")
                    else:
                        logger.info(f"多空比例增量计划：{len(symbol_plans)} 个交易对需要补齐数据。")

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
                            ranges = [(start_ts, end_ts) for start_ts, end_ts in default_range if start_ts is not None and end_ts is not None]

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
                            logger.debug(
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

            logger.info(f"多空比例下载完成（{ratio_type}）：成功 {success_count}/{len(symbols)}，写入 {total_records} 条记录，失败 {failed_count} 个交易对。")

            if failed_count > 0:
                logger.warning("部分多空比例数据下载失败，可调用 get_failed_downloads() 查看详情。")

        except Exception as e:
            logger.error(f"多空比例下载失败：{e}")
            raise MarketDataFetchError(f"批量下载多空比例失败: {e}") from e

    async def download_funding_rate(
        self,
        symbol: str,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 100,
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
