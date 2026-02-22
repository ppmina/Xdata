"""市场指标数据下载器.

专门处理资金费率、持仓量(当日)、多空比例（当日）等市场指标数据的下载。
"""

import asyncio
import time
from typing import Any

from binance import AsyncClient

from cryptoservice.config.logging import get_logger
from cryptoservice.exceptions import MarketDataFetchError, RateLimitError
from cryptoservice.models import FundingRate, LongShortRatio, OpenInterest
from cryptoservice.storage.database import Database as AsyncMarketDB
from cryptoservice.utils.run_id import generate_run_id
from cryptoservice.utils.time_utils import date_to_timestamp_end, date_to_timestamp_start, shift_date, timestamp_to_datetime

from .base_downloader import BaseDownloader, EndpointControlRegistry

logger = get_logger(__name__)

# 非kline数据固定使用最高可用频率 (Binance API 支持的最高频率)
METRICS_PERIOD = "5m"
METRICS_INTERVAL_HOURS = 5 / 60  # 5分钟 = 5/60 小时


class MetricsDownloader(BaseDownloader):
    """市场指标数据下载器."""

    def __init__(
        self,
        client: AsyncClient,
        request_delay: float = 0.5,
        endpoint_controls: EndpointControlRegistry | None = None,
    ):
        """初始化市场指标数据下载器.

        Args:
            client: API 客户端实例.
            request_delay: 请求之间的基础延迟（秒）.
            endpoint_controls: 可选的共享 endpoint 控制状态.
        """
        super().__init__(client, request_delay, endpoint_controls=endpoint_controls)
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

    @staticmethod
    def _long_short_ratio_endpoint_key(ratio_type: str) -> str:
        endpoint_map = {
            "account": "fapi_top_longshort_account_ratio",
            "position": "fapi_top_longshort_position_ratio",
            "global": "fapi_global_longshort_ratio",
            "taker": "fapi_taker_longshort_ratio",
        }
        if ratio_type not in endpoint_map:
            raise ValueError(f"不支持的ratio_type: {ratio_type}")
        return endpoint_map[ratio_type]

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
        self.begin_run_state(run_id=run, stage="metrics", dataset="funding_rate")
        started_at = time.perf_counter()
        expanded_start_time = shift_date(start_time, -3)

        try:
            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            logger.info(
                "download.stage_start",
                run=run,
                stage="metrics",
                dataset="funding_rate",
                status="start",
                duration_ms=0,
                symbols=len(symbols),
                workers=max_workers,
                incremental=incremental,
                start=start_time,
                end=end_time,
                expanded_start=expanded_start_time,
            )

            symbol_plans: dict[str, dict[str, Any]] = {}

            if incremental:
                logger.debug(
                    "download.incremental_start",
                    run=run,
                    stage="metrics",
                    dataset="funding_rate",
                    status="analyzing",
                    symbols=len(symbols),
                    requested_start=start_time,
                    requested_end=end_time,
                )
                planner_start_date = expanded_start_time
                logger.debug(
                    "download.incremental_window",
                    run=run,
                    stage="metrics",
                    dataset="funding_rate",
                    requested_start=start_time,
                    requested_end=end_time,
                    planner_start=planner_start_date,
                    planner_end=end_time,
                )
                symbol_plans = await self.db.plan_metrics_download(
                    symbols=symbols,
                    start_date=planner_start_date,
                    end_date=end_time,
                    data_type="funding_rate",
                    interval_hours=0,  # 对于 funding_rate，这个参数会被忽略
                )

                if not symbol_plans:
                    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                    logger.info(
                        "download.stage_done",
                        run=run,
                        stage="metrics",
                        dataset="funding_rate",
                        status="skipped",
                        duration_ms=elapsed_ms,
                        reason="already_up_to_date",
                        symbols=len(symbols),
                    )
                    return
                symbols = list(symbol_plans.keys())
                logger.info(
                    "download.incremental_plan",
                    run=run,
                    stage="metrics",
                    dataset="funding_rate",
                    status="ready",
                    symbols=len(symbol_plans),
                )

            if not symbols:
                elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                logger.info(
                    "download.stage_done",
                    run=run,
                    stage="metrics",
                    dataset="funding_rate",
                    status="skipped",
                    duration_ms=elapsed_ms,
                    reason="empty_symbol_set",
                    symbols=0,
                )
                return

            lock = asyncio.Lock()
            total_records = 0
            completed_symbols = 0
            symbol_count = len(symbols)
            worker_count = max(1, min(max_workers, symbol_count))
            queue: asyncio.Queue[str | None] = asyncio.Queue()
            for symbol in symbols:
                queue.put_nowait(symbol)
            for _ in range(worker_count):
                queue.put_nowait(None)

            default_range = [
                (
                    date_to_timestamp_start(expanded_start_time) if expanded_start_time else None,
                    date_to_timestamp_end(end_time) if end_time else None,
                )
            ]

            async def process_symbol(symbol: str) -> int:  # noqa: C901
                nonlocal total_records, completed_symbols
                try:
                    logger.debug(
                        "download.symbol_start",
                        run=run,
                        stage="metrics",
                        dataset="funding_rate",
                        symbol=symbol,
                        status="start",
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
                            stage="metrics",
                            dataset="funding_rate",
                            symbol=symbol,
                            status="start",
                            range=self._format_range(range_start, range_end),
                        )

                        funding_rates = await self.download_funding_rate(
                            symbol=symbol,
                            start_ts=range_start,
                            end_ts=range_end,
                            limit=1000,
                            endpoint_max_workers=max_workers,
                        )

                        if not funding_rates or not self.db:
                            logger.debug(
                                "download.range_empty",
                                run=run,
                                stage="metrics",
                                dataset="funding_rate",
                                symbol=symbol,
                                status="empty",
                                range=self._format_range(range_start, range_end),
                            )
                            continue

                        inserted = await self.db.insert_funding_rates(funding_rates)
                        total_inserted_symbol += inserted
                        async with lock:
                            total_records += inserted

                        logger.debug(
                            "download.range_done",
                            run=run,
                            stage="metrics",
                            dataset="funding_rate",
                            symbol=symbol,
                            status="complete",
                            range=self._format_range(range_start, range_end),
                            rows=inserted,
                        )

                        if request_delay > 0:
                            await asyncio.sleep(request_delay)

                    if total_inserted_symbol == 0:
                        logger.debug(
                            "download.symbol_empty",
                            run=run,
                            stage="metrics",
                            dataset="funding_rate",
                            symbol=symbol,
                            status="empty",
                        )
                    else:
                        logger.debug(
                            "download.symbol_done",
                            run=run,
                            stage="metrics",
                            dataset="funding_rate",
                            symbol=symbol,
                            status="complete",
                            rows=total_inserted_symbol,
                        )
                    return total_inserted_symbol

                except RateLimitError:
                    logger.error(
                        "download.symbol_error",
                        run=run,
                        stage="metrics",
                        dataset="funding_rate",
                        symbol=symbol,
                        status="error",
                        terminal=True,
                        error="terminal_rate_limit",
                    )
                    raise
                except Exception as exc:
                    error_message = self._format_exception_message(exc)
                    logger.warning(
                        "download.symbol_error",
                        run=run,
                        stage="metrics",
                        dataset="funding_rate",
                        symbol=symbol,
                        status="error",
                        terminal=False,
                        error=error_message,
                    )
                    plan = symbol_plans.get(symbol)
                    metadata: dict[str, Any] = {
                        "data_type": "funding_rate",
                        "start_time": start_time,
                        "end_time": end_time,
                        "expanded_start_time": expanded_start_time,
                    }
                    if plan:
                        start_ts = plan.get("start_ts")
                        end_ts = plan.get("end_ts")
                        if start_ts is not None:
                            metadata["start_ts"] = start_ts
                        if end_ts is not None:
                            metadata["end_ts"] = end_ts
                    self._record_failed_download(symbol, error_message, metadata)
                    return 0
                finally:
                    async with lock:
                        completed_symbols += 1
                        if completed_symbols % 50 == 0 or completed_symbols == symbol_count:
                            logger.info(
                                "download.progress",
                                run=run,
                                stage="metrics",
                                dataset="funding_rate",
                                status="progress",
                                completed=completed_symbols,
                                total=symbol_count,
                                rows=total_records,
                            )

            async def worker() -> None:
                while True:
                    symbol = await queue.get()
                    try:
                        if symbol is None:
                            return
                        await process_symbol(symbol)
                    finally:
                        queue.task_done()

            try:
                async with asyncio.TaskGroup() as tg:
                    for _ in range(worker_count):
                        tg.create_task(worker())
            except* RateLimitError as grouped_rate_limit:
                elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                logger.error(
                    "download.stage_done",
                    run=run,
                    stage="metrics",
                    dataset="funding_rate",
                    status="aborted",
                    duration_ms=elapsed_ms,
                    total_symbols=symbol_count,
                    completed_symbols=completed_symbols,
                    rows=total_records,
                    terminal=True,
                    error=str(grouped_rate_limit.exceptions[0]),
                )
                raise grouped_rate_limit.exceptions[0] from None

            failed_count = len(self.failed_downloads)
            success_count = symbol_count - failed_count
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)

            logger.info(
                "download.stage_done",
                run=run,
                stage="metrics",
                dataset="funding_rate",
                status="complete",
                duration_ms=elapsed_ms,
                total_symbols=symbol_count,
                successful_symbols=success_count,
                failed_symbols=failed_count,
                rows=total_records,
            )
            if self.failed_downloads:
                logger.warning(
                    "download.partial_failures",
                    run=run,
                    stage="metrics",
                    dataset="funding_rate",
                    status="partial",
                    failed_symbols=failed_count,
                )
        except RateLimitError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "download.stage_done",
                run=run,
                stage="metrics",
                dataset="funding_rate",
                status="error",
                duration_ms=int((time.perf_counter() - started_at) * 1000),
                terminal=False,
                error=str(exc),
            )
            raise MarketDataFetchError(f"Batch funding rate download failed: {exc}") from exc

    async def download_open_interest_batch(  # noqa: C901
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
        """批量异步下载持仓量数据.

        数据频率固定为5m（Binance API支持的最高频率）。

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            db_path: 数据库路径
            request_delay: 请求延迟
            max_workers: 最大并发数
            incremental: 是否启用增量下载（默认True）
            run_id: 运行ID（可选）
        """
        run = run_id or generate_run_id("open_interest")
        self._run_id = run
        self.begin_run_state(run_id=run, stage="metrics", dataset="open_interest")
        try:
            expanded_start_time = shift_date(start_time, -1)
            logger.info(
                "Start downloading open interest data: %s symbols (frequency %s, expanded start %s).",
                len(symbols),
                METRICS_PERIOD,
                expanded_start_time,
            )

            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            # 如果启用增量下载模式，生成下载计划
            symbol_plans: dict[str, dict[str, Any]] = {}

            if incremental:
                logger.debug("incremental_mode_enabled", dataset="open_interest", action="analyzing_data")
                planner_start_date = expanded_start_time
                logger.debug(
                    "download.incremental_window",
                    dataset="open_interest",
                    requested_start=start_time,
                    requested_end=end_time,
                    planner_start=planner_start_date,
                    planner_end=end_time,
                )

                missing_plan = await self.db.plan_metrics_download(
                    symbols=symbols,
                    start_date=planner_start_date,
                    end_date=end_time,
                    data_type="open_interest",
                    interval_hours=METRICS_INTERVAL_HOURS,
                )

                # 过滤出需要下载的交易对
                symbols_to_download = list(missing_plan.keys())
                if not symbols_to_download:
                    logger.info("Open interest data is already up to date, skipping download.")
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
                        logger.info(f"Open interest incremental plan: {len(symbol_plans)} symbols need backfill (examples: {examples}).")
                    else:
                        logger.info(f"Open interest incremental plan: {len(symbol_plans)} symbols need backfill.")

            semaphore = asyncio.Semaphore(max_workers)
            lock = asyncio.Lock()
            total_records = 0

            default_range = [
                (
                    date_to_timestamp_start(expanded_start_time) if expanded_start_time else None,
                    date_to_timestamp_end(end_time) if end_time else None,
                )
            ]

            async def process_symbol(symbol: str):  # noqa: C901
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
                                start_ts=range_start,
                                end_ts=range_end,
                                limit=1000,
                                endpoint_max_workers=max_workers,
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
                    except RateLimitError:
                        raise
                    except Exception as e:
                        error_message = self._format_exception_message(e)
                        logger.warning("download_symbol_error", dataset="open_interest", symbol=symbol, error=error_message)
                        plan = symbol_plans.get(symbol)
                        metadata: dict[str, Any] = {
                            "data_type": "open_interest",
                            "start_time": start_time,
                            "end_time": end_time,
                            "expanded_start_time": expanded_start_time,
                        }
                        if plan:
                            start_ts = plan.get("start_ts")
                            end_ts = plan.get("end_ts")
                            if start_ts is not None:
                                metadata["start_ts"] = start_ts
                            if end_ts is not None:
                                metadata["end_ts"] = end_ts
                        self._record_failed_download(symbol, error_message, metadata)

            tasks = [process_symbol(symbol) for symbol in symbols]
            await asyncio.gather(*tasks)

            # 完整性检查
            success_count = len(symbols) - len(self.failed_downloads)
            failed_count = len(self.failed_downloads)

            logger.info(
                f"Open interest download completed: success {success_count}/{len(symbols)}, wrote {total_records} records, failed {failed_count} symbols."
            )
            if self.failed_downloads:
                logger.warning("Some open interest downloads failed; call get_failed_downloads() for details.")
        except RateLimitError:
            raise
        except Exception as e:
            logger.error(f"Open interest download failed: {e}")
            raise MarketDataFetchError(f"Batch open interest download failed: {e}") from e

    async def download_long_short_ratio_batch(  # noqa: C901
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        db_path: str,
        ratio_type: str = "account",
        request_delay: float = 0.5,
        max_workers: int = 5,
        incremental: bool = True,
        run_id: str | None = None,
    ) -> None:
        """批量异步下载多空比例数据.

        数据频率固定为5m（Binance API支持的最高频率）。

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            db_path: 数据库路径
            ratio_type: 比例类型
            request_delay: 请求延迟
            max_workers: 最大并发数
            incremental: 是否启用增量下载（默认True）
            run_id: 运行ID（可选）
        """
        run = run_id or generate_run_id("long_short_ratio")
        self._run_id = run
        self.begin_run_state(run_id=run, stage="metrics", dataset="long_short_ratio")
        try:
            expanded_start_time = shift_date(start_time, -1)
            logger.info(
                "Start downloading long-short ratio data (%s type): %s symbols (frequency %s, expansion starting point %s).",
                ratio_type,
                len(symbols),
                METRICS_PERIOD,
                expanded_start_time,
            )

            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            # 如果启用增量下载模式，生成下载计划
            symbol_plans: dict[str, dict[str, Any]] = {}

            if incremental:
                logger.debug("incremental_mode_enabled", dataset="long_short_ratio", action="analyzing_data")
                planner_start_date = expanded_start_time
                logger.debug(
                    "download.incremental_window",
                    dataset="long_short_ratio",
                    ratio_type=ratio_type,
                    requested_start=start_time,
                    requested_end=end_time,
                    planner_start=planner_start_date,
                    planner_end=end_time,
                )

                missing_plan = await self.db.plan_metrics_download(
                    symbols=symbols,
                    start_date=planner_start_date,
                    end_date=end_time,
                    data_type="long_short_ratio",
                    interval_hours=METRICS_INTERVAL_HOURS,
                )

                # 过滤出需要下载的交易对
                symbols_to_download = list(missing_plan.keys())
                if not symbols_to_download:
                    logger.info("Long-short ratio data is already up to date, skipping download.")
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
                        logger.info(f"Long-short ratio incremental plan: {len(symbol_plans)} symbols need backfill (examples: {examples}).")
                    else:
                        logger.info(f"Long-short ratio incremental plan: {len(symbol_plans)} symbols need backfill.")

            semaphore = asyncio.Semaphore(max_workers)
            lock = asyncio.Lock()
            total_records = 0

            default_range = [
                (
                    date_to_timestamp_start(expanded_start_time) if expanded_start_time else None,
                    date_to_timestamp_end(end_time) if end_time else None,
                )
            ]

            async def process_symbol(symbol: str):  # noqa: C901
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
                                ratio_type=ratio_type,
                                start_ts=range_start,
                                end_ts=range_end,
                                limit=500,
                                endpoint_max_workers=max_workers,
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
                    except RateLimitError:
                        raise
                    except Exception as e:
                        error_message = self._format_exception_message(e)
                        logger.warning("download_symbol_error", dataset="long_short_ratio", symbol=symbol, error=error_message)
                        plan = symbol_plans.get(symbol)
                        metadata: dict[str, Any] = {
                            "data_type": "long_short_ratio",
                            "ratio_type": ratio_type,
                            "start_time": start_time,
                            "end_time": end_time,
                            "expanded_start_time": expanded_start_time,
                        }
                        if plan:
                            start_ts = plan.get("start_ts")
                            end_ts = plan.get("end_ts")
                            if start_ts is not None:
                                metadata["start_ts"] = start_ts
                            if end_ts is not None:
                                metadata["end_ts"] = end_ts
                        self._record_failed_download(symbol, error_message, metadata)

            tasks = [process_symbol(symbol) for symbol in symbols]
            await asyncio.gather(*tasks)

            # 完整性检查
            success_count = len(symbols) - len(self.failed_downloads)
            failed_count = len(self.failed_downloads)

            logger.info(
                f"Long-short ratio download completed ({ratio_type}): success {success_count}/{len(symbols)}, "
                f"wrote {total_records} records, failed {failed_count} symbols."
            )
            if self.failed_downloads:
                logger.warning("Some long-short ratio downloads failed; call get_failed_downloads() for details.")
        except RateLimitError:
            raise
        except Exception as e:
            logger.error(f"Long-short ratio download failed: {e}")
            raise MarketDataFetchError(f"Batch long-short ratio download failed: {e}") from e

    async def download_funding_rate(
        self,
        symbol: str,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 100,
        start_ts: int | None = None,
        end_ts: int | None = None,
        endpoint_max_workers: int | None = None,
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

            data = await self._handle_async_request_with_retry(
                request_func,
                endpoint_key="fapi_funding_rate",
                endpoint_max_workers=endpoint_max_workers,
            )

            if not data:
                logger.warning("download_empty", dataset="funding_rate", symbol=symbol)
                return []

            result = [FundingRate.from_binance_response(item) for item in data]
            logger.debug("download_success", dataset="funding_rate", symbol=symbol, records=len(result))
            return result

        except RateLimitError:
            raise
        except Exception as e:
            error_message = self._format_exception_message(e)
            logger.error("download_error", dataset="funding_rate", symbol=symbol, error=error_message)
            raise MarketDataFetchError(f"获取资金费率失败: {error_message}") from e

    async def download_open_interest(
        self,
        symbol: str,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 1000,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
        endpoint_max_workers: int | None = None,
    ) -> list[OpenInterest]:
        """异步下载单个交易对的持仓量数据.

        数据频率固定为5m（Binance API支持的最高频率）。
        """
        try:
            logger.debug("download_start", dataset="open_interest", symbol=symbol)

            async def request_func():
                params = {"symbol": symbol, "period": METRICS_PERIOD, "limit": min(limit, 500)}
                if start_ts is not None:
                    params["startTime"] = int(start_ts)
                elif start_time:
                    params["startTime"] = date_to_timestamp_start(start_time)
                if end_ts is not None:
                    params["endTime"] = int(end_ts)
                elif end_time:
                    params["endTime"] = date_to_timestamp_end(end_time)
                return await self.client.futures_open_interest_hist(**params)

            data = await self._handle_async_request_with_retry(
                request_func,
                endpoint_key="fapi_open_interest_hist",
                endpoint_max_workers=endpoint_max_workers,
            )

            if not data:
                logger.warning("download_empty", dataset="open_interest", symbol=symbol)
                return []

            result = [OpenInterest.from_binance_response(item) for item in data]
            logger.debug("download_success", dataset="open_interest", symbol=symbol, records=len(result))
            return result

        except RateLimitError:
            raise
        except Exception as e:
            error_message = self._format_exception_message(e)
            logger.error("download_error", dataset="open_interest", symbol=symbol, error=error_message)
            raise MarketDataFetchError(f"获取持仓量失败: {error_message}") from e

    async def download_long_short_ratio(  # noqa: C901
        self,
        symbol: str,
        ratio_type: str = "account",
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 500,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
        endpoint_max_workers: int | None = None,
    ) -> list[LongShortRatio]:
        """异步下载单个交易对的多空比例数据.

        数据频率固定为5m（Binance API支持的最高频率）。
        """
        try:
            logger.debug("download_start", dataset="long_short_ratio", symbol=symbol, ratio_type=ratio_type)

            endpoint_key = self._long_short_ratio_endpoint_key(ratio_type)

            async def request_func():
                params = {"symbol": symbol, "period": METRICS_PERIOD, "limit": min(limit, 500)}
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

            data = await self._handle_async_request_with_retry(
                request_func,
                endpoint_key=endpoint_key,
                endpoint_max_workers=endpoint_max_workers,
            )

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

        except RateLimitError:
            raise
        except Exception as e:
            error_message = self._format_exception_message(e)
            logger.error("download_error", dataset="long_short_ratio", symbol=symbol, error=error_message)
            raise MarketDataFetchError(f"获取多空比例失败: {error_message}") from e

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
