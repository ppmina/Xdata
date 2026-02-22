"""K线数据下载器.

专门处理K线数据的下载，包括现货和期货K线数据。
"""

import asyncio
import time
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

from binance import AsyncClient

from cryptoservice.config import RetryConfig
from cryptoservice.config.logging import get_logger
from cryptoservice.exceptions import InvalidSymbolError, MarketDataFetchError, RateLimitError
from cryptoservice.models import (
    Freq,
    HistoricalKlinesType,
    IntegrityReport,
    PerpetualMarketTicker,
)
from cryptoservice.storage.database import Database as AsyncMarketDB
from cryptoservice.utils.run_id import generate_run_id

from .base_downloader import BaseDownloader, EndpointControlRegistry

logger = get_logger(__name__)


class KlineDownloader(BaseDownloader):
    """K线数据下载器."""

    def __init__(
        self,
        client: AsyncClient,
        request_delay: float = 0.5,
        endpoint_controls: EndpointControlRegistry | None = None,
    ):
        """初始化K线数据下载器.

        Args:
            client: API 客户端实例.
            request_delay: 请求之间的基础延迟（秒）.
            endpoint_controls: 可选的共享 endpoint 控制状态.
        """
        super().__init__(client, request_delay, endpoint_controls=endpoint_controls)
        self.db: AsyncMarketDB | None = None
        self._run_id: str | None = None

    async def download_single_symbol(
        self,
        symbol: str,
        start_ts: str,
        end_ts: str,
        interval: Freq,
        klines_type: HistoricalKlinesType = HistoricalKlinesType.FUTURES,
        retry_config: RetryConfig | None = None,
        endpoint_max_workers: int | None = None,
    ) -> AsyncGenerator[PerpetualMarketTicker, None]:
        """异步下载单个交易对的K线数据, 并以生成器模式返回."""
        try:
            logger.debug(
                "download.range_start",
                run=self._run_id,
                dataset="kline",
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                interval=interval.value,
            )

            async def request_func():
                return await self.client.get_historical_klines_generator(
                    symbol=symbol,
                    interval=interval.value,
                    start_str=start_ts,
                    end_str=end_ts,
                    limit=1500,
                    klines_type=HistoricalKlinesType.to_binance(klines_type),
                )

            klines_generator = await self._handle_async_request_with_retry(
                request_func,
                retry_config=retry_config,
                endpoint_key="fapi_klines",
                endpoint_max_workers=endpoint_max_workers,
            )

            if not klines_generator:
                logger.debug(
                    "download.range_empty",
                    run=self._run_id,
                    dataset="kline",
                    symbol=symbol,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
                return

            processed_count = 0
            async for kline in klines_generator:
                validated_kline = self._validate_single_kline(kline, symbol)
                if validated_kline:
                    yield PerpetualMarketTicker.from_binance_kline(symbol=symbol, kline=validated_kline)
                    processed_count += 1

            logger.debug(
                "download.range_done",
                run=self._run_id,
                dataset="kline",
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                rows=processed_count,
            )

        except InvalidSymbolError:
            logger.warning(
                "download.invalid_symbol",
                run=self._run_id,
                dataset="kline",
                symbol=symbol,
            )
            raise
        except RateLimitError:
            raise
        except Exception as e:
            logger.error(
                "download.error",
                run=self._run_id,
                dataset="kline",
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                error=str(e),
            )
            self._record_failed_download(
                symbol,
                str(e),
                {
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "interval": interval.value,
                },
            )
            raise MarketDataFetchError(f"下载交易对 {symbol} 数据失败: {e}") from e

    async def download_multiple_symbols(  # noqa: C901
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        interval: Freq,
        db_path: Path,
        max_workers: int = 5,
        retry_config: RetryConfig | None = None,
        incremental: bool = True,
        run_id: str | None = None,
    ) -> IntegrityReport:
        """批量异步下载多个交易对的K线数据."""
        run = run_id or generate_run_id("kline")
        self._run_id = run
        self.begin_run_state(run_id=run, stage="kline", dataset="kline")
        started_at = time.perf_counter()

        if self.db is None:
            self.db = AsyncMarketDB(str(db_path))
        await self.db.initialize()

        plan_ranges: dict[str, list[tuple[str, str]]] = {}

        if incremental:
            logger.debug(
                "download.incremental_start",
                run=run,
                dataset="kline",
                symbols=len(symbols),
                start=start_time,
                end=end_time,
                interval=interval.value,
            )
            missing_plan = await self.db.plan_kline_download(
                symbols=symbols,
                start_date=start_time,
                end_date=end_time,
                freq=interval,
            )

            symbols_to_download = list(missing_plan.keys())
            if not symbols_to_download:
                elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                logger.info(
                    "download.stage_done",
                    run=run,
                    stage="kline",
                    dataset="kline",
                    status="skipped",
                    duration_ms=elapsed_ms,
                    symbols=len(symbols),
                    interval=interval.value,
                    reason="already_up_to_date",
                )
                return IntegrityReport(
                    total_symbols=len(symbols),
                    successful_symbols=len(symbols),
                    failed_symbols=[],
                    missing_periods=[],
                    data_quality_score=1.0,
                    recommendations=["所有数据已是最新状态"],
                )

            plan_ranges = {
                symbol: [
                    (
                        str(plan_info["start_ts"]),
                        str(plan_info["end_ts"]),
                    )
                ]
                for symbol, plan_info in missing_plan.items()
            }
            logger.debug(
                "download.plan_selected",
                run=run,
                dataset="kline",
                selected=len(symbols_to_download),
                total=len(symbols),
            )
            symbols = symbols_to_download

        if not symbols:
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            logger.info(
                "download.stage_done",
                run=run,
                stage="kline",
                dataset="kline",
                status="skipped",
                duration_ms=elapsed_ms,
                symbols=0,
                interval=interval.value,
                reason="empty_symbol_set",
            )
            return IntegrityReport(
                total_symbols=0,
                successful_symbols=0,
                failed_symbols=[],
                missing_periods=[],
                data_quality_score=0.0,
                recommendations=["no symbols to process"],
            )

        start_ts = self._date_to_timestamp_start(start_time)
        end_ts = self._date_to_timestamp_end(end_time)
        default_range = [(start_ts, end_ts)]

        successful_symbols: list[str] = []
        failed_symbols: list[str] = []
        missing_periods: list[dict[str, Any]] = []
        results_lock = asyncio.Lock()
        task_queue: asyncio.Queue[str | None] = asyncio.Queue()
        worker_count = max(1, min(max_workers, len(symbols)))

        for symbol in symbols:
            task_queue.put_nowait(symbol)
        for _ in range(worker_count):
            task_queue.put_nowait(None)

        logger.info(
            "download.stage_start",
            run=run,
            stage="kline",
            dataset="kline",
            status="start",
            duration_ms=0,
            symbols=len(symbols),
            workers=worker_count,
            interval=interval.value,
            incremental=incremental,
        )

        async def worker() -> None:
            while True:
                symbol = await task_queue.get()
                try:
                    if symbol is None:
                        return

                    outcome = await self._process_symbol(
                        symbol=symbol,
                        download_ranges=plan_ranges.get(symbol, default_range),
                        interval=interval,
                        retry_config=retry_config,
                        endpoint_max_workers=max_workers,
                    )
                    async with results_lock:
                        status = outcome["status"]
                        if status == "success":
                            successful_symbols.append(symbol)
                        elif status == "empty":
                            missing_periods.append(outcome["missing"])
                        elif status == "failed":
                            failed_symbols.append(symbol)
                            missing_periods.append(outcome["missing"])
                finally:
                    task_queue.task_done()

        try:
            async with asyncio.TaskGroup() as tg:
                for _ in range(worker_count):
                    tg.create_task(worker())
        except* RateLimitError as grouped_rate_limit:
            raise grouped_rate_limit.exceptions[0] from None

        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        logger.info(
            "download.stage_done",
            run=run,
            stage="kline",
            dataset="kline",
            status="complete",
            duration_ms=elapsed_ms,
            total_symbols=len(symbols),
            successful_symbols=len(successful_symbols),
            failed_symbols=len(failed_symbols),
            missing_periods=len(missing_periods),
        )

        return IntegrityReport(
            total_symbols=len(symbols),
            successful_symbols=len(successful_symbols),
            failed_symbols=failed_symbols,
            missing_periods=missing_periods,
            data_quality_score=len(successful_symbols) / len(symbols) if symbols else 0,
            recommendations=self._generate_recommendations(successful_symbols, failed_symbols),
        )

    async def _process_symbol(
        self,
        *,
        symbol: str,
        download_ranges: list[tuple[str, str]],
        interval: Freq,
        retry_config: RetryConfig | None,
        endpoint_max_workers: int,
    ) -> dict[str, Any]:
        """下载并存储单个交易对数据，返回结果摘要."""
        try:
            total_processed = 0

            for range_start, range_end in download_ranges:
                data_generator = self.download_single_symbol(
                    symbol=symbol,
                    start_ts=range_start,
                    end_ts=range_end,
                    interval=interval,
                    retry_config=retry_config,
                    endpoint_max_workers=endpoint_max_workers,
                )

                chunk: list[PerpetualMarketTicker] = []
                processed_this_range = 0

                async for item in data_generator:
                    chunk.append(item)
                    if len(chunk) >= 1000:
                        if self.db:
                            await self.db.insert_klines(chunk, interval)
                        processed_this_range += len(chunk)
                        chunk = []

                if chunk and self.db:
                    await self.db.insert_klines(chunk, interval)
                    processed_this_range += len(chunk)

                total_processed += processed_this_range

            if total_processed > 0:
                logger.debug(
                    "download.symbol_done",
                    run=self._run_id,
                    stage="kline",
                    dataset="kline",
                    symbol=symbol,
                    status="complete",
                    rows=total_processed,
                )
                return {"status": "success", "rows": total_processed}

            logger.debug(
                "download.symbol_empty",
                run=self._run_id,
                stage="kline",
                dataset="kline",
                symbol=symbol,
                status="empty",
                rows=0,
            )
            overall_start = download_ranges[0][0]
            overall_end = download_ranges[-1][1]
            missing = {
                "symbol": symbol,
                "period": (f"{self._format_timestamp(overall_start)} - {self._format_timestamp(overall_end)}"),
                "reason": "no_data",
            }
            return {"status": "empty", "missing": missing}

        except RateLimitError:
            raise
        except Exception as exc:
            logger.error(
                "download.symbol_error",
                run=self._run_id,
                stage="kline",
                dataset="kline",
                symbol=symbol,
                status="error",
                error=str(exc),
                terminal=False,
            )
            overall_start = download_ranges[0][0]
            overall_end = download_ranges[-1][1]
            missing = {
                "symbol": symbol,
                "period": (f"{self._format_timestamp(overall_start)} - {self._format_timestamp(overall_end)}"),
                "reason": str(exc),
            }
            return {"status": "failed", "missing": missing}

    def _validate_single_kline(self, kline: list, symbol: str) -> list | None:
        """验证单条K线数据质量."""
        try:
            # 检查数据结构
            if len(kline) < 8:
                logger.warning("kline.validation_insufficient_fields", symbol=symbol, stage="kline", dataset="kline")
                return None

            # 检查价格数据有效性
            open_price = float(kline[1])
            high_price = float(kline[2])
            low_price = float(kline[3])
            close_price = float(kline[4])
            volume = float(kline[5])

            # 基础逻辑检查
            if high_price < max(open_price, close_price, low_price):
                logger.warning("kline.validation_invalid_high", symbol=symbol, stage="kline", dataset="kline")
                return None

            if low_price > min(open_price, close_price, high_price):
                logger.warning("kline.validation_invalid_low", symbol=symbol, stage="kline", dataset="kline")
                return None

            if volume < 0:
                logger.warning("kline.validation_negative_volume", symbol=symbol, stage="kline", dataset="kline")
                return None

            return kline

        except (ValueError, IndexError) as e:
            logger.warning("kline.validation_format_error", symbol=symbol, stage="kline", dataset="kline", error=str(e))
            return None

    def _validate_kline_data(self, data: list, symbol: str) -> list:
        """验证K线数据质量."""
        if not data:
            return data

        valid_data = []
        issues = []

        for i, kline in enumerate(data):
            try:
                # 检查数据结构
                if len(kline) < 8:
                    issues.append(f"记录{i}: 数据字段不足")
                    continue

                # 检查价格数据有效性
                open_price = float(kline[1])
                high_price = float(kline[2])
                low_price = float(kline[3])
                close_price = float(kline[4])
                volume = float(kline[5])

                # 基础逻辑检查
                if high_price < max(open_price, close_price, low_price):
                    issues.append(f"记录{i}: 最高价异常")
                    continue

                if low_price > min(open_price, close_price, high_price):
                    issues.append(f"记录{i}: 最低价异常")
                    continue

                if volume < 0:
                    issues.append(f"记录{i}: 成交量为负")
                    continue

                valid_data.append(kline)

            except (ValueError, IndexError) as e:
                issues.append(f"记录{i}: 数据格式错误 - {e}")
                continue

        if issues:
            issue_count = len(issues)
            total_count = len(data)
            if issue_count > total_count * 0.1:  # 超过10%的数据有问题
                logger.warning(
                    "kline_data_quality_issue",
                    symbol=symbol,
                    invalid_records=issue_count,
                    total_records=total_count,
                )

        return valid_data

    def _date_to_timestamp_start(self, date: str) -> str:
        """将日期字符串转换为当天开始的时间戳（UTC）."""
        from cryptoservice.utils import date_to_timestamp_start

        return str(date_to_timestamp_start(date))

    def _date_to_timestamp_end(self, date: str) -> str:
        """将日期字符串转换为次日开始的时间戳（UTC）."""
        from cryptoservice.utils import date_to_timestamp_end

        return str(date_to_timestamp_end(date))

    @staticmethod
    def _format_timestamp(ts: str) -> str:
        """将毫秒时间戳字符串转换为可读时间."""
        from cryptoservice.utils import timestamp_to_datetime

        return timestamp_to_datetime(int(ts)).strftime("%Y-%m-%d %H:%M:%S")

    def _format_range(self, start_ts: str, end_ts: str) -> str:
        """格式化时间区间."""
        return f"{self._format_timestamp(start_ts)} -> {self._format_timestamp(end_ts)}"

    def _generate_recommendations(self, successful_symbols: list[str], failed_symbols: list[str]) -> list[str]:
        """生成建议."""
        recommendations = []
        success_rate = len(successful_symbols) / (len(successful_symbols) + len(failed_symbols))

        if success_rate < 0.5:
            recommendations.append("🚨 数据质量严重不足，建议重新下载")
        elif success_rate < 0.8:
            recommendations.append("⚠️ 数据质量一般，建议检查失败的交易对")
        else:
            recommendations.append("✅ 数据质量良好")

        if failed_symbols:
            recommendations.append(f"📝 {len(failed_symbols)}个交易对下载失败，建议单独重试")

        return recommendations

    def download(self, *args, **kwargs):
        """实现基类的抽象方法."""
        return self.download_multiple_symbols(*args, **kwargs)
