"""K线数据下载器.

专门处理K线数据的下载，包括现货和期货K线数据。
"""

import asyncio
import time
from collections.abc import AsyncGenerator
from pathlib import Path

from binance import AsyncClient

from cryptoservice.config import RetryConfig
from cryptoservice.config.logging import get_logger
from cryptoservice.exceptions import InvalidSymbolError, MarketDataFetchError
from cryptoservice.models import (
    Freq,
    HistoricalKlinesType,
    IntegrityReport,
    PerpetualMarketTicker,
)
from cryptoservice.storage.database import Database as AsyncMarketDB
from cryptoservice.utils.run_id import generate_run_id

from .base_downloader import BaseDownloader

logger = get_logger(__name__)


class KlineDownloader(BaseDownloader):
    """K线数据下载器."""

    def __init__(self, client: AsyncClient, request_delay: float = 0.5):
        """初始化K线数据下载器.

        Args:
            client: API 客户端实例.
            request_delay: 请求之间的基础延迟（秒）.
        """
        super().__init__(client, request_delay)
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

            klines_generator = await self._handle_async_request_with_retry(request_func, retry_config=retry_config)

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

    async def download_multiple_symbols(
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
                logger.info(f"K 线数据已是最新状态：{len(symbols)} 个交易对无需下载。")
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

        start_ts = self._date_to_timestamp_start(start_time)
        end_ts = self._date_to_timestamp_end(end_time)
        default_range = [(start_ts, end_ts)]

        successful_symbols: list[str] = []
        failed_symbols: list[str] = []
        missing_periods: list[dict] = []
        semaphore = asyncio.Semaphore(max_workers)

        logger.info(
            "kline_download_started",
            symbols=len(symbols),
            interval=interval.value,
            incremental=incremental,
        )

        tasks = [
            self._process_symbol(
                symbol=symbol,
                download_ranges=plan_ranges.get(symbol, default_range),
                interval=interval,
                retry_config=retry_config,
                semaphore=semaphore,
                successful_symbols=successful_symbols,
                failed_symbols=failed_symbols,
                missing_periods=missing_periods,
            )
            for symbol in symbols
        ]
        await asyncio.gather(*tasks)

        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        logger.info(
            "kline_download_complete",
            summary=f"{len(successful_symbols)}/{len(symbols)} symbols, {len(failed_symbols)} failed",
            duration_ms=elapsed_ms,
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
        semaphore: asyncio.Semaphore,
        successful_symbols: list[str],
        failed_symbols: list[str],
        missing_periods: list[dict],
    ) -> None:
        """下载并存储单个交易对数据，并更新结果列表."""
        async with semaphore:
            try:
                total_processed = 0

                for range_start, range_end in download_ranges:
                    data_generator = self.download_single_symbol(
                        symbol=symbol,
                        start_ts=range_start,
                        end_ts=range_end,
                        interval=interval,
                        retry_config=retry_config,
                    )

                    chunk: list[PerpetualMarketTicker] = []
                    processed_this_range = 0

                    async for item in data_generator:
                        chunk.append(item)
                        if len(chunk) >= 1000:  # 每1000条数据存一次
                            if self.db:
                                await self.db.insert_klines(chunk, interval)
                            processed_this_range += len(chunk)
                            chunk = []

                    if chunk and self.db:  # 存储剩余的数据
                        await self.db.insert_klines(chunk, interval)
                        processed_this_range += len(chunk)

                    total_processed += processed_this_range

                if total_processed > 0:
                    successful_symbols.append(symbol)
                    logger.debug(
                        "download.symbol_done",
                        run=self._run_id,
                        dataset="kline",
                        symbol=symbol,
                        rows=total_processed,
                    )
                else:
                    logger.debug(
                        "download.symbol_empty",
                        run=self._run_id,
                        dataset="kline",
                        symbol=symbol,
                    )
                    overall_start = download_ranges[0][0]
                    overall_end = download_ranges[-1][1]
                    missing_periods.append(
                        {
                            "symbol": symbol,
                            "period": (f"{self._format_timestamp(overall_start)} - {self._format_timestamp(overall_end)}"),
                            "reason": "no_data",
                        }
                    )

            except Exception as e:
                logger.error(
                    "download.symbol_error",
                    run=self._run_id,
                    dataset="kline",
                    symbol=symbol,
                    error=str(e),
                )
                failed_symbols.append(symbol)
                overall_start = download_ranges[0][0]
                overall_end = download_ranges[-1][1]
                missing_periods.append(
                    {
                        "symbol": symbol,
                        "period": (f"{self._format_timestamp(overall_start)} - {self._format_timestamp(overall_end)}"),
                        "reason": str(e),
                    }
                )

    def _validate_single_kline(self, kline: list, symbol: str) -> list | None:
        """验证单条K线数据质量."""
        try:
            # 检查数据结构
            if len(kline) < 8:
                logger.warning(f"{symbol}: 数据字段不足 - {kline}")
                return None

            # 检查价格数据有效性
            open_price = float(kline[1])
            high_price = float(kline[2])
            low_price = float(kline[3])
            close_price = float(kline[4])
            volume = float(kline[5])

            # 基础逻辑检查
            if high_price < max(open_price, close_price, low_price):
                logger.warning(f"{symbol}: 最高价异常 - {kline}")
                return None

            if low_price > min(open_price, close_price, high_price):
                logger.warning(f"{symbol}: 最低价异常 - {kline}")
                return None

            if volume < 0:
                logger.warning(f"{symbol}: 成交量为负 - {kline}")
                return None

            return kline

        except (ValueError, IndexError) as e:
            logger.warning(f"{symbol}: 数据格式错误 - {kline}, {e}")
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
