"""K线数据下载器.

专门处理K线数据的下载，包括现货和期货K线数据。
"""

import asyncio
import logging
from collections.abc import AsyncGenerator
from pathlib import Path

from binance import AsyncClient

from cryptoservice.config import RetryConfig
from cryptoservice.exceptions import InvalidSymbolError, MarketDataFetchError
from cryptoservice.models import (
    Freq,
    HistoricalKlinesType,
    IntegrityReport,
    PerpetualMarketTicker,
)
from cryptoservice.storage.database import Database as AsyncMarketDB

from .base_downloader import BaseDownloader

logger = logging.getLogger(__name__)


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
            logger.debug(f"下载 {symbol} 的K线数据: {start_ts} - {end_ts}")

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
                logger.debug(f"交易对 {symbol} 在指定时间段内无数据")
                return

            # 数据质量检查和转换
            processed_count = 0
            async for kline in klines_generator:
                validated_kline = self._validate_single_kline(kline, symbol)
                if validated_kline:
                    yield PerpetualMarketTicker.from_binance_kline(symbol=symbol, kline=validated_kline)
                    processed_count += 1

            logger.debug(f"成功处理 {symbol}: {processed_count} 条记录")

        except InvalidSymbolError:
            logger.warning(f"⚠️ 无效交易对: {symbol}")
            raise
        except Exception as e:
            logger.error(f"❌ 下载 {symbol} 失败: {e}")
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
    ) -> IntegrityReport:
        """批量异步下载多个交易对的K线数据."""
        # 初始化数据库
        if self.db is None:
            self.db = AsyncMarketDB(str(db_path))
        await self.db.initialize()

        # 如果启用增量下载模式，生成下载计划
        if incremental:
            logger.info("🔄 增量下载模式已启用，分析现有数据...")
            missing_plan = await self.db.plan_kline_download(
                symbols=symbols,
                start_date=start_time,
                end_date=end_time,
                freq=interval,
            )

            # 过滤出需要下载的交易对
            symbols_to_download = list(missing_plan.keys())
            if not symbols_to_download:
                logger.info("✅ 所有数据已存在，无需下载")
                return IntegrityReport(
                    total_symbols=len(symbols),
                    successful_symbols=len(symbols),
                    failed_symbols=[],
                    missing_periods=[],
                    data_quality_score=1.0,
                    recommendations=["所有数据已是最新状态"],
                )
            else:
                logger.info(f"📋 需要下载数据的交易对: {len(symbols_to_download)}/{len(symbols)}")
                # 使用需要下载的交易对列表替换原始列表
                symbols = symbols_to_download

        # 转换时间格式
        start_ts = self._date_to_timestamp_start(start_time)
        end_ts = self._date_to_timestamp_end(end_time)

        successful_symbols: list[str] = []
        failed_symbols: list[str] = []
        missing_periods: list[dict] = []
        semaphore = asyncio.Semaphore(max_workers)

        logger.info(f"🚀 开始批量下载 {len(symbols)} 个交易对的K线数据 (并发数: {max_workers})")

        # 创建并执行所有任务
        tasks = [
            self._process_symbol(
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                interval=interval,
                start_time=start_time,
                end_time=end_time,
                retry_config=retry_config,
                semaphore=semaphore,
                successful_symbols=successful_symbols,
                failed_symbols=failed_symbols,
                missing_periods=missing_periods,
            )
            for symbol in symbols
        ]
        await asyncio.gather(*tasks)

        # 生成报告
        logger.info(f"📊 下载完成: 成功 {len(successful_symbols)}/{len(symbols)}")

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
        start_ts: str,
        end_ts: str,
        interval: Freq,
        start_time: str,
        end_time: str,
        retry_config: RetryConfig | None,
        semaphore: asyncio.Semaphore,
        successful_symbols: list[str],
        failed_symbols: list[str],
        missing_periods: list[dict],
    ) -> None:
        """下载并存储单个交易对数据，并更新结果列表."""
        async with semaphore:
            try:
                data_generator = self.download_single_symbol(
                    symbol=symbol,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    interval=interval,
                    retry_config=retry_config,
                )

                chunk: list[PerpetualMarketTicker] = []
                processed_count = 0
                async for item in data_generator:
                    chunk.append(item)
                    if len(chunk) >= 1000:  # 每1000条数据存一次
                        if self.db:
                            await self.db.insert_klines(chunk, interval)
                        processed_count += len(chunk)
                        chunk = []

                if chunk and self.db:  # 存储剩余的数据
                    await self.db.insert_klines(chunk, interval)
                    processed_count += len(chunk)

                if processed_count > 0:
                    successful_symbols.append(symbol)
                    logger.debug(f"✅ {symbol}: {processed_count} 条记录")
                else:
                    logger.debug(f"⚠️ {symbol}: 无数据")
                    missing_periods.append(
                        {
                            "symbol": symbol,
                            "period": f"{start_time} - {end_time}",
                            "reason": "no_data",
                        }
                    )

            except Exception as e:
                logger.error(f"❌ {symbol} 失败: {e}")
                failed_symbols.append(symbol)
                missing_periods.append(
                    {
                        "symbol": symbol,
                        "period": f"{start_time} - {end_time}",
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
                logger.warning(f"⚠️ {symbol} 数据质量问题: {issue_count}/{total_count} 条记录异常")

        return valid_data

    def _date_to_timestamp_start(self, date: str) -> str:
        """将日期字符串转换为当天开始的时间戳（UTC）."""
        from cryptoservice.utils import date_to_timestamp_start

        return str(date_to_timestamp_start(date))

    def _date_to_timestamp_end(self, date: str) -> str:
        """将日期字符串转换为次日开始的时间戳（UTC）."""
        from cryptoservice.utils import date_to_timestamp_end

        return str(date_to_timestamp_end(date))

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
