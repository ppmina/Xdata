"""市场指标数据下载器。

专门处理资金费率、持仓量、多空比例等市场指标数据的下载。
"""

import logging
import time
from datetime import datetime, timedelta
from typing import List, Optional

from cryptoservice.models import FundingRate, OpenInterest, LongShortRatio, Freq
from cryptoservice.exceptions import MarketDataFetchError
from cryptoservice.storage import AsyncMarketDB
from .base_downloader import BaseDownloader

logger = logging.getLogger(__name__)


class MetricsDownloader(BaseDownloader):
    """市场指标数据下载器"""

    def __init__(self, client, request_delay: float = 0.5):
        super().__init__(client, request_delay)
        self.db: Optional[AsyncMarketDB] = None

    async def download_funding_rate_batch(
        self,
        symbols: List[str],
        start_time: str,
        end_time: str,
        db_path: str,
        request_delay: float = 0.5,
    ) -> None:
        """批量下载资金费率数据"""
        try:
            logger.info("💰 批量下载资金费率数据")

            if self.db is None:
                self.db = AsyncMarketDB(db_path)

            all_funding_rates = []
            downloaded_count = 0
            failed_count = 0

            for i, symbol in enumerate(symbols):
                try:
                    logger.debug(f"获取 {symbol} 资金费率 ({i + 1}/{len(symbols)})")

                    # 频率限制
                    if request_delay > 0:
                        time.sleep(request_delay)

                    funding_rates = self.download_funding_rate(
                        symbol=symbol,
                        start_time=start_time,
                        end_time=end_time,
                        limit=1000,
                    )

                    if funding_rates:
                        all_funding_rates.extend(funding_rates)
                        downloaded_count += 1
                        logger.debug(f"✅ {symbol}: {len(funding_rates)} 条记录")
                    else:
                        logger.debug(f"⚠️ {symbol}: 无数据")

                except Exception as e:
                    failed_count += 1
                    logger.warning(f"❌ {symbol}: {e}")
                    self._record_failed_download(
                        symbol,
                        str(e),
                        {
                            "data_type": "funding_rate",
                            "start_time": start_time,
                            "end_time": end_time,
                        },
                    )

            # 批量存储
            if all_funding_rates and self.db:
                await self.db.store_funding_rate(all_funding_rates)
                logger.info(f"✅ 存储了 {len(all_funding_rates)} 条资金费率记录")

            logger.info(f"💰 资金费率数据下载完成: 成功 {downloaded_count}/{len(symbols)}，失败 {failed_count}")

        except Exception as e:
            logger.error(f"批量下载资金费率失败: {e}")
            raise MarketDataFetchError(f"批量下载资金费率失败: {e}") from e

    async def download_open_interest_batch(
        self,
        symbols: List[str],
        start_time: str,
        end_time: str,
        db_path: str,
        interval: Freq = Freq.m5,
        request_delay: float = 0.5,
    ) -> None:
        """批量下载持仓量数据"""
        try:
            logger.info("📊 批量下载持仓量数据")

            if self.db is None:
                self.db = AsyncMarketDB(db_path)

            all_open_interests = []
            downloaded_count = 0
            failed_count = 0

            for i, symbol in enumerate(symbols):
                try:
                    logger.debug(f"获取 {symbol} 持仓量 ({i + 1}/{len(symbols)})")

                    # 频率限制
                    if request_delay > 0:
                        time.sleep(request_delay)

                    open_interests = self.download_open_interest(
                        symbol=symbol,
                        period=interval.value,
                        start_time=start_time,
                        end_time=end_time,
                        limit=500,
                    )

                    if open_interests:
                        all_open_interests.extend(open_interests)
                        downloaded_count += 1
                        logger.debug(f"✅ {symbol}: {len(open_interests)} 条记录")
                    else:
                        logger.debug(f"⚠️ {symbol}: 无数据")

                except Exception as e:
                    failed_count += 1
                    logger.warning(f"❌ {symbol}: {e}")
                    self._record_failed_download(
                        symbol,
                        str(e),
                        {
                            "data_type": "open_interest",
                            "start_time": start_time,
                            "end_time": end_time,
                        },
                    )

            # 批量存储
            if all_open_interests and self.db:
                await self.db.store_open_interest(all_open_interests)
                logger.info(f"✅ 存储了 {len(all_open_interests)} 条持仓量记录")

            logger.info(f"📊 持仓量数据下载完成: 成功 {downloaded_count}/{len(symbols)}，失败 {failed_count}")

        except Exception as e:
            logger.error(f"批量下载持仓量失败: {e}")
            raise MarketDataFetchError(f"批量下载持仓量失败: {e}") from e

    async def download_long_short_ratio_batch(
        self,
        symbols: List[str],
        start_time: str,
        end_time: str,
        db_path: str,
        period: str = "5m",
        ratio_type: str = "account",
        request_delay: float = 0.5,
    ) -> None:
        """批量下载多空比例数据"""
        try:
            logger.info(f"📊 批量下载多空比例数据 (类型: {ratio_type})")

            if self.db is None:
                self.db = AsyncMarketDB(db_path)

            # 检查30天限制
            current_time = datetime.now()
            thirty_days_ago = current_time - timedelta(days=30)

            # 解析时间字符串
            try:
                start_dt = datetime.fromisoformat(
                    start_time.replace("Z", "+00:00") if start_time.endswith("Z") else start_time
                )
            except ValueError:
                start_dt = datetime.fromisoformat(start_time)

            # 调整时间范围以符合30天限制
            if start_dt < thirty_days_ago:
                logger.warning("⚠️ 开始时间超出30天限制，调整为最近30天")
                start_time = thirty_days_ago.strftime("%Y-%m-%d")

            all_long_short_ratios = []
            downloaded_count = 0
            failed_count = 0

            for i, symbol in enumerate(symbols):
                try:
                    logger.debug(f"获取 {symbol} 多空比例 ({i + 1}/{len(symbols)})")

                    # 频率限制
                    if request_delay > 0:
                        time.sleep(request_delay)

                    long_short_ratios = self.download_long_short_ratio(
                        symbol=symbol,
                        period=period,
                        ratio_type=ratio_type,
                        start_time=start_time,
                        end_time=end_time,
                        limit=500,
                    )

                    if long_short_ratios:
                        all_long_short_ratios.extend(long_short_ratios)
                        downloaded_count += 1
                        logger.debug(f"✅ {symbol}: {len(long_short_ratios)} 条记录")
                    else:
                        logger.debug(f"⚠️ {symbol}: 无数据")

                except Exception as e:
                    failed_count += 1
                    logger.warning(f"❌ {symbol}: {e}")
                    self._record_failed_download(
                        symbol,
                        str(e),
                        {
                            "data_type": "long_short_ratio",
                            "ratio_type": ratio_type,
                            "start_time": start_time,
                            "end_time": end_time,
                        },
                    )

            # 批量存储
            if all_long_short_ratios and self.db:
                await self.db.store_long_short_ratio(all_long_short_ratios)
                logger.info(f"✅ 存储了 {len(all_long_short_ratios)} 条多空比例记录")

            logger.info(f"📊 多空比例数据下载完成: 成功 {downloaded_count}/{len(symbols)}，失败 {failed_count}")

        except Exception as e:
            logger.error(f"批量下载多空比例失败: {e}")
            raise MarketDataFetchError(f"批量下载多空比例失败: {e}") from e

    def download_funding_rate(
        self,
        symbol: str,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 100,
    ) -> List[FundingRate]:
        """下载单个交易对的资金费率数据"""
        try:

            def request_func():
                params = {"symbol": symbol, "limit": limit}
                if start_time:
                    params["startTime"] = self._date_to_timestamp_start(start_time)
                if end_time:
                    params["endTime"] = self._date_to_timestamp_end(end_time)
                return self.client.futures_funding_rate(**params)

            data = self._handle_request_with_retry(request_func)

            if not data:
                return []

            return [FundingRate.from_binance_response(item) for item in data]

        except Exception as e:
            logger.error(f"获取资金费率失败 {symbol}: {e}")
            raise MarketDataFetchError(f"获取资金费率失败: {e}") from e

    def download_open_interest(
        self,
        symbol: str,
        period: str = "5m",
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 500,
    ) -> List[OpenInterest]:
        """下载单个交易对的持仓量数据"""
        try:

            def request_func():
                params = {"symbol": symbol, "period": period, "limit": min(limit, 500)}
                if start_time:
                    params["startTime"] = self._date_to_timestamp_start(start_time)
                if end_time:
                    params["endTime"] = self._date_to_timestamp_end(end_time)
                return self.client.futures_open_interest_hist(**params)

            data = self._handle_request_with_retry(request_func)

            if not data:
                return []

            return [OpenInterest.from_binance_response(item) for item in data]

        except Exception as e:
            logger.error(f"获取持仓量失败 {symbol}: {e}")
            raise MarketDataFetchError(f"获取持仓量失败: {e}") from e

    def download_long_short_ratio(
        self,
        symbol: str,
        period: str = "5m",
        ratio_type: str = "account",
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 500,
    ) -> List[LongShortRatio]:
        """下载单个交易对的多空比例数据"""
        try:

            def request_func():
                params = {"symbol": symbol, "period": period, "limit": min(limit, 500)}
                if start_time:
                    params["startTime"] = self._date_to_timestamp_start(start_time)
                if end_time:
                    params["endTime"] = self._date_to_timestamp_end(end_time)

                # 根据ratio_type选择API端点
                if ratio_type == "account":
                    return self.client.futures_top_longshort_account_ratio(**params)
                elif ratio_type == "position":
                    return self.client.futures_top_longshort_position_ratio(**params)
                elif ratio_type == "global":
                    return self.client.futures_global_longshort_ratio(**params)
                elif ratio_type == "taker":
                    return self.client.futures_taker_longshort_ratio(**params)
                else:
                    raise ValueError(f"不支持的ratio_type: {ratio_type}")

            data = self._handle_request_with_retry(request_func)

            if not data:
                return []

            return [LongShortRatio.from_binance_response(item, ratio_type) for item in data]

        except Exception as e:
            logger.error(f"获取多空比例失败 {symbol}: {e}")
            raise MarketDataFetchError(f"获取多空比例失败: {e}") from e

    def _date_to_timestamp_start(self, date: str) -> str:
        """将日期字符串转换为当天开始的时间戳"""
        timestamp = int(datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return str(timestamp)

    def _date_to_timestamp_end(self, date: str) -> str:
        """将日期字符串转换为当天结束的时间戳"""
        timestamp = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return str(timestamp)

    def download(self, *args, **kwargs):
        """实现基类的抽象方法"""
        # 这里可以根据参数决定调用哪个具体的下载方法
        if "funding_rate" in kwargs:
            return self.download_funding_rate_batch(*args, **kwargs)
        elif "open_interest" in kwargs:
            return self.download_open_interest_batch(*args, **kwargs)
        elif "long_short_ratio" in kwargs:
            return self.download_long_short_ratio_batch(*args, **kwargs)
        else:
            raise ValueError("请指定要下载的数据类型")
