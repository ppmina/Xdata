"""Binance Vision数据下载器。

专门处理从Binance Vision S3存储下载历史数据。
"""

import logging
import time
import requests
import zipfile
import csv
from datetime import datetime
from typing import List, Optional, Dict
from io import BytesIO
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from cryptoservice.models import OpenInterest, LongShortRatio
from cryptoservice.exceptions import MarketDataFetchError
from cryptoservice.config import RetryConfig
from cryptoservice.storage import AsyncMarketDB
from .base_downloader import BaseDownloader

logger = logging.getLogger(__name__)


class VisionDownloader(BaseDownloader):
    """Binance Vision数据下载器"""

    def __init__(self, client, request_delay: float = 1.0):
        super().__init__(client, request_delay)
        self.db: Optional[AsyncMarketDB] = None
        self.base_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/data/futures/um/daily/metrics"

    async def download_metrics_batch(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        db_path: str,
        data_types: List[str] | None = None,
        request_delay: float = 1.0,
    ) -> None:
        """批量下载指标数据"""
        if data_types is None:
            data_types = ["openInterest", "longShortRatio"]

        try:
            logger.info(f"开始从 Binance Vision 下载指标数据: {data_types}")

            if self.db is None:
                self.db = AsyncMarketDB(db_path)

            # 创建日期范围
            import pandas as pd

            date_range = pd.date_range(start=start_date, end=end_date, freq="D")

            for date in date_range:
                date_str = date.strftime("%Y-%m-%d")
                logger.info(f"处理日期: {date_str}")

                # 下载指标数据
                await self._download_metrics_for_date(symbols, date_str, request_delay)

                # 请求延迟
                if request_delay > 0:
                    time.sleep(request_delay)

            logger.info("✅ Binance Vision 指标数据下载完成")

        except Exception as e:
            logger.error(f"从 Binance Vision 下载指标数据失败: {e}")
            raise MarketDataFetchError(f"从 Binance Vision 下载指标数据失败: {e}") from e

    async def _download_metrics_for_date(
        self,
        symbols: List[str],
        date: str,
        request_delay: float = 1.0,
    ) -> None:
        """下载指定日期的指标数据"""
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            date_str = date_obj.strftime("%Y-%m-%d")

            for symbol in symbols:
                try:
                    # 构建URL
                    url = f"{self.base_url}/{symbol}/{symbol}-metrics-{date_str}.zip"
                    logger.debug(f"下载 {symbol} 指标数据: {url}")

                    # 下载并解析数据
                    retry_config = RetryConfig(max_retries=3, base_delay=2.0)
                    metrics_data = self._download_and_parse_metrics_csv(url, symbol, retry_config)

                    if metrics_data and self.db:
                        # 存储持仓量数据
                        if metrics_data.get("open_interest"):
                            await self.db.store_open_interest(metrics_data["open_interest"])
                            logger.info(
                                f"✅ {symbol}: 存储了 {date_str} {len(metrics_data['open_interest'])} 条持仓量记录"
                            )

                        # 存储多空比例数据
                        if metrics_data.get("long_short_ratio"):
                            await self.db.store_long_short_ratio(metrics_data["long_short_ratio"])
                            logger.info(
                                f"✅ {symbol}: 存储了 {date_str} {len(metrics_data['long_short_ratio'])} 条多空比例记录"
                            )
                    else:
                        logger.warning(f"⚠️ {symbol}: 无法获取指标数据")

                except Exception as e:
                    logger.warning(f"下载 {symbol} 指标数据失败: {e}")
                    self._record_failed_download(
                        symbol,
                        str(e),
                        {
                            "url": url,
                            "date": date_str,
                            "data_type": "metrics",
                        },
                    )

                # 请求延迟
                if request_delay > 0:
                    time.sleep(request_delay)

        except Exception as e:
            logger.error(f"从 Binance Vision 下载指标数据失败: {e}")
            raise

    def _download_and_parse_metrics_csv(
        self,
        url: str,
        symbol: str,
        retry_config: Optional[RetryConfig] = None,
    ) -> Dict[str, List] | None:
        """下载并解析指标CSV数据"""
        if retry_config is None:
            retry_config = RetryConfig(max_retries=3, base_delay=2.0)

        try:
            # 使用增强的会话下载ZIP文件
            session = self._create_enhanced_session()

            def request_func():
                response = session.get(url, timeout=30)
                response.raise_for_status()
                return response.content

            zip_content = self._handle_request_with_retry(request_func, retry_config=retry_config)

            # 解压ZIP文件
            with zipfile.ZipFile(BytesIO(zip_content)) as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith(".csv")]

                if not csv_files:
                    logger.warning(f"ZIP文件中没有找到CSV文件: {url}")
                    return None

                result: dict[str, list] = {"open_interest": [], "long_short_ratio": []}

                # 处理每个CSV文件
                for csv_file in csv_files:
                    try:
                        with zip_file.open(csv_file) as f:
                            content = f.read().decode("utf-8")

                        # 解析CSV内容
                        csv_reader = csv.DictReader(content.splitlines())
                        rows = list(csv_reader)

                        if not rows:
                            logger.warning(f"CSV文件 {csv_file} 为空")
                            continue

                        # 检查数据结构
                        first_row = rows[0]

                        # 解析持仓量数据
                        if "sum_open_interest" in first_row:
                            oi_data = self._parse_oi_data(rows, symbol)
                            result["open_interest"].extend(oi_data)

                        # 解析多空比例数据
                        if any(
                            field in first_row
                            for field in [
                                "sum_toptrader_long_short_ratio",
                                "count_long_short_ratio",
                                "sum_taker_long_short_vol_ratio",
                            ]
                        ):
                            lsr_data = self._parse_lsr_data(rows, symbol, csv_file)
                            result["long_short_ratio"].extend(lsr_data)

                    except Exception as e:
                        logger.warning(f"解析CSV文件 {csv_file} 时出错: {e}")
                        continue

                return result if result["open_interest"] or result["long_short_ratio"] else None

        except Exception as e:
            logger.error(f"下载和解析指标数据失败 {symbol}: {e}")
            return None

    def _parse_oi_data(self, raw_data: List[Dict], symbol: str) -> List[OpenInterest]:
        """解析持仓量数据"""
        open_interests = []

        for row in raw_data:
            try:
                # 解析时间字段
                create_time = row["create_time"]
                timestamp = int(datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

                # 创建OpenInterest对象
                from decimal import Decimal

                open_interest = OpenInterest(
                    symbol=symbol,
                    open_interest=Decimal(str(row["sum_open_interest"])),
                    time=timestamp,
                    open_interest_value=(
                        Decimal(str(row["sum_open_interest_value"])) if row.get("sum_open_interest_value") else None
                    ),
                )
                open_interests.append(open_interest)

            except (ValueError, KeyError) as e:
                logger.warning(f"解析持仓量数据行时出错: {e}, 行数据: {row}")
                continue

        return open_interests

    def _parse_lsr_data(self, raw_data: List[Dict], symbol: str, file_name: str) -> List[LongShortRatio]:
        """解析多空比例数据"""
        long_short_ratios = []

        for row in raw_data:
            try:
                # 解析时间字段
                create_time = row["create_time"]
                timestamp = int(datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

                from decimal import Decimal

                # 处理顶级交易者数据
                if "sum_toptrader_long_short_ratio" in row:
                    ratio_value = Decimal(str(row["sum_toptrader_long_short_ratio"]))

                    # 计算平均比例
                    if "count_toptrader_long_short_ratio" in row:
                        count = Decimal(str(row["count_toptrader_long_short_ratio"]))
                        if count > 0:
                            ratio_value = ratio_value / count

                    # 计算多空账户比例
                    if ratio_value > 0:
                        total = ratio_value + 1
                        long_account = ratio_value / total
                        short_account = Decimal("1") / total
                    else:
                        long_account = Decimal("0.5")
                        short_account = Decimal("0.5")

                    long_short_ratios.append(
                        LongShortRatio(
                            symbol=symbol,
                            long_short_ratio=ratio_value,
                            long_account=long_account,
                            short_account=short_account,
                            timestamp=timestamp,
                            ratio_type="account",
                        )
                    )

                # 处理Taker数据
                if "sum_taker_long_short_vol_ratio" in row:
                    taker_ratio = Decimal(str(row["sum_taker_long_short_vol_ratio"]))

                    if taker_ratio > 0:
                        total = taker_ratio + 1
                        long_vol = taker_ratio / total
                        short_vol = Decimal("1") / total
                    else:
                        long_vol = Decimal("0.5")
                        short_vol = Decimal("0.5")

                    long_short_ratios.append(
                        LongShortRatio(
                            symbol=symbol,
                            long_short_ratio=taker_ratio,
                            long_account=long_vol,
                            short_account=short_vol,
                            timestamp=timestamp,
                            ratio_type="taker",
                        )
                    )

            except (ValueError, KeyError) as e:
                logger.warning(f"解析多空比例数据行时出错: {e}, 行数据: {row}")
                continue

        return long_short_ratios

    def _create_enhanced_session(self) -> requests.Session:
        """创建增强的网络请求会话"""
        session = requests.Session()

        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
            pool_block=False,
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # 设置默认头部
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }
        )

        return session

    def download(self, *args, **kwargs):
        """实现基类的抽象方法"""
        return self.download_metrics_batch(*args, **kwargs)
