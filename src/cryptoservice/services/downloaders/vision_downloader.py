"""Binance Vision数据下载器.

专门处理从Binance Vision S3存储下载历史数据。
"""

import asyncio
import csv
import logging
import zipfile
from datetime import datetime
from decimal import Decimal
from io import BytesIO

import aiohttp
from aiohttp import ClientConnectionError, ClientTimeout
from binance import AsyncClient

from cryptoservice.config import RetryConfig
from cryptoservice.exceptions import MarketDataFetchError
from cryptoservice.models import LongShortRatio, OpenInterest
from cryptoservice.storage.database import Database as AsyncMarketDB

from .base_downloader import BaseDownloader

logger = logging.getLogger(__name__)


class VisionDownloader(BaseDownloader):
    """Binance Vision数据下载器."""

    def __init__(self, client: AsyncClient, request_delay: float = 1.0):
        """初始化Binance Vision数据下载器.

        Args:
            client: API 客户端实例.
            request_delay: 请求之间的基础延迟（秒）.
        """
        super().__init__(client, request_delay)
        self.db: AsyncMarketDB | None = None
        self.base_url = "https://data.binance.vision/data/futures/um/daily/metrics"
        self._session: aiohttp.ClientSession | None = None
        self._session_lock: asyncio.Lock | None = None
        self._client_timeout = ClientTimeout(total=30)

    async def download_metrics_batch(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        db_path: str,
        data_types: list[str] | None = None,
        request_delay: float = 1.0,
        max_workers: int = 5,
    ) -> None:
        """批量异步下载指标数据."""
        if data_types is None:
            data_types = ["openInterest", "longShortRatio"]

        try:
            logger.info(f"开始从 Binance Vision 下载指标数据: {data_types}")

            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            import pandas as pd

            date_range = pd.date_range(start=start_date, end=end_date, freq="D")

            semaphore = asyncio.Semaphore(max_workers)
            tasks = []

            for date in date_range:
                date_str = date.strftime("%Y-%m-%d")
                for symbol in symbols:
                    task = asyncio.create_task(
                        self._download_and_process_symbol_for_date(symbol, date_str, semaphore, request_delay)
                    )
                    tasks.append(task)

            await asyncio.gather(*tasks)
            logger.info("✅ Binance Vision 指标数据下载完成")

        except Exception as e:
            logger.error(f"从 Binance Vision 下载指标数据失败: {e}")
            raise MarketDataFetchError(f"从 Binance Vision 下载指标数据失败: {e}") from e
        finally:
            await self._close_session()

    async def _download_and_process_symbol_for_date(
        self,
        symbol: str,
        date_str: str,
        semaphore: asyncio.Semaphore,
        request_delay: float,
    ) -> None:
        """下载并处理单个交易对在特定日期的数据."""
        async with semaphore:
            try:
                url = f"{self.base_url}/{symbol}/{symbol}-metrics-{date_str}.zip"
                logger.debug(f"下载 {symbol} 指标数据: {url}")

                retry_config = RetryConfig(max_retries=3, base_delay=2.0)
                metrics_data = await self._download_and_parse_metrics_csv(url, symbol, retry_config)

                if metrics_data and self.db:
                    if metrics_data.get("open_interest"):
                        await self.db.insert_open_interests(metrics_data["open_interest"])
                        logger.info(f"✅ {symbol}: 存储了 {date_str} {len(metrics_data['open_interest'])} 条持仓量记录")
                    if metrics_data.get("long_short_ratio"):
                        await self.db.insert_long_short_ratios(metrics_data["long_short_ratio"])
                        logger.info(
                            f"✅ {symbol}: 存储了 {date_str} {len(metrics_data['long_short_ratio'])} 条多空比例记录"
                        )
                else:
                    logger.warning(f"⚠️ {symbol} on {date_str}: 无法获取指标数据")

            except Exception as e:
                logger.warning(f"下载 {symbol} on {date_str} 指标数据失败: {e}")
                self._record_failed_download(symbol, str(e), {"url": url, "date": date_str, "data_type": "metrics"})

            finally:
                if request_delay > 0:
                    await asyncio.sleep(request_delay)

    async def _download_and_parse_metrics_csv(  # noqa: C901
        self,
        url: str,
        symbol: str,
        retry_config: RetryConfig | None = None,
    ) -> dict[str, list] | None:
        """使用aiohttp下载并解析指标CSV数据."""
        if retry_config is None:
            retry_config = RetryConfig(max_retries=3, base_delay=2.0)

        async def request_func():
            session = await self._get_session()
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.read()
            except ClientConnectionError:
                await self._reset_session()
                raise

        try:
            zip_content = await self._handle_async_request_with_retry(request_func, retry_config=retry_config)

            with zipfile.ZipFile(BytesIO(zip_content)) as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith(".csv")]

                if not csv_files:
                    logger.warning(f"ZIP文件中没有找到CSV文件: {url}")
                    return None

                result: dict[str, list] = {"open_interest": [], "long_short_ratio": []}

                for csv_file in csv_files:
                    try:
                        with zip_file.open(csv_file) as f:
                            content = f.read().decode("utf-8")
                        csv_reader = csv.DictReader(content.splitlines())
                        rows = list(csv_reader)
                        if not rows:
                            continue

                        first_row = rows[0]
                        if "sum_open_interest" in first_row:
                            result["open_interest"].extend(self._parse_oi_data(rows, symbol))
                        if any(
                            field in first_row
                            for field in [
                                "sum_toptrader_long_short_ratio",
                                "count_long_short_ratio",
                                "sum_taker_long_short_vol_ratio",
                            ]
                        ):
                            result["long_short_ratio"].extend(self._parse_lsr_data(rows, symbol, csv_file))
                    except Exception as e:
                        logger.warning(f"解析CSV文件 {csv_file} 时出错: {e}")
                        continue
                return result if result["open_interest"] or result["long_short_ratio"] else None
        except Exception as e:
            logger.error(f"下载和解析指标数据失败 {symbol}: {e}")
            return None

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取复用的aiohttp会话实例."""
        if self._session_lock is None:
            self._session_lock = asyncio.Lock()

        async with self._session_lock:
            if self._session is None or self._session.closed:
                connector = aiohttp.TCPConnector(
                    limit=20,
                    ttl_dns_cache=300,
                    enable_cleanup_closed=True,
                    keepalive_timeout=30,
                )
                self._session = aiohttp.ClientSession(
                    timeout=self._client_timeout,
                    connector=connector,
                    trust_env=True,
                )

        return self._session

    async def _close_session(self) -> None:
        """关闭当前的aiohttp会话."""
        session = self._session
        self._session = None

        if session and not session.closed:
            try:
                await session.close()
            except ClientConnectionError as exc:
                logger.debug(f"关闭aiohttp会话时出现SSL问题: {exc}")
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"关闭aiohttp会话时出现异常: {exc}")

    async def _reset_session(self) -> None:
        """重置客户端会话以便下次重建."""
        await self._close_session()

    def _parse_oi_data(self, raw_data: list[dict], symbol: str) -> list[OpenInterest]:
        """解析持仓量数据."""
        open_interests = []

        for row in raw_data:
            try:
                # 解析时间字段
                create_time = row["create_time"]
                timestamp = int(datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

                # 安全获取持仓量值
                oi_value = self._safe_decimal_convert(row.get("sum_open_interest"))
                oi_value_usd = self._safe_decimal_convert(row.get("sum_open_interest_value"))

                # 只有当主要字段有效时才创建记录
                if oi_value is not None:
                    open_interest = OpenInterest(
                        symbol=symbol,
                        open_interest=oi_value,
                        time=timestamp,
                        open_interest_value=oi_value_usd,
                    )
                    open_interests.append(open_interest)

            except (ValueError, KeyError) as e:
                logger.warning(f"解析持仓量数据行时出错: {e}, 行数据: {row}")
                continue

        return open_interests

    def _parse_lsr_data(self, raw_data: list[dict], symbol: str, file_name: str) -> list[LongShortRatio]:  # noqa: C901
        """解析多空比例数据."""
        long_short_ratios = []

        for row in raw_data:
            try:
                # 解析时间字段
                create_time = row["create_time"]
                timestamp = int(datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

                # 处理顶级交易者数据 - 分别处理，确保无损
                try:
                    if "sum_toptrader_long_short_ratio" in row:
                        ratio_sum_str = row["sum_toptrader_long_short_ratio"]
                        count_str = row.get("count_toptrader_long_short_ratio", "")

                        # 安全转换数值，处理空值
                        ratio_sum = self._safe_decimal_convert(ratio_sum_str)
                        count = self._safe_decimal_convert(count_str)

                        if ratio_sum is not None:
                            # 计算平均比例
                            ratio_value = ratio_sum / count if count is not None and count > 0 else ratio_sum

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
                except Exception as e:
                    logger.debug(f"跳过顶级交易者数据处理 {symbol} at {create_time}: {e}")

                # 处理Taker数据 - 独立处理，确保无损
                try:
                    if "sum_taker_long_short_vol_ratio" in row:
                        taker_ratio_str = row["sum_taker_long_short_vol_ratio"]
                        taker_ratio = self._safe_decimal_convert(taker_ratio_str)

                        if taker_ratio is not None:
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
                except Exception as e:
                    logger.debug(f"跳过Taker数据处理 {symbol} at {create_time}: {e}")

            except (ValueError, KeyError) as e:
                logger.warning(f"解析多空比例数据行时出错: {e}, 行数据: {row}")
                continue

        return long_short_ratios

    def _safe_decimal_convert(self, value_str: str | None) -> Decimal | None:
        """安全转换字符串为Decimal，处理空值和无效值.

        Args:
            value_str: 要转换的字符串值

        Returns:
            转换后的Decimal值，如果无法转换则返回None
        """
        if not value_str or value_str.strip() == "":
            return None

        try:
            return Decimal(str(value_str).strip())
        except (ValueError, TypeError):
            return None

    def download(self, *args, **kwargs):
        """实现基类的抽象方法."""
        return self.download_metrics_batch(*args, **kwargs)
