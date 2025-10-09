"""Binance Vision数据下载器.

专门处理从Binance Vision S3存储下载历史数据。
"""

import asyncio
import csv
import logging
import time
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
        self._client_timeout = ClientTimeout(total=60, connect=10)

        # 性能统计
        self._perf_stats = {
            "download_time": 0.0,
            "parse_time": 0.0,
            "db_time": 0.0,
            "download_count": 0,
            "concurrent_count": 0,
            "max_concurrent": 0,
        }
        self._concurrent_lock = asyncio.Lock()

    async def download_metrics_batch(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        db_path: str,
        max_workers: int,
        request_delay: float,
    ) -> None:
        """批量异步下载指标数据.

        Args:
            symbols: 交易对列表
            start_date: 起始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            db_path: 数据库路径
            request_delay: 请求之间的延迟（秒），0表示无延迟
            max_workers: 最大并发下载数（Vision S3下载可以高并发）
        """
        try:
            data_types = ["openInterest", "longShortRatio"]
            # 重置统计
            self._perf_stats = {
                "download_time": 0.0,
                "parse_time": 0.0,
                "db_time": 0.0,
                "download_count": 0,
                "concurrent_count": 0,
                "max_concurrent": 0,
            }

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

            total_tasks = len(tasks)
            logger.info(f"📦 创建了 {total_tasks} 个下载任务，最大并发数: {max_workers}")

            start_time = time.time()
            await asyncio.gather(*tasks)

            elapsed = time.time() - start_time
            logger.info("✅ Binance Vision 指标数据下载完成")
            logger.info("📊 性能统计:")
            logger.info(f"   - 总耗时: {elapsed:.2f}秒")
            logger.info(f"   - 下载任务: {self._perf_stats['download_count']}")
            logger.info(f"   - 最大并发数: {self._perf_stats['max_concurrent']}")

            dl_time = self._perf_stats["download_time"]
            dl_pct = dl_time / elapsed * 100 if elapsed > 0 else 0
            logger.info(f"   - 下载时间: {dl_time:.2f}秒 ({dl_pct:.1f}%)")

            parse_time = self._perf_stats["parse_time"]
            parse_pct = parse_time / elapsed * 100 if elapsed > 0 else 0
            logger.info(f"   - 解析时间: {parse_time:.2f}秒 ({parse_pct:.1f}%)")

            db_time = self._perf_stats["db_time"]
            db_pct = db_time / elapsed * 100 if elapsed > 0 else 0
            logger.info(f"   - 数据库时间: {db_time:.2f}秒 ({db_pct:.1f}%)")

            if self._perf_stats["download_count"] > 0:
                avg_per_task = elapsed / self._perf_stats["download_count"]
                logger.info(f"   - 平均每任务: {avg_per_task:.3f}秒")

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
            # 记录并发数
            async with self._concurrent_lock:
                self._perf_stats["concurrent_count"] += 1
                current = self._perf_stats["concurrent_count"]
                if current > self._perf_stats["max_concurrent"]:
                    self._perf_stats["max_concurrent"] = current
                    if current % 10 == 0:  # 每10个并发打印一次
                        logger.info(f"🚀 当前并发: {current}")

            try:
                url = f"{self.base_url}/{symbol}/{symbol}-metrics-{date_str}.zip"
                logger.debug(f"⬇️  下载 {symbol} {date_str} 指标数据")

                retry_config = RetryConfig(max_retries=3, base_delay=0)

                # 计时：下载
                dl_start = time.time()
                metrics_data = await self._download_and_parse_metrics_csv(url, symbol, retry_config)
                self._perf_stats["download_time"] += time.time() - dl_start

                if metrics_data and self.db:
                    # 计时：数据库插入
                    db_start = time.time()

                    if metrics_data.get("open_interest"):
                        await self.db.insert_open_interests(metrics_data["open_interest"])
                        logger.info(f"✅ {symbol}: 存储了 {date_str} {len(metrics_data['open_interest'])} 条持仓量记录")
                    if metrics_data.get("long_short_ratio"):
                        await self.db.insert_long_short_ratios(metrics_data["long_short_ratio"])
                        logger.info(
                            f"✅ {symbol}: 存储了 {date_str} {len(metrics_data['long_short_ratio'])} 条多空比例记录"
                        )

                    self._perf_stats["db_time"] += time.time() - db_start
                else:
                    logger.warning(f"⚠️ {symbol} on {date_str}: 无法获取指标数据")

                self._perf_stats["download_count"] += 1

            except Exception as e:
                logger.warning(f"下载 {symbol} on {date_str} 指标数据失败: {e}")
                self._record_failed_download(symbol, str(e), {"url": url, "date": date_str, "data_type": "metrics"})

            finally:
                # 减少并发计数
                async with self._concurrent_lock:
                    self._perf_stats["concurrent_count"] -= 1

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
            retry_config = RetryConfig(max_retries=3, base_delay=0)

        # 使用基类的重试机制下载ZIP文件
        async def _download_zip() -> bytes:
            """下载ZIP文件的内部异步函数."""
            session = await self._get_session()
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.read()
            except ClientConnectionError:
                await self._reset_session()
                raise

        try:
            # 使用基类的异步重试处理机制
            zip_content = await self._handle_async_request_with_retry(
                _download_zip,
                retry_config=retry_config,
                symbol=symbol,
            )
        except Exception as e:
            logger.error(f"下载指标数据失败 {symbol}: {e}")
            return None

        try:
            # 计时：解析
            parse_start = time.time()
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

                # 记录解析时间
                self._perf_stats["parse_time"] += time.time() - parse_start

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
                    limit=200,  # 增加TCP连接池以支持更高并发
                    limit_per_host=100,  # 每个主机的连接数限制
                    ttl_dns_cache=300,
                    enable_cleanup_closed=True,
                    force_close=True,  # 强制关闭连接，避免SSL超时（与keepalive互斥）
                )
                self._session = aiohttp.ClientSession(
                    timeout=self._client_timeout,
                    connector=connector,
                    connector_owner=True,
                    trust_env=True,
                )

        return self._session

    async def _close_session(self) -> None:
        """关闭当前的aiohttp会话."""
        session = self._session
        self._session = None

        if session and not session.closed:
            try:
                # 使用较短的超时关闭会话，避免长时间等待
                await asyncio.wait_for(session.close(), timeout=5.0)
            except TimeoutError:
                logger.debug("关闭aiohttp会话超时，强制关闭")
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
                # 解析时间字段（Binance API 返回的是 UTC 时间）
                create_time = row["create_time"]
                from datetime import UTC

                timestamp = int(
                    datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC).timestamp() * 1000
                )

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
                # 解析时间字段（Binance API 返回的是 UTC 时间）
                create_time = row["create_time"]
                from datetime import UTC

                timestamp = int(
                    datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC).timestamp() * 1000
                )

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
