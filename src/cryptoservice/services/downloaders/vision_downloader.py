"""Binance Vision数据下载器.

专门处理从Binance Vision S3存储下载历史数据。
"""

import asyncio
import csv
import time
import zipfile
from datetime import datetime
from decimal import Decimal
from io import BytesIO

import aiohttp
from aiohttp import ClientConnectionError, ClientTimeout
from binance import AsyncClient

from cryptoservice.config import RetryConfig
from cryptoservice.config.logging import get_logger
from cryptoservice.exceptions import MarketDataFetchError
from cryptoservice.models import LongShortRatio, OpenInterest
from cryptoservice.storage.database import Database as AsyncMarketDB
from cryptoservice.utils.time_utils import shift_date

from .base_downloader import BaseDownloader

logger = get_logger(__name__)


class VisionDownloader(BaseDownloader):
    """Binance Vision数据下载器."""

    def __init__(self, client: AsyncClient, request_delay: float = 0):
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

    async def download_metrics_batch(  # noqa: C901
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        db_path: str,
        max_workers: int,
        request_delay: float,
        incremental: bool = True,
    ) -> None:
        """批量异步下载指标数据.

        Args:
            symbols: 交易对列表
            start_date: 起始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            db_path: 数据库路径
            request_delay: 请求之间的延迟（秒），0表示无延迟
            max_workers: 最大并发下载数，决定TCP连接池大小
            incremental: 是否启用增量下载（默认True）
        """
        try:
            data_types = ["openInterest", "longShortRatio"]
            expanded_start_date = shift_date(start_date, -1)
            # 重置统计
            self._perf_stats = {
                "download_time": 0.0,
                "parse_time": 0.0,
                "db_time": 0.0,
                "download_count": 0,
                "concurrent_count": 0,
                "max_concurrent": 0,
            }

            logger.info(
                "准备下载 Binance Vision 指标数据：%s（扩展起点 %s）。",
                ", ".join(data_types),
                expanded_start_date,
            )

            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            import pandas as pd

            date_range = pd.date_range(start=expanded_start_date, end=end_date, freq="D")
            total_candidates = len(symbols) * len(date_range)

            if incremental:
                logger.debug("Vision 下载启用增量模式，开始分析缺失数据。")
                plan = await self.db.incremental.plan_vision_metrics_download(symbols, expanded_start_date, end_date)

                symbol_date_pairs = [(symbol, date_str) for symbol, details in plan.items() for date_str in details.get("missing_dates", [])]
                skipped_count = total_candidates - len(symbol_date_pairs)

                if not symbol_date_pairs:
                    logger.info("Vision 指标数据已是最新状态，跳过下载。")
                    return

                logger.debug(f"增量分析完成：{len(symbol_date_pairs)} 个任务待下载，跳过 {skipped_count} 个。")
                logger.info(f"Vision 增量计划：{len(symbol_date_pairs)} 个任务待下载（跳过 {skipped_count}）。")
            else:
                # 不启用增量下载，下载所有数据
                symbol_date_pairs = [(symbol, date.strftime("%Y-%m-%d")) for date in date_range for symbol in symbols]
                logger.info(f"Vision 下载计划：将处理 {len(symbol_date_pairs)} 个任务（范围 {start_date} ~ {end_date}，扩展起点 {expanded_start_date}）。")

            semaphore = asyncio.Semaphore(max_workers)
            tasks = []

            for symbol, date_str in symbol_date_pairs:
                task = asyncio.create_task(self._download_and_process_symbol_for_date(symbol, date_str, semaphore, request_delay, max_workers))
                tasks.append(task)

            total_tasks = len(tasks)
            logger.debug(f"已创建 {total_tasks} 个下载任务（最大并发 {max_workers}）。")

            start_time = time.time()
            await asyncio.gather(*tasks)

            elapsed = time.time() - start_time

            # 完整性检查
            total_expected = len(symbol_date_pairs)
            success_count = self._perf_stats["download_count"]
            failed_count = len(self.failed_downloads)
            success_rate = (success_count / total_expected * 100) if total_expected > 0 else 0

            dl_time = self._perf_stats["download_time"]
            dl_time / elapsed * 100 if elapsed > 0 else 0
            parse_time = self._perf_stats["parse_time"]
            parse_time / elapsed * 100 if elapsed > 0 else 0
            db_time = self._perf_stats["db_time"]
            db_time / elapsed * 100 if elapsed > 0 else 0
            avg_per_task = elapsed / success_count if success_count > 0 else 0

            logger.info(
                f"Vision 下载完成：成功 {success_count}/{total_expected}，失败 {failed_count} 个任务"
                f"（成功率 {success_rate:.1f}%，平均耗时 {avg_per_task * 1000:.0f} ms/任务）。"
            )

            if failed_count > 0:
                logger.warning("部分 Vision 指标下载失败，可调用 get_failed_downloads() 查看详情。")

        except Exception as e:
            logger.error(f"Vision 指标下载失败：{e}")
            raise MarketDataFetchError(f"从 Binance Vision 下载指标数据失败: {e}") from e
        finally:
            await self._close_session()

    async def _download_and_process_symbol_for_date(
        self,
        symbol: str,
        date_str: str,
        semaphore: asyncio.Semaphore,
        request_delay: float,
        max_workers: int,
    ) -> None:
        """下载并处理单个交易对在特定日期的数据."""
        async with semaphore:
            # 记录并发数
            self._perf_stats["concurrent_count"] += 1
            current = self._perf_stats["concurrent_count"]
            if current > self._perf_stats["max_concurrent"]:
                self._perf_stats["max_concurrent"] = current
                if current % 10 == 0:  # 每10个并发打印一次
                    logger.debug("concurrent_count", current=current)

            try:
                url = f"{self.base_url}/{symbol}/{symbol}-metrics-{date_str}.zip"
                logger.debug("download_symbol_metrics", symbol=symbol, date=date_str)

                retry_config = RetryConfig(max_retries=3, base_delay=0)

                # 计时：下载
                dl_start = time.time()
                metrics_data = await self._download_and_parse_metrics_csv(
                    url,
                    symbol,
                    max_workers,
                    retry_config,
                )
                self._perf_stats["download_time"] += time.time() - dl_start

                if metrics_data and self.db:
                    # 计时：数据库插入
                    db_start = time.time()

                    if metrics_data.get("open_interest"):
                        await self.db.insert_open_interests(metrics_data["open_interest"])
                        logger.debug(f"存储持仓量数据：{symbol}（{date_str}，{len(metrics_data['open_interest'])} 条）。")
                    if metrics_data.get("long_short_ratio"):
                        await self.db.insert_long_short_ratios(metrics_data["long_short_ratio"])
                        logger.debug(f"存储多空比例数据：{symbol}（{date_str}，{len(metrics_data['long_short_ratio'])} 条）。")

                    self._perf_stats["db_time"] += time.time() - db_start
                else:
                    logger.warning("no_metrics_data", symbol=symbol, date=date_str)

                self._perf_stats["download_count"] += 1

            except Exception as e:
                logger.warning("download_metrics_failed", symbol=symbol, date=date_str, error=str(e))
                self._record_failed_download(symbol, str(e), {"url": url, "date": date_str, "data_type": "metrics"})

            finally:
                # 减少并发计数
                self._perf_stats["concurrent_count"] -= 1

        if request_delay > 0:
            await asyncio.sleep(request_delay)

    async def _download_and_parse_metrics_csv(  # noqa: C901
        self,
        url: str,
        symbol: str,
        max_workers: int,
        retry_config: RetryConfig | None = None,
    ) -> dict[str, list] | None:
        """使用aiohttp下载并解析指标CSV数据."""
        if retry_config is None:
            retry_config = RetryConfig(max_retries=3, base_delay=0)

        # 使用基类的重试机制下载ZIP文件
        async def _download_zip() -> bytes:
            """下载ZIP文件的内部异步函数."""
            session = await self._get_session(max_workers)
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.read()

        try:
            # 使用基类的异步重试处理机制
            zip_content = await self._handle_async_request_with_retry(
                _download_zip,
                retry_config=retry_config,
            )
        except Exception as e:
            logger.error("download_zip_failed", symbol=symbol, error=str(e))
            return None

        try:
            # 计时：解析
            parse_start = time.time()
            with zipfile.ZipFile(BytesIO(zip_content)) as zip_file:
                csv_files = [f for f in zip_file.namelist() if f.endswith(".csv")]

                if not csv_files:
                    logger.warning("no_csv_in_zip", url=url)
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
                                "count_toptrader_long_short_ratio",  # toptrader_account
                                "sum_toptrader_long_short_ratio",  # toptrader_position
                                "count_long_short_ratio",  # global_account
                                "sum_taker_long_short_vol_ratio",  # taker_vol
                            ]
                        ):
                            result["long_short_ratio"].extend(self._parse_lsr_data(rows, symbol, csv_file))
                    except Exception as e:
                        logger.warning("parse_csv_error", csv_file=csv_file, error=str(e))
                        continue

                # 记录解析时间
                self._perf_stats["parse_time"] += time.time() - parse_start

                return result if result["open_interest"] or result["long_short_ratio"] else None
        except Exception as e:
            logger.error("parse_metrics_error", symbol=symbol, error=str(e))
            return None

    async def _get_session(self, max_workers: int) -> aiohttp.ClientSession:
        """获取复用的aiohttp会话实例.

        Args:
            max_workers: 最大并发数，用于配置TCP连接池大小.

        Returns:
            aiohttp客户端会话实例.
        """
        if self._session_lock is None:
            self._session_lock = asyncio.Lock()

        async with self._session_lock:
            if self._session is None or self._session.closed:
                # 根据传入的max_workers创建TCP连接池
                # limit = limit_per_host 因为只访问单一主机 (data.binance.vision)
                connector = aiohttp.TCPConnector(
                    limit=max_workers,  # 全局连接池大小等于并发数
                    limit_per_host=max_workers,  # 单主机连接数等于并发数
                    ttl_dns_cache=300,
                    enable_cleanup_closed=True,
                    force_close=False,  # 允许连接复用，提高并发性能
                    keepalive_timeout=30,  # 保持连接30秒
                )
                self._session = aiohttp.ClientSession(
                    timeout=self._client_timeout,
                    connector=connector,
                    connector_owner=True,
                    trust_env=True,
                )
                logger.debug("http_session_created", pool_size=max_workers)

        return self._session

    async def _close_session(self) -> None:
        """关闭当前的aiohttp会话."""
        if self._session_lock is None:
            self._session_lock = asyncio.Lock()

        async with self._session_lock:
            session = self._session
            self._session = None

            if session and not session.closed:
                try:
                    # 使用较短的超时关闭会话，避免长时间等待
                    await asyncio.wait_for(session.close(), timeout=5.0)
                except TimeoutError:
                    logger.debug("session_close_timeout")
                except ClientConnectionError as exc:
                    logger.debug("session_close_connection_error", error=str(exc))
                except Exception as exc:  # noqa: BLE001
                    logger.debug("session_close_error", error=str(exc))

    def _parse_oi_data(self, raw_data: list[dict], symbol: str) -> list[OpenInterest]:
        """解析持仓量数据."""
        open_interests = []

        for row in raw_data:
            try:
                # 解析时间字段（Binance API 返回的是 UTC 时间）
                create_time = row["create_time"]
                from datetime import UTC

                timestamp = int(datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC).timestamp() * 1000)

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
                logger.warning("parse_oi_row_error", error=str(e), row=row)
                continue

        return open_interests

    def _parse_lsr_data(self, raw_data: list[dict], symbol: str, file_name: str) -> list[LongShortRatio]:  # noqa: C901
        """解析多空比例数据.

        从Vision CSV解析4种不同类型的多空比例数据:
        - count_toptrader_long_short_ratio -> toptrader_account (Top 20%账户数比例)
        - sum_toptrader_long_short_ratio -> toptrader_position (Top 20%持仓比例)
        - count_long_short_ratio -> global_account (全体交易者账户数比例)
        - sum_taker_long_short_vol_ratio -> taker_vol (主动买/卖成交量比)
        """
        long_short_ratios = []

        for row in raw_data:
            try:
                # 解析时间字段（Binance API 返回的是 UTC 时间）
                create_time = row["create_time"]
                from datetime import UTC

                timestamp = int(datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC).timestamp() * 1000)

                # 1. 解析 count_toptrader_long_short_ratio (Top 20% 账户数比例)
                # 来源: topLongShortAccountRatio API
                try:
                    if "count_toptrader_long_short_ratio" in row:
                        ratio_value = self._safe_decimal_convert(row["count_toptrader_long_short_ratio"])
                        if ratio_value is not None:
                            long_account, short_account = self._calculate_long_short_accounts(ratio_value)
                            long_short_ratios.append(
                                LongShortRatio(
                                    symbol=symbol,
                                    long_short_ratio=ratio_value,
                                    long_account=long_account,
                                    short_account=short_account,
                                    timestamp=timestamp,
                                    ratio_type="toptrader_account",
                                )
                            )
                except Exception as e:
                    logger.debug("skip_toptrader_account_data", symbol=symbol, time=create_time, error=str(e))

                # 2. 解析 sum_toptrader_long_short_ratio (Top 20% 持仓比例)
                # 来源: topLongShortPositionRatio API
                try:
                    if "sum_toptrader_long_short_ratio" in row:
                        ratio_value = self._safe_decimal_convert(row["sum_toptrader_long_short_ratio"])
                        if ratio_value is not None:
                            long_account, short_account = self._calculate_long_short_accounts(ratio_value)
                            long_short_ratios.append(
                                LongShortRatio(
                                    symbol=symbol,
                                    long_short_ratio=ratio_value,
                                    long_account=long_account,
                                    short_account=short_account,
                                    timestamp=timestamp,
                                    ratio_type="toptrader_position",
                                )
                            )
                except Exception as e:
                    logger.debug("skip_toptrader_position_data", symbol=symbol, time=create_time, error=str(e))

                # 3. 解析 count_long_short_ratio (全体交易者账户数比例)
                # 来源: globalLongShortAccountRatio API
                try:
                    if "count_long_short_ratio" in row:
                        ratio_value = self._safe_decimal_convert(row["count_long_short_ratio"])
                        if ratio_value is not None:
                            long_account, short_account = self._calculate_long_short_accounts(ratio_value)
                            long_short_ratios.append(
                                LongShortRatio(
                                    symbol=symbol,
                                    long_short_ratio=ratio_value,
                                    long_account=long_account,
                                    short_account=short_account,
                                    timestamp=timestamp,
                                    ratio_type="global_account",
                                )
                            )
                except Exception as e:
                    logger.debug("skip_global_account_data", symbol=symbol, time=create_time, error=str(e))

                # 4. 解析 sum_taker_long_short_vol_ratio (Taker 买/卖成交量比)
                # 来源: takerlongshortRatio API
                try:
                    if "sum_taker_long_short_vol_ratio" in row:
                        ratio_value = self._safe_decimal_convert(row["sum_taker_long_short_vol_ratio"])
                        if ratio_value is not None:
                            long_account, short_account = self._calculate_long_short_accounts(ratio_value)
                            long_short_ratios.append(
                                LongShortRatio(
                                    symbol=symbol,
                                    long_short_ratio=ratio_value,
                                    long_account=long_account,
                                    short_account=short_account,
                                    timestamp=timestamp,
                                    ratio_type="taker_vol",
                                )
                            )
                except Exception as e:
                    logger.debug("skip_taker_vol_data", symbol=symbol, time=create_time, error=str(e))

            except (ValueError, KeyError) as e:
                logger.warning("parse_lsr_row_error", error=str(e), row=row)
                continue

        return long_short_ratios

    def _calculate_long_short_accounts(self, ratio_value: Decimal) -> tuple[Decimal, Decimal]:
        """根据多空比例计算多头和空头账户比例.

        Args:
            ratio_value: 多空比例值

        Returns:
            (long_account, short_account) 元组
        """
        if ratio_value > 0:
            total = ratio_value + 1
            long_account = ratio_value / total
            short_account = Decimal("1") / total
        else:
            long_account = Decimal("0.5")
            short_account = Decimal("0.5")
        return long_account, short_account

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
