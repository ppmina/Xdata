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

from cryptoservice.client import BinanceGateway
from cryptoservice.config import RetryConfig
from cryptoservice.config.logging import get_logger
from cryptoservice.exceptions import MarketDataFetchError, RateLimitError
from cryptoservice.models import LongShortRatio, OpenInterest
from cryptoservice.storage.database import Database as AsyncMarketDB
from cryptoservice.utils.run_id import generate_run_id
from cryptoservice.utils.time_utils import shift_date

from .base_downloader import BaseDownloader, EndpointControlRegistry

logger = get_logger(__name__)


class VisionDownloader(BaseDownloader):
    """Binance Vision数据下载器."""

    def __init__(
        self,
        client: BinanceGateway,
        request_delay: float = 0,
        endpoint_controls: EndpointControlRegistry | None = None,
    ):
        """初始化Binance Vision数据下载器.

        Args:
            client: API 客户端实例.
            request_delay: 请求之间的基础延迟（秒）.
            endpoint_controls: 可选的共享 endpoint 控制状态.
        """
        super().__init__(client, request_delay, endpoint_controls=endpoint_controls)
        self.db: AsyncMarketDB | None = None
        self._run_id: str | None = None
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
        run_id: str | None = None,
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
            run_id: 运行ID（可选）
        """
        run = run_id or generate_run_id("vision")
        self._run_id = run
        self.begin_run_state(run_id=run, stage="metrics", dataset="vision-metrics")
        stage_started_at = time.perf_counter()
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
                "download.stage_start",
                run=run,
                stage="metrics",
                dataset="vision-metrics",
                status="start",
                duration_ms=0,
                symbols=len(symbols),
                workers=max_workers,
                incremental=incremental,
                start=start_date,
                end=end_date,
                expanded_start=expanded_start_date,
                data_types=data_types,
            )

            if self.db is None:
                self.db = AsyncMarketDB(db_path)
            await self.db.initialize()

            import pandas as pd

            date_range = pd.date_range(start=expanded_start_date, end=end_date, freq="D")
            total_candidates = len(symbols) * len(date_range)

            if incremental:
                logger.debug(
                    "download.incremental_start",
                    run=run,
                    stage="metrics",
                    dataset="vision-metrics",
                    status="analyzing",
                    symbols=len(symbols),
                    start=expanded_start_date,
                    end=end_date,
                )
                plan = await self.db.incremental.plan_vision_metrics_download(symbols, expanded_start_date, end_date)

                symbol_date_pairs = [(symbol, date_str) for symbol, details in plan.items() for date_str in details.get("missing_dates", [])]
                skipped_count = total_candidates - len(symbol_date_pairs)

                if not symbol_date_pairs:
                    logger.info(
                        "download.stage_done",
                        run=run,
                        stage="metrics",
                        dataset="vision-metrics",
                        status="skipped",
                        duration_ms=int((time.perf_counter() - stage_started_at) * 1000),
                        reason="already_up_to_date",
                        symbols=len(symbols),
                    )
                    return

                logger.debug(
                    "download.incremental_plan",
                    run=run,
                    stage="metrics",
                    dataset="vision-metrics",
                    status="ready",
                    task_count=len(symbol_date_pairs),
                    skipped_count=skipped_count,
                )
            else:
                # 不启用增量下载，下载所有数据
                symbol_date_pairs = [(symbol, date.strftime("%Y-%m-%d")) for date in date_range for symbol in symbols]
                logger.info(
                    "download.plan",
                    run=run,
                    stage="metrics",
                    dataset="vision-metrics",
                    status="ready",
                    task_count=len(symbol_date_pairs),
                    start=start_date,
                    end=end_date,
                    expanded_start=expanded_start_date,
                )

            if not symbol_date_pairs:
                logger.info(
                    "download.stage_done",
                    run=run,
                    stage="metrics",
                    dataset="vision-metrics",
                    status="skipped",
                    duration_ms=int((time.perf_counter() - stage_started_at) * 1000),
                    reason="empty_task_set",
                    symbols=len(symbols),
                )
                return

            worker_count = max(1, min(max_workers, len(symbol_date_pairs)))
            queue: asyncio.Queue[tuple[str, str] | None] = asyncio.Queue()
            for pair in symbol_date_pairs:
                queue.put_nowait(pair)
            for _ in range(worker_count):
                queue.put_nowait(None)

            logger.debug(
                "download.worker_pool_created",
                run=run,
                stage="metrics",
                dataset="vision-metrics",
                status="ready",
                workers=worker_count,
                task_count=len(symbol_date_pairs),
            )

            async def worker() -> None:
                while True:
                    payload = await queue.get()
                    try:
                        if payload is None:
                            return
                        symbol, date_str = payload
                        await self._download_and_process_symbol_for_date(
                            symbol=symbol,
                            date_str=date_str,
                            request_delay=request_delay,
                            max_workers=max_workers,
                        )
                    finally:
                        queue.task_done()

            start_time = time.time()
            try:
                async with asyncio.TaskGroup() as tg:
                    for _ in range(worker_count):
                        tg.create_task(worker())
            except* RateLimitError as grouped_rate_limit:
                elapsed_ms = int((time.perf_counter() - stage_started_at) * 1000)
                logger.error(
                    "download.stage_done",
                    run=run,
                    stage="metrics",
                    dataset="vision-metrics",
                    status="aborted",
                    duration_ms=elapsed_ms,
                    terminal=True,
                    completed=self._perf_stats["download_count"],
                    total_tasks=len(symbol_date_pairs),
                    error=str(grouped_rate_limit.exceptions[0]),
                )
                raise grouped_rate_limit.exceptions[0] from None

            elapsed = time.time() - start_time

            # 完整性检查
            total_expected = len(symbol_date_pairs)
            success_count = self._perf_stats["download_count"]
            failed_count = sum(len(records) for records in self.failed_downloads.values())
            success_rate = (success_count / total_expected * 100) if total_expected > 0 else 0

            avg_per_task = elapsed / success_count if success_count > 0 else 0

            logger.info(
                "download.stage_done",
                run=run,
                stage="metrics",
                dataset="vision-metrics",
                status="complete",
                duration_ms=int((time.perf_counter() - stage_started_at) * 1000),
                total_tasks=total_expected,
                successful_tasks=success_count,
                failed_tasks=failed_count,
                success_rate=round(success_rate, 2),
                avg_task_ms=int(avg_per_task * 1000),
            )
            if failed_count > 0:
                logger.warning(
                    "download.partial_failures",
                    run=run,
                    stage="metrics",
                    dataset="vision-metrics",
                    status="partial",
                    failed_tasks=failed_count,
                )
        except RateLimitError:
            raise
        except Exception as e:
            logger.error(
                "download.stage_done",
                run=run,
                stage="metrics",
                dataset="vision-metrics",
                status="error",
                duration_ms=int((time.perf_counter() - stage_started_at) * 1000),
                terminal=False,
                error=str(e),
            )
            raise MarketDataFetchError(f"Failed to download metrics data from Binance Vision: {e}") from e
        finally:
            await self._close_session()

    async def _download_and_process_symbol_for_date(
        self,
        symbol: str,
        date_str: str,
        request_delay: float,
        max_workers: int,
    ) -> None:
        """下载并处理单个交易对在特定日期的数据."""
        # 记录并发数
        self._perf_stats["concurrent_count"] += 1
        current = self._perf_stats["concurrent_count"]
        if current > self._perf_stats["max_concurrent"]:
            self._perf_stats["max_concurrent"] = current

        try:
            url = f"{self.base_url}/{symbol}/{symbol}-metrics-{date_str}.zip"
            logger.debug(
                "download.symbol_start",
                run=self._run_id,
                stage="metrics",
                dataset="vision-metrics",
                symbol=symbol,
                date=date_str,
                status="start",
            )

            retry_config = RetryConfig(max_retries=3, base_delay=0)

            dl_start = time.time()
            metrics_data = await self._download_and_parse_metrics_csv(
                url,
                symbol,
                max_workers,
                retry_config,
            )
            self._perf_stats["download_time"] += time.time() - dl_start

            if metrics_data and self.db:
                db_start = time.time()

                oi_rows = len(metrics_data.get("open_interest", []))
                lsr_rows = len(metrics_data.get("long_short_ratio", []))
                if oi_rows:
                    await self.db.insert_open_interests(metrics_data["open_interest"])
                if lsr_rows:
                    await self.db.insert_long_short_ratios(metrics_data["long_short_ratio"])

                self._perf_stats["db_time"] += time.time() - db_start
                logger.debug(
                    "download.symbol_done",
                    run=self._run_id,
                    stage="metrics",
                    dataset="vision-metrics",
                    symbol=symbol,
                    date=date_str,
                    status="complete",
                    open_interest_rows=oi_rows,
                    long_short_ratio_rows=lsr_rows,
                )
            else:
                logger.warning(
                    "download.symbol_empty",
                    run=self._run_id,
                    stage="metrics",
                    dataset="vision-metrics",
                    symbol=symbol,
                    date=date_str,
                    status="empty",
                )

            self._perf_stats["download_count"] += 1

        except RateLimitError:
            logger.error(
                "download.symbol_error",
                run=self._run_id,
                stage="metrics",
                dataset="vision-metrics",
                symbol=symbol,
                date=date_str,
                status="error",
                terminal=True,
                error="terminal_rate_limit",
            )
            raise
        except Exception as e:
            logger.warning(
                "download.symbol_error",
                run=self._run_id,
                stage="metrics",
                dataset="vision-metrics",
                symbol=symbol,
                date=date_str,
                status="error",
                terminal=False,
                error=str(e),
            )
            self._record_failed_download(symbol, str(e), {"url": url, "date": date_str, "data_type": "metrics"})

        finally:
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
                endpoint_key="vision_metrics_zip",
                endpoint_max_workers=max_workers,
            )
        except RateLimitError:
            raise
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
