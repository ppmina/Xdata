"""现代化异步数据库存储。

基于aiosqlitepool的高性能异步SQLite存储实现。
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Union, TypeGuard
import pandas as pd
import numpy as np

from cryptoservice.models import Freq, KlineIndex, PerpetualMarketTicker
from .pool_manager import PoolManager

logger = logging.getLogger(__name__)


# 使用新的连接池管理器


class AsyncMarketDB:
    """异步市场数据库管理类。

    基于异步连接池的高性能数据库操作。
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        max_connections: int = 10,
        enable_wal: bool = True,
        enable_optimizations: bool = True,
    ):
        """初始化异步市场数据库。

        Args:
            db_path: 数据库文件路径
            max_connections: 最大连接数
            enable_wal: 是否启用WAL模式
            enable_optimizations: 是否启用SQLite优化
        """
        self.db_path = Path(db_path)
        self.pool = PoolManager(
            db_path=db_path,
            max_connections=max_connections,
            enable_wal=enable_wal,
            enable_optimizations=enable_optimizations,
        )
        self._initialized = False

    async def initialize(self) -> None:
        """初始化数据库。"""
        if self._initialized:
            return

        await self.pool.initialize()
        await self._create_tables()
        self._initialized = True
        logger.info(f"异步数据库初始化完成: {self.db_path}")

    async def _create_tables(self) -> None:
        """创建数据库表结构。"""
        assert self.pool._pool is not None
        async with self.pool._pool.connection() as conn:
            # 市场数据表
            await conn.execute(
                """
                    CREATE TABLE IF NOT EXISTS market_data (
                        symbol TEXT,
                        timestamp INTEGER,
                        freq TEXT,
                        open_price REAL,
                        high_price REAL,
                        low_price REAL,
                        close_price REAL,
                        volume REAL,
                        quote_volume REAL,
                        trades_count INTEGER,
                        taker_buy_volume REAL,
                        taker_buy_quote_volume REAL,
                        taker_sell_volume REAL,
                        taker_sell_quote_volume REAL,
                        PRIMARY KEY (symbol, timestamp, freq)
                    )
                    """
            )

            # 资金费率表
            await conn.execute(
                """
                    CREATE TABLE IF NOT EXISTS funding_rate (
                        symbol TEXT,
                        timestamp INTEGER,
                        funding_rate REAL,
                        funding_time INTEGER,
                        mark_price REAL,
                        index_price REAL,
                        PRIMARY KEY (symbol, timestamp)
                    )
                    """
            )

            # 持仓量表
            await conn.execute(
                """
                    CREATE TABLE IF NOT EXISTS open_interest (
                        symbol TEXT,
                        timestamp INTEGER,
                        interval TEXT,
                        open_interest REAL,
                        open_interest_value REAL,
                        PRIMARY KEY (symbol, timestamp, interval)
                    )
                    """
            )

            # 多空比例表
            await conn.execute(
                """
                    CREATE TABLE IF NOT EXISTS long_short_ratio (
                        symbol TEXT,
                        timestamp INTEGER,
                        period TEXT,
                        ratio_type TEXT,
                        long_short_ratio REAL,
                        long_account REAL,
                        short_account REAL,
                        PRIMARY KEY (symbol, timestamp, period, ratio_type)
                    )
                    """
            )

            # 创建索引
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON market_data(symbol)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON market_data(timestamp)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_freq ON market_data(freq)")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_symbol_freq_timestamp ON market_data(symbol, freq, timestamp)"
            )

            # 新特征表的索引
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_funding_symbol ON funding_rate(symbol)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_funding_timestamp ON funding_rate(timestamp)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_oi_symbol ON open_interest(symbol)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_oi_timestamp ON open_interest(timestamp)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_lsr_symbol ON long_short_ratio(symbol)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_lsr_timestamp ON long_short_ratio(timestamp)")

    async def store_data(
        self,
        data: Union[List[PerpetualMarketTicker], List[List[PerpetualMarketTicker]]],
        freq: Freq,
        batch_size: int = 1000,
    ) -> None:
        """异步存储市场数据。

        Args:
            data: 市场数据列表
            freq: 数据频率
            batch_size: 批量大小
        """
        if not self._initialized:
            await self.initialize()

        # 确保数据是二维列表格式
        if not data:
            logger.warning("No data to store")
            return

        # 使用类型守卫模式判断数据结构
        def is_flat_list(data_list: Any) -> TypeGuard[list[PerpetualMarketTicker]]:
            """判断是否为单层PerpetualMarketTicker列表"""
            return (
                isinstance(data_list, list)
                and bool(data_list)
                and all(isinstance(item, PerpetualMarketTicker) for item in data_list)
            )

        def is_nested_list(
            data_list: Any,
        ) -> TypeGuard[list[list[PerpetualMarketTicker]]]:
            """判断是否为嵌套的PerpetualMarketTicker列表"""
            return (
                isinstance(data_list, list)
                and bool(data_list)
                and all(isinstance(item, list) for item in data_list)
                and all(
                    all(isinstance(subitem, PerpetualMarketTicker) for subitem in sublist)
                    for sublist in data_list
                    if sublist
                )
            )

        if is_flat_list(data):
            flattened_data = data
        elif is_nested_list(data):
            flattened_data = [item for sublist in data for item in sublist]
        else:
            raise ValueError("Invalid data structure")

        # 批量处理
        total_stored = 0
        for i in range(0, len(flattened_data), batch_size):
            batch = flattened_data[i : i + batch_size]
            await self._store_batch(batch, freq)
            total_stored += len(batch)

        symbol = flattened_data[0].symbol if flattened_data else "unknown"
        logger.info(f"异步存储完成: {total_stored} 条记录 ({symbol}, {freq.value})")

    async def _store_batch(
        self,
        batch: List[PerpetualMarketTicker],
        freq: Freq,
    ) -> None:
        """存储单个批次的数据。"""
        assert self.pool._pool is not None
        async with self.pool._pool.connection() as conn:
            records = []
            for ticker in batch:
                volume = float(ticker.raw_data[KlineIndex.VOLUME])
                quote_volume = float(ticker.raw_data[KlineIndex.QUOTE_VOLUME])
                taker_buy_volume = float(ticker.raw_data[KlineIndex.TAKER_BUY_VOLUME])
                taker_buy_quote_volume = float(ticker.raw_data[KlineIndex.TAKER_BUY_QUOTE_VOLUME])

                record = (
                    ticker.symbol,
                    ticker.open_time,
                    freq.value,
                    ticker.raw_data[KlineIndex.OPEN],
                    ticker.raw_data[KlineIndex.HIGH],
                    ticker.raw_data[KlineIndex.LOW],
                    ticker.raw_data[KlineIndex.CLOSE],
                    volume,
                    quote_volume,
                    ticker.raw_data[KlineIndex.TRADES_COUNT],
                    taker_buy_volume,
                    taker_buy_quote_volume,
                    volume - taker_buy_volume,
                    quote_volume - taker_buy_quote_volume,
                )
                records.append(record)

            await conn.execute(
                """
                INSERT OR REPLACE INTO market_data (
                    symbol, timestamp, freq,
                    open_price, high_price, low_price, close_price,
                    volume, quote_volume, trades_count,
                    taker_buy_volume, taker_buy_quote_volume,
                    taker_sell_volume, taker_sell_quote_volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                records,
            )

    async def read_data(
        self,
        start_time: str,
        end_time: str,
        freq: Freq,
        symbols: List[str],
        features: Optional[List[str]] = None,
        raise_on_empty: bool = True,
    ) -> pd.DataFrame:
        """异步读取市场数据。

        Args:
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            freq: 数据频率
            symbols: 交易对列表
            features: 需要读取的特征列表
            raise_on_empty: 当没有数据时是否抛出异常

        Returns:
            pd.DataFrame: 市场数据
        """
        if not self._initialized:
            await self.initialize()

        # 转换时间格式
        start_ts = int(pd.Timestamp(start_time).timestamp() * 1000)
        end_ts = int(pd.Timestamp(end_time).timestamp() * 1000)

        # 构建查询
        if features is None:
            features = [
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "quote_volume",
                "trades_count",
                "taker_buy_volume",
                "taker_buy_quote_volume",
                "taker_sell_volume",
                "taker_sell_quote_volume",
            ]

        columns = ", ".join(features)
        placeholders = ", ".join("?" * len(symbols))

        query = f"""
            SELECT symbol, timestamp, {columns}
            FROM market_data
            WHERE timestamp BETWEEN ? AND ?
            AND freq = ?
            AND symbol IN ({placeholders})
            ORDER BY symbol, timestamp
        """

        params = [start_ts, end_ts, freq.value] + symbols

        assert self.pool._pool is not None
        async with self.pool._pool.connection() as conn:
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()

            if not rows:
                if raise_on_empty:
                    raise ValueError("No data found for the specified criteria")
                return pd.DataFrame()

            # 转换为DataFrame
            columns_list = ["symbol", "timestamp"] + features
            df = pd.DataFrame(rows, columns=columns_list)

            # 设置多级索引
            df = df.set_index(["symbol", "timestamp"])
            return df

    async def store_funding_rate(self, data: List[Any]) -> None:
        """异步存储资金费率数据。

        Args:
            data: FundingRate对象列表
        """
        if not self._initialized:
            await self.initialize()

        if not data:
            logger.warning("No funding rate data to store")
            return

        assert self.pool._pool is not None
        async with self.pool._pool.connection() as conn:
            records = []
            for item in data:
                record = (
                    item.symbol,
                    item.funding_time,
                    float(item.funding_rate),
                    item.funding_time,
                    float(item.mark_price) if item.mark_price else None,
                    float(item.index_price) if item.index_price else None,
                )
                records.append(record)

            await conn.execute(
                """
                INSERT OR REPLACE INTO funding_rate (
                    symbol, timestamp, funding_rate, funding_time, mark_price, index_price
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                records,
            )

            logger.info(f"异步存储资金费率数据完成: {len(records)} 条记录")

    async def store_open_interest(self, data: list) -> None:
        """异步存储持仓量数据。

        Args:
            data: OpenInterest对象列表
        """
        if not self._initialized:
            await self.initialize()

        if not data:
            logger.warning("No open interest data to store")
            return

        assert self.pool._pool is not None
        async with self.pool._pool.connection() as conn:
            records = []
            for item in data:
                record = (
                    item.symbol,
                    item.time,
                    getattr(item, "interval", "5m"),  # 默认5分钟间隔
                    float(item.open_interest),
                    float(item.open_interest_value) if item.open_interest_value else None,
                )
                records.append(record)

            await conn.execute(
                """
                INSERT OR REPLACE INTO open_interest (
                    symbol, timestamp, interval, open_interest, open_interest_value
                ) VALUES (?, ?, ?, ?, ?)
                """,
                records,
            )

            logger.info(f"异步存储持仓量数据完成: {len(records)} 条记录")

    async def store_long_short_ratio(self, data: list) -> None:
        """异步存储多空比例数据。

        Args:
            data: LongShortRatio对象列表
        """
        if not self._initialized:
            await self.initialize()

        if not data:
            logger.warning("No long short ratio data to store")
            return

        assert self.pool._pool is not None
        async with self.pool._pool.connection() as conn:
            records = []
            for item in data:
                record = (
                    item.symbol,
                    item.timestamp,
                    getattr(item, "period", "5m"),  # 默认5分钟周期
                    getattr(item, "ratio_type", "account"),  # 默认账户类型
                    float(item.long_short_ratio),
                    float(item.long_account) if item.long_account else None,
                    float(item.short_account) if item.short_account else None,
                )
                records.append(record)

            await conn.execute(
                """
                INSERT OR REPLACE INTO long_short_ratio (
                    symbol, timestamp, period, ratio_type, long_short_ratio, long_account, short_account
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                records,
            )

            logger.info(f"异步存储多空比例数据完成: {len(records)} 条记录")

    async def get_data_summary(self) -> dict:
        """获取数据库统计信息。

        Returns:
            dict: 统计信息
        """
        if not self._initialized:
            await self.initialize()

        assert self.pool._pool is not None
        async with self.pool._pool.connection() as conn:
            summary = {}

            # 市场数据统计
            cursor = await conn.execute(
                """
                SELECT
                    freq,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    MIN(timestamp) as earliest_timestamp,
                    MAX(timestamp) as latest_timestamp,
                    MIN(date(timestamp/1000, 'unixepoch')) as earliest_date,
                    MAX(date(timestamp/1000, 'unixepoch')) as latest_date
                FROM market_data
                GROUP BY freq
                """
            )
            market_stats = await cursor.fetchall()

            # 资金费率统计
            cursor = await conn.execute(
                """
                SELECT
                    COUNT(*) as record_count,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    MIN(timestamp) as earliest_timestamp,
                    MAX(timestamp) as latest_timestamp
                FROM funding_rate
                """
            )
            funding_stats = await cursor.fetchone()

            summary = {
                "market_data": [
                    {
                        "freq": row[0],
                        "record_count": row[1],
                        "unique_symbols": row[2],
                        "earliest_timestamp": row[3],
                        "latest_timestamp": row[4],
                        "earliest_date": row[5],
                        "latest_date": row[6],
                    }
                    for row in market_stats
                ],
                "funding_rate": (
                    {
                        "record_count": funding_stats[0] if funding_stats else 0,
                        "unique_symbols": funding_stats[1] if funding_stats else 0,
                        "earliest_timestamp": (funding_stats[2] if funding_stats else None),
                        "latest_timestamp": funding_stats[3] if funding_stats else None,
                    }
                    if funding_stats
                    else {}
                ),
            }

            return summary

    async def close(self) -> None:
        """关闭数据库连接。"""
        await self.pool.close()
        self._initialized = False

    async def __aenter__(self):
        """异步上下文管理器入口。"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口。"""
        await self.close()

    async def export_to_files_by_timestamp(
        self,
        output_path: Union[Path, str],
        start_ts: Union[int, str],
        end_ts: Union[int, str],
        freq: Freq,
        symbols: List[str],
        target_freq: Optional[Freq] = None,
        chunk_days: int = 30,  # 每次处理的天数
    ) -> None:
        """异步使用时间戳将数据库数据导出为npy文件格式，支持降采样.

        Args:
            output_path: 输出目录
            start_ts: 开始时间戳 (毫秒，int或str)
            end_ts: 结束时间戳 (毫秒，int或str)
            freq: 原始数据频率
            symbols: 交易对列表
            target_freq: 目标频率，None表示不进行降采样
            chunk_days: 每次处理的天数，用于控制内存使用
        """
        if not self._initialized:
            await self.initialize()

        try:
            # 确保时间戳为整数
            start_timestamp = int(start_ts)
            end_timestamp = int(end_ts)

            # 转换时间戳为日期，用于计算处理范围
            start_datetime = datetime.fromtimestamp(start_timestamp / 1000)
            end_datetime = datetime.fromtimestamp(end_timestamp / 1000)

            logger.info(f"异步导出数据: 时间戳 {start_timestamp} 到 {end_timestamp}")
            logger.info(
                f"日期范围: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')} 到 "
                f"{end_datetime.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            output_path = Path(output_path)

            # 创建日期范围 - 基于时间戳计算实际的日期范围
            start_date = start_datetime.date()
            end_date = end_datetime.date()
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")
            total_days = len(date_range)

            # 使用有效的频率进行导出
            export_freq = target_freq if target_freq is not None else freq

            # 如果总天数少于等于chunk_days，直接处理整个范围，不分块
            if total_days <= chunk_days:
                logger.info(f"处理所有数据: 时间戳 {start_timestamp} 到 {end_timestamp} (总共: {total_days} 天)")

                # 直接使用时间戳读取所有数据
                try:
                    df = await self._read_data_by_timestamp(
                        start_timestamp,
                        end_timestamp,
                        freq,
                        symbols,
                        raise_on_empty=False,
                    )
                except ValueError as e:
                    if "No data found" in str(e):
                        logger.warning(f"时间戳范围 {start_timestamp} 到 {end_timestamp} 未找到数据")
                        return
                    else:
                        raise

                if df.empty:
                    logger.warning(f"时间戳范围 {start_timestamp} 到 {end_timestamp} 未找到数据")
                    return

                # 如果需要降采样
                if target_freq is not None:
                    df = await self._resample_data(df, target_freq)

                # 处理所有数据
                await self._process_dataframe_for_export_by_timestamp(
                    df, output_path, export_freq, start_timestamp, end_timestamp
                )

            else:
                # 按chunk_days分块处理（用于大量数据）
                one_day_ms = 24 * 60 * 60 * 1000  # 一天的毫秒数
                chunk_ms = chunk_days * one_day_ms

                current_ts = start_timestamp
                while current_ts < end_timestamp:
                    chunk_end_ts = min(current_ts + chunk_ms, end_timestamp)

                    chunk_start_datetime = datetime.fromtimestamp(current_ts / 1000)
                    chunk_end_datetime = datetime.fromtimestamp(chunk_end_ts / 1000)

                    logger.info(
                        f"处理数据块: "
                        f"{chunk_start_datetime.strftime('%Y-%m-%d %H:%M:%S')} 到 "
                        f"{chunk_end_datetime.strftime('%Y-%m-%d %H:%M:%S')}"
                    )

                    # 使用时间戳读取数据块
                    try:
                        df = await self._read_data_by_timestamp(
                            current_ts,
                            chunk_end_ts,
                            freq,
                            symbols,
                            raise_on_empty=False,
                        )
                    except ValueError as e:
                        if "No data found" in str(e):
                            logger.warning(f"时间戳范围 {current_ts} 到 {chunk_end_ts} 未找到数据")
                            current_ts = chunk_end_ts
                            continue
                        else:
                            raise

                    if df.empty:
                        logger.warning(f"时间戳范围 {current_ts} 到 {chunk_end_ts} 未找到数据")
                        current_ts = chunk_end_ts
                        continue

                    # 如果需要降采样
                    if target_freq is not None:
                        df = await self._resample_data(df, target_freq)

                    # 处理当前数据块
                    await self._process_dataframe_for_export_by_timestamp(
                        df, output_path, export_freq, current_ts, chunk_end_ts
                    )

                    # 清理内存并让出控制权
                    del df
                    await asyncio.sleep(0.1)  # 给其他协程机会运行
                    current_ts = chunk_end_ts

            logger.info(f"异步导出完成: {output_path}")

        except Exception as e:
            logger.exception(f"异步导出时间戳数据失败: {e}")
            raise

    async def export_to_files(
        self,
        output_path: Union[Path, str],
        start_date: str,
        end_date: str,
        freq: Freq,
        symbols: List[str],
        target_freq: Optional[Freq] = None,
        chunk_days: int = 30,  # 每次处理的天数
    ) -> None:
        """异步将数据库数据导出为npy文件格式，支持降采样.

        Args:
            output_path: 输出目录
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            freq: 原始数据频率
            symbols: 交易对列表
            target_freq: 目标频率，None表示不进行降采样
            chunk_days: 每次处理的天数，用于控制内存使用
        """
        if not self._initialized:
            await self.initialize()

        try:
            output_path = Path(output_path)

            # 创建日期范围
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")
            total_days = len(date_range)

            # 如果总天数少于等于chunk_days，直接处理整个范围，不分块
            if total_days <= chunk_days:
                logger.info(f"异步处理所有数据: {start_date} 到 {end_date} (总共: {total_days} 天)")

                # 读取所有数据
                try:
                    df = await self.read_data(
                        start_date,
                        end_date,
                        freq,
                        symbols,
                        raise_on_empty=False,
                    )
                except ValueError as e:
                    if "No data found" in str(e):
                        logger.warning(f"时间段 {start_date} 到 {end_date} 未找到数据")
                        return
                    else:
                        raise

                if df.empty:
                    logger.warning(f"时间段 {start_date} 到 {end_date} 未找到数据")
                    return

                # 如果需要降采样
                if target_freq is not None:
                    df = await self._resample_data(df, target_freq)
                    freq = target_freq

                # 处理所有数据
                await self._process_dataframe_for_export(df, output_path, freq, date_range)

            else:
                # 按chunk_days分块处理（用于大量数据）
                for chunk_start in range(0, len(date_range), chunk_days):
                    chunk_end = min(chunk_start + chunk_days, len(date_range))
                    chunk_start_date = date_range[chunk_start].strftime("%Y-%m-%d")
                    chunk_end_date = date_range[chunk_end - 1].strftime("%Y-%m-%d")

                    logger.info(f"异步处理数据: {chunk_start_date} 到 {chunk_end_date}")

                    # 读取数据块
                    try:
                        df = await self.read_data(
                            chunk_start_date,
                            chunk_end_date,
                            freq,
                            symbols,
                            raise_on_empty=False,
                        )
                    except ValueError as e:
                        if "No data found" in str(e):
                            logger.warning(f"时间段 {chunk_start_date} 到 {chunk_end_date} 未找到数据")
                            continue
                        else:
                            raise

                    if df.empty:
                        logger.warning(f"时间段 {chunk_start_date} 到 {chunk_end_date} 未找到数据")
                        continue

                    # 如果需要降采样
                    if target_freq is not None:
                        df = await self._resample_data(df, target_freq)
                        freq = target_freq

                    # 处理当前数据块
                    chunk_dates = pd.date_range(chunk_start_date, chunk_end_date, freq="D")
                    await self._process_dataframe_for_export(df, output_path, freq, chunk_dates)

                    # 清理内存并让出控制权
                    del df
                    await asyncio.sleep(0.1)  # 给其他协程机会运行

            logger.info(f"异步导出完成: {output_path}")

        except Exception as e:
            logger.exception(f"异步导出数据失败: {e}")
            raise

    async def _read_data_by_timestamp(
        self,
        start_ts: int,
        end_ts: int,
        freq: Freq,
        symbols: List[str],
        features: Optional[List[str]] = None,
        raise_on_empty: bool = True,
    ) -> pd.DataFrame:
        """使用时间戳读取市场数据的内部实现.

        Args:
            start_ts: 开始时间戳 (毫秒)
            end_ts: 结束时间戳 (毫秒)
            freq: 数据频率
            symbols: 交易对列表
            features: 需要读取的特征列表
            raise_on_empty: 当没有数据时是否抛出异常，False则返回空DataFrame

        Returns:
            pd.DataFrame: 市场数据
        """
        # 构建查询
        if features is None:
            features = [
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "quote_volume",
                "trades_count",
                "taker_buy_volume",
                "taker_buy_quote_volume",
                "taker_sell_volume",
                "taker_sell_quote_volume",
            ]

        columns = ", ".join(features)
        placeholders = ", ".join("?" * len(symbols))

        query = f"""
            SELECT symbol, timestamp, {columns}
            FROM market_data
            WHERE timestamp BETWEEN ? AND ?
            AND freq = ?
            AND symbol IN ({placeholders})
            ORDER BY symbol, timestamp
        """
        params = [start_ts, end_ts, freq.value] + symbols

        # 执行查询
        assert self.pool._pool is not None
        async with self.pool._pool.connection() as conn:
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()

            if not rows:
                if raise_on_empty:
                    raise ValueError("No data found for the specified criteria")
                else:
                    # 返回空的DataFrame，但保持正确的结构
                    empty_df = pd.DataFrame(columns=["symbol", "timestamp"] + features)
                    empty_df = empty_df.set_index(["symbol", "timestamp"])
                    return empty_df

            # 转换为DataFrame
            columns_list = ["symbol", "timestamp"] + features
            df = pd.DataFrame(rows, columns=columns_list)

            # 设置多级索引
            df = df.set_index(["symbol", "timestamp"])
            return df

    async def _resample_data(self, df: pd.DataFrame, target_freq: Freq) -> pd.DataFrame:
        """对数据进行降采样处理.

        Args:
            df: 原始数据
            target_freq: 目标频率

        Returns:
            pd.DataFrame: 降采样后的数据
        """
        # 定义重采样规则 (修复pandas FutureWarning)
        freq_map = {
            Freq.m1: "1min",
            Freq.m3: "3min",
            Freq.m5: "5min",
            Freq.m15: "15min",
            Freq.m30: "30min",
            Freq.h1: "1h",
            Freq.h2: "2h",
            Freq.h4: "4h",
            Freq.h6: "6h",
            Freq.h8: "8h",
            Freq.h12: "12h",
            Freq.d1: "1D",
            Freq.w1: "1W",
            Freq.M1: "1M",
        }

        # 在线程池中执行CPU密集型操作
        loop = asyncio.get_event_loop()

        def resample_sync():
            resampled_dfs = []
            for symbol in df.index.get_level_values("symbol").unique():
                symbol_data = df.loc[symbol]

                # 定义聚合规则
                agg_rules = {
                    "open_price": "first",
                    "high_price": "max",
                    "low_price": "min",
                    "close_price": "last",
                    "volume": "sum",
                    "quote_volume": "sum",
                    "trades_count": "sum",
                    "taker_buy_volume": "sum",
                    "taker_buy_quote_volume": "sum",
                    "taker_sell_volume": "sum",
                    "taker_sell_quote_volume": "sum",
                }

                # 执行重采样
                resampled = symbol_data.resample(freq_map[target_freq]).agg(agg_rules)
                resampled.index = pd.MultiIndex.from_product([[symbol], resampled.index], names=["symbol", "timestamp"])
                resampled_dfs.append(resampled)

            return pd.concat(resampled_dfs)

        return await loop.run_in_executor(None, resample_sync)

    async def _process_dataframe_for_export_by_timestamp(
        self,
        df: pd.DataFrame,
        output_path: Path,
        freq: Freq,
        start_ts: int,
        end_ts: int,
    ) -> None:
        """基于时间戳处理DataFrame并导出为文件的异步辅助方法"""

        # 建立数据库字段名到短字段名的映射关系
        FIELD_MAPPING = {
            # 短字段名: (数据库字段名, 是否需要计算)
            "opn": ("open_price", False),
            "hgh": ("high_price", False),
            "low": ("low_price", False),
            "cls": ("close_price", False),
            "vol": ("volume", False),
            "amt": ("quote_volume", False),
            "tnum": ("trades_count", False),
            "tbvol": ("taker_buy_volume", False),
            "tbamt": ("taker_buy_quote_volume", False),
            "tsvol": ("taker_sell_volume", False),
            "tsamt": ("taker_sell_quote_volume", False),
            # 需要计算的字段
            "vwap": (None, True),  # quote_volume / volume
            "ret": (None, True),  # (close_price - open_price) / open_price
            # 新特征字段 (简化为三个核心特征)
            "fr": ("funding_rate", False),
            "oi": ("open_interest", False),
            "lsr": ("long_short_ratio", False),
        }

        # 定义需要导出的特征（按您指定的顺序 + 新特征）
        features = [
            "cls",
            "hgh",
            "low",
            "tnum",
            "opn",
            "amt",
            "tbvol",
            "tbamt",
            "vol",
            "vwap",
            "ret",
            "tsvol",
            "tsamt",
            # 新特征 (简化为三个核心特征)
            "fr",
            "oi",
            "lsr",
        ]

        # 获取时间戳范围内的所有唯一日期
        timestamps = df.index.get_level_values("timestamp")
        unique_dates = sorted(set(pd.Timestamp(ts).date() for ts in timestamps))

        # 并行处理每一天
        tasks = []
        for date in unique_dates:
            task = self._process_single_date_async(df, date, output_path, freq, features, FIELD_MAPPING)
            tasks.append(task)

        # 分批执行任务，避免过多并发
        batch_size = 5
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            await asyncio.gather(*batch)

            # 给系统一些时间进行垃圾回收
            if i + batch_size < len(tasks):
                await asyncio.sleep(0.1)

    async def _process_dataframe_for_export(
        self, df: pd.DataFrame, output_path: Path, freq: Freq, dates: pd.DatetimeIndex
    ) -> None:
        """处理DataFrame并导出为文件的异步辅助方法"""
        # 建立数据库字段名到短字段名的映射关系
        FIELD_MAPPING = {
            # 短字段名: (数据库字段名, 是否需要计算)
            "opn": ("open_price", False),
            "hgh": ("high_price", False),
            "low": ("low_price", False),
            "cls": ("close_price", False),
            "vol": ("volume", False),
            "amt": ("quote_volume", False),
            "tnum": ("trades_count", False),
            "tbvol": ("taker_buy_volume", False),
            "tbamt": ("taker_buy_quote_volume", False),
            "tsvol": ("taker_sell_volume", False),
            "tsamt": ("taker_sell_quote_volume", False),
            # 需要计算的字段
            "vwap": (None, True),  # quote_volume / volume
            "ret": (None, True),  # (close_price - open_price) / open_price
            # 新特征字段 (简化为三个核心特征)
            "fr": ("funding_rate", False),
            "oi": ("open_interest", False),
            "lsr": ("long_short_ratio", False),
        }

        # 定义需要导出的特征（按您指定的顺序 + 新特征）
        features = [
            "cls",
            "hgh",
            "low",
            "tnum",
            "opn",
            "amt",
            "tbvol",
            "tbamt",
            "vol",
            "vwap",
            "ret",
            "tsvol",
            "tsamt",
            # 新特征 (简化为三个核心特征)
            "fr",
            "oi",
            "lsr",
        ]

        # 并行处理每一天
        tasks = []
        for date in dates:
            task = self._process_single_date_async(df, date.date(), output_path, freq, features, FIELD_MAPPING)
            tasks.append(task)

        # 分批执行任务，避免过多并发
        batch_size = 5
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            await asyncio.gather(*batch)

            # 给系统一些时间进行垃圾回收
            if i + batch_size < len(tasks):
                await asyncio.sleep(0.1)

    async def _process_single_date_async(
        self,
        df: pd.DataFrame,
        date,
        output_path: Path,
        freq: Freq,
        features: List[str],
        field_mapping: dict,
    ) -> None:
        """异步处理单个日期的数据"""
        date_str = date.strftime("%Y%m%d")

        # 获取当天数据
        day_data = df[
            df.index.get_level_values("timestamp").map(
                lambda ts, current_date=date: pd.Timestamp(ts).date() == current_date
            )
        ]

        if day_data.empty:
            return

        # 在线程池中执行数据处理和文件保存
        loop = asyncio.get_event_loop()

        def save_symbols():
            # 保存交易对顺序
            symbols_path = output_path / freq.value / "symbols" / f"{date_str}.pkl"
            symbols_path.parent.mkdir(parents=True, exist_ok=True)
            pd.Series(df.index.get_level_values("symbol").unique()).to_pickle(symbols_path)

        await loop.run_in_executor(None, save_symbols)

        # 并行处理所有特征
        feature_tasks = []
        for short_name in features:
            if short_name in field_mapping:
                task = self._process_single_feature_async(
                    day_data, short_name, field_mapping, output_path, freq, date_str
                )
                feature_tasks.append(task)

        # 分批执行特征任务
        batch_size = 3
        for i in range(0, len(feature_tasks), batch_size):
            batch = feature_tasks[i : i + batch_size]
            await asyncio.gather(*batch)

    async def _process_single_feature_async(
        self,
        day_data: pd.DataFrame,
        short_name: str,
        field_mapping: dict,
        output_path: Path,
        freq: Freq,
        date_str: str,
    ) -> None:
        """异步处理单个特征的数据"""
        loop = asyncio.get_event_loop()

        def process_and_save():
            db_field, needs_calculation = field_mapping[short_name]

            if needs_calculation:
                # 计算衍生字段
                if short_name == "vwap":
                    # VWAP = quote_volume / volume
                    volume_data = day_data["volume"]
                    quote_volume_data = day_data["quote_volume"]
                    feature_data = quote_volume_data / volume_data
                    feature_data = feature_data.fillna(0)  # 处理除零情况
                elif short_name == "ret":
                    # 收益率 = (close_price - open_price) / open_price
                    open_data = day_data["open_price"]
                    close_data = day_data["close_price"]
                    feature_data = (close_data - open_data) / open_data
                    feature_data = feature_data.fillna(0)  # 处理除零情况
                else:
                    return  # 未知的计算字段
            else:
                # 直接从数据库字段获取
                if db_field not in day_data.columns:
                    return  # 跳过不存在的字段
                feature_data = day_data[db_field]

            # 重塑数据为 K x T 矩阵
            pivot_data = feature_data.unstack(level="timestamp")
            array = pivot_data.values

            # 创建存储路径 - 使用短字段名
            save_path = output_path / freq.value / short_name
            save_path.mkdir(parents=True, exist_ok=True)

            # 保存为npy格式
            np.save(save_path / f"{date_str}.npy", array)

        await loop.run_in_executor(None, process_and_save)
