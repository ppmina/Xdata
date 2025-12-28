"""K线数据查询器.

专门处理K线数据的查询操作。
"""

from typing import TYPE_CHECKING, Any

import pandas as pd

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq

from .builder import QueryBuilder

if TYPE_CHECKING:
    from ..connection import ConnectionPool

logger = get_logger(__name__)


class KlineQuery:
    """K线数据查询器.

    专注于K线数据的查询操作。
    """

    def __init__(self, connection_pool: "ConnectionPool"):
        """初始化K线数据查询器.

        Args:
            connection_pool: 数据库连接池
        """
        self.pool = connection_pool

    async def select_by_time_range(self, symbols: list[str], start_time: str, end_time: str, freq: Freq, columns: list[str] | None = None) -> pd.DataFrame:
        """按时间范围查询K线数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD 或时间戳)
            end_time: 结束时间 (YYYY-MM-DD 或时间戳)
            freq: 数据频率
            columns: 需要查询的列，None表示查询所有数据列

        Returns:
            包含K线数据的DataFrame，使用(symbol, timestamp)作为多级索引
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return pd.DataFrame()

        # 默认查询的数据列（不包括主键列）
        if columns is None:
            columns = [
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "close_time",
                "quote_volume",
                "trades_count",
                "taker_buy_volume",
                "taker_buy_quote_volume",
                "taker_sell_volume",
                "taker_sell_quote_volume",
            ]

        # 构建查询，包含索引列
        query_columns = ["symbol", "timestamp"] + columns

        # 使用查询构建器
        time_condition, time_params = QueryBuilder.build_time_filter(start_time, end_time)
        symbol_condition, symbol_params = QueryBuilder.build_symbol_filter(symbols)

        sql, params = (
            QueryBuilder.select("klines", query_columns)
            .where(time_condition, *time_params)
            .where("freq = ?", freq.value)
            .where(symbol_condition, *symbol_params)
            .order_by("symbol, timestamp")
            .build()
        )

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()

        if not rows:
            logger.info(f"未找到K线数据: {symbols}, {start_time} - {end_time}, {freq.value}")
            # 返回空DataFrame但保持正确的结构
            empty_df = pd.DataFrame(columns=query_columns)
            empty_df = empty_df.set_index(["symbol", "timestamp"])
            return empty_df

        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=query_columns)
        df = df.set_index(["symbol", "timestamp"])

        logger.info("select_by_time_range_complete", records=len(df))
        return df

    async def select_by_timestamp_range(self, symbols: list[str], start_ts: int, end_ts: int, freq: Freq, columns: list[str] | None = None) -> pd.DataFrame:
        """按时间戳范围查询K线数据.

        Args:
            symbols: 交易对列表
            start_ts: 开始时间戳 (毫秒)
            end_ts: 结束时间戳 (毫秒)
            freq: 数据频率
            columns: 需要查询的列

        Returns:
            包含K线数据的DataFrame
        """
        # 直接使用时间戳查询
        start_time = str(start_ts)
        end_time = str(end_ts)
        return await self.select_by_time_range(symbols, start_time, end_time, freq, columns)

    async def select_latest(self, symbols: list[str], freq: Freq, limit: int = 1) -> pd.DataFrame:
        """查询最新的K线数据.

        Args:
            symbols: 交易对列表
            freq: 数据频率
            limit: 每个交易对返回的记录数

        Returns:
            包含最新K线数据的DataFrame
        """
        if not symbols:
            return pd.DataFrame()

        # 为每个交易对查询最新数据
        all_data: list[Any] = []

        for symbol in symbols:
            sql, params = (
                QueryBuilder.select("klines").where("symbol = ?", symbol).where("freq = ?", freq.value).order_by("timestamp DESC").limit(limit).build()
            )

            async with self.pool.get_connection() as conn:
                cursor = await conn.execute(sql, params)
                rows = await cursor.fetchall()
                all_data.extend(rows)

        if not all_data:
            return pd.DataFrame()

        # 获取列名
        columns = [
            "symbol",
            "timestamp",
            "freq",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "close_time",
            "quote_volume",
            "trades_count",
            "taker_buy_volume",
            "taker_buy_quote_volume",
            "taker_sell_volume",
            "taker_sell_quote_volume",
        ]

        df = pd.DataFrame(all_data, columns=columns)
        df = df.set_index(["symbol", "timestamp"])

        # 移除freq列，因为它已经是查询条件
        df = df.drop("freq", axis=1)

        return df

    async def get_symbols(self, freq: Freq | None = None) -> list[str]:
        """获取所有交易对.

        Args:
            freq: 数据频率，None表示查询所有频率

        Returns:
            交易对列表
        """
        if freq:
            sql, params = QueryBuilder.select("klines", ["DISTINCT symbol"]).where("freq = ?", freq.value).order_by("symbol").build()
        else:
            sql, params = QueryBuilder.select("klines", ["DISTINCT symbol"]).order_by("symbol").build()

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()

        return [row[0] for row in rows]

    async def get_frequencies(self, symbol: str | None = None) -> list[str]:
        """获取所有频率.

        Args:
            symbol: 交易对，None表示查询所有交易对

        Returns:
            频率列表
        """
        if symbol:
            sql, params = QueryBuilder.select("klines", ["DISTINCT freq"]).where("symbol = ?", symbol).order_by("freq").build()
        else:
            sql, params = QueryBuilder.select("klines", ["DISTINCT freq"]).order_by("freq").build()

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()

        return [row[0] for row in rows]

    async def get_time_range(self, symbol: str, freq: Freq) -> dict:
        """获取指定交易对和频率的时间范围.

        Args:
            symbol: 交易对
            freq: 数据频率

        Returns:
            包含时间范围信息的字典
        """
        sql, params = (
            QueryBuilder.select(
                "klines",
                [
                    "MIN(timestamp) as earliest_timestamp",
                    "MAX(timestamp) as latest_timestamp",
                    "COUNT(*) as record_count",
                    "MIN(date(timestamp/1000, 'unixepoch')) as earliest_date",
                    "MAX(date(timestamp/1000, 'unixepoch')) as latest_date",
                ],
            )
            .where("symbol = ?", symbol)
            .where("freq = ?", freq.value)
            .build()
        )

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, params)
            result = await cursor.fetchone()

        if not result or result[2] == 0:  # record_count == 0
            return {}

        return {
            "earliest_timestamp": result[0],
            "latest_timestamp": result[1],
            "record_count": result[2],
            "earliest_date": result[3],
            "latest_date": result[4],
        }

    async def get_missing_timestamps(self, symbol: str, start_ts: int, end_ts: int, freq: Freq) -> list[int]:
        """获取缺失的时间戳.

        Args:
            symbol: 交易对
            start_ts: 开始时间戳
            end_ts: 结束时间戳
            freq: 数据频率

        Returns:
            缺失的时间戳列表
        """
        # 生成完整的时间戳范围
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

        start_dt = pd.Timestamp(start_ts, unit="ms", tz="UTC")
        end_dt = pd.Timestamp(end_ts, unit="ms", tz="UTC")

        time_range = pd.date_range(
            start=start_dt,
            end=end_dt,
            freq=freq_map.get(freq, "1h"),
            inclusive="left",  # 不包含结束时间
            tz="UTC",
        )
        full_timestamps = {int(ts.timestamp() * 1000) for ts in time_range}

        # 查询现有的时间戳
        sql, params = (
            QueryBuilder.select("klines", ["DISTINCT timestamp"])
            .where("symbol = ?", symbol)
            .where("freq = ?", freq.value)
            .where_between("timestamp", start_ts, end_ts)
            .build()
        )

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()

        existing_timestamps = {row[0] for row in rows}

        # 计算缺失的时间戳
        missing_timestamps = full_timestamps - existing_timestamps
        return sorted(missing_timestamps)

    async def get_data_summary(self, symbol: str | None = None, freq: Freq | None = None) -> dict:
        """获取数据概要统计.

        Args:
            symbol: 交易对，None表示所有交易对
            freq: 数据频率，None表示所有频率

        Returns:
            数据概要统计字典
        """
        conditions = []
        params = []

        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)

        if freq:
            conditions.append("freq = ?")
            params.append(freq.value)

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

        # 构建统计查询
        summary_sql = f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT freq) as unique_frequencies,
                MIN(timestamp) as earliest_timestamp,
                MAX(timestamp) as latest_timestamp,
                MIN(date(timestamp/1000, 'unixepoch')) as earliest_date,
                MAX(date(timestamp/1000, 'unixepoch')) as latest_date
            FROM klines{where_clause}
        """  # noqa: S608

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(summary_sql, params)
            result = await cursor.fetchone()

        if not result:
            return {}

        return {
            "total_records": result[0],
            "unique_symbols": result[1],
            "unique_frequencies": result[2],
            "earliest_timestamp": result[3],
            "latest_timestamp": result[4],
            "earliest_date": result[5],
            "latest_date": result[6],
        }
