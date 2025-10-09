"""指标数据查询器.

专门处理指标数据（资金费率、持仓量、多空比例）的查询操作。
"""

import logging
from typing import TYPE_CHECKING

import pandas as pd

from .builder import QueryBuilder

if TYPE_CHECKING:
    from ..connection import ConnectionPool

logger = logging.getLogger(__name__)


class MetricsQuery:
    """指标数据查询器.

    专注于指标数据的查询操作。
    """

    def __init__(self, connection_pool: "ConnectionPool"):
        """初始化指标数据查询器.

        Args:
            connection_pool: 数据库连接池
        """
        self.pool = connection_pool

    async def select_funding_rates(
        self, symbols: list[str], start_time: str, end_time: str, columns: list[str] | None = None
    ) -> pd.DataFrame:
        """查询资金费率数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            columns: 需要查询的列

        Returns:
            包含资金费率数据的DataFrame
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return pd.DataFrame()

        # 默认查询的数据列
        if columns is None:
            columns = ["funding_rate", "funding_time", "mark_price", "index_price"]

        # 构建查询，包含索引列
        query_columns = ["symbol", "timestamp"] + columns

        # 使用查询构建器
        time_condition, time_params = QueryBuilder.build_time_filter(start_time, end_time)
        symbol_condition, symbol_params = QueryBuilder.build_symbol_filter(symbols)

        sql, params = (
            QueryBuilder.select("funding_rates", query_columns)
            .where(time_condition, *time_params)
            .where(symbol_condition, *symbol_params)
            .order_by("symbol, timestamp")
            .build()
        )

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()

        if not rows:
            logger.info(f"未找到资金费率数据: {symbols}, {start_time} - {end_time}")
            empty_df = pd.DataFrame(columns=query_columns)
            empty_df = empty_df.set_index(["symbol", "timestamp"])
            return empty_df

        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=query_columns)
        df = df.set_index(["symbol", "timestamp"])

        logger.info(f"查询资金费率数据完成: {len(df)} 条记录")
        return df

    async def select_open_interests(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        interval: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """查询持仓量数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            interval: 时间间隔，None表示所有间隔
            columns: 需要查询的列

        Returns:
            包含持仓量数据的DataFrame
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return pd.DataFrame()

        # 默认查询的数据列
        if columns is None:
            columns = ["interval", "open_interest", "open_interest_value"]

        # 构建查询，包含索引列
        query_columns = ["symbol", "timestamp"] + columns

        # 使用查询构建器
        time_condition, time_params = QueryBuilder.build_time_filter(start_time, end_time)
        symbol_condition, symbol_params = QueryBuilder.build_symbol_filter(symbols)

        builder = (
            QueryBuilder.select("open_interests", query_columns)
            .where(time_condition, *time_params)
            .where(symbol_condition, *symbol_params)
        )

        if interval:
            builder = builder.where("interval = ?", interval)

        sql, params = builder.order_by("symbol, timestamp").build()

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()

        if not rows:
            logger.info(f"未找到持仓量数据: {symbols}, {start_time} - {end_time}")
            empty_df = pd.DataFrame(columns=query_columns)
            empty_df = empty_df.set_index(["symbol", "timestamp"])
            return empty_df

        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=query_columns)
        df = df.set_index(["symbol", "timestamp"])

        logger.info(f"查询持仓量数据完成: {len(df)} 条记录")
        return df

    async def select_long_short_ratios(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        period: str | None = None,
        ratio_type: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """查询多空比例数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            period: 时间周期，None表示所有周期
            ratio_type: 比例类型，None表示所有类型
            columns: 需要查询的列

        Returns:
            包含多空比例数据的DataFrame
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return pd.DataFrame()

        # 默认查询的数据列
        if columns is None:
            columns = ["period", "ratio_type", "long_short_ratio", "long_account", "short_account"]

        # 构建查询，包含索引列
        query_columns = ["symbol", "timestamp"] + columns

        # 使用查询构建器
        time_condition, time_params = QueryBuilder.build_time_filter(start_time, end_time)
        symbol_condition, symbol_params = QueryBuilder.build_symbol_filter(symbols)

        builder = (
            QueryBuilder.select("long_short_ratios", query_columns)
            .where(time_condition, *time_params)
            .where(symbol_condition, *symbol_params)
        )

        if period:
            builder = builder.where("period = ?", period)

        if ratio_type:
            builder = builder.where("ratio_type = ?", ratio_type)

        sql, params = builder.order_by("symbol, timestamp").build()

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()

        if not rows:
            logger.info(f"未找到多空比例数据: {symbols}, {start_time} - {end_time}")
            empty_df = pd.DataFrame(columns=query_columns)
            empty_df = empty_df.set_index(["symbol", "timestamp"])
            return empty_df

        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=query_columns)
        df = df.set_index(["symbol", "timestamp"])

        logger.info(f"查询多空比例数据完成: {len(df)} 条记录")
        return df

    async def get_metrics_symbols(self, data_type: str) -> list[str]:
        """获取指标数据的所有交易对.

        Args:
            data_type: 数据类型 ('funding_rate', 'open_interest', 'long_short_ratio')

        Returns:
            交易对列表
        """
        table_map = {
            "funding_rate": "funding_rates",
            "open_interest": "open_interests",
            "long_short_ratio": "long_short_ratios",
        }

        table_name = table_map.get(data_type)
        if not table_name:
            raise ValueError(f"不支持的数据类型: {data_type}")

        sql, params = QueryBuilder.select(table_name, ["DISTINCT symbol"]).order_by("symbol").build()

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()

        return [row[0] for row in rows]

    async def get_metrics_time_range(self, data_type: str, symbol: str) -> dict:
        """获取指标数据的时间范围.

        Args:
            data_type: 数据类型
            symbol: 交易对

        Returns:
            包含时间范围信息的字典
        """
        table_map = {
            "funding_rate": "funding_rates",
            "open_interest": "open_interests",
            "long_short_ratio": "long_short_ratios",
        }

        table_name = table_map.get(data_type)
        if not table_name:
            raise ValueError(f"不支持的数据类型: {data_type}")

        sql, params = (
            QueryBuilder.select(
                table_name,
                [
                    "MIN(timestamp) as earliest_timestamp",
                    "MAX(timestamp) as latest_timestamp",
                    "COUNT(*) as record_count",
                    "MIN(date(timestamp/1000, 'unixepoch')) as earliest_date",
                    "MAX(date(timestamp/1000, 'unixepoch')) as latest_date",
                ],
            )
            .where("symbol = ?", symbol)
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

    async def get_missing_timestamps(
        self, data_type: str, symbol: str, start_ts: int, end_ts: int, interval_hours: int = 8
    ) -> list[int]:
        """获取指标数据缺失的时间戳.

        Args:
            data_type: 数据类型
            symbol: 交易对
            start_ts: 开始时间戳
            end_ts: 结束时间戳
            interval_hours: 数据间隔小时数，默认8小时（资金费率）

        Returns:
            缺失的时间戳列表
        """
        table_map = {
            "funding_rate": "funding_rates",
            "open_interest": "open_interests",
            "long_short_ratio": "long_short_ratios",
        }

        table_name = table_map.get(data_type)
        if not table_name:
            raise ValueError(f"不支持的数据类型: {data_type}")

        # 生成完整的时间戳范围（基于间隔，使用 UTC 时区）
        start_dt = pd.Timestamp(start_ts, unit="ms", tz="UTC")
        end_dt = pd.Timestamp(end_ts, unit="ms", tz="UTC")

        time_range = pd.date_range(start=start_dt, end=end_dt, freq=f"{interval_hours}h", inclusive="left", tz="UTC")
        full_timestamps = {int(ts.timestamp() * 1000) for ts in time_range}

        # 查询现有的时间戳
        sql, params = (
            QueryBuilder.select(table_name, ["DISTINCT timestamp"])
            .where("symbol = ?", symbol)
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

    async def get_metrics_summary(self, data_type: str, symbol: str | None = None) -> dict:
        """获取指标数据概要统计.

        Args:
            data_type: 数据类型
            symbol: 交易对，None表示所有交易对

        Returns:
            数据概要统计字典
        """
        table_map = {
            "funding_rate": "funding_rates",
            "open_interest": "open_interests",
            "long_short_ratio": "long_short_ratios",
        }

        table_name = table_map.get(data_type)
        if not table_name:
            raise ValueError(f"不支持的数据类型: {data_type}")

        builder = QueryBuilder.select(
            table_name,
            [
                "COUNT(*) as total_records",
                "COUNT(DISTINCT symbol) as unique_symbols",
                "MIN(timestamp) as earliest_timestamp",
                "MAX(timestamp) as latest_timestamp",
                "MIN(date(timestamp/1000, 'unixepoch')) as earliest_date",
                "MAX(date(timestamp/1000, 'unixepoch')) as latest_date",
            ],
        )

        if symbol:
            builder = builder.where("symbol = ?", symbol)

        sql, params = builder.build()

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, params)
            result = await cursor.fetchone()

        if not result:
            return {}

        return {
            "data_type": data_type,
            "total_records": result[0],
            "unique_symbols": result[1],
            "earliest_timestamp": result[2],
            "latest_timestamp": result[3],
            "earliest_date": result[4],
            "latest_date": result[5],
        }
