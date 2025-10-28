"""指标数据查询器.

专门处理指标数据（资金费率、持仓量、多空比例）的查询操作。
"""

from typing import TYPE_CHECKING

import pandas as pd

from cryptoservice.config.logging import get_logger
from cryptoservice.utils.time_utils import date_to_timestamp_end, date_to_timestamp_start

from .builder import QueryBuilder

if TYPE_CHECKING:
    from ..connection import ConnectionPool

logger = get_logger(__name__)


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

    async def select_funding_rates(self, symbols: list[str], start_time: str, end_time: str, columns: list[str] | None = None) -> pd.DataFrame:
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

        logger.info("select_funding_rates_complete", records=len(df))
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

        builder = QueryBuilder.select("open_interests", query_columns).where(time_condition, *time_params).where(symbol_condition, *symbol_params)

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

        logger.info("select_open_interests_complete", records=len(df))
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

        builder = QueryBuilder.select("long_short_ratios", query_columns).where(time_condition, *time_params).where(symbol_condition, *symbol_params)

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

        logger.info("select_long_short_ratios_complete", records=len(df))
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

    async def get_missing_timestamps(self, data_type: str, symbol: str, start_ts: int, end_ts: int, interval_hours: int = 8) -> list[int]:
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
        # 资金费率特殊处理：不按固定频率检查，只检查时间范围覆盖
        if data_type == "funding_rate":
            return await self._check_funding_rate_missing(symbol, start_ts, end_ts)

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

        # 修正：如果 end_dt 是某天的 00:00:00，需要包括那一天的完整数据
        # 使用 time_utils 统一处理
        if end_dt.hour == 0 and end_dt.minute == 0 and end_dt.second == 0:
            date_str = end_dt.strftime("%Y-%m-%d")
            query_end_ts = date_to_timestamp_end(date_str)
            # 同时修正 end_dt 用于生成 time_range
            query_end_dt = pd.Timestamp(query_end_ts, unit="ms", tz="UTC")
        else:
            query_end_ts = end_ts
            query_end_dt = end_dt

        # 使用修正后的结束时间生成完整的时间戳范围
        freq_hours = interval_hours if interval_hours and interval_hours > 0 else 1
        freq_delta = pd.to_timedelta(freq_hours, unit="h")
        time_range = pd.date_range(start=start_dt, end=query_end_dt, freq=freq_delta, inclusive="left", tz="UTC")
        expected_count = len(time_range)

        # 快速检查：查询记录数（归一化到整秒，消除毫秒偏差）
        count_sql = (
            f"SELECT COUNT(DISTINCT timestamp / 1000) FROM {table_name} "  # noqa: S608
            "WHERE symbol = ? AND timestamp >= ? AND timestamp < ?"
        )

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(count_sql, (symbol, start_ts, query_end_ts))
            result = await cursor.fetchone()
            actual_count = result[0] if result else 0

        # 如果记录数严格等于预期数量，认为数据完整
        if expected_count > 0 and actual_count == expected_count:
            return []

        # 数据不完整，详细检查缺失的时间戳
        # 生成期望的时间戳（归一化到整秒）
        full_timestamps = {int(ts.timestamp()) for ts in time_range}

        # 查询现有的时间戳（归一化到整秒）
        sql = (
            f"SELECT DISTINCT timestamp / 1000 FROM {table_name} "  # noqa: S608
            "WHERE symbol = ? AND timestamp >= ? AND timestamp < ?"
        )

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(sql, (symbol, start_ts, query_end_ts))
            rows = await cursor.fetchall()

        # 归一化已有的时间戳（去掉毫秒部分）
        existing_timestamps = {int(row[0]) for row in rows}

        # 计算缺失的时间戳（秒级）
        missing_timestamps_sec = full_timestamps - existing_timestamps

        # 转换回毫秒级时间戳
        missing_timestamps = sorted([ts * 1000 for ts in missing_timestamps_sec])
        return missing_timestamps

    async def _check_funding_rate_missing(self, symbol: str, start_ts: int, end_ts: int) -> list[int]:
        """检查资金费率缺失情况（不依赖固定频率）.

        资金费率是事件数据，频率不固定（4h、8h或其他），所以不能按固定间隔检查。
        只检查时间范围是否有数据覆盖，如果完全覆盖则返回空列表。

        Args:
            symbol: 交易对
            start_ts: 开始时间戳（毫秒）
            end_ts: 结束时间戳（毫秒）

        Returns:
            如果需要下载返回 [start_ts, end_ts]，否则返回空列表
        """
        if start_ts >= end_ts:
            # 边界相同，直接返回单个起点以触发一次补全
            return [start_ts]

        # 查询现有数据的时间范围
        time_range = await self.get_metrics_time_range("funding_rate", symbol)

        if not time_range:
            # 没有任何历史数据时，下载整个请求区间
            return [start_ts, end_ts]

        earliest_ts = time_range["earliest_timestamp"]
        latest_ts = time_range["latest_timestamp"]

        # 检查是否需要扩展下载范围
        # 如果数据库中的数据完全覆盖请求范围，则不需要下载
        if earliest_ts <= start_ts and latest_ts >= end_ts:
            return []

        # 否则需要下载整个请求区间，确保规划覆盖完整范围
        segment_start = min(start_ts, end_ts)
        segment_end = max(start_ts, end_ts)

        return [segment_start, segment_end]

    async def get_daily_metrics_status(self, symbol: str, date_str: str) -> dict[str, int]:
        """获取指定日期指标数据的覆盖情况.

        Args:
            symbol: 交易对
            date_str: 日期字符串 (YYYY-MM-DD)

        Returns:
            包含 open_interest 和 long_short_ratio 计数的字典
        """
        start_ts = date_to_timestamp_start(date_str)
        end_ts = date_to_timestamp_end(date_str)

        queries = {
            "open_interest": ("SELECT COUNT(*) FROM open_interests WHERE symbol = ? AND timestamp >= ? AND timestamp < ?"),
            "long_short_ratio": ("SELECT COUNT(*) FROM long_short_ratios WHERE symbol = ? AND timestamp >= ? AND timestamp < ?"),
        }

        results: dict[str, int] = {"open_interest": 0, "long_short_ratio": 0}

        async with self.pool.get_connection() as conn:
            for key, sql in queries.items():
                cursor = await conn.execute(sql, (symbol, start_ts, end_ts))
                row = await cursor.fetchone()
                results[key] = row[0] if row else 0

        return results

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
