"""多空比例数据存储器.

专门处理多空比例数据的存储操作。
"""

from typing import TYPE_CHECKING, Any

from cryptoservice.config.logging import get_logger

if TYPE_CHECKING:
    from ..connection import ConnectionPool

logger = get_logger(__name__)


class RatioStore:
    """多空比例数据存储器."""

    def __init__(self, connection_pool: "ConnectionPool"):
        """初始化多空比例数据存储器.

        Args:
            connection_pool: 数据库连接池
        """
        self.pool = connection_pool

    async def insert(self, long_short_ratios: list[Any], batch_size: int = 1000) -> int:
        """插入多空比例数据.

        Args:
            long_short_ratios: 多空比例数据列表
            batch_size: 批量大小

        Returns:
            插入的记录数
        """
        if not long_short_ratios:
            logger.warning("没有多空比例数据需要插入")
            return 0

        # 转换为记录格式
        records = []
        for item in long_short_ratios:
            record = (
                item.symbol,
                item.timestamp,
                getattr(item, "period", "5m"),  # 默认5分钟周期
                getattr(item, "ratio_type", "account"),  # 默认账户类型
                float(item.long_short_ratio),
                float(item.long_account) if hasattr(item, "long_account") and item.long_account else None,
                float(item.short_account) if hasattr(item, "short_account") and item.short_account else None,
            )
            records.append(record)

        return await self.insert_batch(records, batch_size)

    async def insert_batch(self, records: list[tuple], batch_size: int = 1000) -> int:
        """批量插入记录.

        Args:
            records: 记录元组列表
            batch_size: 批量大小

        Returns:
            插入的记录数
        """
        if not records:
            return 0

        insert_sql = """
            INSERT OR REPLACE INTO long_short_ratios (
                symbol, timestamp, period, ratio_type, long_short_ratio, long_account, short_account
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        total_inserted = 0
        async with self.pool.get_connection() as conn:
            # 分批处理
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                await conn.executemany(insert_sql, batch)
                total_inserted += len(batch)

                if i + batch_size < len(records):
                    await conn.commit()

            await conn.commit()

        logger.info(f"多空比例数据插入完成: {total_inserted} 条记录")
        return total_inserted

    async def delete_by_time_range(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        period: str | None = None,
        ratio_type: str | None = None,
    ) -> int:
        """按时间范围删除数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            period: 时间周期，None表示所有周期
            ratio_type: 比例类型，None表示所有类型

        Returns:
            删除的记录数
        """
        import pandas as pd

        # 转换时间格式
        start_ts = int(pd.Timestamp(start_time).timestamp() * 1000) if isinstance(start_time, str) and not start_time.isdigit() else int(start_time)

        end_ts = int(pd.Timestamp(end_time).timestamp() * 1000) if isinstance(end_time, str) and not end_time.isdigit() else int(end_time)

        conditions = ["timestamp BETWEEN ? AND ?"]
        params: list[Any] = [start_ts, end_ts]

        if symbols:
            placeholders = ",".join("?" * len(symbols))
            conditions.append(f"symbol IN ({placeholders})")
            params.extend(symbols)

        if period:
            conditions.append("period = ?")
            params.append(period)

        if ratio_type:
            conditions.append("ratio_type = ?")
            params.append(ratio_type)

        delete_sql = f"DELETE FROM long_short_ratios WHERE {' AND '.join(conditions)}"  # noqa: S608

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(delete_sql, params)
            deleted_count = cursor.rowcount
            await conn.commit()

        logger.info(f"删除多空比例数据: {deleted_count} 条记录")
        return deleted_count

    async def delete_by_symbol(self, symbol: str, period: str | None = None, ratio_type: str | None = None) -> int:
        """按交易对删除数据.

        Args:
            symbol: 交易对
            period: 时间周期，None表示所有周期
            ratio_type: 比例类型，None表示所有类型

        Returns:
            删除的记录数
        """
        conditions = ["symbol = ?"]
        params = [symbol]

        if period:
            conditions.append("period = ?")
            params.append(period)

        if ratio_type:
            conditions.append("ratio_type = ?")
            params.append(ratio_type)

        delete_sql = f"DELETE FROM long_short_ratios WHERE {' AND '.join(conditions)}"  # noqa: S608

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(delete_sql, params)
            deleted_count = cursor.rowcount
            await conn.commit()

        logger.info(f"删除交易对 {symbol} 的多空比例数据: {deleted_count} 条记录")
        return deleted_count

    async def count(self, symbol: str | None = None, period: str | None = None, ratio_type: str | None = None) -> int:
        """统计记录数量.

        Args:
            symbol: 交易对，None表示所有交易对
            period: 时间周期，None表示所有周期
            ratio_type: 比例类型，None表示所有类型

        Returns:
            记录数量
        """
        conditions = []
        params = []

        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)

        if period:
            conditions.append("period = ?")
            params.append(period)

        if ratio_type:
            conditions.append("ratio_type = ?")
            params.append(ratio_type)

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        count_sql = f"SELECT COUNT(*) FROM long_short_ratios{where_clause}"  # noqa: S608

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(count_sql, params)
            result = await cursor.fetchone()
            return result[0] if result else 0

    async def exists(self, symbol: str, timestamp: int, period: str = "5m", ratio_type: str = "account") -> bool:
        """检查记录是否存在.

        Args:
            symbol: 交易对
            timestamp: 时间戳
            period: 时间周期
            ratio_type: 比例类型

        Returns:
            是否存在
        """
        exists_sql = """
            SELECT 1 FROM long_short_ratios
            WHERE symbol = ? AND timestamp = ? AND period = ? AND ratio_type = ?
            LIMIT 1
        """

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(exists_sql, [symbol, timestamp, period, ratio_type])
            result = await cursor.fetchone()
            return result is not None
