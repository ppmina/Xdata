"""资金费率数据存储器.

专门处理资金费率数据的存储操作。
"""

from typing import TYPE_CHECKING, Any

from cryptoservice.config.logging import get_logger

if TYPE_CHECKING:
    from ..connection import ConnectionPool

logger = get_logger(__name__)


class FundingStore:
    """资金费率数据存储器."""

    def __init__(self, connection_pool: "ConnectionPool"):
        """初始化资金费率数据存储器.

        Args:
            connection_pool: 数据库连接池
        """
        self.pool = connection_pool

    async def insert(self, funding_rates: list[Any], batch_size: int = 1000) -> int:
        """插入资金费率数据.

        Args:
            funding_rates: 资金费率数据列表
            batch_size: 批量大小

        Returns:
            插入的记录数
        """
        if not funding_rates:
            logger.warning("没有资金费率数据需要插入")
            return 0

        # 转换为记录格式
        records = []
        for item in funding_rates:
            record = (
                item.symbol,
                item.funding_time,
                float(item.funding_rate),
                item.funding_time,
                float(item.mark_price) if hasattr(item, "mark_price") and item.mark_price else None,
                float(item.index_price) if hasattr(item, "index_price") and item.index_price else None,
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
            INSERT OR REPLACE INTO funding_rates (
                symbol, timestamp, funding_rate, funding_time, mark_price, index_price
            ) VALUES (?, ?, ?, ?, ?, ?)
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

        logger.info(f"资金费率数据插入完成: {total_inserted} 条记录")
        return total_inserted

    async def delete_by_time_range(self, symbols: list[str], start_time: str, end_time: str) -> int:
        """按时间范围删除数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            删除的记录数
        """
        import pandas as pd

        # 转换时间格式
        start_ts = int(pd.Timestamp(start_time).timestamp() * 1000) if isinstance(start_time, str) and not start_time.isdigit() else int(start_time)

        end_ts = int(pd.Timestamp(end_time).timestamp() * 1000) if isinstance(end_time, str) and not end_time.isdigit() else int(end_time)

        placeholders = ",".join("?" * len(symbols))
        delete_sql = f"""
            DELETE FROM funding_rates
            WHERE timestamp BETWEEN ? AND ?
            AND symbol IN ({placeholders})
        """  # noqa: S608

        params = [start_ts, end_ts] + symbols

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(delete_sql, params)
            deleted_count = cursor.rowcount
            await conn.commit()

        logger.info(f"删除资金费率数据: {deleted_count} 条记录")
        return deleted_count

    async def delete_by_symbol(self, symbol: str) -> int:
        """按交易对删除数据.

        Args:
            symbol: 交易对

        Returns:
            删除的记录数
        """
        delete_sql = "DELETE FROM funding_rates WHERE symbol = ?"

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(delete_sql, [symbol])
            deleted_count = cursor.rowcount
            await conn.commit()

        logger.info(f"删除交易对 {symbol} 的资金费率数据: {deleted_count} 条记录")
        return deleted_count

    async def count(self, symbol: str | None = None) -> int:
        """统计记录数量.

        Args:
            symbol: 交易对，None表示所有交易对

        Returns:
            记录数量
        """
        if symbol:
            count_sql = "SELECT COUNT(*) FROM funding_rates WHERE symbol = ?"
            params = [symbol]
        else:
            count_sql = "SELECT COUNT(*) FROM funding_rates"
            params = []

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(count_sql, params)
            result = await cursor.fetchone()
            return result[0] if result else 0

    async def exists(self, symbol: str, timestamp: int) -> bool:
        """检查记录是否存在.

        Args:
            symbol: 交易对
            timestamp: 时间戳

        Returns:
            是否存在
        """
        exists_sql = """
            SELECT 1 FROM funding_rates
            WHERE symbol = ? AND timestamp = ?
            LIMIT 1
        """

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(exists_sql, [symbol, timestamp])
            result = await cursor.fetchone()
            return result is not None
