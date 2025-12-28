"""K线数据存储器.

专门处理K线数据的存储操作。
"""

from typing import TYPE_CHECKING

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq, PerpetualMarketTicker

if TYPE_CHECKING:
    from ..connection import ConnectionPool

logger = get_logger(__name__)


class KlineStore:
    """K线数据存储器.

    专注于K线数据的CRUD操作。
    """

    def __init__(self, connection_pool: "ConnectionPool"):
        """初始化K线数据存储器.

        Args:
            connection_pool: 数据库连接池
        """
        self.pool = connection_pool

    async def insert(self, klines: list[PerpetualMarketTicker], freq: Freq, batch_size: int = 1000) -> int:
        """插入K线数据.

        Args:
            klines: K线数据列表
            freq: 数据频率
            batch_size: 批量大小

        Returns:
            插入的记录数
        """
        if not klines:
            logger.warning("没有K线数据需要插入")
            return 0

        # 转换为记录格式
        records = []
        for kline in klines:
            record = (
                kline.symbol,
                kline.open_time,
                freq.value,
                kline.open_price,
                kline.high_price,
                kline.low_price,
                kline.close_price,
                kline.volume,
                kline.close_time,
                kline.quote_volume,
                kline.trades_count,
                kline.taker_buy_volume,
                kline.taker_buy_quote_volume,
                kline.volume - kline.taker_buy_volume,  # taker_sell_volume
                kline.quote_volume - kline.taker_buy_quote_volume,  # taker_sell_quote_volume
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
            INSERT OR REPLACE INTO klines (
                symbol, timestamp, freq,
                open_price, high_price, low_price, close_price,
                volume, close_time, quote_volume, trades_count,
                taker_buy_volume, taker_buy_quote_volume,
                taker_sell_volume, taker_sell_quote_volume
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        total_inserted = 0
        async with self.pool.get_connection() as conn:
            # 分批处理
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                await conn.executemany(insert_sql, batch)
                total_inserted += len(batch)

                if i + batch_size < len(records):
                    # 中间批次提交
                    await conn.commit()

            # 最终提交
            await conn.commit()

        symbol = records[0][0] if records else "unknown"
        freq = records[0][2] if records else "unknown"
        logger.info(f"K线数据插入完成: {total_inserted} 条记录 ({symbol}, {freq})")
        return total_inserted

    async def upsert(self, klines: list[PerpetualMarketTicker], freq: Freq) -> int:
        """插入或更新K线数据.

        使用INSERT OR REPLACE语义，与insert方法相同。

        Args:
            klines: K线数据列表
            freq: 数据频率

        Returns:
            处理的记录数
        """
        return await self.insert(klines, freq)

    async def delete_by_time_range(self, symbols: list[str], start_time: str, end_time: str, freq: Freq) -> int:
        """按时间范围删除数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD 或时间戳)
            end_time: 结束时间 (YYYY-MM-DD 或时间戳)
            freq: 数据频率

        Returns:
            删除的记录数
        """
        import pandas as pd

        # 转换时间格式
        start_ts = int(pd.Timestamp(start_time).timestamp() * 1000) if isinstance(start_time, str) and not start_time.isdigit() else int(start_time)

        end_ts = int(pd.Timestamp(end_time).timestamp() * 1000) if isinstance(end_time, str) and not end_time.isdigit() else int(end_time)

        placeholders = ",".join("?" * len(symbols))
        delete_sql = f"""
            DELETE FROM klines
            WHERE timestamp BETWEEN ? AND ?
            AND freq = ?
            AND symbol IN ({placeholders})
        """  # noqa: S608

        params = [start_ts, end_ts, freq.value] + symbols

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(delete_sql, params)
            deleted_count = cursor.rowcount
            await conn.commit()

        logger.info(f"删除K线数据: {deleted_count} 条记录")
        return deleted_count

    async def delete_by_symbol(self, symbol: str, freq: Freq | None = None) -> int:
        """按交易对删除数据.

        Args:
            symbol: 交易对
            freq: 数据频率，None表示删除所有频率

        Returns:
            删除的记录数
        """
        if freq:
            delete_sql = "DELETE FROM klines WHERE symbol = ? AND freq = ?"
            params = [symbol, freq.value]
        else:
            delete_sql = "DELETE FROM klines WHERE symbol = ?"
            params = [symbol]

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(delete_sql, params)
            deleted_count = cursor.rowcount
            await conn.commit()

        logger.info(f"删除交易对 {symbol} 的K线数据: {deleted_count} 条记录")
        return deleted_count

    async def count(self, symbol: str | None = None, freq: Freq | None = None) -> int:
        """统计记录数量.

        Args:
            symbol: 交易对，None表示所有交易对
            freq: 数据频率，None表示所有频率

        Returns:
            记录数量
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
        count_sql = f"SELECT COUNT(*) FROM klines{where_clause}"  # noqa: S608

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(count_sql, params)
            result = await cursor.fetchone()
            return result[0] if result else 0

    async def exists(self, symbol: str, timestamp: int, freq: Freq) -> bool:
        """检查记录是否存在.

        Args:
            symbol: 交易对
            timestamp: 时间戳
            freq: 数据频率

        Returns:
            是否存在
        """
        exists_sql = """
            SELECT 1 FROM klines
            WHERE symbol = ? AND timestamp = ? AND freq = ?
            LIMIT 1
        """

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(exists_sql, [symbol, timestamp, freq.value])
            result = await cursor.fetchone()
            return result is not None

    async def get_latest_timestamp(self, symbol: str, freq: Freq) -> int | None:
        """获取最新时间戳.

        Args:
            symbol: 交易对
            freq: 数据频率

        Returns:
            最新时间戳，如果没有数据则返回None
        """
        latest_sql = """
            SELECT MAX(timestamp) FROM klines
            WHERE symbol = ? AND freq = ?
        """

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(latest_sql, [symbol, freq.value])
            result = await cursor.fetchone()
            return result[0] if result and result[0] else None

    async def get_earliest_timestamp(self, symbol: str, freq: Freq) -> int | None:
        """获取最早时间戳.

        Args:
            symbol: 交易对
            freq: 数据频率

        Returns:
            最早时间戳，如果没有数据则返回None
        """
        earliest_sql = """
            SELECT MIN(timestamp) FROM klines
            WHERE symbol = ? AND freq = ?
        """

        async with self.pool.get_connection() as conn:
            cursor = await conn.execute(earliest_sql, [symbol, freq.value])
            result = await cursor.fetchone()
            return result[0] if result and result[0] else None
