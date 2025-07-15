import datetime
import logging
import queue
import sqlite3
import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Any, TypeGuard

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table

from cryptoservice.models import Freq, KlineIndex, PerpetualMarketTicker

logger = logging.getLogger(__name__)


class DatabaseConnectionPool:
    """线程安全的数据库连接池实现"""

    def __init__(self, db_path: Path | str, max_connections: int = 5):
        """初始化连接池

        Args:
            db_path: 数据库文件路径
            max_connections: 每个线程的最大连接数
        """
        self.db_path = Path(db_path)
        self.max_connections = max_connections
        self._local = threading.local()  # 线程本地存储
        self._lock = threading.Lock()

    def _init_thread_connections(self) -> None:
        """初始化当前线程的连接队列"""
        if not hasattr(self._local, "connections"):
            self._local.connections = queue.Queue(maxsize=self.max_connections)
            for _ in range(self.max_connections):
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row
                self._local.connections.put(conn)

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """获取当前线程的数据库连接"""
        self._init_thread_connections()
        connection = self._local.connections.get()
        try:
            yield connection
        finally:
            self._local.connections.put(connection)

    def close_all(self) -> None:
        """关闭所有连接"""
        if hasattr(self._local, "connections"):
            while not self._local.connections.empty():
                connection = self._local.connections.get()
                connection.close()


class MarketDB:
    """市场数据库管理类，专注于存储和读取交易对数据."""

    def __init__(self, db_path: Path | str, use_pool: bool = False, max_connections: int = 5):
        """初始化 MarketDB.

        Args:
            db_path: 数据库文件路径
            use_pool: 是否使用连接池
            max_connections: 连接池最大连接数
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 连接池相关
        self._use_pool = use_pool
        self._pool = DatabaseConnectionPool(self.db_path, max_connections) if use_pool else None

        # 初始化数据库
        self._init_db()

    def _init_db(self) -> None:
        """初始化数据库表结构"""
        with sqlite3.connect(self.db_path) as conn:
            # 原有的market_data表
            conn.execute(
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
            conn.execute(
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
            conn.execute(
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
            conn.execute(
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON market_data(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON market_data(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_freq ON market_data(freq)")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_symbol_freq_timestamp
                ON market_data(symbol, freq, timestamp)
                """
            )

            # 新特征表的索引
            conn.execute("CREATE INDEX IF NOT EXISTS idx_funding_symbol ON funding_rate(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_funding_timestamp ON funding_rate(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_oi_symbol ON open_interest(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_oi_timestamp ON open_interest(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lsr_symbol ON long_short_ratio(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lsr_timestamp ON long_short_ratio(timestamp)")

    @contextmanager
    def _get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """获取数据库连接的内部实现"""
        if self._use_pool and self._pool is not None:
            with self._pool.get_connection() as conn:
                yield conn
        else:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.close()

    def store_data(
        self,
        data: list[PerpetualMarketTicker] | list[list[PerpetualMarketTicker]],
        freq: Freq,
    ) -> None:
        """存储市场数据.

        Args:
            data: 市场数据列表，可以是单层列表或嵌套列表
            freq: 数据频率
        """
        try:
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

            # 根据数据结构类型进行处理
            if is_flat_list(data):
                # 单层列表情况 - 不需要cast，TypeGuard已经确保了类型
                flattened_data = data
            elif is_nested_list(data):
                # 嵌套列表情况 - 不需要额外的类型检查，TypeGuard已经确保了类型
                flattened_data = [item for sublist in data for item in sublist]
            else:
                raise TypeError(f"Unexpected data structure: {type(data).__name__}")

            if not flattened_data:
                logger.warning("No data to store")
                return

            records = []
            for ticker in flattened_data:
                volume = float(ticker.raw_data[KlineIndex.VOLUME])
                quote_volume = float(ticker.raw_data[KlineIndex.QUOTE_VOLUME])
                taker_buy_volume = float(ticker.raw_data[KlineIndex.TAKER_BUY_VOLUME])
                taker_buy_quote_volume = float(ticker.raw_data[KlineIndex.TAKER_BUY_QUOTE_VOLUME])

                record = {
                    "symbol": ticker.symbol,
                    "timestamp": ticker.open_time,
                    "freq": freq.value,
                    "open_price": ticker.raw_data[KlineIndex.OPEN],
                    "high_price": ticker.raw_data[KlineIndex.HIGH],
                    "low_price": ticker.raw_data[KlineIndex.LOW],
                    "close_price": ticker.raw_data[KlineIndex.CLOSE],
                    "volume": volume,
                    "quote_volume": quote_volume,
                    "trades_count": ticker.raw_data[KlineIndex.TRADES_COUNT],
                    "taker_buy_volume": taker_buy_volume,
                    "taker_buy_quote_volume": taker_buy_quote_volume,
                    "taker_sell_volume": str(volume - taker_buy_volume),
                    "taker_sell_quote_volume": str(quote_volume - taker_buy_quote_volume),
                }
                records.append(record)

            with self._get_connection() as conn:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO market_data (
                        symbol, timestamp, freq,
                        open_price, high_price, low_price, close_price,
                        volume, quote_volume, trades_count,
                        taker_buy_volume, taker_buy_quote_volume,
                        taker_sell_volume, taker_sell_quote_volume
                    ) VALUES (
                        :symbol, :timestamp, :freq,
                        :open_price, :high_price, :low_price, :close_price,
                        :volume, :quote_volume, :trades_count,
                        :taker_buy_volume, :taker_buy_quote_volume,
                        :taker_sell_volume, :taker_sell_quote_volume
                    )
                """,
                    records,
                )
                conn.commit()  # 确保数据被写入

                # 添加写入完成的日志
                symbol = records[0]["symbol"] if records else "unknown"
                logger.info(f"Successfully stored {len(records)} records for {symbol} with frequency {freq.value}")

        except Exception:
            logger.exception("Failed to store market data")
            raise

    def store_funding_rate(self, data: list) -> None:
        """存储资金费率数据.

        Args:
            data: FundingRate对象列表
        """
        try:
            if not data:
                logger.warning("No funding rate data to store")
                return

            records = []
            for item in data:
                record = {
                    "symbol": item.symbol,
                    "timestamp": item.funding_time,
                    "funding_rate": float(item.funding_rate),
                    "funding_time": item.funding_time,
                    "mark_price": float(item.mark_price) if item.mark_price else None,
                    "index_price": (float(item.index_price) if item.index_price else None),
                }
                records.append(record)

            with self._get_connection() as conn:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO funding_rate (
                        symbol, timestamp, funding_rate, funding_time, mark_price, index_price
                    ) VALUES (
                        :symbol, :timestamp, :funding_rate, :funding_time, :mark_price, :index_price
                    )
                    """,
                    records,
                )
                conn.commit()

            logger.info(f"Successfully stored {len(records)} funding rate records")

        except Exception:
            logger.exception("Failed to store funding rate data")
            raise

    def store_open_interest(self, data: list) -> None:
        """存储持仓量数据.

        Args:
            data: OpenInterest对象列表
        """
        try:
            if not data:
                logger.warning("No open interest data to store")
                return

            records = []
            for item in data:
                record = {
                    "symbol": item.symbol,
                    "timestamp": item.time,
                    "interval": getattr(item, "interval", "5m"),  # 默认5m间隔
                    "open_interest": float(item.open_interest),
                    "open_interest_value": (float(item.open_interest_value) if item.open_interest_value else None),
                }
                records.append(record)

            with self._get_connection() as conn:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO open_interest (
                        symbol, timestamp, interval, open_interest, open_interest_value
                    ) VALUES (
                        :symbol, :timestamp, :interval, :open_interest, :open_interest_value
                    )
                    """,
                    records,
                )
                conn.commit()

            logger.info(f"Successfully stored {len(records)} open interest records")

        except Exception:
            logger.exception("Failed to store open interest data")
            raise

    def store_long_short_ratio(self, data: list) -> None:
        """存储多空比例数据.

        Args:
            data: LongShortRatio对象列表
        """
        try:
            if not data:
                logger.warning("No long short ratio data to store")
                return

            records = []
            for item in data:
                record = {
                    "symbol": item.symbol,
                    "timestamp": item.timestamp,
                    "period": getattr(item, "period", "5m"),  # 默认5m周期
                    "ratio_type": item.ratio_type,
                    "long_short_ratio": float(item.long_short_ratio),
                    "long_account": (float(item.long_account) if item.long_account else None),
                    "short_account": (float(item.short_account) if item.short_account else None),
                }
                records.append(record)

            with self._get_connection() as conn:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO long_short_ratio (
                        symbol, timestamp, period, ratio_type, long_short_ratio, long_account, short_account
                    ) VALUES (
                        :symbol, :timestamp, :period, :ratio_type, :long_short_ratio, :long_account, :short_account
                    )
                    """,
                    records,
                )
                conn.commit()

            logger.info(f"Successfully stored {len(records)} long short ratio records")

        except Exception:
            logger.exception("Failed to store long short ratio data")
            raise

    def read_data(
        self,
        start_time: str,
        end_time: str,
        freq: Freq,
        symbols: list[str],
        features: list[str] | None = None,
        raise_on_empty: bool = True,
    ) -> pd.DataFrame:
        """读取市场数据.

        Args:
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            freq: 数据频率
            symbols: 交易对列表
            features: 需要读取的特征列表，None表示读取所有特征
            raise_on_empty: 当没有数据时是否抛出异常，False则返回空DataFrame

        Returns:
            pd.DataFrame: 市场数据，多级索引 (symbol, timestamp)
        """
        try:
            # 转换时间格式
            start_ts = int(pd.Timestamp(start_time).timestamp() * 1000)
            end_ts = int(pd.Timestamp(end_time).timestamp() * 1000)

            return self._read_data_by_timestamp(start_ts, end_ts, freq, symbols, features, raise_on_empty)

        except Exception:
            logger.exception("Failed to read market data")
            raise

    def read_data_by_timestamp(
        self,
        start_ts: int | str,
        end_ts: int | str,
        freq: Freq,
        symbols: list[str],
        features: list[str] | None = None,
        raise_on_empty: bool = True,
    ) -> pd.DataFrame:
        """使用时间戳读取市场数据.

        Args:
            start_ts: 开始时间戳 (毫秒，int或str)
            end_ts: 结束时间戳 (毫秒，int或str)
            freq: 数据频率
            symbols: 交易对列表
            features: 需要读取的特征列表，None表示读取所有特征
            raise_on_empty: 当没有数据时是否抛出异常，False则返回空DataFrame

        Returns:
            pd.DataFrame: 市场数据，多级索引 (symbol, timestamp)
        """
        try:
            # 确保时间戳为整数
            start_timestamp = int(start_ts)
            end_timestamp = int(end_ts)

            return self._read_data_by_timestamp(start_timestamp, end_timestamp, freq, symbols, features, raise_on_empty)

        except Exception:
            logger.exception("Failed to read market data by timestamp")
            raise

    def read_funding_rate(
        self,
        start_time: str,
        end_time: str,
        symbols: list[str],
        raise_on_empty: bool = True,
    ) -> pd.DataFrame:
        """读取资金费率数据.

        Args:
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            symbols: 交易对列表
            raise_on_empty: 当没有数据时是否抛出异常

        Returns:
            pd.DataFrame: 资金费率数据
        """
        try:
            # 转换时间格式
            start_ts = int(pd.Timestamp(start_time).timestamp() * 1000)
            end_ts = int(pd.Timestamp(end_time).timestamp() * 1000)

            query = """
                SELECT symbol, timestamp, funding_rate, funding_time, mark_price, index_price
                FROM funding_rate
                WHERE timestamp BETWEEN ? AND ?
                AND symbol IN ({})
                ORDER BY symbol, timestamp
            """.format(",".join("?" * len(symbols)))

            params = [start_ts, end_ts] + symbols

            with self._get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=tuple(params), parse_dates={"timestamp": "ms"})

            if df.empty:
                if raise_on_empty:
                    raise ValueError("No funding rate data found for the specified criteria")
                else:
                    empty_df = pd.DataFrame(
                        columns=[
                            "symbol",
                            "timestamp",
                            "funding_rate",
                            "funding_time",
                            "mark_price",
                            "index_price",
                        ]
                    )
                    empty_df = empty_df.set_index(["symbol", "timestamp"])
                    return empty_df

            df = df.set_index(["symbol", "timestamp"])
            return df

        except Exception:
            logger.exception("Failed to read funding rate data")
            raise

    def read_open_interest(
        self,
        start_time: str,
        end_time: str,
        symbols: list[str],
        interval: str = "5m",
        raise_on_empty: bool = True,
    ) -> pd.DataFrame:
        """读取持仓量数据.

        Args:
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            symbols: 交易对列表
            interval: 时间间隔
            raise_on_empty: 当没有数据时是否抛出异常

        Returns:
            pd.DataFrame: 持仓量数据
        """
        try:
            # 转换时间格式
            start_ts = int(pd.Timestamp(start_time).timestamp() * 1000)
            end_ts = int(pd.Timestamp(end_time).timestamp() * 1000)

            query = """
                SELECT symbol, timestamp, interval, open_interest, open_interest_value
                FROM open_interest
                WHERE timestamp BETWEEN ? AND ?
                AND interval = ?
                AND symbol IN ({})
                ORDER BY symbol, timestamp
            """.format(",".join("?" * len(symbols)))

            params: list[Any] = [start_ts, end_ts, interval] + symbols

            with self._get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params, parse_dates={"timestamp": "ms"})

            if df.empty:
                if raise_on_empty:
                    raise ValueError("No open interest data found for the specified criteria")
                else:
                    empty_df = pd.DataFrame(
                        columns=[
                            "symbol",
                            "timestamp",
                            "interval",
                            "open_interest",
                            "open_interest_value",
                        ]
                    )
                    empty_df = empty_df.set_index(["symbol", "timestamp"])
                    return empty_df

            df = df.set_index(["symbol", "timestamp"])
            return df

        except Exception:
            logger.exception("Failed to read open interest data")
            raise

    def read_long_short_ratio(
        self,
        start_time: str,
        end_time: str,
        symbols: list[str],
        period: str = "5m",
        ratio_type: str = "account",
        raise_on_empty: bool = True,
    ) -> pd.DataFrame:
        """读取多空比例数据.

        Args:
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            symbols: 交易对列表
            period: 时间周期
            ratio_type: 比例类型
            raise_on_empty: 当没有数据时是否抛出异常

        Returns:
            pd.DataFrame: 多空比例数据
        """
        try:
            # 转换时间格式
            start_ts = int(pd.Timestamp(start_time).timestamp() * 1000)
            end_ts = int(pd.Timestamp(end_time).timestamp() * 1000)

            query = """
                SELECT symbol, timestamp, period, ratio_type, long_short_ratio, long_account, short_account
                FROM long_short_ratio
                WHERE timestamp BETWEEN ? AND ?
                AND period = ?
                AND ratio_type = ?
                AND symbol IN ({})
                ORDER BY symbol, timestamp
            """.format(",".join("?" * len(symbols)))

            params: list[Any] = [start_ts, end_ts, period, ratio_type] + symbols

            with self._get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params, parse_dates={"timestamp": "ms"})

            if df.empty:
                if raise_on_empty:
                    raise ValueError("No long short ratio data found for the specified criteria")
                else:
                    empty_df = pd.DataFrame(
                        columns=[
                            "symbol",
                            "timestamp",
                            "period",
                            "ratio_type",
                            "long_short_ratio",
                            "long_account",
                            "short_account",
                        ]
                    )
                    empty_df = empty_df.set_index(["symbol", "timestamp"])
                    return empty_df

            df = df.set_index(["symbol", "timestamp"])
            return df

        except Exception:
            logger.exception("Failed to read long short ratio data")
            raise

    def _read_data_by_timestamp(
        self,
        start_ts: int,
        end_ts: int,
        freq: Freq,
        symbols: list[str],
        features: list[str] | None = None,
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
        query = f"""
            SELECT symbol, timestamp, {columns}
            FROM market_data
            WHERE timestamp BETWEEN ? AND ?
            AND freq = ?
            AND symbol IN ({",".join("?" * len(symbols))})
            ORDER BY symbol, timestamp
        """
        params = [start_ts, end_ts, freq.value] + symbols

        # 执行查询
        with self._get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params, parse_dates={"timestamp": "ms"})

        if df.empty:
            if raise_on_empty:
                raise ValueError("No data found for the specified criteria")
            else:
                # 返回空的DataFrame，但保持正确的结构
                empty_df = pd.DataFrame(columns=["symbol", "timestamp"] + features)
                empty_df = empty_df.set_index(["symbol", "timestamp"])
                return empty_df

        # 设置多级索引
        df = df.set_index(["symbol", "timestamp"])
        return df

    def get_available_dates(
        self,
        symbol: str,
        freq: Freq,
    ) -> list[str]:
        """获取指定交易对的可用日期列表.

        Args:
            symbol: 交易对
            freq: 数据频率

        Returns:
            List[str]: 可用日期列表 (YYYY-MM-DD格式)
        """
        try:
            with self._get_connection() as conn:
                query = """
                    SELECT DISTINCT date(timestamp/1000, 'unixepoch') as date
                    FROM market_data
                    WHERE symbol = ? AND freq = ?
                    ORDER BY date
                """
                cursor = conn.execute(query, (symbol, freq.value))
                return [row[0] for row in cursor.fetchall()]

        except Exception:
            logger.exception("Failed to get available dates")
            raise

    def export_to_files_by_timestamp(
        self,
        output_path: Path | str,
        start_ts: int | str,
        end_ts: int | str,
        freq: Freq,
        symbols: list[str],
        target_freq: Freq | None = None,
        chunk_days: int = 30,  # 每次处理的天数
    ) -> None:
        """使用时间戳将数据库数据导出为npy文件格式，支持降采样.

        Args:
            output_path: 输出目录
            start_ts: 开始时间戳 (毫秒，int或str)
            end_ts: 结束时间戳 (毫秒，int或str)
            freq: 原始数据频率
            symbols: 交易对列表
            target_freq: 目标频率，None表示不进行降采样
            chunk_days: 每次处理的天数，用于控制内存使用
        """
        try:
            # 确保时间戳为整数
            start_timestamp = int(start_ts)
            end_timestamp = int(end_ts)

            # 转换时间戳为日期，用于计算处理范围
            from datetime import datetime

            start_datetime = datetime.fromtimestamp(start_timestamp / 1000)
            end_datetime = datetime.fromtimestamp(end_timestamp / 1000)

            logger.info(f"Exporting data from timestamp {start_timestamp} to {end_timestamp}")
            logger.info(
                f"Date range: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')} to "
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
                logger.info(
                    f"Processing all data from timestamp {start_timestamp} to {end_timestamp} "
                    f"(total: {total_days} days)"
                )

                # 直接使用时间戳读取所有数据
                try:
                    df = self._read_data_by_timestamp(
                        start_timestamp,
                        end_timestamp,
                        freq,
                        symbols,
                        raise_on_empty=False,
                    )
                except ValueError as e:
                    if "No data found" in str(e):
                        logger.warning(f"No data found for timestamp range {start_timestamp} to {end_timestamp}")
                        return
                    else:
                        raise

                if df.empty:
                    logger.warning(f"No data found for timestamp range {start_timestamp} to {end_timestamp}")
                    return

                # 如果需要降采样
                if target_freq is not None:
                    df = self._resample_data(df, target_freq)

                # 处理所有数据
                self._process_dataframe_for_export_by_timestamp(
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
                        f"Processing data chunk from "
                        f"{chunk_start_datetime.strftime('%Y-%m-%d %H:%M:%S')} to "
                        f"{chunk_end_datetime.strftime('%Y-%m-%d %H:%M:%S')}"
                    )

                    # 使用时间戳读取数据块
                    try:
                        df = self._read_data_by_timestamp(
                            current_ts,
                            chunk_end_ts,
                            freq,
                            symbols,
                            raise_on_empty=False,
                        )
                    except ValueError as e:
                        if "No data found" in str(e):
                            logger.warning(f"No data found for timestamp range {current_ts} to {chunk_end_ts}")
                            current_ts = chunk_end_ts
                            continue
                        else:
                            raise

                    if df.empty:
                        logger.warning(f"No data found for timestamp range {current_ts} to {chunk_end_ts}")
                        current_ts = chunk_end_ts
                        continue

                    # 如果需要降采样
                    if target_freq is not None:
                        df = self._resample_data(df, target_freq)

                    # 处理当前数据块
                    self._process_dataframe_for_export_by_timestamp(
                        df, output_path, export_freq, current_ts, chunk_end_ts
                    )

                    # 清理内存
                    del df
                    current_ts = chunk_end_ts

            logger.info(f"Successfully exported data to {output_path}")

        except Exception as e:
            logger.exception(f"Failed to export data by timestamp: {e}")
            raise

    def export_to_files(
        self,
        output_path: Path | str,
        start_date: str,
        end_date: str,
        freq: Freq,
        symbols: list[str],
        target_freq: Freq | None = None,
        chunk_days: int = 30,  # 每次处理的天数
    ) -> None:
        """将数据库数据导出为npy文件格式，支持降采样.

        Args:
            output_path: 输出目录
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            freq: 原始数据频率
            symbols: 交易对列表
            target_freq: 目标频率，None表示不进行降采样
            chunk_days: 每次处理的天数，用于控制内存使用
        """
        try:
            output_path = Path(output_path)

            # 创建日期范围
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")
            total_days = len(date_range)

            # 如果总天数少于等于chunk_days，直接处理整个范围，不分块
            if total_days <= chunk_days:
                logger.info(f"Processing all data from {start_date} to {end_date} (total: {total_days} days)")

                # 读取所有数据
                try:
                    df = self.read_data(
                        start_date,
                        end_date,
                        freq,
                        symbols,
                        raise_on_empty=False,
                    )
                except ValueError as e:
                    if "No data found" in str(e):
                        logger.warning(f"No data found for period {start_date} to {end_date}")
                        return
                    else:
                        raise

                if df.empty:
                    logger.warning(f"No data found for period {start_date} to {end_date}")
                    return

                # 如果需要降采样
                if target_freq is not None:
                    df = self._resample_data(df, target_freq)
                    freq = target_freq

                # 处理所有数据
                self._process_dataframe_for_export(df, output_path, freq, date_range)

            else:
                # 按chunk_days分块处理（用于大量数据）
                for chunk_start in range(0, len(date_range), chunk_days):
                    chunk_end = min(chunk_start + chunk_days, len(date_range))
                    chunk_start_date = date_range[chunk_start].strftime("%Y-%m-%d")
                    chunk_end_date = date_range[chunk_end - 1].strftime("%Y-%m-%d")

                    logger.info(f"Processing data from {chunk_start_date} to {chunk_end_date}")

                    # 读取数据块
                    try:
                        df = self.read_data(
                            chunk_start_date,
                            chunk_end_date,
                            freq,
                            symbols,
                            raise_on_empty=False,
                        )
                    except ValueError as e:
                        if "No data found" in str(e):
                            logger.warning(f"No data found for period {chunk_start_date} to {chunk_end_date}")
                            continue
                        else:
                            raise

                    if df.empty:
                        logger.warning(f"No data found for period {chunk_start_date} to {chunk_end_date}")
                        continue

                    # 如果需要降采样
                    if target_freq is not None:
                        df = self._resample_data(df, target_freq)
                        freq = target_freq

                    # 处理当前数据块
                    chunk_dates = pd.date_range(chunk_start_date, chunk_end_date, freq="D")
                    self._process_dataframe_for_export(df, output_path, freq, chunk_dates)

                    # 清理内存
                    del df

            logger.info(f"Successfully exported data to {output_path}")

        except Exception as e:
            logger.exception(f"Failed to export data: {e}")
            raise

    def _process_dataframe_for_export(
        self, df: pd.DataFrame, output_path: Path, freq: Freq, dates: pd.DatetimeIndex
    ) -> None:
        """处理DataFrame并导出为文件的辅助方法"""
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

        # 处理每一天
        for date in dates:
            date_str = date.strftime("%Y%m%d")
            # 保存交易对顺序
            symbols_path = output_path / freq.value / date_str / "universe_token.pkl"
            symbols_path.parent.mkdir(parents=True, exist_ok=True)
            pd.Series(df.index.get_level_values("symbol").unique()).to_pickle(symbols_path)

            # 获取当天数据
            timestamps = df.index.get_level_values("timestamp")
            day_data = df[
                df.index.get_level_values("timestamp").isin(
                    [ts for ts in timestamps if pd.Timestamp(ts).date() == date.date()]
                )
            ]
            if day_data.empty:
                continue

            # 为每个特征创建并存储数据
            for short_name in features:
                # 检查特征是否存在于数据中
                if short_name not in FIELD_MAPPING:
                    continue

                db_field, needs_calculation = FIELD_MAPPING[short_name]

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
                        continue  # 未知的计算字段
                else:
                    # 直接从数据库字段获取
                    if db_field not in day_data.columns:
                        continue  # 跳过不存在的字段
                    feature_data = day_data[db_field]

                # 重塑数据为 K x T 矩阵
                pivot_data = feature_data.unstack(level="timestamp")
                array = pivot_data.values

                # 创建存储路径 - 使用短字段名
                save_path = output_path / freq.value / date_str / short_name
                save_path.mkdir(parents=True, exist_ok=True)

                # 保存为npy格式
                np.save(save_path / f"{date_str}.npy", array)

    def _process_dataframe_for_export_by_timestamp(
        self,
        df: pd.DataFrame,
        output_path: Path,
        freq: Freq,
        start_ts: int,
        end_ts: int,
    ) -> None:
        """基于时间戳处理DataFrame并导出为文件的辅助方法"""

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

        # 处理每一天
        for date in unique_dates:
            date_str = date.strftime("%Y%m%d")

            # 保存交易对顺序
            symbols_path = output_path / freq.value / "symbols" / f"{date_str}.pkl"
            symbols_path.parent.mkdir(parents=True, exist_ok=True)
            pd.Series(df.index.get_level_values("symbol").unique()).to_pickle(symbols_path)

            # 获取当天数据
            day_data = df[
                df.index.get_level_values("timestamp").map(
                    lambda ts, current_date=date: pd.Timestamp(ts).date() == current_date
                )
            ]

            if day_data.empty:
                continue

            # 为每个特征创建并存储数据
            for short_name in features:
                # 检查特征是否存在于数据中
                if short_name not in FIELD_MAPPING:
                    continue

                db_field, needs_calculation = FIELD_MAPPING[short_name]

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
                        continue  # 未知的计算字段
                else:
                    # 直接从数据库字段获取
                    if db_field not in day_data.columns:
                        continue  # 跳过不存在的字段
                    feature_data = day_data[db_field]

                # 重塑数据为 K x T 矩阵
                pivot_data = feature_data.unstack(level="timestamp")
                array = pivot_data.values

                # 创建存储路径 - 使用短字段名
                save_path = output_path / freq.value / short_name
                save_path.mkdir(parents=True, exist_ok=True)

                # 保存为npy格式
                np.save(save_path / f"{date_str}.npy", array)

    def _resample_data(self, df: pd.DataFrame, target_freq: Freq) -> pd.DataFrame:
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

    def visualize_data(
        self,
        symbol: str,
        start_time: str,
        end_time: str,
        freq: Freq,
        max_rows: int = 20,
    ) -> None:
        """可视化显示数据库中的数据.

        Args:
            symbol: 交易对
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            freq: 数据频率
            max_rows: 最大显示行数
        """
        try:
            # 读取数据
            df = self.read_data(start_time, end_time, freq, [symbol])
            if df.empty:
                logger.warning(f"No data found for {symbol}")
                return

            # 创建表格
            console = Console()
            table = Table(
                title=f"Market Data for {symbol} ({start_time} to {end_time})",
                show_header=True,
                header_style="bold magenta",
            )

            # 添加列
            table.add_column("Timestamp", style="cyan")
            for col in df.columns:
                table.add_column(col.replace("_", " ").title(), justify="right")

            # 添加行
            def is_tuple_index(idx: Any) -> TypeGuard[tuple[Any, pd.Timestamp]]:
                """判断索引是否为包含时间戳的元组"""
                return isinstance(idx, tuple) and len(idx) > 1 and isinstance(idx[1], pd.Timestamp)

            for idx, row in df.head(max_rows).iterrows():
                if is_tuple_index(idx):
                    timestamp = idx[1].strftime("%Y-%m-%d %H:%M:%S")
                else:
                    timestamp = str(idx)
                values = [f"{x:.8f}" if isinstance(x, float) else str(x) for x in row]
                table.add_row(timestamp, *values)

            # 显示表格
            console.print(table)

            if len(df) > max_rows:
                console.print(f"[yellow]Showing {max_rows} rows out of {len(df)} total rows[/yellow]")

        except Exception as e:
            logger.exception(f"Failed to visualize data: {e}")
            raise

    def is_date_matching(self, ts: Any, target_date: datetime.date) -> bool:
        """判断时间戳是否匹配目标日期"""
        # 确保返回布尔值，而不是Any类型
        return bool(pd.Timestamp(ts).date() == target_date)

    def process_dataframe_by_date(
        self,
        df: pd.DataFrame,
        date: datetime.date,
        feature_processor: Callable[[pd.DataFrame, str], None],
    ) -> None:
        """按日期处理数据框并应用特征处理函数"""
        timestamps = df.index.get_level_values("timestamp")
        # 不使用.values，直接使用Series作为布尔索引
        date_mask = pd.Series(timestamps).map(lambda ts: pd.Timestamp(ts).date() == date)
        # 使用布尔Series进行索引
        day_data = df.loc[date_mask]

        if day_data.empty:
            return

        # 应用特征处理函数
        for feature in df.columns:
            feature_processor(day_data, feature)

    def _handle_funding_rate_frequency(
        self,
        funding_rate_data: pd.DataFrame,
        target_freq: Freq,
        target_index: pd.Index | None = None,
    ) -> pd.DataFrame:
        """处理资金费率频率问题。

        资金费率的最小粒度是小时级别，如果目标频率更细（如分钟级），需要进行频率调整。
        对于缺失的时间戳，使用 NaN 填充。

        Args:
            funding_rate_data: 资金费率数据
            target_freq: 目标频率
            target_index: 目标时间索引

        Returns:
            pd.DataFrame: 频率调整后的数据
        """
        try:
            # 如果目标频率是小时或更粗的粒度，直接返回
            if target_freq in [
                Freq.h1,
                Freq.h2,
                Freq.h4,
                Freq.h6,
                Freq.h8,
                Freq.h12,
                Freq.d1,
                Freq.w1,
                Freq.M1,
            ]:
                return funding_rate_data

            # 对于分钟级别的频率，需要进行上采样
            if target_index is not None:
                # 使用目标索引进行重新索引，缺失值用 NaN 填充
                funding_rate_data = funding_rate_data.reindex(target_index, method="ffill")
                # 资金费率通常每8小时更新一次，所以用前向填充是合适的
                # 但是为了保持数据的真实性，我们可能需要在某些时间点设置为 NaN

            return funding_rate_data

        except Exception as e:
            logger.warning(f"处理资金费率频率时出错: {e}")
            return funding_rate_data

    def close(self) -> None:
        """关闭数据库连接"""
        if self._use_pool and self._pool is not None:
            self._pool.close_all()
            self._pool = None

    def __enter__(self) -> "MarketDB":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """退出上下文管理器时关闭数据库连接"""
        self.close()

    # 新增的数据查询功能
    def get_data_summary(self) -> dict[str, Any]:
        """获取数据库中数据的概况统计.

        Returns:
            dict: 包含各种统计信息的字典
        """
        try:
            with self._get_connection() as conn:
                # 获取市场数据统计
                market_stats = conn.execute(
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
                ).fetchall()

                # 获取资金费率统计
                funding_stats = conn.execute(
                    """
                    SELECT
                        COUNT(*) as record_count,
                        COUNT(DISTINCT symbol) as unique_symbols,
                        MIN(timestamp) as earliest_timestamp,
                        MAX(timestamp) as latest_timestamp
                    FROM funding_rate
                """
                ).fetchone()

                # 获取持仓量统计
                oi_stats = conn.execute(
                    """
                    SELECT
                        interval,
                        COUNT(*) as record_count,
                        COUNT(DISTINCT symbol) as unique_symbols,
                        MIN(timestamp) as earliest_timestamp,
                        MAX(timestamp) as latest_timestamp
                    FROM open_interest
                    GROUP BY interval
                """
                ).fetchall()

                # 获取多空比例统计
                lsr_stats = conn.execute(
                    """
                    SELECT
                        period,
                        ratio_type,
                        COUNT(*) as record_count,
                        COUNT(DISTINCT symbol) as unique_symbols,
                        MIN(timestamp) as earliest_timestamp,
                        MAX(timestamp) as latest_timestamp
                    FROM long_short_ratio
                    GROUP BY period, ratio_type
                """
                ).fetchall()

                return {
                    "market_data": [dict(row) for row in market_stats],
                    "funding_rate": dict(funding_stats) if funding_stats else {},
                    "open_interest": [dict(row) for row in oi_stats],
                    "long_short_ratio": [dict(row) for row in lsr_stats],
                }

        except Exception:
            logger.exception("Failed to get data summary")
            raise

    def get_symbol_data_range(self, symbol: str, freq: Freq | None = None) -> dict[str, Any]:
        """获取指定交易对的数据时间范围.

        Args:
            symbol: 交易对
            freq: 数据频率，None表示获取所有频率

        Returns:
            dict: 包含时间范围信息的字典
        """
        try:
            with self._get_connection() as conn:
                if freq is None:
                    # 获取所有频率的数据范围
                    query = """
                        SELECT
                            freq,
                            COUNT(*) as record_count,
                            MIN(timestamp) as earliest_timestamp,
                            MAX(timestamp) as latest_timestamp,
                            MIN(date(timestamp/1000, 'unixepoch')) as earliest_date,
                            MAX(date(timestamp/1000, 'unixepoch')) as latest_date
                        FROM market_data
                        WHERE symbol = ?
                        GROUP BY freq
                    """
                    result = conn.execute(query, (symbol,)).fetchall()
                    return {
                        "symbol": symbol,
                        "frequencies": [dict(row) for row in result],
                    }
                else:
                    # 获取指定频率的数据范围
                    query = """
                        SELECT
                            COUNT(*) as record_count,
                            MIN(timestamp) as earliest_timestamp,
                            MAX(timestamp) as latest_timestamp,
                            MIN(date(timestamp/1000, 'unixepoch')) as earliest_date,
                            MAX(date(timestamp/1000, 'unixepoch')) as latest_date
                        FROM market_data
                        WHERE symbol = ? AND freq = ?
                    """
                    result = conn.execute(query, (symbol, freq.value)).fetchone()
                    result_dict = {
                        "symbol": symbol,
                        "frequency": freq.value,
                    }
                    if result:
                        result_dict.update(dict(result))
                    return result_dict

        except Exception:
            logger.exception("Failed to get symbol data range")
            raise

    def check_data_completeness(
        self,
        symbol: str,
        start_time: str,
        end_time: str,
        freq: Freq,
        expected_interval_minutes: int | None = None,
    ) -> dict[str, Any]:
        """检查数据的完整性.

        Args:
            symbol: 交易对
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            freq: 数据频率
            expected_interval_minutes: 预期的时间间隔（分钟），None表示自动推断

        Returns:
            dict: 包含完整性检查结果的字典
        """
        try:
            # 自动推断时间间隔
            if expected_interval_minutes is None:
                freq_to_minutes = {
                    Freq.m1: 1,
                    Freq.m3: 3,
                    Freq.m5: 5,
                    Freq.m15: 15,
                    Freq.m30: 30,
                    Freq.h1: 60,
                    Freq.h2: 120,
                    Freq.h4: 240,
                    Freq.h6: 360,
                    Freq.h8: 480,
                    Freq.h12: 720,
                    Freq.d1: 1440,
                }
                expected_interval_minutes = freq_to_minutes.get(freq, 1)

            # 读取数据
            df = self.read_data(start_time, end_time, freq, [symbol], raise_on_empty=False)

            if df.empty:
                return {
                    "symbol": symbol,
                    "period": f"{start_time} to {end_time}",
                    "frequency": freq.value,
                    "total_records": 0,
                    "missing_records": 0,
                    "completeness_rate": 0.0,
                    "missing_periods": [],
                }

            # 获取实际时间戳
            actual_timestamps = sorted(df.index.get_level_values("timestamp").unique())

            # 生成预期的时间戳序列
            start_ts = pd.Timestamp(start_time)
            end_ts = pd.Timestamp(end_time)
            expected_timestamps = pd.date_range(start=start_ts, end=end_ts, freq=f"{expected_interval_minutes}min")

            # 转换为毫秒时间戳
            expected_ts_ms = [int(ts.timestamp() * 1000) for ts in expected_timestamps]
            actual_ts_ms = [int(pd.Timestamp(ts).timestamp() * 1000) for ts in actual_timestamps]

            # 找出缺失的时间戳
            missing_ts = set(expected_ts_ms) - set(actual_ts_ms)
            missing_periods = [pd.Timestamp(ts, unit="ms").strftime("%Y-%m-%d %H:%M:%S") for ts in sorted(missing_ts)]

            return {
                "symbol": symbol,
                "period": f"{start_time} to {end_time}",
                "frequency": freq.value,
                "expected_records": len(expected_ts_ms),
                "actual_records": len(actual_ts_ms),
                "missing_records": len(missing_ts),
                "completeness_rate": ((len(actual_ts_ms) / len(expected_ts_ms)) * 100 if expected_ts_ms else 0),
                "missing_periods": missing_periods[:10],  # 只显示前10个缺失时间点
            }

        except Exception:
            logger.exception("Failed to check data completeness")
            raise

    def get_latest_data(self, symbols: list[str], freq: Freq, limit: int = 1) -> pd.DataFrame:
        """获取最新的数据点.

        Args:
            symbols: 交易对列表
            freq: 数据频率
            limit: 每个symbol返回的记录数

        Returns:
            pd.DataFrame: 最新数据
        """
        try:
            placeholders = ",".join("?" * len(symbols))
            query = f"""
                SELECT symbol, timestamp,
                       open_price, high_price, low_price, close_price,
                       volume, quote_volume, trades_count,
                       taker_buy_volume, taker_buy_quote_volume,
                       taker_sell_volume, taker_sell_quote_volume
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
                    FROM market_data
                    WHERE freq = ? AND symbol IN ({placeholders})
                ) ranked
                WHERE rn <= ?
                ORDER BY symbol, timestamp DESC
            """

            params = tuple([freq.value] + symbols + [limit])

            with self._get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=params, parse_dates={"timestamp": "ms"})

            if df.empty:
                return pd.DataFrame()

            df = df.set_index(["symbol", "timestamp"])
            return df

        except Exception:
            logger.exception("Failed to get latest data")
            raise

    def get_combined_data(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        include_funding_rate: bool = False,
        include_open_interest: bool = False,
        include_long_short_ratio: bool = False,
        oi_interval: str = "5m",
        lsr_period: str = "5m",
        lsr_ratio_type: str = "account",
    ) -> pd.DataFrame:
        """获取合并的多类型数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            freq: 数据频率
            include_funding_rate: 是否包含资金费率数据
            include_open_interest: 是否包含持仓量数据
            include_long_short_ratio: 是否包含多空比例数据
            oi_interval: 持仓量时间间隔
            lsr_period: 多空比例时间周期
            lsr_ratio_type: 多空比例类型

        Returns:
            pd.DataFrame: 合并后的数据
        """
        try:
            # 获取基础市场数据
            base_data = self.read_data(start_time, end_time, freq, symbols, raise_on_empty=False)

            if base_data.empty:
                return pd.DataFrame()

            # 如果不需要额外数据，直接返回基础数据
            if not any([include_funding_rate, include_open_interest, include_long_short_ratio]):
                return base_data

            # 获取额外数据并合并
            additional_data = []

            if include_funding_rate:
                try:
                    funding_data = self.read_funding_rate(start_time, end_time, symbols, raise_on_empty=False)
                    if not funding_data.empty:
                        additional_data.append(funding_data[["funding_rate"]])
                except Exception as e:
                    logger.warning(f"Failed to get funding rate data: {e}")

            if include_open_interest:
                try:
                    oi_data = self.read_open_interest(start_time, end_time, symbols, oi_interval, raise_on_empty=False)
                    if not oi_data.empty:
                        additional_data.append(oi_data[["open_interest", "open_interest_value"]])
                except Exception as e:
                    logger.warning(f"Failed to get open interest data: {e}")

            if include_long_short_ratio:
                try:
                    lsr_data = self.read_long_short_ratio(
                        start_time,
                        end_time,
                        symbols,
                        lsr_period,
                        lsr_ratio_type,
                        raise_on_empty=False,
                    )
                    if not lsr_data.empty:
                        additional_data.append(lsr_data[["long_short_ratio", "long_account", "short_account"]])
                except Exception as e:
                    logger.warning(f"Failed to get long short ratio data: {e}")

            # 合并数据
            if additional_data:
                for data in additional_data:
                    base_data = base_data.join(data, how="left")

            return base_data

        except Exception:
            logger.exception("Failed to get combined data")
            raise

    def get_symbols_list(self, freq: Freq | None = None) -> list[str]:
        """获取数据库中所有交易对列表.

        Args:
            freq: 数据频率，None表示获取所有频率的交易对

        Returns:
            list[str]: 交易对列表
        """
        try:
            with self._get_connection() as conn:
                if freq is None:
                    query = "SELECT DISTINCT symbol FROM market_data ORDER BY symbol"
                    result = conn.execute(query).fetchall()
                else:
                    query = "SELECT DISTINCT symbol FROM market_data WHERE freq = ? ORDER BY symbol"
                    result = conn.execute(query, (freq.value,)).fetchall()

                return [row[0] for row in result]

        except Exception:
            logger.exception("Failed to get symbols list")
            raise

    def data_exists(self, symbol: str, start_time: str, end_time: str, freq: Freq) -> bool:
        """检查指定时间范围内是否存在数据.

        Args:
            symbol: 交易对
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            freq: 数据频率

        Returns:
            bool: 是否存在数据
        """
        try:
            start_ts = int(pd.Timestamp(start_time).timestamp() * 1000)
            end_ts = int(pd.Timestamp(end_time).timestamp() * 1000)

            with self._get_connection() as conn:
                query = """
                    SELECT COUNT(*) as count
                    FROM market_data
                    WHERE symbol = ? AND freq = ? AND timestamp BETWEEN ? AND ?
                """
                result = conn.execute(query, (symbol, freq.value, start_ts, end_ts)).fetchone()
                return result[0] > 0 if result else False

        except Exception:
            logger.exception("Failed to check data existence")
            raise

    def get_aggregated_data(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        agg_period: str = "1D",  # 聚合周期：1D, 1W, 1M等
        agg_functions: Any = None,
    ) -> pd.DataFrame:
        """获取聚合数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            freq: 原始数据频率
            agg_period: 聚合周期
            agg_functions: 聚合函数字典，key为列名，value为聚合函数

        Returns:
            pd.DataFrame: 聚合后的数据
        """
        try:
            # 默认聚合函数
            if agg_functions is None:
                agg_functions = {
                    "open_price": "first",
                    "high_price": "max",
                    "low_price": "min",
                    "close_price": "last",
                    "volume": "sum",
                    "quote_volume": "sum",
                    "trades_count": "sum",
                }

            # 获取原始数据
            df = self.read_data(start_time, end_time, freq, symbols, raise_on_empty=False)

            if df.empty:
                return pd.DataFrame()

            # 按symbol分组聚合
            aggregated_dfs = []
            for symbol in symbols:
                if symbol in df.index.get_level_values("symbol"):
                    symbol_data = df.loc[symbol]

                    # 执行聚合 - 使用字典类型转换确保类型正确
                    agg_data = symbol_data.resample(agg_period).agg(dict(agg_functions))

                    # 添加symbol级别的索引 - 使用list()确保类型正确
                    agg_data.index = pd.MultiIndex.from_product(
                        [[symbol], list(agg_data.index)], names=["symbol", "timestamp"]
                    )
                    aggregated_dfs.append(agg_data)

            if aggregated_dfs:
                return pd.concat(aggregated_dfs)
            else:
                return pd.DataFrame()

        except Exception:
            logger.exception("Failed to get aggregated data")
            raise

    def get_data_statistics(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        features: list[str] | None = None,
    ) -> pd.DataFrame:
        """获取数据的统计信息.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            freq: 数据频率
            features: 需要统计的特征列表

        Returns:
            pd.DataFrame: 统计信息
        """
        try:
            # 获取数据
            df = self.read_data(start_time, end_time, freq, symbols, features, raise_on_empty=False)

            if df.empty:
                return pd.DataFrame()

            # 按symbol分组计算统计信息
            stats_list = []
            for symbol in symbols:
                if symbol in df.index.get_level_values("symbol"):
                    symbol_data = df.loc[symbol]

                    # 计算统计信息
                    stats = symbol_data.describe()
                    stats.index = pd.MultiIndex.from_product(
                        [[symbol], list(stats.index)], names=["symbol", "statistic"]
                    )
                    stats_list.append(stats)

            if stats_list:
                return pd.concat(stats_list)
            else:
                return pd.DataFrame()

        except Exception:
            logger.exception("Failed to get data statistics")
            raise
