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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON market_data(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON market_data(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_freq ON market_data(freq)")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_symbol_freq_timestamp
                ON market_data(symbol, freq, timestamp)
                """
            )

    @contextmanager
    def _get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """获取数据库连接的内部实现"""
        if self._use_pool and self._pool is not None:
            with self._pool.get_connection() as conn:
                yield conn
        else:
            conn = sqlite3.connect(self.db_path)
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
                logger.info(
                    f"Successfully stored {len(records)} records for {symbol} "
                    f"with frequency {freq.value}"
                )

        except Exception:
            logger.exception("Failed to store market data")
            raise

    def read_data(
        self,
        start_time: str,
        end_time: str,
        freq: Freq,
        symbols: list[str],
        features: list[str] | None = None,
    ) -> pd.DataFrame:
        """读取市场数据.

        Args:
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            freq: 数据频率
            symbols: 交易对列表
            features: 需要读取的特征列表，None表示读取所有特征

        Returns:
            pd.DataFrame: 市场数据，多级索引 (symbol, timestamp)
        """
        try:
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
                raise ValueError("No data found for the specified criteria")

            # 设置多级索引
            df = df.set_index(["symbol", "timestamp"])
            return df

        except Exception:
            logger.exception("Failed to read market data")
            raise

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

            # 计算日期范围
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")

            # 按chunk_days分块处理
            for chunk_start in range(0, len(date_range), chunk_days):
                chunk_end = min(chunk_start + chunk_days, len(date_range))
                chunk_start_date = date_range[chunk_start].strftime("%Y-%m-%d")
                chunk_end_date = date_range[chunk_end - 1].strftime("%Y-%m-%d")

                logger.info(f"Processing data from {chunk_start_date} to {chunk_end_date}")

                # 读取数据块
                df = self.read_data(chunk_start_date, chunk_end_date, freq, symbols)
                if df.empty:
                    logger.warning(
                        f"No data found for period {chunk_start_date} to {chunk_end_date}"
                    )
                    continue

                # 如果需要降采样
                if target_freq is not None:
                    df = self._resample_data(df, target_freq)
                    freq = target_freq

                # 定义需要导出的特征
                features = [
                    "close_price",
                    "volume",
                    "quote_volume",
                    "high_price",
                    "low_price",
                    "open_price",
                    "trades_count",
                    "taker_buy_volume",
                    "taker_buy_quote_volume",
                    "taker_sell_volume",
                    "taker_sell_quote_volume",
                ]

                # 处理当前数据块中的每一天
                chunk_dates = pd.date_range(chunk_start_date, chunk_end_date, freq="D")
                for date in chunk_dates:
                    date_str = date.strftime("%Y%m%d")
                    # 保存交易对顺序
                    symbols_path = output_path / freq.value / date_str / "universe_token.pkl"
                    symbols_path.parent.mkdir(parents=True, exist_ok=True)
                    pd.Series(df.index.get_level_values("symbol").unique()).to_pickle(symbols_path)

                    # 获取当天数据
                    timestamps = df.index.get_level_values("timestamp")
                    # 将timestamps转换为datetime类型
                    datetime_timestamps = pd.to_datetime(timestamps)
                    # 现在可以安全地访问date属性
                    _dates = [ts.date() for ts in datetime_timestamps]

                    # 或者使用更简洁的方式

                    # 显式转换为datetime对象列表
                    _datetime_list = [pd.Timestamp(ts).to_pydatetime() for ts in timestamps]
                    # 然后访问date属性
                    day_data = df[
                        df.index.get_level_values("timestamp").isin(
                            [ts for ts in timestamps if pd.Timestamp(ts).date() == date.date()]
                        )
                    ]
                    if day_data.empty:
                        continue

                    # 为每个特征创建并存储数据
                    for feature in features:
                        # 重塑数据为 K x T 矩阵
                        pivot_data = day_data[feature].unstack(level="timestamp")
                        array = pivot_data.values

                        # 创建存储路径
                        save_path = output_path / freq.value / date_str / feature
                        save_path.mkdir(parents=True, exist_ok=True)

                        # 保存为npy格式
                        np.save(save_path / f"{date_str}.npy", array)

                # 清理内存
                del df

            logger.info(f"Successfully exported data to {output_path}")

        except Exception as e:
            logger.exception(f"Failed to export data: {e}")
            raise

    def _resample_data(self, df: pd.DataFrame, target_freq: Freq) -> pd.DataFrame:
        """对数据进行降采样处理.

        Args:
            df: 原始数据
            target_freq: 目标频率

        Returns:
            pd.DataFrame: 降采样后的数据
        """
        # 定义重采样规则
        freq_map = {
            Freq.m1: "1T",
            Freq.m3: "3T",
            Freq.m5: "5T",
            Freq.m15: "15T",
            Freq.m30: "30T",
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
            resampled.index = pd.MultiIndex.from_product(
                [[symbol], resampled.index], names=["symbol", "timestamp"]
            )
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
                console.print(
                    f"[yellow]Showing {max_rows} rows out of {len(df)} total rows[/yellow]"
                )

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
