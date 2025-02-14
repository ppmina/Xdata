import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table

from cryptoservice.models import Freq, KlineIndex, PerpetualMarketTicker

logger = logging.getLogger(__name__)


class MarketDB:
    """市场数据库管理类，专注于存储和读取交易对数据."""

    def __init__(self, db_path: Path | str):
        """初始化数据库连接.

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self._conn = None

    def _init_db(self) -> None:
        """初始化数据库表结构."""
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

            # 创建索引以优化查询
            conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON market_data(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON market_data(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_freq ON market_data(freq)")
        return None

    def store_data(
        self,
        data: List[List[PerpetualMarketTicker]],
        freq: Freq,
    ) -> None:
        """存储市场数据.

        Args:
            data: 市场数据列表
            freq: 数据频率
        """
        try:
            # 展平数据
            flattened_data = [item for sublist in data for item in sublist]
            if not flattened_data:
                return

            # 准备插入数据
            records = []
            for ticker in flattened_data:
                record = {
                    "symbol": ticker.symbol,
                    "timestamp": ticker.open_time,
                    "freq": freq.value,
                    "open_price": ticker.raw_data[KlineIndex.OPEN],
                    "high_price": ticker.raw_data[KlineIndex.HIGH],
                    "low_price": ticker.raw_data[KlineIndex.LOW],
                    "close_price": ticker.raw_data[KlineIndex.CLOSE],
                    "volume": ticker.raw_data[KlineIndex.VOLUME],
                    "quote_volume": ticker.raw_data[KlineIndex.QUOTE_VOLUME],
                    "trades_count": ticker.raw_data[KlineIndex.TRADES_COUNT],
                    "taker_buy_volume": ticker.raw_data[KlineIndex.TAKER_BUY_VOLUME],
                    "taker_buy_quote_volume": ticker.raw_data[KlineIndex.TAKER_BUY_QUOTE_VOLUME],
                    "taker_sell_volume": str(
                        float(ticker.raw_data[KlineIndex.VOLUME])
                        - float(ticker.raw_data[KlineIndex.TAKER_BUY_VOLUME])
                    ),
                    "taker_sell_quote_volume": str(
                        float(ticker.raw_data[KlineIndex.QUOTE_VOLUME])
                        - float(ticker.raw_data[KlineIndex.TAKER_BUY_QUOTE_VOLUME])
                    ),
                }
                records.append(record)

            # 批量插入数据
            with sqlite3.connect(self.db_path) as conn:
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

        except Exception as e:
            logger.exception("Failed to store market data")
            raise

    def read_data(
        self,
        start_time: str,
        end_time: str,
        freq: Freq,
        symbols: List[str],
        features: Optional[List[str]] = None,
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
                AND symbol IN ({','.join('?' * len(symbols))})
                ORDER BY symbol, timestamp
            """
            params = [start_ts, end_ts, freq.value] + symbols

            # 执行查询
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(query, conn, params=params, parse_dates={"timestamp": "ms"})

            if df.empty:
                raise ValueError("No data found for the specified criteria")

            # 设置多级索引
            df = df.set_index(["symbol", "timestamp"])
            return df

        except Exception as e:
            logger.exception("Failed to read market data")
            raise

    def get_available_dates(
        self,
        symbol: str,
        freq: Freq,
    ) -> List[str]:
        """获取指定交易对的可用日期列表.

        Args:
            symbol: 交易对
            freq: 数据频率

        Returns:
            List[str]: 可用日期列表 (YYYY-MM-DD格式)
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT DISTINCT date(timestamp/1000, 'unixepoch') as date
                    FROM market_data
                    WHERE symbol = ? AND freq = ?
                    ORDER BY date
                """
                cursor = conn.execute(query, (symbol, freq.value))
                return [row[0] for row in cursor.fetchall()]

        except Exception as e:
            logger.exception("Failed to get available dates")
            raise

    def export_to_files(
        self,
        output_path: Path | str,
        start_date: str,
        end_date: str,
        freq: Freq,
        symbols: List[str],
        target_freq: Optional[Freq] = None,
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
                # 保存交易对顺序
                symbols_path = output_path / freq.value / "universe_token.pkl"
                symbols_path.parent.mkdir(parents=True, exist_ok=True)
                pd.Series(df.index.get_level_values("symbol").unique()).to_pickle(symbols_path)

                # 处理当前数据块中的每一天
                chunk_dates = pd.date_range(chunk_start_date, chunk_end_date, freq="D")
                for date in chunk_dates:
                    date_str = date.strftime("%Y%m%d")

                    # 获取当天数据
                    day_data = df[df.index.get_level_values("timestamp").date == date.date()]
                    if day_data.empty:
                        continue

                    # 为每个特征创建并存储数据
                    for feature in features:
                        # 重塑数据为 K x T 矩阵
                        pivot_data = day_data[feature].unstack(level="timestamp")
                        array = pivot_data.values

                        # 创建存储路径
                        save_path = output_path / freq.value / feature
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
            for idx, row in df.head(max_rows).iterrows():
                timestamp = idx[1].strftime("%Y-%m-%d %H:%M:%S")
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

    def close(self) -> None:
        """关闭数据库连接."""
        if self._conn:
            self._conn.close()
            self._conn = None


if __name__ == "__main__":
    db = MarketDB("data/market.db")

    db.export_to_files(
        output_path="data/market",
        start_date="2024-01-01",
        end_date="2024-01-08",
        freq=Freq.m1,
        symbols=["BTCUSDT"],
        target_freq=Freq.h1,
    )
