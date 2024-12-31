"""数据存储工具函数."""

import logging
from datetime import datetime
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from cryptoservice.config import settings
from cryptoservice.models import Freq, PerpetualMarketTicker

logger = logging.getLogger(__name__)


class StorageUtils:
    """数据存储工具类.
    store_kdtv_data: 存储 KDTV 格式数据
    store_feature_data: 存储特征数据
    store_universe: 存储交易对列表
    visualize_npy_data: 可视化 npy 数据
    """

    console = Console()

    @staticmethod
    def _resolve_path(data_path: Path | str, base_dir: Path | str | None = None) -> Path:
        """解析路径，将相对路径转换为绝对路径.

        Args:
            data_path: 输入路径，可以是相对路径或绝对路径
            base_dir: 基准目录，用于解析相对路径。如果为 None，则使用当前目录

        Returns:
            Path: 解析后的绝对路径
        """
        try:
            path = Path(data_path)
            if not path.is_absolute():
                base = Path(base_dir) if base_dir else Path.cwd()
                path = base / path
            return path.resolve()
        except Exception as e:
            raise ValueError(f"Failed to resolve path '{data_path}': {str(e)}")

    @staticmethod
    def store_kdtv_data(
        data: List[PerpetualMarketTicker],
        date: str,
        freq: str,
        univ: str,
        data_path: Path | str = settings.DATA_STORAGE["MARKET_DATA"],
    ) -> None:
        """存储 KDTV 格式数据.

        Args:
            data: 市场数据列表
            date: 日期 (YYYYMMDD)
            freq: 频率 (如 'H1')
            univ: 数据集名称
            data_path: 数据存储根目录
        """
        data_path = StorageUtils._resolve_path(data_path)
        df = pd.DataFrame([d.__dict__ for d in data])
        df["D"] = pd.to_datetime(df["open_time"]).dt.strftime("%Y%m%d")
        df["T"] = pd.to_datetime(df["open_time"]).dt.strftime("%H%M%S")
        df["K"] = df["symbol"]

        df = df.set_index(["K", "D", "T"]).sort_index()
        array = df[["last_price", "volume", "quote_volume", "high_price", "low_price"]].values

        save_path = data_path / univ / freq / f"{date}.npy"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        np.save(save_path, array)

    @staticmethod
    def store_feature_data(
        data: List[List[PerpetualMarketTicker]],
        interval: Freq,
        data_path: Path | str = settings.DATA_STORAGE["PERPETUAL_DATA"],
    ) -> None:
        """存储特征数据，按照 KDTV (Key-Date-Time-Value) 格式组织.

        Args:
            data: 市场数据嵌套列表，每个内部列表包含一组市场数据
            interval: 频率 (如 'h1')
            data_path: 数据存储根目录
        """
        data_path = StorageUtils._resolve_path(data_path)

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
            ) as progress:
                flattened_data = [item for sublist in data for item in sublist]
                # 获取起始日期
                start_date = pd.Timestamp(flattened_data[0].open_time, unit="ms").date()
                end_date = pd.Timestamp(flattened_data[-1].open_time, unit="ms").date()
                storage_task = progress.add_task(
                    "[green]存储数据", total=len(pd.date_range(start_date, end_date, freq="D"))
                )

                df = pd.DataFrame([d.__dict__ for d in flattened_data])
                df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)

                # 构建 KDTV 格式
                df["D"] = df["datetime"].dt.strftime("%Y%m%d")  # Date
                df["T"] = df["datetime"].dt.strftime("%H%M%S")  # Time
                df["K"] = df["symbol"]  # Key (symbol)

                # 设置多级索引
                df = df.set_index(["K", "D", "T"]).sort_index()

                # 定义需要保存的数据列（Value）
                value_columns = [
                    "count",
                    "last_price",
                    "volume",
                    "quote_volume",
                    "high_price",
                    "low_price",
                    "open_price",
                    "taker_buy_volume",
                    "taker_buy_quote_volume",
                ]

                # 按日期分组并重塑数据为二维数组 (symbols × time)
                for date in pd.date_range(start_date, end_date, freq="D"):
                    date_str = date.strftime("%Y%m%d")
                    date_data = df[df.index.get_level_values("D") == date_str]

                    for column in value_columns:
                        # 重塑数据为二维数组，行为symbols，列为时间点
                        pivot_data = date_data[column].unstack(level="T")  # K × T matrix
                        array = pivot_data.values

                        save_path = data_path / interval / column / f"{date_str}.npy"
                        save_path.parent.mkdir(parents=True, exist_ok=True)
                        np.save(save_path, array)
                    progress.advance(storage_task)

                # 保存列名信息
                columns_path = data_path / interval / "columns.csv"
                symbols = df["symbol"].unique().tolist()
                with open(columns_path, "w") as f:
                    f.write(",".join(symbols))  # 将列表转换为逗号分隔的字符串

        except Exception as e:
            logger.exception("特征数据存储失败")
            raise

    @staticmethod
    def store_universe(
        symbols: List[str], data_path: Path | str = settings.DATA_STORAGE["PERPETUAL_DATA"]
    ) -> None:
        """存储交易对列表.

        Args:
            symbols: 交易对列表
            data_path: 数据存储根目录
        """
        data_path = StorageUtils._resolve_path(data_path)
        save_path = data_path / f"universe_token.pkl"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        pd.Series(symbols).to_pickle(save_path)

    @staticmethod
    def visualize_npy_data(
        file_path: Path | str,
        max_rows: int = 10,
        headers: List[str] | None = None,
        index: List[str] | None = None,
    ) -> None:
        """在终端可视化显示 npy 数据.

        Args:
            file_path: npy 文件路径
            max_rows: 最大显示行数
            headers: 列标题
            index: 行索引

        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 数据格式错误
        """
        file_path = StorageUtils._resolve_path(file_path)

        try:
            # 检查文件是否存在
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")

            # 检查文件扩展名
            if file_path.suffix != ".npy":
                raise ValueError(f"Invalid file format: {file_path.suffix}, expected .npy")

            # 加载数据
            data = np.load(file_path, allow_pickle=True)

            # 验证数据维度
            if not isinstance(data, np.ndarray):
                raise ValueError(f"Expected numpy array, got {type(data)}")
            if len(data.shape) != 2:
                raise ValueError(f"Expected 2D array, got {len(data.shape)}D")

            # 限制显示行数
            if len(data) > max_rows:
                data = data[:max_rows]
                StorageUtils.console.print(
                    f"[yellow]Showing first {max_rows} rows of {len(data)} total rows[/]"
                )

            # 创建表格
            table = Table(show_header=True, header_style="bold magenta")

            # 验证并添加列
            n_cols = data.shape[1]
            if headers and len(headers) != n_cols:
                raise ValueError(
                    f"Headers length ({len(headers)}) doesn't match data columns ({n_cols})"
                )

            table.add_column("Index", style="cyan")
            for header in headers or [f"Col_{i}" for i in range(n_cols)]:
                table.add_column(str(header), justify="right")

            # 验证并添加行
            if index and len(index) < len(data):
                StorageUtils.console.print(
                    "[yellow]Warning: Index length is less than data length[/]"
                )

            for i, row in enumerate(data):
                try:
                    idx = index[i] if index and i < len(index) else f"Row_{i}"
                    formatted_values = [
                        f"{x:.4f}" if isinstance(x, (float, np.floating)) else str(x) for x in row
                    ]
                    table.add_row(idx, *formatted_values)
                except Exception as e:
                    StorageUtils.console.print(f"[yellow]Warning: Error formatting row {i}: {e}[/]")
                    continue

            StorageUtils.console.print(table)

        except Exception as e:
            logger.exception("数据可视化失败: {}", str(e))
            raise
