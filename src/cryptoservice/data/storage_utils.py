"""数据存储工具函数."""

import logging
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

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

    @staticmethod
    def store_kdtv_data(
        data: List[PerpetualMarketTicker],
        date: str,
        freq: str,
        univ: str,
        data_path: Path = settings.DATA_STORAGE["MARKET_DATA"],
    ) -> None:
        """存储 KDTV 格式数据.

        Args:
            data: 市场数据列表
            date: 日期 (YYYYMMDD)
            freq: 频率 (如 'H1')
            univ: 数据集名称
            data_path: 数据存储根目录
        """
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
        data: List[PerpetualMarketTicker],
        date: str,
        freq: Freq,
        market: str,
        feature: str,
        symbols: List[str],
        data_path: Path = settings.DATA_STORAGE["PERPETUAL_DATA"],
    ) -> None:
        """存储特征数据.

        Args:
            data: 市场数据列表
            date: 日期 (YYYYMMDD)
            freq: 频率 (如 'H1')
            market: 市场类型 (如 'SWAP')
            feature: 特征名称
            symbols: 交易对列表
            data_path: 数据存储根目录
        """
        feature_mapping = {
            "cls": "last_price",
            "hgh": "high_price",
            "low": "low_price",
            "opn": "open_price",
            "vwap": "weighted_avg_price",
            "vol": "volume",
            "amt": "quote_volume",
            "num": "count",
        }
        try:
            df = pd.DataFrame([d.__dict__ for d in data])
            # 转换时间戳为UTC时间
            df["datetime"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            # 使用日期和小时组合作为索引
            df["time_key"] = df["datetime"].dt.strftime("%Y%m%d_%H")

            # 调试信息
            logger.info(f"Time range: {df['datetime'].min()} to {df['datetime'].max()}")
            logger.info(f"Time key distribution:\n{df.groupby('time_key').size()}")

            # 透视表转换
            feature_data = df.pivot(
                index="time_key", columns="symbol", values=feature_mapping[feature]
            ).reindex(columns=symbols)

            # 只保留小时部分作为最终索引
            feature_data.index = [int(idx.split("_")[1]) for idx in feature_data.index]

            save_path = data_path / freq / market / feature / f"{date}.npy"
            save_path.parent.mkdir(parents=True, exist_ok=True)
            np.save(save_path, feature_data.values)
        except Exception as e:
            logger.error(f"Error storing feature data: {e}")
            raise

    @staticmethod
    def store_universe(
        symbols: List[str], market: str, data_path: Path = settings.DATA_STORAGE["PERPETUAL_DATA"]
    ) -> None:
        """存储交易对列表.

        Args:
            symbols: 交易对列表
            market: 市场类型
            data_path: 数据存储根目录
        """
        save_path = data_path / f"univ_TOKEN_{market}.pkl"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        pd.Series(symbols).to_pickle(save_path)

    @staticmethod
    def visualize_npy_data(
        file_path: Path,
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
            FileNotFoundError: 文件不��在
            ValueError: 数据格式错误
        """
        try:
            import numpy as np
            from rich.console import Console
            from rich.table import Table

            console = Console()

            # 检查文件是否存在
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")

            # 检查文件扩展名
            if file_path.suffix != ".npy":
                raise ValueError(f"Invalid file format: {file_path.suffix}, expected .npy")

            # 加载数据
            try:
                data = np.load(file_path, allow_pickle=True)
            except Exception as e:
                raise ValueError(f"Failed to load numpy data: {e}")

            # 验证数据维度
            if not isinstance(data, np.ndarray):
                raise ValueError(f"Expected numpy array, got {type(data)}")
            if len(data.shape) != 2:
                raise ValueError(f"Expected 2D array, got {len(data.shape)}D")

            # 限制显示行数
            if len(data) > max_rows:
                data = data[:max_rows]
                console.print(f"[yellow]Showing first {max_rows} rows of {len(data)} total rows[/]")

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
                console.print("[yellow]Warning: Index length is less than data length[/]")

            for i, row in enumerate(data):
                try:
                    idx = index[i] if index and i < len(index) else f"Row_{i}"
                    formatted_values = [
                        f"{x:.4f}" if isinstance(x, (float, np.floating)) else str(x) for x in row
                    ]
                    table.add_row(idx, *formatted_values)
                except Exception as e:
                    console.print(f"[yellow]Warning: Error formatting row {i}: {e}[/]")
                    continue

            console.print(table)

        except Exception as e:
            console = Console(stderr=True)
            console.print(f"[red]Error visualizing data: {e}[/]")
            raise
