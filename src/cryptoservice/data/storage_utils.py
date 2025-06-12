"""数据存储工具函数."""

import logging
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from rich.console import Console
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
            raise ValueError(f"Failed to resolve path '{data_path}': {str(e)}") from e

    @staticmethod
    def store_kdtv_data(
        data: List[List[PerpetualMarketTicker]],
        date: str,
        freq: Freq,
        data_path: Path | str,
    ) -> None:
        """存储 KDTV 格式数据.

        Args:
            data: 市场数据列表
            date: 日期 (YYYYMMDD)
            freq: 频率
            data_path: 数据存储根目录
        """
        data_path = StorageUtils._resolve_path(data_path)

        try:
            # 展平数据并转换为 DataFrame
            flattened_data = [item for sublist in data for item in sublist]
            if not flattened_data:
                return

            # 转换为DataFrame
            df = pd.DataFrame([d.__dict__ for d in flattened_data])
            df["datetime"] = pd.to_datetime(df["open_time"])

            # 构建 KDTV 格式
            df["D"] = df["datetime"].dt.strftime("%Y%m%d")
            df["T"] = df["datetime"].dt.strftime("%H%M%S")
            df["K"] = df["symbol"]

            # 设置多级索引
            df = df.set_index(["K", "D", "T"]).sort_index()

            # 获取当前日期的数据
            date_data = df[df.index.get_level_values("D") == date]

            # 定义需要保存的特征
            features = [
                "close_price",
                "quote_volume",
                "high_price",
                "low_price",
                "open_price",
                "volume",
                "trades_count",
                "taker_buy_volume",
                "taker_buy_quote_volume",
            ]

            # 为每个特征存储数据
            for feature in features:
                # 获取特征数据并重塑为矩阵
                feature_data = date_data[feature]
                pivot_data = pd.pivot_table(
                    feature_data.reset_index(),
                    values=feature,
                    index="K",
                    columns="T",
                    aggfunc="mean",
                )
                array = pivot_data.values

                # 存储路径: data_path/freq/feature/YYYYMMDD.npy
                save_path = data_path / freq / feature / f"{date}.npy"
                save_path.parent.mkdir(parents=True, exist_ok=True)
                np.save(save_path, array)

            # 计算并存储衍生特征
            taker_sell_volume = date_data["volume"] - date_data["taker_buy_volume"]
            taker_sell_quote_volume = date_data["quote_volume"] - date_data["taker_buy_quote_volume"]

            for feature, feature_data in [
                ("taker_sell_volume", taker_sell_volume),
                ("taker_sell_quote_volume", taker_sell_quote_volume),
            ]:
                pivot_data = pd.pivot_table(
                    feature_data.reset_index(),
                    values=feature,
                    index="K",
                    columns="T",
                    aggfunc="mean",
                )
                array = pivot_data.values
                save_path = data_path / freq / feature / f"{date}.npy"
                save_path.parent.mkdir(parents=True, exist_ok=True)
                np.save(save_path, array)

        except Exception:
            logger.exception("KDTV 数据存储失败")
            raise

    @staticmethod
    def store_universe(
        symbols: List[str],
        data_path: Path | str = settings.DATA_STORAGE["PERPETUAL_DATA"],
    ) -> None:
        """存储交易对列表.

        Args:
            symbols: 交易对列表
            data_path: 数据存储根目录
        """
        data_path = StorageUtils._resolve_path(data_path)
        save_path = data_path / "universe_token.pkl"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        pd.Series(symbols).to_pickle(save_path)

    @staticmethod
    def read_kdtv_data(
        start_date: str,
        end_date: str,
        freq: Freq,
        features: List[str] | None = None,
        data_path: Path | str = settings.DATA_STORAGE["PERPETUAL_DATA"],
    ) -> pd.DataFrame:
        """读取 KDTV 格式数据.

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            freq: 频率
            features: 需要读取的特征列表
            data_path: 数据存储根目录

        Returns:
            pd.DataFrame: 多级索引的 DataFrame (K, D, T)
        """
        if features is None:
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
            ]

        try:
            data_path = StorageUtils._resolve_path(data_path)

            # 生成日期范围
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")
            dates = [d.strftime("%Y%m%d") for d in date_range]

            # 读取交易对列表
            universe_path = data_path / freq / "universe_token.pkl"
            if not universe_path.exists():
                raise FileNotFoundError(f"Universe file not found: {universe_path}")
            symbols = pd.read_pickle(universe_path)

            all_data = []

            # 按日期读取数据
            for date in dates:
                date_data = []

                # 读取每个特征的数据
                for feature in features:
                    file_path = data_path / freq / feature / f"{date}.npy"
                    if not file_path.exists():
                        logger.warning(f"Data file not found: {file_path}")
                        continue

                    array = np.load(file_path, allow_pickle=True)
                    if array.dtype == object:
                        array = array.astype(np.float64)

                    # 构建时间索引
                    times = pd.date_range(start=pd.Timestamp(date), periods=array.shape[1], freq=freq)

                    # 创建 DataFrame
                    df = pd.DataFrame(
                        array,
                        index=symbols[: len(array)],
                        columns=times,
                    )
                    stacked_series = df.stack()  # 将时间转为索引
                    stacked_series.name = feature
                    date_data.append(stacked_series)

                if date_data:
                    # 合并同一天的所有特征
                    day_df = pd.concat(date_data, axis=1)
                    day_df.index.names = ["symbol", "time"]
                    all_data.append(day_df)

            if not all_data:
                raise ValueError("No valid data found")

            # 合并所有日期的数据
            result = pd.concat(all_data)
            result.index.names = ["symbol", "time"]
            return result

        except Exception as e:
            logger.exception(f"Failed to read KDTV data: {e}")
            raise

    @staticmethod
    def read_and_visualize_kdtv(
        date: str,
        freq: Freq,
        data_path: Path | str,
        max_rows: int = 24,
        max_symbols: int = 5,
    ) -> None:
        """读取并可视化 KDTV 格式数据.

        Args:
            date: 日期 (YYYY-MM-DD)
            freq: 频率
            data_path: 数据存储根目录
            max_rows: 最大显示行数
            max_symbols: 最大显示交易对数量
        """
        try:
            # 修改调用方式，确保参数正确
            df = StorageUtils.read_kdtv_data(
                start_date=date,
                end_date=date,  # 只读取单日数据
                freq=freq,
                data_path=data_path,
            )

            # 获取所有可用的交易对
            available_symbols = df.index.get_level_values("symbol").unique()

            # 限制显示的交易对数量
            selected_symbols = available_symbols[:max_symbols]

            if not selected_symbols.empty:
                # 筛选数据
                df = df.loc[selected_symbols].head(max_rows)

                # 创建表格
                table = Table(
                    title=f"KDTV Data - {date} ({freq})",
                    show_header=True,
                    header_style="bold magenta",
                )

                # 添加列
                table.add_column("Time", style="cyan")
                table.add_column("Symbol", style="green")
                for col in df.columns:
                    table.add_column(col, justify="right")

                # 添加数据行
                for idx, row in df.iterrows():
                    if isinstance(idx, tuple) and len(idx) == 2:
                        symbol, time = idx
                        values = [(f"{x:.4f}" if isinstance(x, (float, np.floating)) else str(x)) for x in row]
                        table.add_row(str(time), str(symbol), *values)

                StorageUtils.console.print(table)
            else:
                logger.warning("No data available to display")

        except Exception as e:
            logger.exception(f"Failed to visualize KDTV data: {e}")
            raise

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
                StorageUtils.console.print(f"[yellow]Showing first {max_rows} rows of {len(data)} total rows[/]")

            # 创建表格
            table = Table(show_header=True, header_style="bold magenta")

            # 验证并添加列
            n_cols = data.shape[1]
            if headers and len(headers) != n_cols:
                raise ValueError(f"Headers length ({len(headers)}) doesn't match data columns ({n_cols})")

            table.add_column("Index", style="cyan")
            for header in headers or [f"Col_{i}" for i in range(n_cols)]:
                table.add_column(str(header), justify="right")

            # 验证并添加行
            if index and len(index) < len(data):
                StorageUtils.console.print("[yellow]Warning: Index length is less than data length[/]")

            for i, row in enumerate(data):
                try:
                    idx = index[i] if index and i < len(index) else f"Row_{i}"
                    formatted_values = [f"{x:.4f}" if isinstance(x, (float, np.floating)) else str(x) for x in row]
                    table.add_row(idx, *formatted_values)
                except Exception as e:
                    StorageUtils.console.print(f"[yellow]Warning: Error formatting row {i}: {e}[/]")
                    continue

            StorageUtils.console.print(table)

        except Exception as e:
            logger.exception("数据可视化失败: {}", str(e))
            raise
