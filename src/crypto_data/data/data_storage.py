"""数据存储服务，提供 Parquet 和 KDTV 两种存储格式."""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal, Optional

import numpy as np
import pandas as pd
from joblib import Parallel, delayed

from crypto_data.models import MarketTicker


class DataStorage:
    def __init__(
        self,
        data_path: str = "data/market",
        storage_type: Literal["parquet", "kdtv"] = "kdtv",  # 默认使用 kdtv 格式
    ):
        """初始化数据存储服务.

        Args:
            data_path: 数据存储根目录
            storage_type: 存储类型，'parquet' 或 'kdtv'
        """
        self.data_path = Path(data_path)
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.storage_type = storage_type

    def store_klines_data(
        self,
        data: List[MarketTicker],
        start_date: int,
        end_date: int,
        freq: str,
        univ: str,
        n_jobs: int = 10,
    ) -> None:
        """存储K线数据，支持并行处理."""
        # 按日期分组
        data_by_date = self._group_by_date(data)

        if self.storage_type == "kdtv":
            # 并行处理每天的数据
            Parallel(n_jobs=n_jobs)(
                delayed(self._store_kdtv)(daily_data, date, freq, univ)
                for date, daily_data in data_by_date.items()
            )
        else:
            # Parquet存储
            Parallel(n_jobs=n_jobs)(
                delayed(self._store_parquet)(daily_data, date)
                for date, daily_data in data_by_date.items()
            )

    def _group_by_date(self, data: List[MarketTicker]) -> Dict[str, List[MarketTicker]]:
        """将数据按日期分组."""
        grouped_data: Dict[str, List[MarketTicker]] = {}
        for ticker in data:
            date = datetime.fromtimestamp(int(ticker.open_time) / 1000).strftime("%Y%m%d")
            if date not in grouped_data:
                grouped_data[date] = []
            grouped_data[date].append(ticker)
        return grouped_data

    def _store_kdtv(self, data: List[MarketTicker], date: str, freq: str, univ: str) -> None:
        """使用 KDTV 格式存储单日数据."""
        df = pd.DataFrame([d.__dict__ for d in data])

        # 构建 KDTV 格式
        df["D"] = pd.to_datetime(df["open_time"]).dt.strftime("%Y%m%d")
        df["T"] = pd.to_datetime(df["open_time"]).dt.strftime("%H%M%S")
        df["K"] = df["symbol"]

        # 设置索引并排序
        df = df.set_index(["K", "D", "T"]).sort_index()

        # 转换为数组格式
        array = df[["last_price", "volume", "quote_volume", "high_price", "low_price"]].values

        # 构建保存路径
        save_path = self.data_path / univ / freq / f"{date}.npy"
        save_path.parent.mkdir(parents=True, exist_ok=True)

        # 保存数据
        np.save(save_path, array)

    def read_klines_data(
        self,
        start_date: int,
        end_date: int,
        freq: str,
        univ: str,
        symbols: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """读取K线数据."""
        dfs = []
        current = pd.Timestamp(str(start_date))
        end = pd.Timestamp(str(end_date))

        while current <= end:
            date = current.strftime("%Y%m%d")
            if self.storage_type == "kdtv":
                file_path = self.data_path / univ / freq / f"{date}.npy"
                if file_path.exists():
                    df = self._read_kdtv_file(file_path, symbols)
                    dfs.append(df)
            else:
                file_path = self.data_path / f"{date}.parquet"
                if file_path.exists():
                    df = pd.read_parquet(file_path)
                    if symbols:
                        df = df[df["symbol"].isin(symbols)]
                    dfs.append(df)

            current += pd.Timedelta(days=1)

        return pd.concat(dfs) if dfs else pd.DataFrame()

    def _read_kdtv_file(self, file_path: Path, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """读取单个 KDTV 文件."""
        data = np.load(file_path)
        df = pd.DataFrame(
            data, columns=["last_price", "volume", "quote_volume", "high_price", "low_price"]
        )
        if symbols:
            df = df[df.index.get_level_values("K").isin(symbols)]
        return df

    def _store_parquet(self, data: List[MarketTicker], date: str) -> None:
        """使用 Parquet 格式存储."""
        df = pd.DataFrame([d.__dict__ for d in data])
        file_path = self.data_path / f"{date}.parquet"
        df.to_parquet(file_path, compression="snappy")

        """使用 KDTV 格式存储."""
        df = pd.DataFrame([d.__dict__ for d in data])
        # 使用 open_time 替代 timestamp
        df["D"] = pd.to_datetime(df["open_time"]).dt.strftime("%Y%m%d")
        df["T"] = pd.to_datetime(df["open_time"]).dt.strftime("%H%M%S")
        df["K"] = df["symbol"]

        # 构建 KDTV 格式数据
        kdtv_data = df[
            [
                "K",  # symbol
                "D",  # date
                "T",  # time
                "last_price",  # 作为 value
                "volume",
                "quote_volume",
                "high_price",
                "low_price",
            ]
        ].values

        # 保存为 .npy 文件
        file_path = self.data_path / f"{date}.npy"
        np.save(file_path, kdtv_data)

    def read_market_data(
        self, start_date: str, end_date: str, symbol: Optional[str] = None
    ) -> pd.DataFrame:
        """读取指定时间范围的市场数据.

        Args:
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            symbol: 可选的币种过滤

        Returns:
            DataFrame: 市场数据
        """
        if self.storage_type == "parquet":
            return self._read_parquet(start_date, end_date, symbol)
        else:
            return self._read_kdtv(start_date, end_date, symbol)

    def _read_parquet(
        self, start_date: str, end_date: str, symbol: Optional[str] = None
    ) -> pd.DataFrame:
        """从 Parquet 文件读取数据."""
        dfs = []
        current = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)

        while current <= end:
            file_path = self.data_path / f'{current.strftime("%Y%m%d")}.parquet'
            if file_path.exists():
                df = pd.read_parquet(file_path)
                if symbol:
                    df = df[df["symbol"] == symbol]
                dfs.append(df)
            current += pd.Timedelta(days=1)

        return pd.concat(dfs) if dfs else pd.DataFrame()

    def _read_kdtv(
        self, start_date: str, end_date: str, symbol: Optional[str] = None
    ) -> pd.DataFrame:
        """从 KDTV 格式文件读取数据."""
        dfs = []
        current = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)

        while current <= end:
            file_path = self.data_path / f'{current.strftime("%Y%m%d")}.npy'
            if file_path.exists():
                data = np.load(file_path)
                df = pd.DataFrame(data, columns=["K", "D", "T", "V"])
                if symbol:
                    df = df[df["K"] == symbol]
                dfs.append(df)
            current += pd.Timedelta(days=1)

        return pd.concat(dfs) if dfs else pd.DataFrame()
