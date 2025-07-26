"""异步数据导出工具.

基于异步数据库的高性能数据导出功能。
"""

import asyncio
import logging
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from cryptoservice.models import Freq

from .async_storage_db import AsyncMarketDB

logger = logging.getLogger(__name__)


class AsyncDataExporter:
    """异步数据导出器."""

    def __init__(self, db: AsyncMarketDB):
        """初始化异步数据导出器.

        Args:
            db: 异步市场数据库实例
        """
        self.db = db

    async def export_to_numpy(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        output_path: str | Path,
        features: list[str] | None = None,
        chunk_size: int = 1000000,  # 1M记录一批
    ) -> None:
        """异步导出数据为NumPy格式.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 频率
            output_path: 输出路径
            features: 特征列表
            chunk_size: 分块大小
        """
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"开始异步导出NumPy数据: {len(symbols)} 个交易对")

        # 获取数据
        df = await self.db.read_data(
            start_time=start_time,
            end_time=end_time,
            freq=freq,
            symbols=symbols,
            features=features,
            raise_on_empty=False,
        )

        if df.empty:
            logger.warning("没有数据可导出")
            return

        # 按日期分组导出
        await self._export_by_date(df, output_path, freq, chunk_size)

        logger.info(f"NumPy数据导出完成: {output_path}")

    async def _export_by_date(
        self,
        df: pd.DataFrame,
        output_path: Path,
        freq: Freq,
        chunk_size: int,
    ) -> None:
        """按日期分组导出数据。."""
        timestamps = df.index.get_level_values("timestamp")
        dates = sorted({pd.to_datetime(ts, unit="ms").date() for ts in timestamps})

        # 并发处理多个日期
        tasks = []
        for date in dates:
            task = self._export_single_date(df, date, output_path, freq)
            tasks.append(task)

        # 分批执行任务，避免内存过载
        batch_size = 10
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            await asyncio.gather(*batch)

            # 给系统一些时间进行垃圾回收
            if i + batch_size < len(tasks):
                await asyncio.sleep(0.1)

    async def _export_single_date(
        self,
        df: pd.DataFrame,
        date,
        output_path: Path,
        freq: Freq,
    ) -> None:
        """导出单个日期的数据。."""
        date_str = date.strftime("%Y%m%d")

        # 筛选当天数据
        timestamps = df.index.get_level_values("timestamp")
        day_mask = pd.to_datetime(timestamps, unit="ms").date == date
        day_data = df.loc[day_mask]

        if day_data.empty:
            return

        # 保存交易对顺序
        symbols_path = output_path / freq.value / "symbols"
        symbols_path.mkdir(parents=True, exist_ok=True)

        symbols = day_data.index.get_level_values("symbol").unique()
        pd.Series(symbols).to_pickle(symbols_path / f"{date_str}.pkl")

        # 异步处理每个特征
        tasks = []
        for feature in day_data.columns:
            task = self._export_single_feature(day_data, feature, output_path, freq, date_str)
            tasks.append(task)

        await asyncio.gather(*tasks)

    async def _export_single_feature(
        self,
        day_data: pd.DataFrame,
        feature: str,
        output_path: Path,
        freq: Freq,
        date_str: str,
    ) -> None:
        """导出单个特征的数据。."""
        # 在线程池中执行CPU密集型操作
        loop = asyncio.get_event_loop()

        def process_feature():
            try:
                # 重塑数据为 K x T 矩阵
                feature_data = day_data[feature].unstack("timestamp")
                array = feature_data.values

                # 创建存储路径
                save_path = output_path / freq.value / feature
                save_path.mkdir(parents=True, exist_ok=True)

                # 保存为npy文件
                np.save(save_path / f"{date_str}.npy", array)

                return len(array)
            except Exception as e:
                logger.error(f"处理特征 {feature} 时出错: {e}")
                return 0

        # 在线程池中执行
        count = await loop.run_in_executor(None, process_feature)

        if count > 0:
            logger.debug(f"特征 {feature} 导出完成: {count} 个交易对")

    async def export_to_csv(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        output_path: str | Path,
        features: list[str] | None = None,
        chunk_size: int = 100000,
    ) -> None:
        """异步导出数据为CSV格式。.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 频率
            output_path: 输出路径
            features: 特征列表
            chunk_size: 分块大小
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"开始异步导出CSV数据: {len(symbols)} 个交易对")

        # 获取数据
        df = await self.db.read_data(
            start_time=start_time,
            end_time=end_time,
            freq=freq,
            symbols=symbols,
            features=features,
            raise_on_empty=False,
        )

        if df.empty:
            logger.warning("没有数据可导出")
            return

        # 在线程池中处理CSV导出
        loop = asyncio.get_event_loop()

        def process_csv():
            # 重置索引使其成为列
            df_reset = df.reset_index()

            # 转换时间戳为可读格式
            df_reset["datetime"] = pd.to_datetime(df_reset["timestamp"], unit="ms")

            # 分块保存大文件
            if len(df_reset) > chunk_size:
                for i in range(0, len(df_reset), chunk_size):
                    chunk = df_reset.iloc[i : i + chunk_size]
                    chunk_path = output_path.parent / f"{output_path.stem}_part_{i // chunk_size + 1}.csv"
                    chunk.to_csv(chunk_path, index=False)
            else:
                df_reset.to_csv(output_path, index=False)

        await loop.run_in_executor(None, process_csv)

        logger.info(f"CSV数据导出完成: {output_path}")

    async def export_to_parquet(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        output_path: str | Path,
        features: list[str] | None = None,
        compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
    ) -> None:
        """异步导出数据为Parquet格式。.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 频率
            output_path: 输出路径
            features: 特征列表
            compression: 压缩方式
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"开始异步导出Parquet数据: {len(symbols)} 个交易对")

        # 获取数据
        df = await self.db.read_data(
            start_time=start_time,
            end_time=end_time,
            freq=freq,
            symbols=symbols,
            features=features,
            raise_on_empty=False,
        )

        if df.empty:
            logger.warning("没有数据可导出")
            return

        # 在线程池中处理Parquet导出
        loop = asyncio.get_event_loop()

        def process_parquet():
            # 重置索引
            df_reset = df.reset_index()

            # 保存为Parquet，使用压缩
            df_reset.to_parquet(output_path, compression=compression, index=False)

        await loop.run_in_executor(None, process_parquet)

        logger.info(f"Parquet数据导出完成: {output_path}")

    async def export_summary_statistics(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        output_path: str | Path,
    ) -> None:
        """异步导出数据统计摘要。.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 频率
            output_path: 输出路径
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"开始异步导出统计摘要: {len(symbols)} 个交易对")

        # 获取数据
        df = await self.db.read_data(
            start_time=start_time,
            end_time=end_time,
            freq=freq,
            symbols=symbols,
            raise_on_empty=False,
        )

        if df.empty:
            logger.warning("没有数据可导出")
            return

        # 在线程池中计算统计信息
        loop = asyncio.get_event_loop()

        def process_statistics():
            # 按交易对分组计算统计信息
            stats_list = []

            for symbol in symbols:
                if symbol in df.index.get_level_values("symbol"):
                    symbol_data = df.loc[symbol]

                    # 计算基本统计信息
                    stats = symbol_data.describe()
                    stats["symbol"] = symbol
                    stats["count"] = len(symbol_data)
                    stats["start_time"] = start_time
                    stats["end_time"] = end_time
                    stats["freq"] = freq.value

                    stats_list.append(stats)

            if stats_list:
                # 合并所有统计信息
                combined_stats = pd.concat(stats_list, axis=0, ignore_index=True)

                # 保存为CSV
                combined_stats.to_csv(output_path, index=False)

        await loop.run_in_executor(None, process_statistics)

        logger.info(f"统计摘要导出完成: {output_path}")
