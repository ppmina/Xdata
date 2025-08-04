"""NumPy格式导出器.

专门处理NumPy格式的数据导出。
"""

import asyncio
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from cryptoservice.models import Freq

from ..queries import KlineQuery
from ..resampler import DataResampler

logger = logging.getLogger(__name__)


class NumpyExporter:
    """NumPy格式导出器.

    将K线数据导出为NumPy .npy文件格式，支持按日期分组和特征分离。
    """

    def __init__(self, kline_query: KlineQuery, resampler: DataResampler | None = None):
        """初始化NumPy导出器.

        Args:
            kline_query: K线数据查询器
            resampler: 数据重采样器，可选
        """
        self.kline_query = kline_query
        self.resampler = resampler

    async def export_klines(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        output_path: Path,
        target_freq: Freq | None = None,
        chunk_days: int = 30,
    ) -> None:
        """导出K线数据为NumPy格式.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            freq: 数据频率
            output_path: 输出路径
            target_freq: 目标频率，如果指定则进行重采样
            chunk_days: 分块天数，用于大数据集处理
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return

        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"开始导出NumPy数据: {len(symbols)} 个交易对 ({start_time} - {end_time})")

        # 获取数据
        df = await self.kline_query.select_by_time_range(symbols, start_time, end_time, freq)

        if df.empty:
            logger.warning("没有数据可导出")
            return

        # 重采样（如果需要）
        export_freq = freq
        if target_freq and self.resampler:
            logger.info(f"重采样数据从 {freq.value} 到 {target_freq.value}")
            df = await self.resampler.resample(df, target_freq)
            export_freq = target_freq

        # 按日期分组导出
        await self._export_by_dates(df, output_path, export_freq)

        logger.info(f"NumPy数据导出完成: {output_path}")

    async def _export_by_dates(self, df: pd.DataFrame, output_path: Path, freq: Freq) -> None:
        """按日期分组导出数据.

        Args:
            df: 数据DataFrame
            output_path: 输出路径
            freq: 数据频率
        """
        # 获取所有唯一日期
        timestamps = df.index.get_level_values("timestamp")
        unique_dates = sorted({pd.Timestamp(ts, unit="ms").date() for ts in timestamps})

        logger.info(f"按日期导出数据: {len(unique_dates)} 天")

        # 并发处理多个日期
        tasks = []
        for date in unique_dates:
            task = self._export_single_date(df, date, output_path, freq)
            tasks.append(task)

        # 分批执行任务，避免内存过载
        batch_size = 5
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            await asyncio.gather(*batch)

            # 给系统一些时间进行垃圾回收
            if i + batch_size < len(tasks):
                await asyncio.sleep(0.1)

    async def _export_single_date(self, df: pd.DataFrame, date, output_path: Path, freq: Freq) -> None:
        """导出单个日期的数据.

        Args:
            df: 数据DataFrame
            date: 日期对象
            output_path: 输出路径
            freq: 数据频率
        """
        date_str = date.strftime("%Y%m%d")

        # 筛选当天数据
        timestamps = df.index.get_level_values("timestamp")
        day_mask = pd.Series(timestamps).apply(lambda ts: pd.Timestamp(ts, unit="ms").date() == date)
        day_data = df[day_mask.values]

        if day_data.empty:
            return

        logger.debug(f"导出日期 {date_str}: {len(day_data)} 条记录")

        # 保存交易对顺序
        await self._save_symbols(day_data, output_path, freq, date_str)

        # 并行处理所有特征
        feature_tasks = []
        for feature in day_data.columns:
            task = self._export_single_feature(day_data, feature, output_path, freq, date_str)
            feature_tasks.append(task)

        # 分批执行特征导出任务
        batch_size = 3
        for i in range(0, len(feature_tasks), batch_size):
            batch = feature_tasks[i : i + batch_size]
            await asyncio.gather(*batch)

    async def _save_symbols(self, day_data: pd.DataFrame, output_path: Path, freq: Freq, date_str: str) -> None:
        """保存交易对顺序信息.

        Args:
            day_data: 当天数据
            output_path: 输出路径
            freq: 数据频率
            date_str: 日期字符串
        """
        loop = asyncio.get_event_loop()

        def save_symbols():
            symbols_path = output_path / freq.value / "symbols"
            symbols_path.mkdir(parents=True, exist_ok=True)

            symbols = day_data.index.get_level_values("symbol").unique()
            pd.Series(symbols).to_pickle(symbols_path / f"{date_str}.pkl")

        await loop.run_in_executor(None, save_symbols)

    async def _export_single_feature(
        self, day_data: pd.DataFrame, feature: str, output_path: Path, freq: Freq, date_str: str
    ) -> None:
        """导出单个特征的数据.

        Args:
            day_data: 当天数据
            feature: 特征名称
            output_path: 输出路径
            freq: 数据频率
            date_str: 日期字符串
        """
        loop = asyncio.get_event_loop()

        def process_and_save():
            try:
                # 重塑数据为 K x T 矩阵 (交易对 x 时间)
                feature_data = day_data[feature].unstack("timestamp")
                array = feature_data.values

                # 处理缺失值
                if np.isnan(array).any():
                    logger.debug(f"特征 {feature} 包含缺失值，使用前向填充")
                    # 使用前向填充处理缺失值
                    df_filled = pd.DataFrame(array, index=feature_data.index, columns=feature_data.columns)
                    df_filled = df_filled.ffill(axis=1)
                    array = df_filled.values

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
        count = await loop.run_in_executor(None, process_and_save)

        if count > 0:
            logger.debug(f"特征 {feature} 导出完成: {count} 个交易对")

    async def export_with_custom_features(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        output_path: Path,
        feature_mapping: dict[str, str] | None = None,
    ) -> None:
        """使用自定义特征映射导出数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            output_path: 输出路径
            feature_mapping: 特征映射 {原始列名: 导出文件名}
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return

        # 默认特征映射（使用简短名称）
        if feature_mapping is None:
            feature_mapping = {
                "open_price": "opn",
                "high_price": "hgh",
                "low_price": "low",
                "close_price": "cls",
                "volume": "vol",
                "quote_volume": "amt",
                "trades_count": "tnum",
                "taker_buy_volume": "tbvol",
                "taker_buy_quote_volume": "tbamt",
                "taker_sell_volume": "tsvol",
                "taker_sell_quote_volume": "tsamt",
            }

        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"开始导出NumPy数据（自定义特征）: {len(symbols)} 个交易对")

        # 获取数据
        columns = list(feature_mapping.keys())
        df = await self.kline_query.select_by_time_range(symbols, start_time, end_time, freq, columns=columns)

        if df.empty:
            logger.warning("没有数据可导出")
            return

        # 重命名列
        df = df.rename(columns=feature_mapping)

        # 按日期分组导出
        await self._export_by_dates(df, output_path, freq)

        logger.info(f"NumPy数据导出完成: {output_path}")

    async def export_summary_info(
        self, symbols: list[str], start_time: str, end_time: str, freq: Freq, output_path: Path
    ) -> dict[str, Any]:
        """导出数据概要信息.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            output_path: 输出路径

        Returns:
            概要信息字典
        """
        output_path = Path(output_path)

        # 获取数据概要
        df = await self.kline_query.select_by_time_range(symbols, start_time, end_time, freq, columns=["close_price"])

        summary: dict[str, Any]
        if df.empty:
            summary = {"status": "no_data", "symbols": symbols, "period": f"{start_time} - {end_time}"}
        else:
            timestamps = df.index.get_level_values("timestamp")
            summary = {
                "status": "success",
                "symbols": symbols,
                "actual_symbols": list(df.index.get_level_values("symbol").unique()),
                "period": f"{start_time} - {end_time}",
                "frequency": freq.value,
                "total_records": len(df),
                "date_range": {
                    "start": pd.Timestamp(timestamps.min(), unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
                    "end": pd.Timestamp(timestamps.max(), unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
                },
                "unique_dates": len({pd.Timestamp(ts, unit="ms").date() for ts in timestamps}),
            }

        # 保存概要信息
        summary_path = output_path / freq.value / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)

        import json

        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        logger.info(f"概要信息已保存: {summary_path}")
        return summary
