"""数据重采样器.

提供K线数据重采样功能，支持将高频数据转换为低频数据。
"""

import asyncio
import logging
from typing import Any

import pandas as pd

from cryptoservice.models import Freq

logger = logging.getLogger(__name__)


class DataResampler:
    """数据重采样器.

    专注于K线数据的重采样操作。
    """

    # 频率映射到pandas频率字符串
    FREQ_MAP = {
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

    # K线数据聚合规则
    AGG_RULES = {
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

    @classmethod
    async def resample(cls, df: pd.DataFrame, target_freq: Freq) -> pd.DataFrame:
        """重采样K线数据到目标频率.

        Args:
            df: 原始K线数据DataFrame，使用(symbol, timestamp)作为多级索引
            target_freq: 目标频率

        Returns:
            重采样后的DataFrame，保持相同的索引结构
        """
        if df.empty:
            logger.warning("输入DataFrame为空，无法重采样")
            return df

        pandas_freq = cls.FREQ_MAP.get(target_freq)
        if not pandas_freq:
            raise ValueError(f"不支持的目标频率: {target_freq}")

        logger.info(f"开始重采样数据到 {target_freq.value}")

        # 在线程池中执行重采样操作（CPU密集型）
        loop = asyncio.get_event_loop()
        result_df = await loop.run_in_executor(None, cls._resample_sync, df, pandas_freq, cls.AGG_RULES)

        logger.info("数据重采样完成")
        return result_df

    @staticmethod
    def _resample_sync(df: pd.DataFrame, pandas_freq: str, agg_rules: dict[str, str]) -> pd.DataFrame:
        """同步重采样实现.

        Args:
            df: 原始数据
            pandas_freq: pandas频率字符串
            agg_rules: 聚合规则

        Returns:
            重采样后的DataFrame
        """
        if df.index.nlevels != 2 or df.index.names != ["symbol", "timestamp"]:
            raise ValueError("DataFrame必须使用(symbol, timestamp)作为多级索引")

        resampled_dfs = []

        # 按交易对分组处理
        for symbol in df.index.get_level_values("symbol").unique():
            symbol_data = df.loc[symbol].copy()

            # 将时间戳索引转换为DatetimeIndex
            symbol_data.index = pd.to_datetime(symbol_data.index, unit="ms")

            # 过滤出存在于聚合规则中的列
            available_columns = [col for col in symbol_data.columns if col in agg_rules]
            if not available_columns:
                logger.warning(f"交易对 {symbol} 没有可重采样的列")
                continue

            # 使用可用列的聚合规则
            symbol_agg_rules = {col: agg_rules[col] for col in available_columns}

            # 执行重采样
            try:
                resampled = symbol_data[available_columns].resample(pandas_freq).agg(symbol_agg_rules)

                # 移除空的时间段
                resampled = resampled.dropna(how="all")

                if resampled.empty:
                    logger.warning(f"交易对 {symbol} 重采样后数据为空")
                    continue

                # 将DatetimeIndex转换回时间戳
                resampled.index = (resampled.index.astype("int64") // 10**6).astype("int64")

                # 重建多级索引
                resampled.index = pd.MultiIndex.from_product([[symbol], resampled.index], names=["symbol", "timestamp"])

                resampled_dfs.append(resampled)

            except Exception as e:
                logger.error(f"重采样交易对 {symbol} 时出错: {e}")
                continue

        if not resampled_dfs:
            logger.warning("所有交易对重采样失败，返回空DataFrame")
            return pd.DataFrame()

        # 合并所有交易对的重采样结果
        result_df = pd.concat(resampled_dfs, axis=0)

        # 按索引排序
        result_df = result_df.sort_index()

        return result_df

    @classmethod
    async def resample_with_validation(cls, df: pd.DataFrame, source_freq: Freq, target_freq: Freq) -> pd.DataFrame:
        """带验证的重采样操作.

        Args:
            df: 原始数据
            source_freq: 源频率
            target_freq: 目标频率

        Returns:
            重采样后的数据

        Raises:
            ValueError: 当频率转换不合理时
        """
        # 验证频率转换的合理性
        if not cls._is_valid_frequency_conversion(source_freq, target_freq):
            raise ValueError(f"不支持从 {source_freq.value} 重采样到 {target_freq.value}")

        return await cls.resample(df, target_freq)

    @classmethod
    def _is_valid_frequency_conversion(cls, source_freq: Freq, target_freq: Freq) -> bool:
        """验证频率转换是否合理.

        Args:
            source_freq: 源频率
            target_freq: 目标频率

        Returns:
            是否为有效的转换
        """
        # 频率优先级（数值越小频率越高）
        freq_priority = {
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
            Freq.w1: 10080,
            Freq.M1: 43200,  # 近似值
        }

        source_priority = freq_priority.get(source_freq, 0)
        target_priority = freq_priority.get(target_freq, 0)

        # 只能从高频率重采样到低频率
        return source_priority < target_priority

    @classmethod
    async def batch_resample(cls, df: pd.DataFrame, target_frequencies: list[Freq]) -> dict[Freq, pd.DataFrame]:
        """批量重采样到多个目标频率.

        Args:
            df: 原始数据
            target_frequencies: 目标频率列表

        Returns:
            {频率: DataFrame} 重采样结果字典
        """
        if df.empty:
            return {freq: pd.DataFrame() for freq in target_frequencies}

        logger.info(f"开始批量重采样到 {len(target_frequencies)} 个频率")

        # 并发执行多个重采样任务
        tasks = []
        for freq in target_frequencies:
            task = cls.resample(df.copy(), freq)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 整理结果
        resampled_data: dict[Freq, pd.DataFrame] = {}
        for freq, result in zip(target_frequencies, results, strict=False):
            if isinstance(result, Exception):
                logger.error(f"重采样到 {freq.value} 失败: {result}")
                resampled_data[freq] = pd.DataFrame()
            else:
                assert isinstance(result, pd.DataFrame)  # noqa: S101
                resampled_data[freq] = result

        logger.info("批量重采样完成")
        return resampled_data

    @classmethod
    def get_supported_conversions(cls, source_freq: Freq) -> list[Freq]:
        """获取支持的转换目标频率.

        Args:
            source_freq: 源频率

        Returns:
            支持的目标频率列表
        """
        supported = []
        for target_freq in Freq:
            if cls._is_valid_frequency_conversion(source_freq, target_freq):
                supported.append(target_freq)

        return supported

    @classmethod
    async def validate_data_for_resampling(cls, df: pd.DataFrame) -> dict[str, Any]:
        """验证数据是否适合重采样.

        Args:
            df: 要验证的数据

        Returns:
            验证结果字典
        """
        validation: dict[str, Any] = {"is_valid": True, "errors": [], "warnings": [], "info": {}}

        # 检查索引结构
        if df.index.nlevels != 2 or df.index.names != ["symbol", "timestamp"]:
            validation["is_valid"] = False
            validation["errors"].append("DataFrame必须使用(symbol, timestamp)作为多级索引")

        # 检查必需的列
        required_columns = ["open_price", "high_price", "low_price", "close_price", "volume"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            validation["is_valid"] = False
            validation["errors"].append(f"缺少必需的列: {missing_columns}")

        # 检查数据完整性
        if df.empty:
            validation["warnings"].append("数据为空")
        else:
            # 检查每个交易对的时间戳连续性
            for symbol in df.index.get_level_values("symbol").unique():
                symbol_data = df.loc[symbol]
                timestamps = symbol_data.index.values

                if len(timestamps) < 2:
                    validation["warnings"].append(f"交易对 {symbol} 数据点过少")
                    continue

                # 检查时间戳是否单调递增
                if not pd.Series(timestamps).is_monotonic_increasing:
                    validation["warnings"].append(f"交易对 {symbol} 时间戳不是单调递增")

        # 统计信息
        validation["info"] = {
            "total_records": len(df),
            "symbols_count": len(df.index.get_level_values("symbol").unique()),
            "columns": list(df.columns),
            "time_range": {
                "start": df.index.get_level_values("timestamp").min() if not df.empty else None,
                "end": df.index.get_level_values("timestamp").max() if not df.empty else None,
            },
        }

        return validation
