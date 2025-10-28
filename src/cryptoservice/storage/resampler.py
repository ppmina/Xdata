"""数据重采样器.

提供K线数据重采样功能，支持将原始数据转换为低频数据或高频数据。
"""

import asyncio
from typing import Any

import pandas as pd

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq

logger = get_logger(__name__)


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
    async def resample_metrics(
        cls,
        metrics_df: pd.DataFrame,
        target_freq: Freq,
        agg_strategy: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """重采样 Metrics 数据到目标频率.

        专门处理非 OHLCV 类型的指标数据（资金费率、持仓量、多空比例等）。

        Args:
            metrics_df: 原始 Metrics 数据，使用 (symbol, timestamp) 多级索引
            target_freq: 目标频率
            agg_strategy: 自定义聚合策略，格式 {列名: 聚合方法}
                         支持的方法: "last", "mean", "median", "max", "min", "sum"
                         默认使用 "last"（取时间段内最后一个值）

        Returns:
            重采样后的 DataFrame

        Example:
            >>> # 将 5分钟级持仓量重采样到日线，取每日最后值
            >>> daily_oi = await DataResampler.resample_metrics(
            ...     oi_df,
            ...     Freq.d1,
            ...     {
            ...         "open_interest": "last"
            ...     },
            ... )
        """
        if metrics_df.empty:
            logger.warning("输入 Metrics DataFrame 为空，无法重采样")
            return metrics_df

        pandas_freq = cls.FREQ_MAP.get(target_freq)
        if not pandas_freq:
            raise ValueError(f"不支持的目标频率: {target_freq}")

        # 默认聚合策略：所有列都使用 "last"
        if agg_strategy is None:
            agg_strategy = dict.fromkeys(metrics_df.columns, "last")

        logger.info("resample_metrics_start", target_freq=target_freq.value)

        loop = asyncio.get_event_loop()
        result_df = await loop.run_in_executor(None, cls._resample_metrics_sync, metrics_df, pandas_freq, agg_strategy)

        logger.info("resample_metrics_complete", original_records=len(metrics_df), resampled_records=len(result_df))
        return result_df

    @staticmethod
    def _resample_metrics_sync(df: pd.DataFrame, pandas_freq: str, agg_strategy: dict[str, str]) -> pd.DataFrame:
        """同步执行 Metrics 重采样.

        Args:
            df: 原始数据
            pandas_freq: pandas 频率字符串
            agg_strategy: 聚合策略

        Returns:
            重采样后的 DataFrame
        """
        if df.index.nlevels != 2 or df.index.names != ["symbol", "timestamp"]:
            raise ValueError("DataFrame 必须使用 (symbol, timestamp) 作为多级索引")

        resampled_dfs = []

        # 按交易对分组处理
        for symbol in df.index.get_level_values("symbol").unique():
            symbol_data = df.loc[symbol].copy()

            # 将时间戳索引转换为 DatetimeIndex
            symbol_data.index = pd.to_datetime(symbol_data.index, unit="ms")

            # 筛选出存在于数据中的列
            available_columns = [col for col in symbol_data.columns if col in agg_strategy]
            if not available_columns:
                logger.warning(f"交易对 {symbol} 没有可重采样的列")
                continue

            # 构建该 symbol 的聚合规则
            symbol_agg = {col: agg_strategy[col] for col in available_columns}

            try:
                # 执行重采样
                resampled = symbol_data[available_columns].resample(pandas_freq).agg(symbol_agg)

                # 移除空的时间段
                resampled = resampled.dropna(how="all")

                if resampled.empty:
                    logger.warning(f"交易对 {symbol} 重采样后数据为空")
                    continue

                # 转换回毫秒时间戳
                resampled.index = (resampled.index.astype("int64") // 10**6).astype("int64")

                # 重建多级索引
                resampled.index = pd.MultiIndex.from_product([[symbol], resampled.index], names=["symbol", "timestamp"])

                resampled_dfs.append(resampled)

            except Exception as e:
                logger.error(f"重采样交易对 {symbol} 时出错: {e}")
                continue

        if not resampled_dfs:
            logger.warning("所有交易对重采样失败，返回空 DataFrame")
            return pd.DataFrame()

        result_df = pd.concat(resampled_dfs, axis=0).sort_index()
        return result_df

    @classmethod
    async def align_to_kline_timestamps(
        cls,
        metrics_df: pd.DataFrame,
        kline_df: pd.DataFrame,
        method: str = "asof",
        tolerance_ms: int = 24 * 60 * 60 * 1000,  # 默认容差 24 小时 (86400000 ms)
        return_original_timestamps: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
        """将 Metrics 数据对齐到 Kline 数据的时间点.

        使用 "as-of" 合并策略：对于每个 kline 时间点，找到该时间点之前
        最近的 metrics 值。这是金融数据处理的标准做法，可以避免未来数据泄露。

        Args:
            metrics_df: Metrics 数据，(symbol, timestamp) 多级索引
            kline_df: Kline 数据，(symbol, timestamp) 多级索引，作为时间基准
            method: 对齐方法
                - "asof": 向前查找（使用 <= 该时间点的最近值）**推荐**
                - "ffill": 前向填充（先 reindex 再填充）
                - "nearest": 最近值（可能使用未来数据，不推荐用于实时场景）
            tolerance_ms: 时间容差（毫秒），默认 24 小时，适配低频更新的 metrics（如资金费率每 8 小时）
            return_original_timestamps: 是否返回原始的 metrics timestamp

        Returns:
            对齐后的 Metrics 数据，时间戳与 kline_df 完全一致
            如果 return_original_timestamps=True，返回 (对齐后的数据, 原始timestamp的DataFrame)

        Example:
            >>> # 将持仓量数据对齐到日线 K线时间点
            >>> aligned_oi = await DataResampler.align_to_kline_timestamps(
            ...     oi_df,
            ...     kline_df,
            ...     method="asof",
            ...     tolerance_ms=3600000,
            ... )

            >>> # 同时获取原始 timestamp
            >>> (
            ...     aligned_oi,
            ...     original_ts,
            ... ) = await DataResampler.align_to_kline_timestamps(
            ...     oi_df,
            ...     kline_df,
            ...     return_original_timestamps=True,
            ... )
        """
        if metrics_df.empty:
            logger.warning("Metrics 数据为空，无法对齐")
            return metrics_df

        if kline_df.empty:
            logger.warning("Kline 数据为空，无法对齐")
            return pd.DataFrame()

        logger.info("align_to_kline_timestamps_start", method=method)

        loop = asyncio.get_event_loop()
        if return_original_timestamps:
            result = await loop.run_in_executor(None, cls._align_timestamps_sync, metrics_df, kline_df, method, tolerance_ms, return_original_timestamps)
            assert isinstance(result, tuple), "Expected tuple when return_original_timestamps=True"  # noqa: S101
            result_df, original_ts_df = result
            assert isinstance(result_df, pd.DataFrame), "Expected DataFrame"  # noqa: S101
            assert isinstance(original_ts_df, pd.DataFrame), "Expected DataFrame"  # noqa: S101
            logger.info(
                "align_to_kline_timestamps_complete",
                original_records=len(result_df),
                original_ts_records=len(original_ts_df),
            )
            return result_df, original_ts_df
        else:
            result = await loop.run_in_executor(None, cls._align_timestamps_sync, metrics_df, kline_df, method, tolerance_ms, False)
            # 当 return_original_timestamps=False 时，返回值是 DataFrame
            assert isinstance(result, pd.DataFrame), "Expected DataFrame when return_original_timestamps=False"  # noqa: S101
            logger.info(f"时间点对齐完成: {len(result)} 条记录")
            return result

    @staticmethod
    def _align_timestamps_sync(  # noqa: C901
        metrics_df: pd.DataFrame,
        kline_df: pd.DataFrame,
        method: str,
        tolerance_ms: int,
        return_original_timestamps: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
        """同步执行时间点对齐.

        Args:
            metrics_df: Metrics 数据
            kline_df: Kline 数据（时间基准）
            method: 对齐方法
            tolerance_ms: 时间容差（毫秒）
            return_original_timestamps: 是否返回原始timestamp

        Returns:
            对齐后的 DataFrame，或 (对齐后的DataFrame, 原始timestamp的DataFrame)
        """
        if metrics_df.index.nlevels != 2 or kline_df.index.nlevels != 2:
            raise ValueError("两个 DataFrame 必须使用 (symbol, timestamp) 多级索引")

        # 获取 kline 的所有 symbols
        kline_symbols = set(kline_df.index.get_level_values("symbol").unique())
        metrics_symbols = set(metrics_df.index.get_level_values("symbol").unique())

        # 只处理两者都有的 symbols
        common_symbols = kline_symbols & metrics_symbols
        if not common_symbols:
            logger.warning("Kline 和 Metrics 没有共同的交易对")
            return pd.DataFrame()

        aligned_dfs = []
        original_ts_dfs: list[pd.DataFrame] = [] if return_original_timestamps else []

        for symbol in common_symbols:
            try:
                # 提取单个 symbol 的数据
                kline_symbol = kline_df.loc[symbol].copy()
                metrics_symbol = metrics_df.loc[symbol].copy()

                # 重置索引，准备进行 merge_asof
                kline_symbol = kline_symbol.reset_index()  # timestamp 成为列
                metrics_symbol = metrics_symbol.reset_index()

                # 保存原始 timestamp（用于跟踪）
                if return_original_timestamps:
                    metrics_symbol["_original_timestamp"] = metrics_symbol["timestamp"]

                if method == "asof":
                    # 使用 merge_asof: 对于每个 kline timestamp，
                    # 找到 <= 该时间的最近 metrics 值
                    aligned = pd.merge_asof(
                        kline_symbol[["timestamp"]],
                        metrics_symbol,
                        on="timestamp",
                        direction="backward",  # 向前查找
                        tolerance=tolerance_ms,  # 容差
                        suffixes=("_kline", "_metrics"),
                    )

                elif method == "ffill":
                    # 先 reindex 到 kline 时间点，再前向填充
                    if return_original_timestamps:
                        # 创建一个映射，记录每个 timestamp 的原始值
                        metrics_ts_map = metrics_symbol.set_index("timestamp")["_original_timestamp"]

                    metrics_symbol = metrics_symbol.set_index("timestamp")
                    kline_timestamps = kline_symbol["timestamp"].values

                    aligned = metrics_symbol.reindex(kline_timestamps, method="ffill")
                    aligned = aligned.reset_index()

                    if return_original_timestamps:
                        # ffill 原始 timestamp
                        aligned["_original_timestamp"] = metrics_ts_map.reindex(kline_timestamps, method="ffill").values

                elif method == "nearest":
                    # reindex 到最近值（可能是未来数据）
                    if return_original_timestamps:
                        metrics_ts_map = metrics_symbol.set_index("timestamp")["_original_timestamp"]

                    metrics_symbol = metrics_symbol.set_index("timestamp")
                    kline_timestamps = kline_symbol["timestamp"].values

                    aligned = metrics_symbol.reindex(kline_timestamps, method="nearest", tolerance=tolerance_ms)
                    aligned = aligned.reset_index()

                    if return_original_timestamps:
                        aligned["_original_timestamp"] = metrics_ts_map.reindex(kline_timestamps, method="nearest", tolerance=tolerance_ms).values

                else:
                    raise ValueError(f"不支持的对齐方法: {method}")

                # 提取原始 timestamp（如果需要）
                if return_original_timestamps and "_original_timestamp" in aligned.columns:
                    original_ts_df = pd.DataFrame(
                        {
                            "symbol": symbol,
                            "timestamp": aligned["timestamp"],
                            "original_timestamp": aligned["_original_timestamp"],
                        }
                    )
                    original_ts_df = original_ts_df.set_index(["symbol", "timestamp"])
                    original_ts_dfs.append(original_ts_df)

                    # 从对齐后的数据中移除原始 timestamp 列
                    aligned = aligned.drop(columns=["_original_timestamp"], errors="ignore")

                # 添加 symbol 列并设置多级索引
                aligned["symbol"] = symbol
                aligned = aligned.set_index(["symbol", "timestamp"])

                aligned_dfs.append(aligned)

            except Exception as e:
                logger.error(f"对齐交易对 {symbol} 时出错: {e}")
                continue

        if not aligned_dfs:
            logger.warning("所有交易对对齐失败")
            if return_original_timestamps:
                return pd.DataFrame(), pd.DataFrame()
            return pd.DataFrame()

        result_df = pd.concat(aligned_dfs, axis=0).sort_index()

        if return_original_timestamps and original_ts_dfs:
            original_ts_result = pd.concat(original_ts_dfs, axis=0).sort_index()
            return result_df, original_ts_result

        return result_df if not return_original_timestamps else (result_df, pd.DataFrame())

    @classmethod
    async def resample_and_align(
        cls,
        metrics_df: pd.DataFrame,
        kline_df: pd.DataFrame,
        target_freq: Freq,
        agg_strategy: dict[str, str] | None = None,
        align_method: str = "asof",
        tolerance_ms: int = 24 * 60 * 60 * 1000,  # 默认容差 24 小时 (86400000 ms)
        return_original_timestamps: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
        """一站式：重采样 Metrics 数据并对齐到 Kline 时间点.

        这是推荐的统一接口，确保数据的时间一致性。

        工作流程：
        1. 将 metrics 数据重采样到目标频率（如日线）
        2. 使用 as-of 策略对齐到 kline 的实际时间点
        3. 返回与 kline 时间戳完全一致的数据

        Args:
            metrics_df: 原始 Metrics 数据（高频，如 5分钟）
            kline_df: Kline 数据（作为时间基准）
            target_freq: 目标频率（应与 kline 频率一致）
            agg_strategy: 重采样聚合策略
            align_method: 时间对齐方法（推荐 "asof"）
            tolerance_ms: 时间容差（毫秒），默认 24 小时
            return_original_timestamps: 是否返回原始 timestamp（对齐前的实际时间）

        Returns:
            重采样并对齐后的 Metrics 数据，如果 return_original_timestamps=True，
            则返回 (aligned_df, original_timestamp_df)

        Example:
            >>> # 将 5分钟持仓量重采样为日线，并对齐到日线 K线时间点
            >>> aligned_oi = await DataResampler.resample_and_align(
            ...     oi_5m_df,
            ...     kline_1d_df,
            ...     target_freq=Freq.d1,
            ...     agg_strategy={
            ...         "open_interest": "last"
            ...     },
            ...     align_method="asof",
            ... )
        """
        logger.info("resample_and_align_start")

        # 步骤 1: 重采样到目标频率
        resampled = await cls.resample_metrics(metrics_df, target_freq, agg_strategy)

        if resampled.empty:
            logger.warning("重采样后数据为空")
            if return_original_timestamps:
                return resampled, pd.DataFrame()
            return resampled

        # 步骤 2: 对齐到 kline 时间点
        if return_original_timestamps:
            result = await cls.align_to_kline_timestamps(resampled, kline_df, align_method, tolerance_ms, return_original_timestamps=True)
            assert isinstance(result, tuple), "Expected tuple when return_original_timestamps=True"  # noqa: S101
            aligned, original_ts = result
            assert isinstance(aligned, pd.DataFrame), "Expected DataFrame"  # noqa: S101
            assert isinstance(original_ts, pd.DataFrame), "Expected DataFrame"  # noqa: S101
            logger.info(
                "resample_and_align_complete",
                original_records=len(resampled),
                resampled_records=len(aligned),
                original_ts_records=len(original_ts),
            )
            return aligned, original_ts
        else:
            result = await cls.align_to_kline_timestamps(resampled, kline_df, align_method, tolerance_ms)
            # 当 return_original_timestamps=False 时（默认），返回值是 DataFrame
            assert isinstance(result, pd.DataFrame), "Expected DataFrame when return_original_timestamps=False"  # noqa: S101
            logger.info("重采样并对齐完成")
            return result

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
