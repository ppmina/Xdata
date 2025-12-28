"""NumPy格式导出器.

专门处理NumPy格式的数据导出。
"""

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq

from ..queries import KlineQuery
from ..resampler import DataResampler

if TYPE_CHECKING:
    from ..queries import MetricsQuery

logger = get_logger(__name__)


class NumpyExporter:
    """NumPy格式导出器.

    将K线数据和Metrics数据导出为NumPy .npy文件格式，支持按日期分组和特征分离。
    """

    # 默认字段映射：长字段名 -> 缩写形式
    DEFAULT_FIELD_MAPPING = {
        # K线数据字段
        "open_price": "opn",
        "high_price": "hgh",
        "low_price": "low",
        "close_price": "cls",
        "volume": "vol",
        "close_time": "ctm",
        "quote_volume": "amt",
        "trades_count": "tnum",
        "taker_buy_volume": "tbvol",
        "taker_buy_quote_volume": "tbamt",
        "taker_sell_volume": "tsvol",
        "taker_sell_quote_volume": "tsamt",
        # Metrics 数据字段（保持缩写）
        "funding_rate": "fr",
        "open_interest": "oi",
        "long_short_ratio": "lsr",
    }

    def __init__(
        self,
        kline_query: KlineQuery,
        resampler: DataResampler | None = None,
        metrics_query: "MetricsQuery | None" = None,
    ):
        """初始化NumPy导出器.

        Args:
            kline_query: K线数据查询器
            resampler: 数据重采样器，可选
            metrics_query: Metrics数据查询器，可选（用于导出指标数据）
        """
        self.kline_query = kline_query
        self.resampler = resampler
        self.metrics_query = metrics_query
        self._file_lock = asyncio.Lock()  # 添加文件锁以防止并发读写冲突

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

        logger.info("export_klines_start", symbols=len(symbols), start_time=start_time, end_time=end_time)

        # 获取数据
        df = await self.kline_query.select_by_time_range(symbols, start_time, end_time, freq)

        if df.empty:
            logger.warning("没有数据可导出")
            return

        # 重采样（如果需要）
        export_freq = freq
        if target_freq and self.resampler:
            logger.info("resample_klines_start", source_freq=freq.value, target_freq=target_freq.value)
            df = await self.resampler.resample(df, target_freq)
            export_freq = target_freq

        # 按日期分组导出
        await self._export_by_dates(df, output_path, export_freq)

        logger.info("export_klines_complete", path=output_path)

    async def _export_by_dates(self, df: pd.DataFrame, output_path: Path, freq: Freq, timestamp_dfs: dict[str, pd.DataFrame] | None = None) -> None:
        """按日期分组导出数据.

        Args:
            df: 数据DataFrame
            output_path: 输出路径
            freq: 数据频率
            timestamp_dfs: 各类数据的原始 timestamp，格式：{"kline_timestamp": df, "fr_timestamp": df, ...}
        """
        # 获取所有唯一日期
        timestamps = df.index.get_level_values("timestamp")
        unique_dates = sorted({pd.Timestamp(ts, unit="ms").date() for ts in timestamps})

        logger.info("export_by_dates_start", dates=len(unique_dates))

        # 并发处理多个日期
        tasks = []
        for date in unique_dates:
            task = self._export_single_date(df, date, output_path, freq, timestamp_dfs)
            tasks.append(task)

        # 分批执行任务，避免内存过载
        batch_size = 5
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
        timestamp_dfs: dict[str, pd.DataFrame] | None = None,
    ) -> None:
        """导出单个日期的数据.

        Args:
            df: 数据DataFrame
            date: 日期对象
            output_path: 输出路径
            freq: 数据频率
            timestamp_dfs: 各类数据的原始 timestamp
        """
        date_str = date.strftime("%Y%m%d")

        # 筛选当天数据
        timestamps = df.index.get_level_values("timestamp")
        day_mask = pd.Series(timestamps).apply(lambda ts: pd.Timestamp(ts, unit="ms").date() == date)
        day_data = df[day_mask.values]

        if day_data.empty:
            return

        logger.debug("export_single_date_start", date=date_str, records=len(day_data))

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

        # 导出 timestamp 数据
        if timestamp_dfs:
            await self._export_timestamps(timestamp_dfs, day_mask, output_path, freq, date_str)

    async def _export_timestamps(self, timestamp_dfs: dict[str, pd.DataFrame], day_mask: pd.Series, output_path: Path, freq: Freq, date_str: str) -> None:
        """导出 timestamp 数据为单个合并的 .npy 文件.

        将多个 timestamp 按约定顺序合并：kline, oi/lsr, fr

        Args:
            timestamp_dfs: 各类数据的原始 timestamp
            day_mask: 当天数据的mask
            output_path: 输出路径
            freq: 数据频率
            date_str: 日期字符串
        """
        if not timestamp_dfs:
            return

        loop = asyncio.get_event_loop()

        def merge_and_save_timestamps():
            """合并所有 timestamp 并保存为单个文件."""
            try:
                # 定义顺序：kline, oi, lsr, fr
                ordered_keys = ["kline_timestamp", "oi_timestamp", "lsr_timestamp", "fr_timestamp"]

                merged_columns = {}

                for ts_name in ordered_keys:
                    if ts_name not in timestamp_dfs:
                        continue

                    ts_df = timestamp_dfs[ts_name]

                    # 筛选当天数据
                    day_ts_data = ts_df[day_mask.values]

                    if day_ts_data.empty:
                        logger.debug("export_timestamps_empty", type=ts_name)
                        continue

                    # 重塑为 n x T 矩阵
                    ts_array = day_ts_data["timestamp"].unstack("timestamp").values
                    merged_columns[ts_name] = ts_array
                    logger.debug("export_timestamps_complete", type=ts_name, shape=ts_array.shape)

                if not merged_columns:
                    logger.debug("没有 timestamp 数据可导出")
                    return

                # 收集要合并的 timestamp 数组，保持 ordered_keys 的顺序
                # 类似于 list.append (push) 的方式，先收集再合并，比逐步 concatenate 更高效且安全
                ts_arrays = []
                for ts_name in ordered_keys:
                    if ts_name in merged_columns:
                        ts_arrays.append(merged_columns[ts_name])

                # 使用 np.stack 在 axis=0 维度堆叠
                # 最终形状：(n_types, n_symbols, T)
                # 注意：ts_array 本身是 (n_symbols, T)，stack 后变成 (n_types, n_symbols, T)
                merged_array = np.stack(ts_arrays, axis=0)

                # 将 NaN 替换为 0（表示数据不存在），并转换为 int64
                # 0 不是有效的 timestamp，下游可以用它来识别缺失数据
                merged_array = np.nan_to_num(merged_array, nan=0.0)
                merged_array = merged_array.astype(np.int64)

                # 创建输出目录
                save_path = output_path / "timestamp"
                save_path.mkdir(parents=True, exist_ok=True)

                # 保存合并后的 timestamp 数组
                np.save(save_path / f"{date_str}.npy", merged_array)

                logger.debug("export_timestamps_complete", shape=merged_array.shape, types=list(merged_columns.keys()))

            except Exception as e:
                logger.error("export_timestamps_failed", error=str(e))
                import traceback

                traceback.print_exc()

        await loop.run_in_executor(None, merge_and_save_timestamps)

    async def _save_symbols(self, day_data: pd.DataFrame, output_path: Path, freq: Freq, date_str: str) -> None:
        """保存交易对顺序信息.

        Args:
            day_data: 当天数据
            output_path: 输出路径
            freq: 数据频率
            date_str: 日期字符串
        """
        symbols_path = output_path / "univ_dct2.json"
        symbols_path.parent.mkdir(parents=True, exist_ok=True)

        # 使用锁确保文件读写的线程安全
        async with self._file_lock:
            loop = asyncio.get_event_loop()

            def save_symbols():
                symbols = day_data.index.get_level_values("symbol").unique().tolist()

                # 读取现有数据（如果文件存在）
                payload = {}
                if symbols_path.exists():
                    try:
                        with open(symbols_path, encoding="utf-8") as fp:
                            payload = json.load(fp)
                    except (json.JSONDecodeError, OSError) as e:
                        logger.warning("load_symbols_file_failed", path=symbols_path, error=str(e))
                        payload = {}

                # 添加当前日期的symbols
                payload[date_str] = symbols

                # 写入文件
                with open(symbols_path, "w", encoding="utf-8") as fp:
                    json.dump(payload, fp, ensure_ascii=False, indent=2)

            await loop.run_in_executor(None, save_symbols)

    async def _export_single_feature(self, day_data: pd.DataFrame, feature: str, output_path: Path, freq: Freq, date_str: str) -> None:
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
                    logger.debug("export_single_feature_missing_values", feature=feature)
                    # 使用前向填充处理缺失值
                    df_filled = pd.DataFrame(array, index=feature_data.index, columns=feature_data.columns)
                    df_filled = df_filled.ffill(axis=1)
                    array = df_filled.values

                # 创建存储路径
                save_path = output_path / feature
                save_path.mkdir(parents=True, exist_ok=True)

                # 保存为npy文件
                np.save(save_path / f"{date_str}.npy", array)

                return len(array)
            except Exception as e:
                logger.error("export_single_feature_failed", feature=feature, error=str(e))
                return 0

        # 在线程池中执行
        count = await loop.run_in_executor(None, process_and_save)

        if count > 0:
            logger.debug("export_single_feature_complete", feature=feature, records=count)

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
                "close_time": "ctm",
                "quote_volume": "amt",
                "trades_count": "tnum",
                "taker_buy_volume": "tbvol",
                "taker_buy_quote_volume": "tbamt",
                "taker_sell_volume": "tsvol",
                "taker_sell_quote_volume": "tsamt",
            }

        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info("export_with_custom_features_start", symbols=len(symbols))

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

        logger.info("export_with_custom_features_complete", path=output_path)

    async def export_summary_info(self, symbols: list[str], start_time: str, end_time: str, freq: Freq, output_path: Path) -> dict[str, Any]:
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
        summary_path = output_path / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)

        import json

        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        logger.info("export_summary_info_complete", path=summary_path)
        return summary

    async def export_combined_data(  # noqa: C901
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        source_freq: Freq,
        export_freq: Freq,
        output_path: Path,
        include_klines: bool = True,
        include_metrics: bool = True,
        metrics_config: dict[str, Any] | None = None,
        field_mapping: dict[str, str] | None = None,
    ) -> None:
        """导出 Kline + Metrics 合并数据.

        这是统一的导出接口，支持同时导出 K线和指标数据，
        自动处理重采样、时间对齐和字段映射。

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            end_time: 结束时间 (YYYY-MM-DD)
            source_freq: 源数据频率（必须存在于数据库中）
            export_freq: 导出数据频率（可以与源频率不同，会自动重采样）
            output_path: 输出路径
            include_klines: 是否包含 K线数据
            include_metrics: 是否包含 Metrics 数据
            metrics_config: Metrics 配置字典，例如：
                {
                    "funding_rate": True,  # 启用资金费率
                    "open_interest": True,  # 启用持仓量
                    "long_short_ratio": {"ratio_type": "taker"},  # 启用多空比例，指定类型
                }
            field_mapping: 自定义字段映射（默认使用 DEFAULT_FIELD_MAPPING）

        Raises:
            ValueError: 当源数据频率不存在于数据库时

        Example:
            >>> # 导出日线 K线 + Metrics 数据
            >>> await exporter.export_combined_data(
            ...     symbols=[
            ...         "BTCUSDT",
            ...         "ETHUSDT",
            ...     ],
            ...     start_time="2024-01-01",
            ...     end_time="2024-01-31",
            ...     source_freq=Freq.d1,  # 从数据库读取日线数据
            ...     export_freq=Freq.d1,  # 导出为日线
            ...     output_path=Path(
            ...         "./data/exports/daily"
            ...     ),
            ...     include_metrics=True,
            ...     metrics_config={
            ...         "funding_rate": True,
            ...         "open_interest": True,
            ...         "long_short_ratio": {
            ...             "ratio_type": "taker"
            ...         },
            ...     },
            ... )

            >>> # 从小时线重采样为日线
            >>> await exporter.export_combined_data(
            ...     symbols=[
            ...         "BTCUSDT",
            ...         "ETHUSDT",
            ...     ],
            ...     start_time="2024-01-01",
            ...     end_time="2024-01-31",
            ...     source_freq=Freq.h1,  # 从数据库读取小时线
            ...     export_freq=Freq.d1,  # 导出为日线
            ...     output_path=Path(
            ...         "./data/exports/daily"
            ...     ),
            ... )
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return

        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            "export_combined_data_start",
            symbols=len(symbols),
            start_time=start_time,
            end_time=end_time,
            source_freq=source_freq.value,
            export_freq=export_freq.value,
        )

        # 1. 获取 K线数据（使用 source_freq）
        combined_df = pd.DataFrame()
        if include_klines:
            logger.info("select_kline_data_start", source_freq=source_freq.value)
            kline_df = await self.kline_query.select_by_time_range(symbols, start_time, end_time, source_freq)

            if kline_df.empty:
                error_msg = f"数据库中不存在频率为 {source_freq.value} 的 K线数据。请先下载该频率的数据，或更改 source_freq 参数。"
                logger.error(error_msg)
                raise ValueError(error_msg)

            logger.info("select_kline_data_complete", records=len(kline_df))
            combined_df = kline_df.copy()

            # 重采样 K线数据（如果需要）
            if export_freq != source_freq:
                if not self.resampler:
                    raise ValueError("需要重采样但 resampler 未初始化")

                logger.info("resample_kline_data_start", source_freq=source_freq.value, export_freq=export_freq.value)
                combined_df = await self.resampler.resample(combined_df, export_freq)
                logger.info("resample_kline_data_complete", records=len(combined_df))

        # 2. 合并 Metrics 数据（如果需要）
        timestamp_dfs = {}  # 存储各类数据的原始 timestamp

        # 提取 K线的 timestamp
        if not combined_df.empty:
            kline_timestamps = self._extract_timestamps(combined_df)
            timestamp_dfs["kline_timestamp"] = kline_timestamps

        if include_metrics and self.metrics_query and not combined_df.empty:
            metrics_df, metrics_timestamps = await self._fetch_and_merge_metrics(combined_df, symbols, start_time, end_time, export_freq, metrics_config)
            if not metrics_df.empty:
                # 合并到 combined_df
                combined_df = pd.concat([combined_df, metrics_df], axis=1, join="outer")

            # 合并 metrics 的 timestamp
            timestamp_dfs.update(metrics_timestamps)

        if combined_df.empty:
            logger.warning("没有数据可导出")
            return

        # 3. 重命名字段为缩写形式
        if field_mapping is None:
            field_mapping = self.DEFAULT_FIELD_MAPPING
        combined_df = self._rename_fields(combined_df, field_mapping)

        # 4. 导出数据（包括 timestamp）
        await self._export_by_dates(combined_df, output_path, export_freq, timestamp_dfs)

        logger.info("export_combined_data_complete", columns=len(combined_df.columns), records=len(combined_df))
        logger.info("export_combined_data_timestamp_types", types=list(timestamp_dfs.keys()))

    async def _fetch_and_merge_metrics(  # noqa: C901
        self,
        kline_df: pd.DataFrame,
        symbols: list[str],
        start_time: str,
        end_time: str,
        target_freq: Freq,
        metrics_config: dict[str, Any] | None = None,
    ) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """获取并合并 Metrics 数据到 K线时间点.

        内部方法：处理数据获取、重采样、对齐。

        Args:
            kline_df: K线数据（作为时间基准）
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            target_freq: 目标频率
            metrics_config: Metrics 配置

        Returns:
            (合并后的 Metrics DataFrame, 原始 timestamp 字典)
            timestamp 字典格式: {"fr": DataFrame, "oi": DataFrame, "lsr": DataFrame}
        """
        if metrics_config is None:
            metrics_config = {
                "funding_rate": True,
                "open_interest": True,
                "long_short_ratio": {"ratio_type": "taker"},
            }

        if not self.metrics_query or not self.resampler:
            logger.warning("MetricsQuery 或 Resampler 未初始化，跳过 Metrics 数据")
            return pd.DataFrame(), {}

        metrics_dfs = []
        timestamp_dfs = {}  # 存储原始 timestamp

        # 资金费率
        if metrics_config.get("funding_rate"):
            try:
                logger.info("fetch_funding_rate_data_start")
                fr_df_raw = await self.metrics_query.select_funding_rates(symbols, start_time, end_time, columns=["funding_rate"])
                if not fr_df_raw.empty:
                    # 重采样并对齐到 kline 时间点，同时获取原始 timestamp
                    result = await self.resampler.resample_and_align(
                        fr_df_raw,
                        kline_df,
                        target_freq,
                        agg_strategy={"funding_rate": "last"},
                        align_method="asof",
                        return_original_timestamps=True,
                    )
                    assert isinstance(result, tuple), "Expected tuple"  # noqa: S101
                    fr_df, fr_original_ts = result
                    assert isinstance(fr_df, pd.DataFrame), "Expected DataFrame"  # noqa: S101
                    assert isinstance(fr_original_ts, pd.DataFrame), "Expected DataFrame"  # noqa: S101

                    # 保存原始 timestamp
                    if not fr_original_ts.empty:
                        # 转换为与特征数据相同的格式
                        fr_ts_df = pd.DataFrame({"timestamp": fr_original_ts["original_timestamp"]}, index=fr_original_ts.index)
                        timestamp_dfs["fr_timestamp"] = fr_ts_df

                    metrics_dfs.append(fr_df)
                    logger.info("fetch_funding_rate_data_complete", records=len(fr_df))
            except Exception as e:
                logger.warning("fetch_funding_rate_data_failed", error=str(e))

        # 持仓量
        if metrics_config.get("open_interest"):
            try:
                logger.info("fetch_open_interest_data_start")
                oi_df_raw = await self.metrics_query.select_open_interests(symbols, start_time, end_time, columns=["open_interest"])
                if not oi_df_raw.empty:
                    # 重采样并对齐，同时获取原始 timestamp
                    result = await self.resampler.resample_and_align(
                        oi_df_raw,
                        kline_df,
                        target_freq,
                        agg_strategy={"open_interest": "last"},
                        align_method="asof",
                        return_original_timestamps=True,
                    )
                    assert isinstance(result, tuple), "Expected tuple"  # noqa: S101
                    oi_df, oi_original_ts = result
                    assert isinstance(oi_df, pd.DataFrame), "Expected DataFrame"  # noqa: S101
                    assert isinstance(oi_original_ts, pd.DataFrame), "Expected DataFrame"  # noqa: S101

                    # 保存原始 timestamp
                    if not oi_original_ts.empty:
                        oi_ts_df = pd.DataFrame({"timestamp": oi_original_ts["original_timestamp"]}, index=oi_original_ts.index)
                        timestamp_dfs["oi_timestamp"] = oi_ts_df

                    metrics_dfs.append(oi_df)
                    logger.info("fetch_open_interest_data_complete", records=len(oi_df))
            except Exception as e:
                logger.warning("fetch_open_interest_data_failed", error=str(e))

        # 多空比例
        lsr_config = metrics_config.get("long_short_ratio")
        if lsr_config:
            try:
                ratio_type = lsr_config.get("ratio_type", "taker") if isinstance(lsr_config, dict) else "taker"
                logger.info("fetch_long_short_ratio_data_start", ratio_type=ratio_type)

                lsr_df_raw = await self.metrics_query.select_long_short_ratios(
                    symbols, start_time, end_time, ratio_type=ratio_type, columns=["long_short_ratio"]
                )
                if not lsr_df_raw.empty:
                    # 重采样并对齐，同时获取原始 timestamp
                    result = await self.resampler.resample_and_align(
                        lsr_df_raw,
                        kline_df,
                        target_freq,
                        agg_strategy={"long_short_ratio": "last"},
                        align_method="asof",
                        return_original_timestamps=True,
                    )
                    assert isinstance(result, tuple), "Expected tuple"  # noqa: S101
                    lsr_df, lsr_original_ts = result
                    assert isinstance(lsr_df, pd.DataFrame), "Expected DataFrame"  # noqa: S101
                    assert isinstance(lsr_original_ts, pd.DataFrame), "Expected DataFrame"  # noqa: S101

                    # 保存原始 timestamp
                    if not lsr_original_ts.empty:
                        lsr_ts_df = pd.DataFrame({"timestamp": lsr_original_ts["original_timestamp"]}, index=lsr_original_ts.index)
                        timestamp_dfs["lsr_timestamp"] = lsr_ts_df

                    metrics_dfs.append(lsr_df)
                    logger.info("fetch_and_merge_metrics_success", metrics_df=len(lsr_df))
            except Exception as e:
                logger.warning("fetch_and_merge_metrics_failed", error=str(e))
        logger.info("fetch_and_merge_metrics_complete", metrics_dfs=len(metrics_dfs), timestamp_dfs=len(timestamp_dfs))
        if not metrics_dfs:
            logger.warning("没有 Metrics 数据可合并")
            return pd.DataFrame(), timestamp_dfs

        # 合并所有 metrics 数据
        merged_metrics = pd.concat(metrics_dfs, axis=1, join="outer")
        logger.info("fetch_and_merge_metrics_complete", metrics_dfs=len(metrics_dfs))

        return merged_metrics, timestamp_dfs

    @staticmethod
    def _extract_timestamps(df: pd.DataFrame) -> pd.DataFrame:
        """从 DataFrame 中提取 timestamp 索引.

        Args:
            df: 数据 DataFrame，使用 (symbol, timestamp) 多级索引

        Returns:
            只包含 timestamp 的 DataFrame，索引为 (symbol, timestamp)
        """
        if df.empty:
            return pd.DataFrame()

        # 创建一个 DataFrame，只包含 timestamp
        # 使用 MultiIndex 的 timestamp level 作为数据
        timestamps = pd.DataFrame({"timestamp": df.index.get_level_values("timestamp")}, index=df.index)

        return timestamps

    @staticmethod
    def _rename_fields(df: pd.DataFrame, field_mapping: dict[str, str]) -> pd.DataFrame:
        """重命名字段为缩写形式.

        Args:
            df: 数据 DataFrame
            field_mapping: 字段映射字典

        Returns:
            重命名后的 DataFrame
        """
        if df.empty:
            return df

        columns_to_rename = {col: field_mapping[col] for col in df.columns if col in field_mapping}

        if columns_to_rename:
            df = df.rename(columns=columns_to_rename)
            logger.debug("rename_fields_complete", fields=len(columns_to_rename))

        return df
