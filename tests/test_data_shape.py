"""数据 Shape 变化测试.

验证在不同输入参数下数据流的 shape 变化是正确的，确保数据处理链路的鲁棒性，包括：
1. 不同 symbols 数量时的 shape 变化
2. 不同时间范围时的 shape 变化
3. 不同频率时的 shape 变化
4. 重采样前后的 shape 变化
5. 对齐操作前后的 shape 变化
6. 合并操作后的 shape 变化
"""

import numpy as np
import pandas as pd
import pytest

from cryptoservice.models import Freq
from cryptoservice.storage.resampler import DataResampler

# ==============================================================================
# 测试辅助函数
# ==============================================================================


def create_mock_kline_df(
    symbols: list[str],
    start_ts: int,
    end_ts: int,
    freq_ms: int,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """创建模拟的 K线 DataFrame.

    Args:
        symbols: 交易对列表
        start_ts: 开始时间戳（毫秒）
        end_ts: 结束时间戳（毫秒）
        freq_ms: 频率（毫秒）
        columns: 要包含的列，默认为标准 OHLCV 列

    Returns:
        带有 (symbol, timestamp) 多级索引的 DataFrame
    """
    if columns is None:
        columns = [
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "quote_volume",
            "trades_count",
            "taker_buy_volume",
            "taker_buy_quote_volume",
        ]

    timestamps = list(range(start_ts, end_ts, freq_ms))
    data = []

    for symbol in symbols:
        for ts in timestamps:
            row = {"symbol": symbol, "timestamp": ts}
            for col in columns:
                if col in ["open_price", "high_price", "low_price", "close_price"]:
                    row[col] = 100.0 + np.random.randn() * 10
                else:
                    row[col] = 1000.0 + np.random.randn() * 100
            data.append(row)

    df = pd.DataFrame(data)
    df = df.set_index(["symbol", "timestamp"])
    return df.sort_index()


def create_mock_metrics_df(
    symbols: list[str],
    start_ts: int,
    end_ts: int,
    freq_ms: int,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """创建模拟的 Metrics DataFrame.

    Args:
        symbols: 交易对列表
        start_ts: 开始时间戳（毫秒）
        end_ts: 结束时间戳（毫秒）
        freq_ms: 频率（毫秒）
        columns: 要包含的列

    Returns:
        带有 (symbol, timestamp) 多级索引的 DataFrame
    """
    if columns is None:
        columns = ["funding_rate"]

    timestamps = list(range(start_ts, end_ts, freq_ms))
    data = []

    for symbol in symbols:
        for ts in timestamps:
            row = {"symbol": symbol, "timestamp": ts}
            for col in columns:
                row[col] = np.random.randn() * 0.001
            data.append(row)

    df = pd.DataFrame(data)
    df = df.set_index(["symbol", "timestamp"])
    return df.sort_index()


def calculate_expected_timestamps(start_ts: int, end_ts: int, freq_ms: int) -> int:
    """计算预期的时间戳数量."""
    return len(list(range(start_ts, end_ts, freq_ms)))


# ==============================================================================
# 测试类：基础 Shape 验证
# ==============================================================================


class TestBasicShapeConsistency:
    """测试基础数据 shape 的一致性."""

    def test_kline_df_shape_basic(self):
        """测试基础 K线 DataFrame 的 shape.

        验证：
        - 行数 = symbols数量 × 时间戳数量
        - 列数 = 特征数量
        """
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        n_symbols = len(symbols)
        freq_ms = 3600000  # 1小时
        start_ts = 1700000000000
        end_ts = start_ts + freq_ms * 24  # 24小时

        n_timestamps = calculate_expected_timestamps(start_ts, end_ts, freq_ms)
        columns = ["open_price", "high_price", "low_price", "close_price", "volume"]
        n_columns = len(columns)

        df = create_mock_kline_df(symbols, start_ts, end_ts, freq_ms, columns)

        # 验证总行数
        expected_rows = n_symbols * n_timestamps
        assert len(df) == expected_rows, f"Expected {expected_rows} rows, got {len(df)}"

        # 验证列数
        assert len(df.columns) == n_columns, f"Expected {n_columns} columns, got {len(df.columns)}"

        # 验证多级索引
        assert df.index.names == ["symbol", "timestamp"]

        # 验证每个 symbol 的行数
        for symbol in symbols:
            symbol_rows = len(df.loc[symbol])
            assert symbol_rows == n_timestamps, f"Symbol {symbol} expected {n_timestamps} rows, got {symbol_rows}"

    def test_metrics_df_shape_basic(self):
        """测试基础 Metrics DataFrame 的 shape."""
        symbols = ["BTCUSDT", "ETHUSDT"]
        n_symbols = len(symbols)
        freq_ms = 8 * 3600000  # 8小时
        start_ts = 1700000000000
        end_ts = start_ts + freq_ms * 10  # 10个数据点

        n_timestamps = calculate_expected_timestamps(start_ts, end_ts, freq_ms)
        columns = ["funding_rate", "open_interest"]

        df = create_mock_metrics_df(symbols, start_ts, end_ts, freq_ms, columns)

        expected_rows = n_symbols * n_timestamps
        assert len(df) == expected_rows
        assert len(df.columns) == len(columns)

    @pytest.mark.parametrize("n_symbols", [1, 5, 10, 50])
    def test_shape_scales_with_symbols(self, n_symbols: int):
        """测试 shape 随 symbols 数量正确扩展."""
        symbols = [f"TOKEN{i}USDT" for i in range(n_symbols)]
        freq_ms = 3600000
        start_ts = 1700000000000
        end_ts = start_ts + freq_ms * 24

        n_timestamps = calculate_expected_timestamps(start_ts, end_ts, freq_ms)

        df = create_mock_kline_df(symbols, start_ts, end_ts, freq_ms)

        expected_rows = n_symbols * n_timestamps
        assert len(df) == expected_rows, f"With {n_symbols} symbols, expected {expected_rows} rows, got {len(df)}"

    @pytest.mark.parametrize("n_hours", [1, 6, 24, 72, 168])
    def test_shape_scales_with_time_range(self, n_hours: int):
        """测试 shape 随时间范围正确扩展."""
        symbols = ["BTCUSDT"]
        freq_ms = 3600000  # 1小时
        start_ts = 1700000000000
        end_ts = start_ts + freq_ms * n_hours

        n_timestamps = calculate_expected_timestamps(start_ts, end_ts, freq_ms)

        df = create_mock_kline_df(symbols, start_ts, end_ts, freq_ms)

        expected_rows = n_timestamps
        assert len(df) == expected_rows, f"With {n_hours} hours, expected {expected_rows} rows, got {len(df)}"


# ==============================================================================
# 测试类：重采样 Shape 变化
# ==============================================================================


class TestResamplingShapeChanges:
    """测试重采样操作的 shape 变化."""

    @pytest.mark.asyncio
    async def test_hourly_to_daily_resample_shape(self):
        """测试从小时线重采样到日线的 shape 变化.

        验证：
        - 行数减少为原来的 1/24
        - 列数保持不变
        """
        symbols = ["BTCUSDT", "ETHUSDT"]
        n_symbols = len(symbols)
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        # 创建3天的小时级数据（对齐到天）
        start_ts = 1700006400000  # 2023-11-15 00:00:00 UTC
        end_ts = start_ts + day_ms * 3  # 3天

        hourly_df = create_mock_kline_df(symbols, start_ts, end_ts, hour_ms)

        # 原始 shape
        n_hourly_timestamps = calculate_expected_timestamps(start_ts, end_ts, hour_ms)
        assert len(hourly_df) == n_symbols * n_hourly_timestamps

        # 重采样到日线
        daily_df = await DataResampler.resample(hourly_df, Freq.d1)

        # 验证重采样后的 shape
        n_daily_timestamps = 3  # 3天
        expected_daily_rows = n_symbols * n_daily_timestamps
        assert len(daily_df) == expected_daily_rows, f"Expected {expected_daily_rows} daily rows, got {len(daily_df)}"

        # 验证列数不变
        assert len(daily_df.columns) == len(hourly_df.columns)

    @pytest.mark.asyncio
    async def test_minute_to_hourly_resample_shape(self):
        """测试从分钟线重采样到小时线的 shape 变化."""
        symbols = ["BTCUSDT"]
        minute_ms = 60000  # 1分钟
        hour_ms = 3600000  # 1小时

        # 创建4小时的分钟级数据（对齐到小时）
        start_ts = 1700006400000  # 对齐到小时
        end_ts = start_ts + hour_ms * 4  # 4小时

        minute_df = create_mock_kline_df(symbols, start_ts, end_ts, minute_ms)

        # 原始数据应该有 4 * 60 = 240 行（每分钟一行）
        n_minute_timestamps = calculate_expected_timestamps(start_ts, end_ts, minute_ms)
        assert len(minute_df) == n_minute_timestamps

        # 重采样到小时线
        hourly_df = await DataResampler.resample(minute_df, Freq.h1)

        # 验证重采样后的 shape：4行（每小时一行）
        expected_hourly_rows = 4
        assert len(hourly_df) == expected_hourly_rows, f"Expected {expected_hourly_rows} hourly rows, got {len(hourly_df)}"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "source_freq_ms,target_freq,expected_ratio",
        [
            (60000, Freq.m5, 5),  # 1m -> 5m: 1/5
            (60000, Freq.m15, 15),  # 1m -> 15m: 1/15
            (60000, Freq.h1, 60),  # 1m -> 1h: 1/60
            (3600000, Freq.h4, 4),  # 1h -> 4h: 1/4
            (3600000, Freq.d1, 24),  # 1h -> 1d: 1/24
        ],
    )
    async def test_resample_shape_ratio(self, source_freq_ms: int, target_freq: Freq, expected_ratio: int):
        """测试不同频率转换的 shape 比率."""
        symbols = ["BTCUSDT"]

        # 确保时间范围能整除目标频率
        n_target_bars = 10
        target_freq_ms = source_freq_ms * expected_ratio
        start_ts = 1700006400000  # 对齐到天
        end_ts = start_ts + target_freq_ms * n_target_bars

        source_df = create_mock_kline_df(symbols, start_ts, end_ts, source_freq_ms)

        n_source_rows = len(source_df)

        # 重采样
        target_df = await DataResampler.resample(source_df, target_freq)

        n_target_rows = len(target_df)

        # 验证比率
        actual_ratio = n_source_rows / n_target_rows
        assert abs(actual_ratio - expected_ratio) < 0.01, f"Expected ratio {expected_ratio}, got {actual_ratio}"

    @pytest.mark.asyncio
    async def test_resample_preserves_symbols(self):
        """测试重采样保留所有 symbols."""
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 2

        hourly_df = create_mock_kline_df(symbols, start_ts, end_ts, hour_ms)

        daily_df = await DataResampler.resample(hourly_df, Freq.d1)

        # 验证所有 symbols 都被保留
        source_symbols = set(hourly_df.index.get_level_values("symbol").unique())
        target_symbols = set(daily_df.index.get_level_values("symbol").unique())

        assert source_symbols == target_symbols, f"Symbols mismatch: source={source_symbols}, target={target_symbols}"


# ==============================================================================
# 测试类：对齐操作 Shape 变化
# ==============================================================================


class TestAlignmentShapeChanges:
    """测试对齐操作的 shape 变化."""

    @pytest.mark.asyncio
    async def test_align_metrics_to_kline_shape(self):
        """测试 metrics 对齐到 kline 后的 shape.

        验证：对齐后的 metrics shape 与 kline 的 shape 一致。
        """
        symbols = ["BTCUSDT", "ETHUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 3  # 3天

        # K线数据：日线（3个数据点）
        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms)

        # Metrics 数据：每8小时（更高频）
        metrics_freq_ms = 8 * hour_ms
        metrics_df = create_mock_metrics_df(symbols, start_ts, end_ts, metrics_freq_ms, ["funding_rate"])

        # 原始 metrics 有更多行
        assert len(metrics_df) > len(kline_df)

        # 对齐到 kline 时间点
        aligned_df = await DataResampler.align_to_kline_timestamps(metrics_df, kline_df, method="asof")

        # 对齐后的行数应该与 kline 相同
        assert len(aligned_df) == len(kline_df), f"Aligned shape {len(aligned_df)} != kline shape {len(kline_df)}"

        # 验证索引完全一致
        assert aligned_df.index.equals(kline_df.index), "Aligned index should match kline index"

    @pytest.mark.asyncio
    async def test_align_preserves_columns(self):
        """测试对齐操作保留所有列."""
        symbols = ["BTCUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 2

        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms)

        # 多列 metrics
        metrics_columns = ["funding_rate", "open_interest", "long_short_ratio"]
        metrics_df = create_mock_metrics_df(symbols, start_ts, end_ts, hour_ms, metrics_columns)

        aligned_df = await DataResampler.align_to_kline_timestamps(metrics_df, kline_df, method="asof")

        # 验证所有列都被保留
        assert set(aligned_df.columns) == set(metrics_columns), f"Columns mismatch: expected {metrics_columns}, got {list(aligned_df.columns)}"

    @pytest.mark.asyncio
    async def test_align_with_partial_symbol_overlap(self):
        """测试部分 symbol 重叠时的对齐 shape."""
        kline_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        metrics_symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"]  # 只有2个重叠
        common_symbols = set(kline_symbols) & set(metrics_symbols)

        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 2

        kline_df = create_mock_kline_df(kline_symbols, start_ts, end_ts, day_ms)
        metrics_df = create_mock_metrics_df(metrics_symbols, start_ts, end_ts, hour_ms, ["funding_rate"])

        aligned_df = await DataResampler.align_to_kline_timestamps(metrics_df, kline_df, method="asof")

        # 只有共同的 symbols 会被对齐
        aligned_symbols = set(aligned_df.index.get_level_values("symbol").unique())
        assert aligned_symbols == common_symbols, f"Expected {common_symbols}, got {aligned_symbols}"


# ==============================================================================
# 测试类：合并操作 Shape 变化
# ==============================================================================


class TestConcatShapeChanges:
    """测试合并操作的 shape 变化."""

    @pytest.mark.asyncio
    async def test_concat_kline_and_metrics_shape(self):
        """测试合并 K线和 Metrics 数据后的 shape."""
        symbols = ["BTCUSDT", "ETHUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 3

        # K线数据
        kline_columns = ["open_price", "high_price", "low_price", "close_price", "volume"]
        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms, kline_columns)

        # Metrics 数据（对齐后）
        metrics_columns = ["funding_rate", "open_interest"]
        metrics_df = create_mock_metrics_df(symbols, start_ts, end_ts, hour_ms, metrics_columns)

        # 对齐 metrics 到 kline
        aligned_metrics = await DataResampler.align_to_kline_timestamps(metrics_df, kline_df, method="asof")

        # 合并
        combined_df = pd.concat([kline_df, aligned_metrics], axis=1)

        # 验证 shape
        assert len(combined_df) == len(kline_df), "Combined rows should match kline rows"
        expected_columns = len(kline_columns) + len(metrics_columns)
        assert len(combined_df.columns) == expected_columns, f"Expected {expected_columns} columns, got {len(combined_df.columns)}"

    @pytest.mark.asyncio
    async def test_concat_multiple_metrics_shape(self):
        """测试合并多个 Metrics 数据后的 shape."""
        symbols = ["BTCUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 2

        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms)

        # 多个不同的 metrics
        fr_df = create_mock_metrics_df(symbols, start_ts, end_ts, 8 * hour_ms, ["funding_rate"])
        oi_df = create_mock_metrics_df(symbols, start_ts, end_ts, 5 * 60000, ["open_interest"])
        lsr_df = create_mock_metrics_df(symbols, start_ts, end_ts, hour_ms, ["long_short_ratio"])

        # 分别对齐
        aligned_fr = await DataResampler.align_to_kline_timestamps(fr_df, kline_df, method="asof")
        aligned_oi = await DataResampler.align_to_kline_timestamps(oi_df, kline_df, method="asof")
        aligned_lsr = await DataResampler.align_to_kline_timestamps(lsr_df, kline_df, method="asof")

        # 合并所有 metrics
        all_metrics = pd.concat([aligned_fr, aligned_oi, aligned_lsr], axis=1)

        # 验证 shape
        assert len(all_metrics) == len(kline_df)
        assert len(all_metrics.columns) == 3  # fr, oi, lsr


# ==============================================================================
# 测试类：边界条件下的 Shape
# ==============================================================================


class TestEdgeCaseShapes:
    """测试边界条件下的 shape."""

    def test_single_symbol_shape(self):
        """测试单个 symbol 的 shape."""
        symbols = ["BTCUSDT"]
        freq_ms = 3600000
        start_ts = 1700000000000
        end_ts = start_ts + freq_ms * 24

        df = create_mock_kline_df(symbols, start_ts, end_ts, freq_ms)

        n_timestamps = calculate_expected_timestamps(start_ts, end_ts, freq_ms)
        assert len(df) == n_timestamps
        assert len(df.index.get_level_values("symbol").unique()) == 1

    def test_single_timestamp_shape(self):
        """测试单个时间戳的 shape."""
        symbols = ["BTCUSDT", "ETHUSDT"]
        freq_ms = 3600000
        start_ts = 1700000000000
        end_ts = start_ts + freq_ms  # 只有1个时间戳

        df = create_mock_kline_df(symbols, start_ts, end_ts, freq_ms)

        assert len(df) == len(symbols)  # 每个 symbol 1行

    @pytest.mark.asyncio
    async def test_resample_exact_frequency_boundary(self):
        """测试精确频率边界的重采样 shape."""
        symbols = ["BTCUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        # 精确24小时 = 1天
        start_ts = 1700006400000  # 对齐到天
        end_ts = start_ts + day_ms

        hourly_df = create_mock_kline_df(symbols, start_ts, end_ts, hour_ms)
        assert len(hourly_df) == 24

        daily_df = await DataResampler.resample(hourly_df, Freq.d1)
        assert len(daily_df) == 1

    @pytest.mark.asyncio
    async def test_empty_metrics_alignment(self):
        """测试空 metrics 数据对齐后的 shape."""
        symbols = ["BTCUSDT"]
        day_ms = 24 * 3600000

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 2

        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms)

        # 创建空的 metrics DataFrame
        empty_metrics = pd.DataFrame(columns=["funding_rate"])
        empty_metrics.index = pd.MultiIndex.from_tuples([], names=["symbol", "timestamp"])

        # 对齐空 metrics 应该返回空结果
        aligned_df = await DataResampler.align_to_kline_timestamps(empty_metrics, kline_df, method="asof")

        # 空 metrics 对齐后仍然为空
        assert aligned_df.empty

    def test_large_symbol_count_shape(self):
        """测试大量 symbols 的 shape 处理."""
        n_symbols = 100
        symbols = [f"TOKEN{i}USDT" for i in range(n_symbols)]
        freq_ms = 3600000
        start_ts = 1700000000000
        end_ts = start_ts + freq_ms * 10  # 10小时

        df = create_mock_kline_df(symbols, start_ts, end_ts, freq_ms)

        n_timestamps = 10
        expected_rows = n_symbols * n_timestamps
        assert len(df) == expected_rows

        # 验证每个 symbol 的数据完整
        for symbol in symbols:
            assert len(df.loc[symbol]) == n_timestamps


# ==============================================================================
# 测试类：Resample + Align 完整流程 Shape
# ==============================================================================


class TestResampleAndAlignShape:
    """测试 resample_and_align 完整流程的 shape."""

    @pytest.mark.asyncio
    async def test_resample_and_align_shape(self):
        """测试 resample_and_align 的 shape 变化."""
        symbols = ["BTCUSDT", "ETHUSDT"]
        minute_ms = 60000  # 1分钟
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 3

        # 日线 K线
        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms)

        # 分钟级 metrics
        metrics_df = create_mock_metrics_df(symbols, start_ts, end_ts, 5 * minute_ms, ["open_interest"])

        # 原始 metrics 有大量数据点
        original_metrics_rows = len(metrics_df)

        # resample_and_align
        aligned_df = await DataResampler.resample_and_align(
            metrics_df,
            kline_df,
            target_freq=Freq.d1,
            agg_strategy={"open_interest": "last"},
            align_method="asof",
        )

        # 对齐后的行数应该与 kline 相同
        assert len(aligned_df) == len(kline_df), (
            f"After resample_and_align: expected {len(kline_df)} rows, got {len(aligned_df)} (original metrics had {original_metrics_rows} rows)"
        )

    @pytest.mark.asyncio
    async def test_resample_and_align_with_original_timestamps(self):
        """测试 resample_and_align 返回原始时间戳时的 shape."""
        symbols = ["BTCUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 2

        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms)
        metrics_df = create_mock_metrics_df(symbols, start_ts, end_ts, hour_ms, ["funding_rate"])

        result = await DataResampler.resample_and_align(
            metrics_df,
            kline_df,
            target_freq=Freq.d1,
            agg_strategy={"funding_rate": "last"},
            align_method="asof",
            return_original_timestamps=True,
        )

        aligned_df, original_ts_df = result

        # 两个返回值的行数应该相同
        assert len(aligned_df) == len(original_ts_df), f"Aligned ({len(aligned_df)}) and original_ts ({len(original_ts_df)}) should have same length"

        # 都应该与 kline 行数相同
        assert len(aligned_df) == len(kline_df)


# ==============================================================================
# 测试类：Shape 不变性验证
# ==============================================================================


class TestShapeInvariants:
    """测试 shape 不变性约束."""

    @pytest.mark.asyncio
    async def test_resample_never_increases_rows(self):
        """测试重采样永远不会增加行数（只能降采样）."""
        symbols = ["BTCUSDT"]
        hour_ms = 3600000

        start_ts = 1700006400000
        end_ts = start_ts + hour_ms * 48  # 48小时

        hourly_df = create_mock_kline_df(symbols, start_ts, end_ts, hour_ms)
        original_rows = len(hourly_df)

        # 尝试多种降采样
        for target_freq in [Freq.h4, Freq.d1]:
            resampled_df = await DataResampler.resample(hourly_df, target_freq)
            assert len(resampled_df) <= original_rows, f"Resampling to {target_freq} increased rows from {original_rows} to {len(resampled_df)}"

    @pytest.mark.asyncio
    async def test_alignment_never_exceeds_kline_rows(self):
        """测试对齐永远不会超过 kline 的行数."""
        symbols = ["BTCUSDT", "ETHUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 5

        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms)
        kline_rows = len(kline_df)

        # 高频 metrics
        metrics_df = create_mock_metrics_df(symbols, start_ts, end_ts, 5 * 60000, ["funding_rate"])

        aligned_df = await DataResampler.align_to_kline_timestamps(metrics_df, kline_df, method="asof")

        assert len(aligned_df) <= kline_rows, f"Alignment produced {len(aligned_df)} rows, exceeding kline rows {kline_rows}"

    def test_multiindex_structure_preserved(self):
        """测试多级索引结构始终保持."""
        symbols = ["BTCUSDT"]
        freq_ms = 3600000
        start_ts = 1700000000000
        end_ts = start_ts + freq_ms * 10

        df = create_mock_kline_df(symbols, start_ts, end_ts, freq_ms)

        # 验证多级索引结构
        assert df.index.nlevels == 2
        assert df.index.names == ["symbol", "timestamp"]
        assert df.index.is_monotonic_increasing

    @pytest.mark.asyncio
    async def test_symbol_count_preserved_after_resample(self):
        """测试重采样后 symbol 数量保持不变."""
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 3

        hourly_df = create_mock_kline_df(symbols, start_ts, end_ts, hour_ms)
        daily_df = await DataResampler.resample(hourly_df, Freq.d1)

        source_symbol_count = len(hourly_df.index.get_level_values("symbol").unique())
        target_symbol_count = len(daily_df.index.get_level_values("symbol").unique())

        assert source_symbol_count == target_symbol_count == len(symbols)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
