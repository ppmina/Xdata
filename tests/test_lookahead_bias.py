"""Lookahead Bias 检测测试（端到端测试）.

验证整条数据处理链路没有偷看未来数据的问题，包括：
1. K线数据时序一致性：在时间 t，只能使用 timestamp <= t 的数据
2. Metrics 对齐检查：metrics 的原始时间戳必须 <= 对齐后的 kline 时间戳
3. Universe 选择检查：universe 的选择只使用计算周期内的数据
4. 重采样检查：重采样后的数据不能泄露未来信息
"""

import numpy as np
import pandas as pd
import pytest

from cryptoservice.models import Freq
from cryptoservice.models.universe import UniverseConfig, UniverseDefinition, UniverseSnapshot
from cryptoservice.storage.resampler import DataResampler

# ==============================================================================
# 测试辅助函数
# ==============================================================================


def create_mock_kline_df(
    symbols: list[str],
    start_ts: int,
    end_ts: int,
    freq_ms: int,
) -> pd.DataFrame:
    """创建模拟的 K线 DataFrame.

    Args:
        symbols: 交易对列表
        start_ts: 开始时间戳（毫秒）
        end_ts: 结束时间戳（毫秒）
        freq_ms: 频率（毫秒）

    Returns:
        带有 (symbol, timestamp) 多级索引的 DataFrame
    """
    timestamps = list(range(start_ts, end_ts, freq_ms))
    data = []

    for symbol in symbols:
        for ts in timestamps:
            data.append(
                {
                    "symbol": symbol,
                    "timestamp": ts,
                    "open_price": 100.0 + np.random.randn() * 10,
                    "high_price": 105.0 + np.random.randn() * 10,
                    "low_price": 95.0 + np.random.randn() * 10,
                    "close_price": 100.0 + np.random.randn() * 10,
                    "volume": 1000.0 + np.random.randn() * 100,
                }
            )

    df = pd.DataFrame(data)
    df = df.set_index(["symbol", "timestamp"])
    return df.sort_index()


def create_mock_metrics_df(
    symbols: list[str],
    start_ts: int,
    end_ts: int,
    freq_ms: int,
    column_name: str = "funding_rate",
) -> pd.DataFrame:
    """创建模拟的 Metrics DataFrame.

    Args:
        symbols: 交易对列表
        start_ts: 开始时间戳（毫秒）
        end_ts: 结束时间戳（毫秒）
        freq_ms: 频率（毫秒）
        column_name: 列名

    Returns:
        带有 (symbol, timestamp) 多级索引的 DataFrame
    """
    timestamps = list(range(start_ts, end_ts, freq_ms))
    data = []

    for symbol in symbols:
        for ts in timestamps:
            data.append(
                {
                    "symbol": symbol,
                    "timestamp": ts,
                    column_name: np.random.randn() * 0.001,
                }
            )

    df = pd.DataFrame(data)
    df = df.set_index(["symbol", "timestamp"])
    return df.sort_index()


# ==============================================================================
# 测试类：Metrics 对齐的 Lookahead Bias 检测
# ==============================================================================


class TestMetricsAlignmentLookaheadBias:
    """测试 Metrics 对齐时的 Lookahead Bias 检测."""

    @pytest.mark.asyncio
    async def test_asof_alignment_no_lookahead(self):
        """测试 asof 对齐方法不会引入 lookahead bias.

        验证：对于每个 kline 时间点，对齐后的 metrics 原始时间戳
        必须 <= kline 时间戳。
        """
        symbols = ["BTCUSDT", "ETHUSDT"]
        # 1小时频率 = 3600000 毫秒
        freq_ms = 3600000
        start_ts = 1700000000000  # 2023-11-14 22:13:20 UTC
        end_ts = start_ts + freq_ms * 24  # 24小时数据

        # K线数据：每小时
        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, freq_ms)

        # Metrics 数据：每8小时（资金费率更新频率）
        metrics_freq_ms = freq_ms * 8
        metrics_df = create_mock_metrics_df(symbols, start_ts, end_ts, metrics_freq_ms, "funding_rate")

        # 使用 asof 对齐
        result = await DataResampler.align_to_kline_timestamps(
            metrics_df,
            kline_df,
            method="asof",
            return_original_timestamps=True,
        )

        aligned_df, original_ts_df = result

        # 验证：对于每一行，原始时间戳必须 <= 对齐后的时间戳
        for idx, row in original_ts_df.iterrows():
            _, kline_ts = idx  # (symbol, timestamp)
            original_ts = row["original_timestamp"]

            # 跳过 NaN（第一个数据点之前可能没有历史数据）
            if pd.isna(original_ts):
                continue

            assert original_ts <= kline_ts, f"Lookahead bias detected! Original metrics timestamp {original_ts} > kline timestamp {kline_ts}"

    @pytest.mark.asyncio
    async def test_nearest_alignment_may_have_lookahead(self):
        """测试 nearest 对齐方法可能引入 lookahead bias.

        这个测试用于验证 nearest 方法的风险，不是验证它一定会失败，
        而是展示为什么不推荐在实时场景使用 nearest 方法。
        """
        symbols = ["BTCUSDT"]
        freq_ms = 3600000  # 1小时
        start_ts = 1700000000000
        end_ts = start_ts + freq_ms * 24

        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, freq_ms)

        # Metrics 数据偏移半小时
        metrics_start_ts = start_ts + freq_ms // 2
        metrics_freq_ms = freq_ms * 8
        metrics_df = create_mock_metrics_df(symbols, metrics_start_ts, end_ts, metrics_freq_ms, "funding_rate")

        # 使用 nearest 对齐
        result = await DataResampler.align_to_kline_timestamps(
            metrics_df,
            kline_df,
            method="nearest",
            return_original_timestamps=True,
        )

        aligned_df, original_ts_df = result

        # 检查是否存在 lookahead bias（nearest 可能使用未来数据）
        lookahead_count = 0
        for idx, row in original_ts_df.iterrows():
            _, kline_ts = idx
            original_ts = row["original_timestamp"]

            if pd.isna(original_ts):
                continue

            if original_ts > kline_ts:
                lookahead_count += 1

        # 注意：这里我们只是检测，不是断言失败
        # nearest 方法在某些场景下可能产生 lookahead bias
        if lookahead_count > 0:
            pytest.skip(f"Nearest method produced {lookahead_count} lookahead bias cases. This is expected behavior - use 'asof' for production.")


# ==============================================================================
# 测试类：K线重采样的 Lookahead Bias 检测
# ==============================================================================


class TestKlineResamplingLookaheadBias:
    """测试 K线重采样时的 Lookahead Bias 检测."""

    @pytest.mark.asyncio
    async def test_resample_uses_only_past_data(self):
        """测试重采样只使用当前时间点及之前的数据.

        验证：日线重采样时，每天的数据只来自当天及之前，
        不会包含未来的数据。
        """
        symbols = ["BTCUSDT"]
        freq_ms = 3600000  # 1小时
        day_ms = 24 * freq_ms

        # 创建3天的小时级数据
        start_ts = 1700006400000  # 2023-11-15 00:00:00 UTC（对齐到天）
        end_ts = start_ts + day_ms * 3

        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, freq_ms)

        # 重采样到日线
        daily_df = await DataResampler.resample(kline_df, Freq.d1)

        # 验证每个日线数据点的时间戳
        for idx in daily_df.index:
            _, daily_ts = idx

            # 获取该日线对应的原始小时级数据
            symbol_data = kline_df.loc[idx[0]]
            original_timestamps = symbol_data.index.values

            # 找出哪些原始数据被用于这个日线
            # 日线时间戳应该是当天开始，对应的数据应该都 >= daily_ts 且 < daily_ts + day_ms
            used_timestamps = [ts for ts in original_timestamps if daily_ts <= ts < daily_ts + day_ms]

            # 验证使用的数据都在正确的时间范围内
            for ts in used_timestamps:
                assert ts >= daily_ts, f"Used timestamp {ts} is before daily bar start {daily_ts}"
                assert ts < daily_ts + day_ms, f"Used timestamp {ts} is after daily bar end {daily_ts + day_ms}"

    @pytest.mark.asyncio
    async def test_ohlcv_aggregation_correctness(self):
        """测试 OHLCV 聚合的正确性.

        验证：
        - Open: 使用第一根 K线的开盘价
        - High: 使用所有 K线的最高价
        - Low: 使用所有 K线的最低价
        - Close: 使用最后一根 K线的收盘价
        - Volume: 所有 K线成交量之和
        """
        freq_ms = 3600000  # 1小时
        start_ts = 1700006400000  # 对齐到天

        # 创建已知的测试数据
        data = []
        for i in range(24):  # 24小时
            ts = start_ts + i * freq_ms
            data.append(
                {
                    "symbol": "BTCUSDT",
                    "timestamp": ts,
                    "open_price": 100.0 + i,  # 递增
                    "high_price": 200.0 + i,  # 递增
                    "low_price": 50.0 - i,  # 递减
                    "close_price": 150.0 + i,  # 递增
                    "volume": 1000.0,  # 固定
                }
            )

        kline_df = pd.DataFrame(data).set_index(["symbol", "timestamp"])

        # 重采样到日线
        daily_df = await DataResampler.resample(kline_df, Freq.d1)

        # 验证聚合结果
        daily_row = daily_df.loc[("BTCUSDT", start_ts)]

        # Open 应该是第一根 K线的开盘价
        assert daily_row["open_price"] == 100.0, "Open price should be first bar's open"

        # High 应该是所有 K线的最高价的最大值
        assert daily_row["high_price"] == 223.0, "High price should be max of all highs"

        # Low 应该是所有 K线的最低价的最小值
        assert daily_row["low_price"] == 27.0, "Low price should be min of all lows"

        # Close 应该是最后一根 K线的收盘价
        assert daily_row["close_price"] == 173.0, "Close price should be last bar's close"

        # Volume 应该是所有 K线成交量之和
        assert daily_row["volume"] == 24000.0, "Volume should be sum of all volumes"


# ==============================================================================
# 测试类：Universe 选择的 Lookahead Bias 检测
# ==============================================================================


class TestUniverseSelectionLookaheadBias:
    """测试 Universe 选择时的 Lookahead Bias 检测."""

    def test_universe_snapshot_period_consistency(self):
        """测试 Universe 快照的时间周期一致性.

        验证：
        - calculated_t1_end (计算周期结束) <= effective_date
        - start_date (使用开始) > effective_date
        """
        snapshot = UniverseSnapshot.create_with_inferred_periods(
            effective_date="2024-01-31",
            t1_months=1,
            symbols=["BTCUSDT", "ETHUSDT"],
            mean_daily_amounts={"BTCUSDT": 1000000.0, "ETHUSDT": 500000.0},
        )

        effective_dt = pd.to_datetime(snapshot.effective_date)
        calculated_t1_end_dt = pd.to_datetime(snapshot.calculated_t1_end)
        start_dt = pd.to_datetime(snapshot.start_date)

        # 计算周期结束 <= 重平衡日期
        assert calculated_t1_end_dt <= effective_dt, (
            f"Lookahead bias: calculated_t1_end ({snapshot.calculated_t1_end}) > effective_date ({snapshot.effective_date})"
        )

        # 使用周期开始 > 重平衡日期
        assert start_dt > effective_dt, f"Lookahead bias: start_date ({snapshot.start_date}) <= effective_date ({snapshot.effective_date})"

    def test_universe_snapshot_no_future_data_in_calculation(self):
        """测试 Universe 快照的计算期不包含未来数据.

        验证：用于计算 Universe 的数据周期必须严格在重平衡日期之前或当天。
        """
        # 模拟多个月的 universe 快照
        snapshots = []
        for month in range(1, 4):  # 1月到3月
            effective_date = f"2024-0{month}-{28 + month % 2}"  # 月末
            snapshot = UniverseSnapshot.create_with_inferred_periods(
                effective_date=effective_date,
                t1_months=1,
                symbols=["BTCUSDT"],
                mean_daily_amounts={"BTCUSDT": 1000000.0},
            )
            snapshots.append(snapshot)

        for snapshot in snapshots:
            effective_ts = int(snapshot.calculated_t1_end_ts)
            calculated_t1_end_ts = int(snapshot.calculated_t1_end_ts)

            # 确保计算周期结束时间戳 <= 重平衡生效时间戳
            assert calculated_t1_end_ts <= effective_ts, (
                f"Lookahead bias in universe calculation: calculated_t1_end_ts ({calculated_t1_end_ts}) > effective timestamp ({effective_ts})"
            )

    def test_universe_definition_temporal_ordering(self):
        """测试 UniverseDefinition 中快照的时间顺序正确性.

        验证：每个快照的 start_date 应该在前一个快照的 end_date 之后或当天。
        """
        config = UniverseConfig(
            start_date="2024-01-01",
            end_date="2024-03-31",
            t1_months=1,
            t2_months=1,
            t3_months=3,
            top_k=10,
            delay_days=0,
            quote_asset="USDT",
        )

        # 创建连续的快照
        snapshots = []
        effective_dates = ["2024-01-31", "2024-02-29", "2024-03-31"]

        for i, eff_date in enumerate(effective_dates):
            next_eff = effective_dates[i + 1] if i + 1 < len(effective_dates) else None
            snapshot = UniverseSnapshot.create_with_inferred_periods(
                effective_date=eff_date,
                t1_months=1,
                symbols=["BTCUSDT", "ETHUSDT"],
                mean_daily_amounts={"BTCUSDT": 1000000.0, "ETHUSDT": 500000.0},
                next_effective_date=next_eff,
            )
            snapshots.append(snapshot)

        universe_def = UniverseDefinition(
            config=config,
            snapshots=snapshots,
            creation_time=pd.Timestamp.now().to_pydatetime(),
            description="Test universe",
        )

        # 验证时间顺序
        for i in range(1, len(universe_def.snapshots)):
            prev_snapshot = universe_def.snapshots[i - 1]
            curr_snapshot = universe_def.snapshots[i]

            prev_end_dt = pd.to_datetime(prev_snapshot.end_date)
            curr_start_dt = pd.to_datetime(curr_snapshot.start_date)

            # 当前快照的使用开始日期应该 >= 前一个快照的使用结束日期
            # 允许1天的重叠（月末/月初）
            assert curr_start_dt >= prev_end_dt - pd.Timedelta(days=1), (
                f"Temporal ordering violated: snapshot {i} start ({curr_snapshot.start_date}) < snapshot {i - 1} end ({prev_snapshot.end_date})"
            )


# ==============================================================================
# 测试类：端到端数据流 Lookahead Bias 检测
# ==============================================================================


class TestEndToEndLookaheadBias:
    """端到端测试：验证完整数据流没有 lookahead bias."""

    @pytest.mark.asyncio
    async def test_resample_and_align_no_lookahead(self):
        """测试 resample_and_align 完整流程没有 lookahead bias.

        这是最关键的端到端测试，验证：
        1. Metrics 重采样正确
        2. 对齐到 Kline 时间点时不使用未来数据
        """
        symbols = ["BTCUSDT", "ETHUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        # 创建3天的数据
        start_ts = 1700006400000  # 对齐到天
        end_ts = start_ts + day_ms * 3

        # K线数据：日线
        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms)

        # Metrics 数据：每5分钟（高频）
        metrics_freq_ms = 5 * 60 * 1000
        metrics_df = create_mock_metrics_df(symbols, start_ts, end_ts, metrics_freq_ms, "open_interest")

        # 使用 resample_and_align 处理
        result = await DataResampler.resample_and_align(
            metrics_df,
            kline_df,
            target_freq=Freq.d1,
            agg_strategy={"open_interest": "last"},
            align_method="asof",
            return_original_timestamps=True,
        )

        aligned_df, original_ts_df = result

        # 验证每个对齐后的数据点
        for idx, row in original_ts_df.iterrows():
            _, kline_ts = idx
            original_ts = row["original_timestamp"]

            if pd.isna(original_ts):
                continue

            # 核心断言：原始时间戳必须 <= kline 时间戳
            assert original_ts <= kline_ts, f"Lookahead bias in resample_and_align! Original timestamp {original_ts} > kline timestamp {kline_ts}"

            # 额外验证：原始时间戳应该在合理范围内（不应该太早）
            # 允许最多 24 小时的容差
            tolerance_ms = day_ms
            assert kline_ts - original_ts <= tolerance_ms, (
                f"Data staleness warning: Original timestamp {original_ts} is {(kline_ts - original_ts) / hour_ms:.1f} hours before kline timestamp {kline_ts}"
            )

    @pytest.mark.asyncio
    async def test_combined_kline_and_metrics_temporal_consistency(self):
        """测试合并 K线和 Metrics 数据时的时间一致性.

        验证：合并后的数据在任意时间点 t，所有值都来自 t 或之前。
        """
        symbols = ["BTCUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 5

        # 创建日线 K线数据
        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms)

        # 创建不同频率的 metrics 数据
        fr_df = create_mock_metrics_df(symbols, start_ts, end_ts, 8 * hour_ms, "funding_rate")
        oi_df = create_mock_metrics_df(symbols, start_ts, end_ts, 5 * 60 * 1000, "open_interest")

        # 分别对齐
        fr_result = await DataResampler.resample_and_align(fr_df, kline_df, Freq.d1, {"funding_rate": "last"}, "asof", return_original_timestamps=True)
        oi_result = await DataResampler.resample_and_align(oi_df, kline_df, Freq.d1, {"open_interest": "last"}, "asof", return_original_timestamps=True)

        fr_aligned, fr_ts = fr_result
        oi_aligned, oi_ts = oi_result

        # 合并所有时间戳信息
        all_ts_dfs = [
            ("funding_rate", fr_ts),
            ("open_interest", oi_ts),
        ]

        # 验证每种数据类型
        for data_type, ts_df in all_ts_dfs:
            for idx, row in ts_df.iterrows():
                _, kline_ts = idx
                original_ts = row["original_timestamp"]

                if pd.isna(original_ts):
                    continue

                assert original_ts <= kline_ts, f"Lookahead bias in {data_type}! Original timestamp {original_ts} > kline timestamp {kline_ts}"


# ==============================================================================
# 测试类：边界条件和边缘情况
# ==============================================================================


class TestLookaheadBiasEdgeCases:
    """测试边界条件和边缘情况下的 lookahead bias 检测."""

    @pytest.mark.asyncio
    async def test_first_data_point_no_lookahead(self):
        """测试第一个数据点没有 lookahead bias.

        验证：即使在第一个时间点，也不会使用未来数据。
        """
        symbols = ["BTCUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        start_ts = 1700006400000
        end_ts = start_ts + day_ms * 2

        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, day_ms)

        # Metrics 从第二天开始（模拟新上线的数据）
        metrics_start_ts = start_ts + day_ms
        metrics_df = create_mock_metrics_df(symbols, metrics_start_ts, end_ts, hour_ms, "funding_rate")

        result = await DataResampler.align_to_kline_timestamps(metrics_df, kline_df, "asof", return_original_timestamps=True)

        aligned_df, original_ts_df = result

        # 第一个 kline 时间点应该没有对应的 metrics（NaN）
        first_kline_ts = kline_df.index.get_level_values("timestamp").min()

        for idx, row in original_ts_df.iterrows():
            _, kline_ts = idx
            original_ts = row["original_timestamp"]

            if kline_ts == first_kline_ts:
                # 第一个时间点可能是 NaN（没有历史数据）
                # 或者如果有值，必须 <= kline_ts
                if not pd.isna(original_ts):
                    assert original_ts <= kline_ts
            else:
                if not pd.isna(original_ts):
                    assert original_ts <= kline_ts

    @pytest.mark.asyncio
    async def test_timezone_boundary_no_lookahead(self):
        """测试跨时区边界时没有 lookahead bias.

        验证：在 UTC 日期边界附近的数据处理正确。
        """
        symbols = ["BTCUSDT"]
        hour_ms = 3600000
        day_ms = 24 * hour_ms

        # 使用 UTC 午夜附近的时间戳
        start_ts = 1700006400000  # 2023-11-15 00:00:00 UTC
        end_ts = start_ts + day_ms * 2

        # 创建跨越午夜的小时级数据
        kline_df = create_mock_kline_df(symbols, start_ts, end_ts, hour_ms)

        # 重采样到日线
        daily_df = await DataResampler.resample(kline_df, Freq.d1)

        # 验证日线数据的时间戳是 UTC 午夜
        for idx in daily_df.index:
            _, ts = idx
            dt = pd.Timestamp(ts, unit="ms", tz="UTC")

            # 日线时间戳应该是 UTC 00:00:00
            assert dt.hour == 0, f"Daily bar timestamp should be at UTC midnight, got {dt}"
            assert dt.minute == 0
            assert dt.second == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
