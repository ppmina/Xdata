"""Metrics asof cold-start policy tests for NumpyExporter."""

from pathlib import Path
from unittest.mock import AsyncMock

import numpy as np
import pandas as pd
import pytest

from cryptoservice.models import Freq
from cryptoservice.storage import NumpyExporter
from cryptoservice.utils.time_utils import shift_date


def _make_kline_df() -> pd.DataFrame:
    data = [
        {
            "symbol": "BTCUSDT",
            "timestamp": 1704153600000,
            "close_time": 1704239999999,
            "open_price": 1.0,
            "high_price": 1.0,
            "low_price": 1.0,
            "close_price": 1.0,
            "volume": 1.0,
        },
        {
            "symbol": "BTCUSDT",
            "timestamp": 1704240000000,
            "close_time": 1704326399999,
            "open_price": 1.0,
            "high_price": 1.0,
            "low_price": 1.0,
            "close_price": 1.0,
            "volume": 1.0,
        },
    ]
    return pd.DataFrame(data).set_index(["symbol", "timestamp"])


def _make_single_metric_df(column: str) -> pd.DataFrame:
    data = [
        {
            "symbol": "BTCUSDT",
            "timestamp": 1704153600000,
            column: 1.0,
        }
    ]
    return pd.DataFrame(data).set_index(["symbol", "timestamp"])


def _make_alignment_result(index: pd.MultiIndex, column: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    aligned = pd.DataFrame({column: [np.nan, 1.0]}, index=index)
    original_ts = pd.DataFrame({"original_timestamp": [np.nan, 1704153600000]}, index=index)
    return aligned, original_ts


@pytest.mark.asyncio
async def test_fetch_funding_rate_uses_3d_lookback_and_48h_tolerance():
    """Funding rate should query D-3 and align with 48h tolerance."""
    kline_df = _make_kline_df()
    raw_df = _make_single_metric_df("funding_rate")

    mock_kline_query = AsyncMock()
    mock_resampler = AsyncMock()
    mock_metrics_query = AsyncMock()
    mock_metrics_query.select_funding_rates = AsyncMock(return_value=raw_df)
    mock_resampler.resample_and_align = AsyncMock(return_value=_make_alignment_result(kline_df.index, "funding_rate"))

    exporter = NumpyExporter(mock_kline_query, mock_resampler, mock_metrics_query)

    await exporter._fetch_funding_rate(["BTCUSDT"], "2024-01-03", "2024-01-03", kline_df, Freq.d1)

    select_kwargs = mock_metrics_query.select_funding_rates.await_args.kwargs
    assert select_kwargs["columns"] == ["funding_rate"]
    assert mock_metrics_query.select_funding_rates.await_args.args[1] == shift_date("2024-01-03", -3)

    align_kwargs = mock_resampler.resample_and_align.await_args.kwargs
    assert align_kwargs["align_method"] == "asof"
    assert align_kwargs["use_close_time"] is True
    assert align_kwargs["return_original_timestamps"] is True
    assert align_kwargs["tolerance_ms"] == 172800000
    assert align_kwargs["nan_warn_ratio_threshold"] == exporter.METRICS_MISSING_WARN_THRESHOLD


@pytest.mark.asyncio
async def test_fetch_open_interest_and_lsr_use_1d_lookback_and_6h_tolerance():
    """Open interest / long-short ratio should query D-1 and align with 6h tolerance."""
    kline_df = _make_kline_df()

    mock_kline_query = AsyncMock()
    mock_resampler = AsyncMock()
    mock_metrics_query = AsyncMock()
    mock_metrics_query.select_open_interests = AsyncMock(return_value=_make_single_metric_df("open_interest"))
    mock_metrics_query.select_long_short_ratio_by_type = AsyncMock(return_value=_make_single_metric_df("lsr_ta"))
    mock_resampler.resample_and_align = AsyncMock(side_effect=[
        _make_alignment_result(kline_df.index, "open_interest"),
        _make_alignment_result(kline_df.index, "lsr_ta"),
    ])

    exporter = NumpyExporter(mock_kline_query, mock_resampler, mock_metrics_query)

    await exporter._fetch_open_interest(["BTCUSDT"], "2024-01-03", "2024-01-03", kline_df, Freq.d1, include_value=False)
    await exporter._fetch_long_short_ratio(["BTCUSDT"], "2024-01-03", "2024-01-03", kline_df, Freq.d1, "toptrader_account")

    assert mock_metrics_query.select_open_interests.await_args.args[1] == shift_date("2024-01-03", -1)
    oi_align_kwargs = mock_resampler.resample_and_align.await_args_list[0].kwargs
    assert oi_align_kwargs["tolerance_ms"] == 21600000
    assert oi_align_kwargs["nan_warn_ratio_threshold"] == exporter.METRICS_MISSING_WARN_THRESHOLD

    assert mock_metrics_query.select_long_short_ratio_by_type.await_args.args[1] == shift_date("2024-01-03", -1)
    lsr_align_kwargs = mock_resampler.resample_and_align.await_args_list[1].kwargs
    assert lsr_align_kwargs["tolerance_ms"] == 21600000
    assert lsr_align_kwargs["nan_warn_ratio_threshold"] == exporter.METRICS_MISSING_WARN_THRESHOLD


@pytest.mark.asyncio
async def test_fetch_and_merge_metrics_reports_full_missing_when_metric_unavailable():
    """Coverage should report 100% missing when enabled metrics return empty data."""
    kline_df = _make_kline_df()

    exporter = NumpyExporter(AsyncMock(), AsyncMock(), AsyncMock())
    exporter._fetch_funding_rate = AsyncMock(return_value=(None, None, "fr_timestamp"))
    exporter._fetch_open_interest = AsyncMock(return_value=(None, None, "oi_timestamp"))
    exporter._fetch_long_short_ratio = AsyncMock(return_value=(None, None, "lsr_timestamp"))

    merged_df, timestamp_dfs, coverage = await exporter._fetch_and_merge_metrics(
        kline_df=kline_df,
        symbols=["BTCUSDT"],
        start_time="2024-01-03",
        end_time="2024-01-03",
        target_freq=Freq.d1,
        metrics_config={
            "funding_rate": True,
            "open_interest": {"include_value": False},
            "long_short_ratio": {"toptrader_account": True},
        },
    )

    assert merged_df.empty
    assert timestamp_dfs == {}
    assert coverage["funding_rate"]["missing_ratio"] == 1.0
    assert coverage["open_interest"]["missing_ratio"] == 1.0
    assert coverage["lsr_ta"]["missing_ratio"] == 1.0
    assert coverage["funding_rate"]["missing_count"] == len(kline_df)


def test_strict_filter_drops_symbol_day_when_required_metric_contains_nan():
    """Strict filter should drop symbol-day groups with required metric NaN."""
    exporter = NumpyExporter(AsyncMock(), AsyncMock(), AsyncMock())

    rows = [
        {"symbol": "BTCUSDT", "timestamp": 1704153600000, "close_time": 1704239999999, "funding_rate": 0.1},
        {"symbol": "BTCUSDT", "timestamp": 1704157200000, "close_time": 1704239999999, "funding_rate": 0.2},
        {"symbol": "ETHUSDT", "timestamp": 1704153600000, "close_time": 1704239999999, "funding_rate": np.nan},
        {"symbol": "ETHUSDT", "timestamp": 1704157200000, "close_time": 1704239999999, "funding_rate": 0.2},
    ]
    combined_df = pd.DataFrame(rows).set_index(["symbol", "timestamp"])
    ts_df = pd.DataFrame({"timestamp": combined_df["close_time"].values}, index=combined_df.index)
    policy = exporter._resolve_reliability_policy({"mode": "strict_100"})

    filtered_df, filtered_ts, strict_filter = exporter._apply_strict_metrics_filter(
        combined_df=combined_df,
        timestamp_dfs={"close_timestamp": ts_df},
        required_columns=["funding_rate"],
        reliability_policy=policy,
    )

    kept_symbols = filtered_df.index.get_level_values("symbol").unique().tolist()
    assert kept_symbols == ["BTCUSDT"]
    assert strict_filter["drop_reason_counts"]["missing_required_metrics_after_asof"] == 1
    assert strict_filter["dropped_symbol_days"][0]["symbol"] == "ETHUSDT"
    assert set(filtered_ts["close_timestamp"].index.get_level_values("symbol")) == {"BTCUSDT"}


def test_strict_filter_drops_symbol_day_when_required_column_missing():
    """Strict filter should drop symbol-day when required column is absent."""
    exporter = NumpyExporter(AsyncMock(), AsyncMock(), AsyncMock())
    rows = [{"symbol": "BTCUSDT", "timestamp": 1704153600000, "close_time": 1704239999999, "open_price": 1.0}]
    combined_df = pd.DataFrame(rows).set_index(["symbol", "timestamp"])
    policy = exporter._resolve_reliability_policy({"mode": "strict_100"})

    filtered_df, _, strict_filter = exporter._apply_strict_metrics_filter(
        combined_df=combined_df,
        timestamp_dfs={},
        required_columns=["funding_rate"],
        reliability_policy=policy,
    )

    assert filtered_df.empty
    assert strict_filter["drop_reason_counts"]["missing_required_columns"] == 1
    assert strict_filter["dropped_symbol_days"][0]["missing_columns"] == ["funding_rate"]


@pytest.mark.asyncio
async def test_export_combined_data_skips_day_when_strict_filter_drops_all(tmp_path):
    """Strict mode should skip day when all symbol-day groups are dropped."""
    kline_df = _make_kline_df()
    metrics_df = pd.DataFrame({"funding_rate": [np.nan, np.nan]}, index=kline_df.index)
    ts_df = pd.DataFrame({"timestamp": [np.nan, np.nan]}, index=kline_df.index)

    exporter = NumpyExporter(AsyncMock(), AsyncMock(), AsyncMock())
    exporter.kline_query.select_by_time_range = AsyncMock(return_value=kline_df)
    exporter._fetch_and_merge_metrics = AsyncMock(return_value=(metrics_df, {"fr_timestamp": ts_df}, {}))

    result = await exporter.export_combined_data(
        symbols=["BTCUSDT"],
        start_time="2024-01-03",
        end_time="2024-01-03",
        source_freq=Freq.d1,
        export_freq=Freq.d1,
        output_path=Path(tmp_path),
        include_klines=True,
        include_metrics=True,
        metrics_config={"funding_rate": True, "open_interest": False, "long_short_ratio": False},
    )

    assert result["day_status"] == "skipped"
    assert result["skip_reason"] == "strict_metrics_empty_day"
    assert result["strict_metrics_filter"]["skipped"] is True
    assert result["strict_metrics_filter"]["drop_reason_counts"]["missing_required_metrics_after_asof"] == 2
    assert not (Path(tmp_path) / "univ_dct2.json").exists()


def test_strict_invariant_raises_when_required_metric_nan_survives():
    """Strict invariant should fail fast if required metrics still contain NaN."""
    rows = [{"symbol": "BTCUSDT", "timestamp": 1704153600000, "funding_rate": np.nan}]
    combined_df = pd.DataFrame(rows).set_index(["symbol", "timestamp"])

    with pytest.raises(ValueError, match="strict_100 invariant violated"):
        NumpyExporter._validate_strict_required_columns(combined_df, ["funding_rate"])
