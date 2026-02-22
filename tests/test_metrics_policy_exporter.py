"""Metrics asof cold-start policy tests for NumpyExporter."""

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
