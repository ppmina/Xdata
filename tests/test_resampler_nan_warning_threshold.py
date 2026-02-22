"""Tests for NaN warning threshold behavior in DataResampler alignment."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

import cryptoservice.storage.resampler as resampler_module
from cryptoservice.storage.resampler import DataResampler


def _make_kline_df(rows: int = 10) -> pd.DataFrame:
    symbol = "BTCUSDT"
    start_ts = 1700000000000
    step_ms = 5 * 60 * 1000
    records = []
    for idx in range(rows):
        records.append(
            {
                "symbol": symbol,
                "timestamp": start_ts + idx * step_ms,
                "close_time": start_ts + (idx + 1) * step_ms - 1,
            }
        )
    return pd.DataFrame(records).set_index(["symbol", "timestamp"])


def _make_metrics_df(metric_start_index: int, rows: int = 10) -> pd.DataFrame:
    symbol = "BTCUSDT"
    start_ts = 1700000000000
    step_ms = 5 * 60 * 1000
    records = []
    for idx in range(metric_start_index, rows):
        records.append(
            {
                "symbol": symbol,
                "timestamp": start_ts + idx * step_ms,
                "open_interest": float(idx),
            }
        )
    return pd.DataFrame(records).set_index(["symbol", "timestamp"])


def _has_event(calls: list, event: str) -> bool:
    return any(call.args and call.args[0] == event for call in calls)


def test_align_nan_warning_uses_debug_when_below_threshold(monkeypatch) -> None:
    kline_df = _make_kline_df(rows=10)
    metrics_df = _make_metrics_df(metric_start_index=1, rows=10)  # 1/10 NaN

    mock_logger = MagicMock()
    monkeypatch.setattr(resampler_module, "logger", mock_logger)

    aligned_df = DataResampler._align_timestamps_sync(
        metrics_df=metrics_df,
        kline_df=kline_df,
        method="asof",
        tolerance_ms=24 * 60 * 60 * 1000,
        use_close_time=False,
        include_equal=True,
        nan_warn_ratio_threshold=0.2,
    )
    assert isinstance(aligned_df, pd.DataFrame)
    assert _has_event(mock_logger.debug.call_args_list, "Alignment missing values below warning threshold")
    assert not _has_event(mock_logger.warning.call_args_list, "Alignment results contain missing values")


def test_align_nan_warning_warns_when_above_threshold(monkeypatch) -> None:
    kline_df = _make_kline_df(rows=10)
    metrics_df = _make_metrics_df(metric_start_index=3, rows=10)  # 3/10 NaN

    mock_logger = MagicMock()
    monkeypatch.setattr(resampler_module, "logger", mock_logger)

    aligned_df = DataResampler._align_timestamps_sync(
        metrics_df=metrics_df,
        kline_df=kline_df,
        method="asof",
        tolerance_ms=24 * 60 * 60 * 1000,
        use_close_time=False,
        include_equal=True,
        nan_warn_ratio_threshold=0.2,
    )
    assert isinstance(aligned_df, pd.DataFrame)
    assert _has_event(mock_logger.warning.call_args_list, "Alignment results contain missing values")
    assert not _has_event(mock_logger.debug.call_args_list, "Alignment missing values below warning threshold")


def test_align_nan_warning_threshold_zero_keeps_backward_compatibility(monkeypatch) -> None:
    kline_df = _make_kline_df(rows=10)
    metrics_df = _make_metrics_df(metric_start_index=1, rows=10)  # 1/10 NaN

    mock_logger = MagicMock()
    monkeypatch.setattr(resampler_module, "logger", mock_logger)

    aligned_df = DataResampler._align_timestamps_sync(
        metrics_df=metrics_df,
        kline_df=kline_df,
        method="asof",
        tolerance_ms=24 * 60 * 60 * 1000,
        use_close_time=False,
        include_equal=True,
        nan_warn_ratio_threshold=0.0,
    )
    assert isinstance(aligned_df, pd.DataFrame)
    assert _has_event(mock_logger.warning.call_args_list, "Alignment results contain missing values")
