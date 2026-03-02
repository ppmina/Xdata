"""Fixed-point rewrite tests for incremental planning."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from cryptoservice.storage.incremental import IncrementalManager, rewrite_points_fixed_point
from cryptoservice.utils import date_to_timestamp_start, shift_date


@dataclass(frozen=True)
class _Segment:
    start: int
    end: int
    kind: str


def _merge_segments(run: list[_Segment]) -> _Segment:
    return _Segment(
        start=min(segment.start for segment in run),
        end=max(segment.end for segment in run),
        kind="merged",
    )


def test_rewrite_points_merges_maximal_run_and_inserts_filler() -> None:
    points = [
        _Segment(start=0, end=1, kind="raw"),
        _Segment(start=1, end=2, kind="raw"),
        _Segment(start=2, end=3, kind="raw"),
    ]

    rewritten = rewrite_points_fixed_point(
        points,
        is_continuous=lambda a, b: b.start <= a.end,
        can_merge=lambda run: len(run) >= 2 and all(point.kind == "raw" for point in run),
        merge=_merge_segments,
        make_filler=lambda _run, merged: _Segment(start=merged.start - 1, end=merged.start, kind="filler"),
    )

    assert rewritten == [
        _Segment(start=-1, end=0, kind="filler"),
        _Segment(start=0, end=3, kind="merged"),
    ]


def test_rewrite_points_non_mergeable_run_advances_by_one() -> None:
    points = [
        _Segment(start=0, end=1, kind="block"),
        _Segment(start=1, end=2, kind="raw"),
        _Segment(start=2, end=3, kind="raw"),
    ]

    rewritten = rewrite_points_fixed_point(
        points,
        is_continuous=lambda a, b: b.start <= a.end,
        can_merge=lambda run: len(run) >= 2 and run[0].kind == "raw" and all(point.kind == "raw" for point in run),
        merge=_merge_segments,
        make_filler=lambda _run, merged: _Segment(start=merged.start - 1, end=merged.start, kind="filler"),
    )

    assert rewritten[0] == _Segment(start=0, end=1, kind="block")
    assert any(segment.kind == "merged" and segment.start == 1 and segment.end == 3 for segment in rewritten)


def test_rewrite_points_repeats_until_fixed_point() -> None:
    rewritten = rewrite_points_fixed_point(
        [3],
        is_continuous=lambda _a, _b: False,
        can_merge=lambda run: run[0] > 1,
        merge=lambda run: run[0] - 1,
        make_filler=lambda _run, _merged: None,
    )

    assert rewritten == [1]


def test_rewrite_points_supports_none_filler() -> None:
    points = [
        _Segment(start=0, end=1, kind="raw"),
        _Segment(start=1, end=2, kind="raw"),
    ]

    rewritten = rewrite_points_fixed_point(
        points,
        is_continuous=lambda a, b: b.start <= a.end,
        can_merge=lambda run: len(run) >= 2 and all(point.kind == "raw" for point in run),
        merge=_merge_segments,
        make_filler=lambda _run, _merged: None,
    )

    assert rewritten == [_Segment(start=0, end=2, kind="merged")]


def test_rewrite_points_raises_when_max_passes_exceeded() -> None:
    with pytest.raises(ValueError, match="did not converge"):
        rewrite_points_fixed_point(
            [1],
            is_continuous=lambda _a, _b: False,
            can_merge=lambda _run: True,
            merge=lambda run: run[0],
            make_filler=lambda _run, _merged: None,
            max_passes=3,
        )


@pytest.mark.asyncio
async def test_plan_metrics_download_keeps_single_range_schema_and_unions() -> None:
    start_date = "2024-10-01"
    end_date = "2024-10-01"
    expanded_start = shift_date(start_date, -1)
    start_bound = date_to_timestamp_start(expanded_start)

    metrics_query = AsyncMock()
    metrics_query.get_missing_timestamps = AsyncMock(return_value=[start_bound + 3_600_000 * 3, start_bound + 3_600_000 * 4])

    manager = IncrementalManager(kline_query=AsyncMock(), metrics_query=metrics_query)
    plan = await manager.plan_metrics_download(
        symbols=["BTCUSDT"],
        start_date=start_date,
        end_date=end_date,
        data_type="open_interest",
        interval_hours=1,
    )

    assert "BTCUSDT" in plan
    symbol_plan = plan["BTCUSDT"]
    assert {"start_ts", "end_ts", "start_time", "end_time", "missing_count", "interval_ms"}.issubset(symbol_plan.keys())
    assert symbol_plan["start_ts"] == start_bound + 3_600_000 * 2
    assert symbol_plan["end_ts"] == start_bound + 3_600_000 * 5
    assert symbol_plan["missing_count"] == 2
    assert symbol_plan["interval_ms"] == 3_600_000


@pytest.mark.asyncio
async def test_plan_metrics_download_filler_start_is_clamped_to_start_bound() -> None:
    start_date = "2024-10-01"
    end_date = "2024-10-01"
    expanded_start = shift_date(start_date, -1)
    start_bound = date_to_timestamp_start(expanded_start)

    metrics_query = AsyncMock()
    metrics_query.get_missing_timestamps = AsyncMock(return_value=[start_bound + 10 * 60 * 1000, start_bound + 20 * 60 * 1000])

    manager = IncrementalManager(kline_query=AsyncMock(), metrics_query=metrics_query)
    plan = await manager.plan_metrics_download(
        symbols=["BTCUSDT"],
        start_date=start_date,
        end_date=end_date,
        data_type="long_short_ratio",
        interval_hours=1,
    )

    assert "BTCUSDT" in plan
    symbol_plan = plan["BTCUSDT"]
    assert symbol_plan["start_ts"] == start_bound
    assert symbol_plan["end_ts"] == start_bound + 80 * 60 * 1000


@pytest.mark.asyncio
async def test_plan_metrics_download_excludes_symbols_without_missing_timestamps() -> None:
    metrics_query = AsyncMock()
    metrics_query.get_missing_timestamps = AsyncMock(return_value=[])

    manager = IncrementalManager(kline_query=AsyncMock(), metrics_query=metrics_query)
    plan = await manager.plan_metrics_download(
        symbols=["BTCUSDT"],
        start_date="2024-10-01",
        end_date="2024-10-01",
        data_type="open_interest",
        interval_hours=1,
    )

    assert plan == {}


class TestVisionPlannerThreshold:
    """Verify vision planner requires full coverage per frequency."""

    @pytest.mark.asyncio
    async def test_sparse_5m_data_is_marked_missing(self) -> None:
        """A symbol-day with 1 row (stub from interrupted download) must be re-downloaded."""
        from cryptoservice.models import Freq

        metrics_query = AsyncMock()
        metrics_query.get_daily_metrics_status = AsyncMock(
            return_value={"open_interest": 1, "long_short_ratio": 1},
        )

        manager = IncrementalManager(kline_query=AsyncMock(), metrics_query=metrics_query)
        plan = await manager._plan_universe_vision(
            {"BTCUSDT": [("2024-10-02", "2024-10-02")]},
            metrics_freq=Freq.m5,
        )

        assert "BTCUSDT" in plan
        assert "2024-10-02" in plan["BTCUSDT"].missing_dates

    @pytest.mark.asyncio
    async def test_full_5m_data_is_marked_complete(self) -> None:
        """A symbol-day with 288 rows (full 5m coverage) should not appear in the plan."""
        from cryptoservice.models import Freq

        metrics_query = AsyncMock()
        metrics_query.get_daily_metrics_status = AsyncMock(
            return_value={"open_interest": 288, "long_short_ratio": 288},
        )

        manager = IncrementalManager(kline_query=AsyncMock(), metrics_query=metrics_query)
        plan = await manager._plan_universe_vision(
            {"BTCUSDT": [("2024-10-01", "2024-10-01")]},
            metrics_freq=Freq.m5,
        )

        assert plan == {}

    @pytest.mark.asyncio
    async def test_missing_one_row_triggers_redownload(self) -> None:
        """Even 287 out of 288 rows (one missing) must trigger a re-download."""
        from cryptoservice.models import Freq

        metrics_query = AsyncMock()
        metrics_query.get_daily_metrics_status = AsyncMock(
            return_value={"open_interest": 287, "long_short_ratio": 288},
        )

        manager = IncrementalManager(kline_query=AsyncMock(), metrics_query=metrics_query)
        plan = await manager._plan_universe_vision(
            {"BTCUSDT": [("2024-10-01", "2024-10-01")]},
            metrics_freq=Freq.m5,
        )

        assert "BTCUSDT" in plan

    @pytest.mark.asyncio
    async def test_threshold_adapts_to_hourly_freq(self) -> None:
        """For 1h metrics, expected rows = 24; 23 rows should trigger re-download."""
        from cryptoservice.models import Freq

        metrics_query = AsyncMock()
        metrics_query.get_daily_metrics_status = AsyncMock(
            return_value={"open_interest": 23, "long_short_ratio": 24},
        )

        manager = IncrementalManager(kline_query=AsyncMock(), metrics_query=metrics_query)
        plan = await manager._plan_universe_vision(
            {"ETHUSDT": [("2024-10-03", "2024-10-03")]},
            metrics_freq=Freq.h1,
        )

        assert "ETHUSDT" in plan

    @pytest.mark.asyncio
    async def test_full_hourly_data_is_complete(self) -> None:
        """For 1h metrics, 24 rows per day is complete."""
        from cryptoservice.models import Freq

        metrics_query = AsyncMock()
        metrics_query.get_daily_metrics_status = AsyncMock(
            return_value={"open_interest": 24, "long_short_ratio": 24},
        )

        manager = IncrementalManager(kline_query=AsyncMock(), metrics_query=metrics_query)
        plan = await manager._plan_universe_vision(
            {"ETHUSDT": [("2024-10-03", "2024-10-03")]},
            metrics_freq=Freq.h1,
        )

        assert plan == {}

    @pytest.mark.asyncio
    async def test_legacy_collect_vision_plan_uses_expected_count(self) -> None:
        """The legacy _collect_vision_plan should also require full coverage."""
        import pandas as pd

        from cryptoservice.models import Freq

        metrics_query = AsyncMock()
        metrics_query.get_daily_metrics_status = AsyncMock(
            return_value={"open_interest": 1, "long_short_ratio": 1},
        )

        manager = IncrementalManager(kline_query=AsyncMock(), metrics_query=metrics_query)
        date_range = pd.date_range(start="2024-10-02", end="2024-10-02", freq="D", tz="UTC")
        plan, complete_count = await manager._collect_vision_plan(["BTCUSDT"], date_range, metrics_freq=Freq.m5)

        assert "BTCUSDT" in plan
        assert complete_count == 0

    def test_expected_rows_per_day_values(self) -> None:
        """Verify _expected_rows_per_day returns correct counts for common frequencies."""
        from cryptoservice.models import Freq

        manager = IncrementalManager(kline_query=AsyncMock(), metrics_query=AsyncMock())
        assert manager._expected_rows_per_day(Freq.m1) == 1440
        assert manager._expected_rows_per_day(Freq.m5) == 288
        assert manager._expected_rows_per_day(Freq.m15) == 96
        assert manager._expected_rows_per_day(Freq.h1) == 24
        assert manager._expected_rows_per_day(Freq.h4) == 6
        assert manager._expected_rows_per_day(Freq.d1) == 1
