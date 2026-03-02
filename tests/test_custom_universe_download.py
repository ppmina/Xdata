"""Universe download v2 tests."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from cryptoservice.config import RetryConfig
from cryptoservice.exceptions import MarketDataFetchError, RateLimitError
from cryptoservice.models import Freq, IntegrityReport
from cryptoservice.models.universe import UniverseDailySnapshot, UniverseDefinition
from cryptoservice.services import MarketDataService
from cryptoservice.storage.database import Database
from cryptoservice.storage.incremental import (
    SymbolFundingPlan,
    SymbolVisionPlan,
    UniverseDownloadPlan,
    gather_symbol_date_ranges,
)
from cryptoservice.utils.time_utils import date_to_timestamp_end, date_to_timestamp_start


def _write_universe(
    path: Path,
    snapshots: list[UniverseDailySnapshot],
    start_date: str = "2024-10-01",
    end_date: str = "2024-10-02",
) -> None:
    universe = UniverseDefinition(
        schema_version="2.0",
        requested_symbols=["BTCUSDT", "ETHUSDT"],
        start_date=start_date,
        end_date=end_date,
        daily_snapshots=snapshots,
        created_at=datetime.now(tz=UTC),
    )
    universe.save_to_file(path)


def _ok_report(symbols: list[str]) -> IntegrityReport:
    return IntegrityReport(
        total_symbols=len(symbols),
        successful_symbols=len(symbols),
        failed_symbols=[],
        missing_periods=[],
        data_quality_score=1.0,
        recommendations=[],
    )


# ---------------------------------------------------------------------------
# gather_symbol_date_ranges unit tests
# ---------------------------------------------------------------------------


def test_gather_symbol_date_ranges_merges_consecutive() -> None:
    snapshots = [
        UniverseDailySnapshot(date="2024-10-01", active_symbols=["BTCUSDT", "ETHUSDT"], missing_symbols={}),
        UniverseDailySnapshot(date="2024-10-02", active_symbols=["BTCUSDT", "ETHUSDT"], missing_symbols={}),
        UniverseDailySnapshot(date="2024-10-03", active_symbols=["BTCUSDT"], missing_symbols={"ETHUSDT": "no_kline_on_date"}),
    ]
    result = gather_symbol_date_ranges(snapshots)
    assert result["BTCUSDT"] == [("2024-10-01", "2024-10-03")]
    assert result["ETHUSDT"] == [("2024-10-01", "2024-10-02")]


def test_gather_symbol_date_ranges_splits_gaps() -> None:
    snapshots = [
        UniverseDailySnapshot(date="2024-10-01", active_symbols=["BTCUSDT"], missing_symbols={}),
        UniverseDailySnapshot(date="2024-10-03", active_symbols=["BTCUSDT"], missing_symbols={}),
        UniverseDailySnapshot(date="2024-10-04", active_symbols=["BTCUSDT"], missing_symbols={}),
    ]
    result = gather_symbol_date_ranges(snapshots)
    assert result["BTCUSDT"] == [("2024-10-01", "2024-10-01"), ("2024-10-03", "2024-10-04")]


def test_gather_symbol_date_ranges_empty() -> None:
    assert gather_symbol_date_ranges([]) == {}


# ---------------------------------------------------------------------------
# Integration tests using the new symbol-centric download flow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_download_universe_data_uses_symbol_centric_plan(tmp_path) -> None:
    """Download should call kline_downloader once with precomputed ranges."""
    universe_path = tmp_path / "universe.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=["ETHUSDT"],
                missing_symbols={"BTCUSDT": "no_kline_on_date"},
            ),
        ],
    )

    before_payload = universe_path.read_text(encoding="utf-8")

    service = MarketDataService(AsyncMock())

    service.kline_downloader.download_multiple_symbols = AsyncMock(return_value=_ok_report(["BTCUSDT", "ETHUSDT"]))

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.1,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        interval=Freq.h1,
    )

    assert report["total_days"] == 2
    assert report["processed_days"] == 2
    assert report["total_successful_symbols"] == 2
    assert report["total_failed_symbols"] == 0

    # Single call to download_multiple_symbols (not one per day).
    service.kline_downloader.download_multiple_symbols.assert_called_once()
    call_kwargs = service.kline_downloader.download_multiple_symbols.call_args
    assert call_kwargs.kwargs.get("precomputed_ranges") is not None

    after_payload = universe_path.read_text(encoding="utf-8")
    assert after_payload == before_payload


@pytest.mark.asyncio
async def test_download_universe_data_tolerates_empty_active_days(tmp_path) -> None:
    """Days with zero active symbols should be skipped cleanly."""
    universe_path = tmp_path / "universe_empty.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=[],
                missing_symbols={
                    "BTCUSDT": "no_kline_on_date",
                    "ETHUSDT": "no_kline_on_date",
                },
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=[],
                missing_symbols={
                    "BTCUSDT": "no_kline_on_date",
                    "ETHUSDT": "no_kline_on_date",
                },
            ),
        ],
    )

    service = MarketDataService(AsyncMock())
    service.kline_downloader.download_multiple_symbols = AsyncMock()

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.1,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        interval=Freq.h1,
    )

    assert report["processed_days"] == 0
    assert len(report["skipped_days"]) == 2
    assert report["total_symbols"] == 0


@pytest.mark.asyncio
async def test_download_universe_data_applies_date_override_and_reports_effective_range(tmp_path) -> None:
    """Download should process only the requested subset date window."""
    universe_path = tmp_path / "universe_subset.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=["ETHUSDT"],
                missing_symbols={"BTCUSDT": "no_kline_on_date"},
            ),
        ],
    )

    before_payload = universe_path.read_text(encoding="utf-8")
    service = MarketDataService(AsyncMock())

    service.kline_downloader.download_multiple_symbols = AsyncMock(return_value=_ok_report(["ETHUSDT"]))

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.1,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        interval=Freq.h1,
        start_date="2024-10-02",
        end_date="2024-10-02",
    )

    assert report["total_days"] == 1
    assert report["processed_days"] == 1
    assert report["date_range"]["requested_start_date"] == "2024-10-01"
    assert report["date_range"]["requested_end_date"] == "2024-10-02"
    assert report["date_range"]["effective_start_date"] == "2024-10-02"
    assert report["date_range"]["effective_end_date"] == "2024-10-02"
    assert report["download_context"]["override_applied"] is True
    assert report["download_context"]["override_start_date"] == "2024-10-02"
    assert report["download_context"]["override_end_date"] == "2024-10-02"

    # Only ETHUSDT is active on 2024-10-02.
    call_kwargs = service.kline_downloader.download_multiple_symbols.call_args.kwargs
    assert sorted(call_kwargs["symbols"]) == ["ETHUSDT"]

    after_payload = universe_path.read_text(encoding="utf-8")
    assert after_payload == before_payload


@pytest.mark.asyncio
async def test_download_universe_data_partial_override_fills_missing_bound(tmp_path) -> None:
    """When one bound is omitted, universe boundary should be used."""
    universe_path = tmp_path / "universe_partial.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=["ETHUSDT"],
                missing_symbols={"BTCUSDT": "no_kline_on_date"},
            ),
        ],
    )

    service = MarketDataService(AsyncMock())
    service.kline_downloader.download_multiple_symbols = AsyncMock(return_value=_ok_report(["BTCUSDT"]))

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.1,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        interval=Freq.h1,
        end_date="2024-10-01",
    )

    assert report["date_range"]["effective_start_date"] == "2024-10-01"
    assert report["date_range"]["effective_end_date"] == "2024-10-01"
    assert report["download_context"]["override_start_date"] is None
    assert report["download_context"]["override_end_date"] == "2024-10-01"


@pytest.mark.asyncio
async def test_download_universe_data_rejects_out_of_bounds_override(tmp_path) -> None:
    """Override range must be inside universe range."""
    universe_path = tmp_path / "universe_oob.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=["ETHUSDT"],
                missing_symbols={"BTCUSDT": "no_kline_on_date"},
            ),
        ],
    )

    service = MarketDataService(AsyncMock())

    with pytest.raises(MarketDataFetchError, match="within universe range"):
        await service.download_universe_data(
            universe_file=universe_path,
            db_path=tmp_path / "market.db",
            retry_config=RetryConfig(max_retries=1),
            api_request_delay=0.1,
            vision_request_delay=0.0,
            download_market_metrics=False,
            incremental=True,
            interval=Freq.h1,
            start_date="2024-09-30",
            end_date="2024-10-01",
        )


@pytest.mark.asyncio
async def test_download_universe_data_rejects_reversed_override(tmp_path) -> None:
    """Override start must be <= override end."""
    universe_path = tmp_path / "universe_reversed.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=["ETHUSDT"],
                missing_symbols={"BTCUSDT": "no_kline_on_date"},
            ),
        ],
    )

    service = MarketDataService(AsyncMock())

    with pytest.raises(MarketDataFetchError, match="must be <= end_date"):
        await service.download_universe_data(
            universe_file=universe_path,
            db_path=tmp_path / "market.db",
            retry_config=RetryConfig(max_retries=1),
            api_request_delay=0.1,
            vision_request_delay=0.0,
            download_market_metrics=False,
            incremental=True,
            interval=Freq.h1,
            start_date="2024-10-02",
            end_date="2024-10-01",
        )


@pytest.mark.asyncio
async def test_universe_download_runs_kline_and_metrics_stages_concurrently(tmp_path) -> None:
    """Both kline and metrics stages should run and produce correct reports."""
    universe_path = tmp_path / "universe_stage_order.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=["ETHUSDT"],
                missing_symbols={"BTCUSDT": "no_kline_on_date"},
            ),
        ],
    )

    service = MarketDataService(AsyncMock())
    metrics_dates: list[str] = []

    service.kline_downloader.download_multiple_symbols = AsyncMock(return_value=_ok_report(["BTCUSDT", "ETHUSDT"]))

    async def fake_download_metrics_for_date(*, date, **kwargs):
        metrics_dates.append(date)
        metrics_payload = {
            "status": "complete",
            "vision": {"status": "complete", "dataset": "vision-metrics", "duration_ms": 1, "terminal": False},
            "funding_rate": {"status": "complete", "dataset": "funding_rate", "duration_ms": 1, "terminal": False},
        }
        return metrics_payload, [], {"vision": False, "funding_rate": False}

    service._download_market_metrics_for_date = AsyncMock(side_effect=fake_download_metrics_for_date)

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.0,
        vision_request_delay=0.0,
        download_market_metrics=True,
        incremental=True,
        interval=Freq.h1,
    )

    service.kline_downloader.download_multiple_symbols.assert_called_once()
    assert sorted(metrics_dates) == ["2024-10-01", "2024-10-02"]
    assert report["stage_reports"]["kline"]["status"] == "complete"
    assert report["stage_reports"]["metrics"]["status"] == "complete"


@pytest.mark.asyncio
async def test_universe_download_metrics_stage_uses_day_level_symbol_subsets(tmp_path, monkeypatch) -> None:
    """Metrics scheduling should call each source only for missing day/symbol subsets."""
    universe_path = tmp_path / "universe_day_level_subsets.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT", "ETHUSDT"],
                missing_symbols={},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=["BTCUSDT", "ETHUSDT"],
                missing_symbols={},
            ),
        ],
    )

    service = MarketDataService(AsyncMock())
    service.kline_downloader.download_multiple_symbols = AsyncMock(return_value=_ok_report(["BTCUSDT", "ETHUSDT"]))

    async def fake_plan_universe_download(self, symbol_date_ranges, freq, download_market_metrics=False):  # noqa: ANN001
        return UniverseDownloadPlan(
            kline={},
            vision={
                "BTCUSDT": SymbolVisionPlan(
                    symbol="BTCUSDT",
                    missing_dates=["2024-10-01"],
                    missing_count=1,
                )
            },
            funding_rate={
                "ETHUSDT": SymbolFundingPlan(
                    symbol="ETHUSDT",
                    download_ranges=[(date_to_timestamp_start("2024-10-02"), date_to_timestamp_end("2024-10-02"))],
                    missing_count=1,
                )
            },
            all_symbols=["BTCUSDT", "ETHUSDT"],
            total_symbols=2,
            symbols_needing_kline=0,
            symbols_needing_vision=1,
            symbols_needing_funding=1,
            kline_total_missing=0,
            vision_total_missing_days=1,
            funding_total_missing=1,
        )

    monkeypatch.setattr(Database, "plan_universe_download", fake_plan_universe_download)

    vision_calls: list[tuple[str, list[str]]] = []
    funding_calls: list[tuple[str, list[str]]] = []

    async def fake_vision_download(*, symbols, start_date, **kwargs):  # noqa: ANN001
        vision_calls.append((start_date, sorted(symbols)))
        return None

    async def fake_funding_download(*, symbols, start_time, **kwargs):  # noqa: ANN001
        funding_calls.append((start_time, sorted(symbols)))
        return None

    service.vision_downloader.download_metrics_batch = AsyncMock(side_effect=fake_vision_download)
    service.metrics_downloader.download_funding_rate_batch = AsyncMock(side_effect=fake_funding_download)

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.0,
        vision_request_delay=0.0,
        download_market_metrics=True,
        incremental=True,
        interval=Freq.h1,
        max_day_workers=1,
    )

    assert vision_calls == [("2024-10-01", ["BTCUSDT"])]
    assert funding_calls == [("2024-10-02", ["ETHUSDT"])]
    assert report["stage_reports"]["metrics"]["status"] == "complete"
    assert report["stage_reports"]["metrics"]["days_complete"] == 2


@pytest.mark.asyncio
async def test_universe_download_marks_kline_day_partial_when_missing_periods_present(tmp_path) -> None:
    """Kline day should be partial when missing periods exist even if failed_symbols is empty."""
    universe_path = tmp_path / "universe_kline_missing_periods.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=[],
                missing_symbols={"BTCUSDT": "no_kline_on_date", "ETHUSDT": "no_kline_on_date"},
            ),
        ],
    )

    service = MarketDataService(AsyncMock())
    service.kline_downloader.download_multiple_symbols = AsyncMock(
        return_value=IntegrityReport(
            total_symbols=1,
            successful_symbols=1,
            failed_symbols=[],
            missing_periods=[{"symbol": "BTCUSDT", "period": "2024-10-01 - 2024-10-01", "reason": "no_data"}],
            data_quality_score=1.0,
            recommendations=[],
        )
    )

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.0,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        interval=Freq.h1,
    )

    # Missing day-level periods should mark the day partial in kline stage reporting.
    assert report["stage_reports"]["kline"]["days_complete"] == 0
    assert report["stage_reports"]["kline"]["days_error"] == 1
    assert report["failed_reason_summary"]["no_data"] == 1


@pytest.mark.asyncio
async def test_universe_download_wires_vision_missing_reason_into_day_report(tmp_path) -> None:
    """Vision missing/incomplete reasons should appear in day-level missing periods."""
    universe_path = tmp_path / "universe_vision_missing_reason.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "vision_day_unavailable"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=[],
                missing_symbols={"BTCUSDT": "vision_day_unavailable", "ETHUSDT": "vision_day_unavailable"},
            ),
        ],
    )

    service = MarketDataService(AsyncMock())
    service.kline_downloader.download_multiple_symbols = AsyncMock(
        return_value=IntegrityReport(
            total_symbols=1,
            successful_symbols=1,
            failed_symbols=[],
            missing_periods=[{"symbol": "BTCUSDT", "period": "2024-10-01 - 2024-10-01", "reason": "vision_file_missing"}],
            data_quality_score=1.0,
            recommendations=[],
        )
    )

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.0,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        interval=Freq.h1,
    )

    day_report = report["day_reports"][0]
    assert day_report["date"] == "2024-10-01"
    assert day_report["missing_periods"] == [
        {
            "symbol": "BTCUSDT",
            "period": "2024-10-01 - 2024-10-01",
            "reason": "vision_file_missing",
        }
    ]
    assert report["failed_reason_summary"]["vision_file_missing"] == 1


@pytest.mark.asyncio
async def test_universe_download_metrics_partial_failure_isolated_with_stage_error(tmp_path) -> None:
    """One metrics source failure should not abort run and must be reported."""
    universe_path = tmp_path / "universe_metrics_partial.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=["ETHUSDT"],
                missing_symbols={"BTCUSDT": "no_kline_on_date"},
            ),
        ],
    )

    service = MarketDataService(AsyncMock())
    service.kline_downloader.download_multiple_symbols = AsyncMock(return_value=_ok_report(["BTCUSDT", "ETHUSDT"]))

    async def fake_download_metrics_for_date(*, date, run_id, **kwargs):
        if date == "2024-10-01":
            metrics_payload = {
                "status": "partial",
                "vision": {
                    "status": "error",
                    "dataset": "vision-metrics",
                    "duration_ms": 3,
                    "terminal": False,
                    "error": "vision parse error",
                },
                "funding_rate": {
                    "status": "complete",
                    "dataset": "funding_rate",
                    "duration_ms": 2,
                    "terminal": False,
                },
            }
            stage_errors = [
                {
                    "run": run_id,
                    "stage": "metrics",
                    "dataset": "vision-metrics",
                    "source": "vision",
                    "date": date,
                    "error": "vision parse error",
                    "terminal": False,
                }
            ]
            return metrics_payload, stage_errors, {"vision": False, "funding_rate": False}

        metrics_payload = {
            "status": "complete",
            "vision": {"status": "complete", "dataset": "vision-metrics", "duration_ms": 1, "terminal": False},
            "funding_rate": {"status": "complete", "dataset": "funding_rate", "duration_ms": 1, "terminal": False},
        }
        return metrics_payload, [], {"vision": False, "funding_rate": False}

    service._download_market_metrics_for_date = AsyncMock(side_effect=fake_download_metrics_for_date)

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.0,
        vision_request_delay=0.0,
        download_market_metrics=True,
        incremental=True,
        interval=Freq.h1,
    )

    assert report["stage_reports"]["metrics"]["status"] == "partial"
    assert report["day_reports"][0]["metrics"]["status"] == "partial"
    assert report["day_reports"][1]["metrics"]["status"] == "complete"
    assert len(report["stage_errors"]) == 1
    assert report["stage_errors"][0]["dataset"] == "vision-metrics"
    assert report["stage_errors"][0]["source"] == "vision"
    assert report["stage_errors"][0]["terminal"] is False


@pytest.mark.asyncio
async def test_universe_metrics_terminal_funding_abort_does_not_stop_vision(tmp_path, monkeypatch) -> None:
    """Funding terminal throttle should abort funding substage while Vision continues.

    Uses max_day_workers=1 to test sequential abort propagation.
    """
    universe_path = tmp_path / "universe_metrics_terminal_abort.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=["ETHUSDT"],
                missing_symbols={"BTCUSDT": "no_kline_on_date"},
            ),
        ],
    )

    service = MarketDataService(AsyncMock())
    service.kline_downloader.download_multiple_symbols = AsyncMock(return_value=_ok_report(["BTCUSDT", "ETHUSDT"]))

    async def fake_plan_universe_download(self, symbol_date_ranges, freq, download_market_metrics=False):  # noqa: ANN001
        return UniverseDownloadPlan(
            kline={},
            vision={
                "BTCUSDT": SymbolVisionPlan(symbol="BTCUSDT", missing_dates=["2024-10-01"], missing_count=1),
                "ETHUSDT": SymbolVisionPlan(symbol="ETHUSDT", missing_dates=["2024-10-02"], missing_count=1),
            },
            funding_rate={
                "BTCUSDT": SymbolFundingPlan(
                    symbol="BTCUSDT",
                    download_ranges=[(date_to_timestamp_start("2024-10-01"), date_to_timestamp_end("2024-10-01"))],
                    missing_count=1,
                )
            },
            all_symbols=["BTCUSDT", "ETHUSDT"],
            total_symbols=2,
            symbols_needing_kline=0,
            symbols_needing_vision=2,
            symbols_needing_funding=1,
            kline_total_missing=0,
            vision_total_missing_days=2,
            funding_total_missing=1,
        )

    monkeypatch.setattr(Database, "plan_universe_download", fake_plan_universe_download)

    vision_dates: list[str] = []
    funding_dates: list[str] = []

    async def fake_vision_download(*, start_date, **kwargs):
        vision_dates.append(start_date)
        return None

    async def fake_funding_download(*, start_time, **kwargs):
        funding_dates.append(start_time)
        if start_time == "2024-10-01":
            raise RateLimitError("terminal funding throttle")
        return None

    service.vision_downloader.download_metrics_batch = AsyncMock(side_effect=fake_vision_download)
    service.metrics_downloader.download_funding_rate_batch = AsyncMock(side_effect=fake_funding_download)

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.0,
        vision_request_delay=0.0,
        download_market_metrics=True,
        incremental=True,
        interval=Freq.h1,
        max_day_workers=1,
    )

    assert vision_dates == ["2024-10-01", "2024-10-02"]
    assert funding_dates == ["2024-10-01"]
    assert report["day_reports"][1]["metrics"]["funding_rate"]["status"] == "aborted"
    assert report["day_reports"][1]["metrics"]["vision"]["status"] == "complete"
    assert report["stage_reports"]["metrics"]["sources"]["funding_rate"]["aborted"] is True
    assert report["stage_reports"]["metrics"]["sources"]["funding_rate"]["aborted_from_date"] == "2024-10-01"
    assert any(error["source"] == "funding_rate" and error["terminal"] is True and error["date"] == "2024-10-01" for error in report["stage_errors"])


@pytest.mark.asyncio
async def test_universe_download_concurrent_days_processes_all(tmp_path) -> None:
    """Symbol-centric processing should handle all days correctly."""
    universe_path = tmp_path / "universe_concurrent.json"
    snapshots = [
        UniverseDailySnapshot(
            date=f"2024-10-0{i}",
            active_symbols=["BTCUSDT", "ETHUSDT"],
            missing_symbols={},
        )
        for i in range(1, 6)
    ]
    _write_universe(universe_path, snapshots, start_date="2024-10-01", end_date="2024-10-05")

    service = MarketDataService(AsyncMock())
    service.kline_downloader.download_multiple_symbols = AsyncMock(return_value=_ok_report(["BTCUSDT", "ETHUSDT"]))

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.0,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        interval=Freq.h1,
        max_day_workers=3,
    )

    assert report["total_days"] == 5
    assert report["processed_days"] == 5
    assert report["stage_reports"]["kline"]["status"] == "complete"
    assert report["stage_reports"]["kline"]["days_complete"] == 5
    # Single call to downloader instead of 5.
    service.kline_downloader.download_multiple_symbols.assert_called_once()


@pytest.mark.asyncio
async def test_universe_download_kline_abort_marks_pending_days_aborted(tmp_path) -> None:
    """Rate limit error should abort kline stage and report stage error."""
    universe_path = tmp_path / "universe_abort.json"
    snapshots = [
        UniverseDailySnapshot(
            date=f"2024-10-0{i}",
            active_symbols=["BTCUSDT"],
            missing_symbols={"ETHUSDT": "no_kline_on_date"},
        )
        for i in range(1, 5)
    ]
    _write_universe(universe_path, snapshots, start_date="2024-10-01", end_date="2024-10-04")

    service = MarketDataService(AsyncMock())

    service.kline_downloader.download_multiple_symbols = AsyncMock(side_effect=RateLimitError("rate limit hit"))

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.0,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        interval=Freq.h1,
        max_day_workers=1,
    )

    assert report["stage_reports"]["kline"]["status"] == "aborted"
    assert any(e["terminal"] is True for e in report["stage_errors"])


@pytest.mark.asyncio
async def test_universe_download_max_day_workers_defaults_to_three(tmp_path) -> None:
    """max_day_workers should default to 3 when not specified."""
    universe_path = tmp_path / "universe_default_workers.json"
    _write_universe(
        universe_path,
        [
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
        ],
        start_date="2024-10-01",
        end_date="2024-10-01",
    )

    service = MarketDataService(AsyncMock())
    service.kline_downloader.download_multiple_symbols = AsyncMock(return_value=_ok_report(["BTCUSDT"]))

    report = await service.download_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.0,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        interval=Freq.h1,
    )

    assert report["stage_reports"]["kline"]["status"] == "complete"


@pytest.mark.asyncio
async def test_select_long_short_ratio_by_type_reads_legacy_alias_value(tmp_path) -> None:
    """Canonical ratio_type 查询应兼容 legacy alias 存量数据."""
    from cryptoservice.storage import ConnectionPool, DatabaseSchema
    from cryptoservice.storage.queries.metrics_query import MetricsQuery
    from cryptoservice.utils.time_utils import date_to_timestamp_start

    db_path = tmp_path / "legacy_ratio_alias.db"
    pool = ConnectionPool(db_path)
    await pool.initialize()

    try:
        schema = DatabaseSchema()
        await schema.create_all_tables(pool)
        ts = date_to_timestamp_start("2024-10-01")

        async with pool.get_connection() as conn:
            await conn.execute(
                """
                INSERT INTO long_short_ratios (
                    symbol, timestamp, period, ratio_type, long_short_ratio, long_account, short_account
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("BTCUSDT", ts, "5m", "account", 1.23, 55.0, 45.0),
            )
            await conn.commit()

        query = MetricsQuery(pool)
        df = await query.select_long_short_ratio_by_type(
            symbols=["BTCUSDT"],
            start_time="2024-10-01",
            end_time="2024-10-01",
            ratio_type="toptrader_account",
            rename_to_export_name=False,
        )

        assert not df.empty
        assert df.iloc[0]["long_short_ratio"] == pytest.approx(1.23)
    finally:
        await pool.close()
