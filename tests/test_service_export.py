"""Service export v2 tests."""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from cryptoservice.models import Freq
from cryptoservice.models.universe import UniverseDailySnapshot, UniverseDefinition
from cryptoservice.services import MarketDataService


class _FakeNumpyExporter:
    async def export_combined_data(
        self,
        symbols,
        start_time,
        end_time,
        source_freq,
        export_freq,
        output_path,
        include_klines,
        include_metrics,
        metrics_config,
        field_mapping,
    ):
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        symbol_file = output_dir / "univ_dct2.json"
        payload = {}
        if symbol_file.exists():
            with open(symbol_file, encoding="utf-8") as fp:
                payload = json.load(fp)

        date_key = start_time.replace("-", "")
        payload[date_key] = [symbols[0]] if symbols else []

        with open(symbol_file, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)

        if include_metrics:
            dropped_symbol_days = [
                {
                    "symbol": symbol,
                    "date": start_time,
                    "reason": "missing_required_metrics_after_asof",
                    "missing_columns": ["funding_rate"],
                }
                for symbol in symbols[1:]
            ]
            return {
                "day_status": "exported",
                "metrics_missing_coverage": {
                    "funding_rate": {
                        "metric_family": "funding_rate",
                        "missing_count": 1,
                        "total_count": 2,
                        "missing_ratio": 0.5,
                        "lookback_days": 3,
                        "tolerance_ms": 172800000,
                    }
                },
                "strict_metrics_filter": {
                    "mode": (metrics_config or {}).get("reliability_policy", {}).get("mode", "strict_100"),
                    "coverage_scope": "all_enabled",
                    "drop_unit": "symbol_day",
                    "empty_day_behavior": "skip",
                    "required_columns": ["funding_rate"],
                    "kept_symbol_days": [{"symbol": symbols[0], "date": start_time}] if symbols else [],
                    "dropped_symbol_days": dropped_symbol_days,
                    "drop_reason_counts": {
                        "missing_required_metrics_after_asof": len(dropped_symbol_days)
                    }
                    if dropped_symbol_days
                    else {},
                    "skipped": False,
                    "skip_reason": None,
                },
            }
        return {
            "day_status": "exported",
            "metrics_missing_coverage": {},
            "strict_metrics_filter": {
                "mode": (metrics_config or {}).get("reliability_policy", {}).get("mode", "strict_100"),
                "coverage_scope": "all_enabled",
                "drop_unit": "symbol_day",
                "empty_day_behavior": "skip",
                "required_columns": [],
                "kept_symbol_days": [{"symbol": symbol, "date": start_time} for symbol in symbols],
                "dropped_symbol_days": [],
                "drop_reason_counts": {},
                "skipped": False,
                "skip_reason": None,
            },
        }


class _FakeDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        self.numpy_exporter = _FakeNumpyExporter()

    async def initialize(self):
        return None

    async def close(self):
        return None


def _build_universe() -> UniverseDefinition:
    return UniverseDefinition(
        schema_version="2.0",
        requested_symbols=["BTCUSDT", "ETHUSDT"],
        start_date="2024-01-01",
        end_date="2024-01-02",
        daily_snapshots=[
            UniverseDailySnapshot(
                date="2024-01-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-01-02",
                active_symbols=["BTCUSDT", "ETHUSDT"],
                missing_symbols={},
            ),
        ],
        created_at=datetime.now(tz=UTC),
        description="test",
    )


@pytest.mark.asyncio
async def test_export_universe_definition_writes_report_and_merges_missing(monkeypatch, tmp_path):
    """Export report should merge define-time and export-time missing maps."""
    service = MarketDataService(AsyncMock())
    universe_def = _build_universe()

    import cryptoservice.services.market_service as market_service_module

    monkeypatch.setattr(market_service_module, "Database", _FakeDatabase)

    report = await service._export_universe_definition(
        universe_def=universe_def,
        db_path=tmp_path / "market.db",
        output_path=tmp_path / "exports",
        source_freq=Freq.h1,
        export_freq=Freq.h1,
        include_klines=True,
        include_metrics=False,
        metrics_config=None,
        metrics_reliability="strict_100",
        field_mapping=None,
        universe_file=str(tmp_path / "universe.json"),
    )

    assert Path(report["report_path"]).exists()
    assert report["define_missing"]["2024-01-01"] == {"ETHUSDT": "no_kline_on_date"}
    assert report["export_missing"]["2024-01-02"] == ["ETHUSDT"]
    assert report["merged_missing"]["2024-01-01"]["ETHUSDT"] == "no_kline_on_date"
    assert report["merged_missing"]["2024-01-02"]["ETHUSDT"] == "missing_in_export"


@pytest.mark.asyncio
async def test_export_universe_data_reads_v2_file(monkeypatch, tmp_path):
    """Public export API should read universe file and produce report."""
    service = MarketDataService(AsyncMock())
    universe = _build_universe()

    universe_path = tmp_path / "universe.json"
    universe.save_to_file(universe_path)
    before_payload = universe_path.read_text(encoding="utf-8")

    import cryptoservice.services.market_service as market_service_module

    monkeypatch.setattr(market_service_module, "Database", _FakeDatabase)

    report = await service.export_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        export_base_path=tmp_path / "exports",
        source_freq=Freq.h1,
        export_freq=Freq.h1,
        include_klines=True,
        include_metrics=False,
    )

    assert "output_path" not in report
    assert "db_path" not in report
    assert "universe_file" not in report
    assert "source_freq" not in report
    assert "export_freq" not in report
    assert Path(report["report_path"]).exists()
    after_payload = universe_path.read_text(encoding="utf-8")
    assert after_payload == before_payload


@pytest.mark.asyncio
async def test_export_universe_data_applies_date_override_and_updates_report(monkeypatch, tmp_path):
    """Export should process subset dates and reflect effective range in report/path."""
    service = MarketDataService(AsyncMock())
    universe = _build_universe()

    universe_path = tmp_path / "universe_override.json"
    universe.save_to_file(universe_path)
    before_payload = universe_path.read_text(encoding="utf-8")

    import cryptoservice.services.market_service as market_service_module

    monkeypatch.setattr(market_service_module, "Database", _FakeDatabase)

    report = await service.export_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        export_base_path=tmp_path / "exports",
        source_freq=Freq.h1,
        export_freq=Freq.h1,
        include_klines=True,
        include_metrics=False,
        start_date="2024-01-02",
        end_date="2024-01-02",
    )

    report_path = Path(report["report_path"])
    assert report_path.exists()
    assert "univ_2024-01-02_2024-01-02_2" in report_path.as_posix()
    assert report["total_days"] == 1
    assert report["date_range"]["requested_start_date"] == "2024-01-01"
    assert report["date_range"]["requested_end_date"] == "2024-01-02"
    assert report["date_range"]["effective_start_date"] == "2024-01-02"
    assert report["date_range"]["effective_end_date"] == "2024-01-02"
    assert report["export_context"]["override_applied"] is True
    assert report["export_context"]["override_start_date"] == "2024-01-02"
    assert report["export_context"]["override_end_date"] == "2024-01-02"

    after_payload = universe_path.read_text(encoding="utf-8")
    assert after_payload == before_payload


@pytest.mark.asyncio
async def test_export_universe_data_partial_override_fills_missing_bound(monkeypatch, tmp_path):
    """Export should fill omitted bound from universe range."""
    service = MarketDataService(AsyncMock())
    universe = _build_universe()

    universe_path = tmp_path / "universe_partial.json"
    universe.save_to_file(universe_path)

    import cryptoservice.services.market_service as market_service_module

    monkeypatch.setattr(market_service_module, "Database", _FakeDatabase)

    report = await service.export_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        export_base_path=tmp_path / "exports",
        source_freq=Freq.h1,
        export_freq=Freq.h1,
        include_klines=True,
        include_metrics=False,
        end_date="2024-01-01",
    )

    assert report["date_range"]["effective_start_date"] == "2024-01-01"
    assert report["date_range"]["effective_end_date"] == "2024-01-01"
    assert report["export_context"]["override_start_date"] is None
    assert report["export_context"]["override_end_date"] == "2024-01-01"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("start_date", "end_date", "pattern"),
    [
        ("2023-12-31", "2024-01-01", "within universe range"),
        ("2024-01-02", "2024-01-01", "must be <= end_date"),
    ],
)
async def test_export_universe_data_rejects_invalid_override_range(monkeypatch, tmp_path, start_date, end_date, pattern):
    """Export should fail fast for out-of-range or reversed override windows."""
    service = MarketDataService(AsyncMock())
    universe = _build_universe()
    universe_path = tmp_path / "universe_invalid_range.json"
    universe.save_to_file(universe_path)

    import cryptoservice.services.market_service as market_service_module

    monkeypatch.setattr(market_service_module, "Database", _FakeDatabase)

    with pytest.raises(ValueError, match=pattern):
        await service.export_universe_data(
            universe_file=universe_path,
            db_path=tmp_path / "market.db",
            export_base_path=tmp_path / "exports",
            source_freq=Freq.h1,
            export_freq=Freq.h1,
            include_klines=True,
            include_metrics=False,
            start_date=start_date,
            end_date=end_date,
        )


@pytest.mark.asyncio
async def test_export_universe_data_includes_metrics_missing_coverage(monkeypatch, tmp_path):
    """Report should contain per-day metrics missing coverage when exporter returns it."""
    service = MarketDataService(AsyncMock())
    universe = _build_universe()
    universe_path = tmp_path / "universe_metrics_coverage.json"
    universe.save_to_file(universe_path)

    import cryptoservice.services.market_service as market_service_module

    monkeypatch.setattr(market_service_module, "Database", _FakeDatabase)

    report = await service.export_universe_data(
        universe_file=universe_path,
        db_path=tmp_path / "market.db",
        export_base_path=tmp_path / "exports",
        source_freq=Freq.h1,
        export_freq=Freq.h1,
        include_klines=True,
        include_metrics=True,
    )

    assert "metrics_missing_coverage" in report
    assert "2024-01-01" in report["metrics_missing_coverage"]
    assert report["metrics_missing_coverage"]["2024-01-01"]["funding_rate"]["missing_ratio"] == 0.5
    assert "metrics_strict_exclusions" in report
    assert "2024-01-02" in report["metrics_strict_exclusions"]
    assert report["stats"]["metrics_strict_exclusion_date_count"] == 1
    assert report["stats"]["metrics_strict_dropped_symbol_day_count"] == 1
    assert report["export_context"]["metrics_reliability"] == "strict_100"
