"""Service 导出能力测试."""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from cryptoservice.models import Freq, UniverseConfig, UniverseDefinition, UniverseSnapshot
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

        days = pd.date_range(start=start_time, end=end_time, freq="D", tz="UTC")
        for idx, day in enumerate(days):
            date_key = day.strftime("%Y%m%d")
            if idx == 0 and symbols:
                payload[date_key] = [symbols[0]]

        with open(symbol_file, "w", encoding="utf-8") as fp:
            json.dump(payload, fp, ensure_ascii=False, indent=2)


class _FakeDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        self.numpy_exporter = _FakeNumpyExporter()

    async def initialize(self):
        return None

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_export_universe_definition_writes_report_and_merges_missing(monkeypatch, tmp_path):
    """_export_universe_definition 应产出 report.json 并正确合并缺失来源."""
    service = MarketDataService(AsyncMock())

    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-02",
        t1_months=1,
        t2_months=1,
        t3_months=1,
        delay_days=7,
        quote_asset="USDT",
        top_k=2,
    )
    snapshot = UniverseSnapshot.create_with_dates_and_timestamps(
        usage_t1_start="2024-01-01",
        usage_t1_end="2024-01-02",
        calculated_t1_start="2023-12-01",
        calculated_t1_end="2023-12-31",
        symbols=["BTCUSDT", "ETHUSDT"],
        mean_daily_amounts={"BTCUSDT": 100.0, "ETHUSDT": 50.0},
        metadata={
            "daily_existence_check": {
                "missing_by_date": {
                    "2024-01-01": ["ETHUSDT"],
                }
            }
        },
    )
    universe_def = UniverseDefinition(
        config=config,
        snapshots=[snapshot],
        creation_time=datetime.now(tz=UTC),
        description="test",
    )

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
        field_mapping=None,
    )

    report_path = Path(report["report_path"])
    assert report_path.exists()

    assert report["define_missing"]["2024-01-01"] == ["ETHUSDT"]
    assert "2024-01-01" in report["export_missing"]
    assert "2024-01-02" in report["export_missing"]
    assert set(report["merged_missing"]["2024-01-01"]) == {"ETHUSDT"}
    assert set(report["merged_missing"]["2024-01-02"]) == {"BTCUSDT", "ETHUSDT"}


@pytest.mark.asyncio
async def test_export_custom_universe_data_returns_report(monkeypatch, tmp_path):
    """export_custom_universe_data 应返回报告并包含 symbols 划分信息."""
    service = MarketDataService(AsyncMock())

    service.get_perpetual_symbols = AsyncMock(return_value=["BTCUSDT", "ETHUSDT"])

    import cryptoservice.services.market_service as market_service_module

    monkeypatch.setattr(market_service_module, "Database", _FakeDatabase)

    report = await service.export_custom_universe_data(
        symbols=["btcusdt", "BADSYMBOL", "ETHUSDT"],
        start_date="2024-01-01",
        end_date="2024-01-02",
        db_path=tmp_path / "market.db",
        export_base_path=tmp_path / "exports",
        source_freq=Freq.h1,
        export_freq=Freq.h1,
        include_klines=True,
        include_metrics=False,
    )

    assert report["requested_symbols"] == 3
    assert report["normalized_symbols"] == ["BTCUSDT", "BADSYMBOL", "ETHUSDT"]
    assert report["valid_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert report["skipped_symbols"] == ["BADSYMBOL"]
    assert Path(report["universe_file"]).exists()
    assert Path(report["report_path"]).exists()
    assert Path(report["output_path"]).exists()
