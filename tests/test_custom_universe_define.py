"""自定义 universe 定义测试."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from cryptoservice.models import UniverseDefinition
from cryptoservice.services import MarketDataService


@pytest.mark.asyncio
async def test_define_custom_universe_with_daily_check_filters_missing_symbols(tmp_path):
    """自定义定义应按日级存在性过滤缺失 symbol."""
    service = MarketDataService(AsyncMock())
    service.get_perpetual_symbols = AsyncMock(return_value=["BTCUSDT", "ETHUSDT"])

    async def fake_check(symbol: str, start_date: str, end_date: str, request_delay: float = 0.0) -> list[str]:
        if symbol == "ETHUSDT":
            return ["2024-10-02"]
        return []

    service.universe_manager._check_symbol_daily_existence = AsyncMock(side_effect=fake_check)

    output_path = tmp_path / "custom_universe.json"
    universe_def = await service.define_custom_universe_with_daily_check(
        symbols=["BTCUSDT", "ETHUSDT"],
        start_date="2024-10-01",
        end_date="2024-10-03",
        output_path=output_path,
        daily_check_workers=2,
    )

    snapshot = universe_def.snapshots[0]
    assert snapshot.symbols == ["BTCUSDT"]
    assert snapshot.mean_daily_amounts == {"BTCUSDT": 0.0}

    metadata = snapshot.metadata or {}
    assert metadata["daily_existence_check"]["removed_symbols"] == ["ETHUSDT"]
    assert metadata["daily_existence_check"]["missing_by_date"] == {"2024-10-02": ["ETHUSDT"]}

    assert output_path.exists()
    loaded = UniverseDefinition.load_from_file(output_path)
    assert loaded.snapshots[0].symbols == ["BTCUSDT"]


@pytest.mark.asyncio
async def test_define_custom_universe_with_daily_check_records_skipped_symbols(tmp_path):
    """自定义定义应记录无效 symbol 的跳过信息."""
    service = MarketDataService(AsyncMock())
    service.get_perpetual_symbols = AsyncMock(return_value=["BTCUSDT"])
    service.universe_manager._check_symbol_daily_existence = AsyncMock(return_value=[])

    output_path = tmp_path / "custom_universe_skipped.json"
    universe_def = await service.define_custom_universe_with_daily_check(
        symbols=["BTCUSDT", "BADSYMBOL"],
        start_date="2024-10-01",
        end_date="2024-10-03",
        output_path=output_path,
    )

    snapshot = universe_def.snapshots[0]
    metadata = snapshot.metadata or {}
    assert metadata["valid_symbols"] == ["BTCUSDT"]
    assert metadata["skipped_symbols"] == ["BADSYMBOL"]

    assert Path(output_path).exists()
