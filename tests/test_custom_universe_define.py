"""Universe define v2 tests."""

import json
from unittest.mock import AsyncMock

import pytest

from cryptoservice.models import UniverseDefinition
from cryptoservice.services import MarketDataService


@pytest.mark.asyncio
async def test_define_universe_builds_daily_truth_table(tmp_path) -> None:
    """Define should generate one daily snapshot and strict partitions."""
    service = MarketDataService(AsyncMock())
    service.get_perpetual_symbols = AsyncMock(return_value=["BTCUSDT", "ETHUSDT"])

    async def fake_status(symbol: str, date: str, endpoint_max_workers: int = 5) -> str:
        if symbol == "ETHUSDT" and date == "2024-10-02":
            return "not_full_day_on_date"
        if symbol in {"BTCUSDT", "ETHUSDT"}:
            return "active"
        return "no_kline_on_date"

    service._check_symbol_date_status = AsyncMock(side_effect=fake_status)

    output_path = tmp_path / "universe.json"
    universe = await service.define_universe(
        symbols=["btcusdt", "ETHUSDT", "BADSYMBOL", "BTCUSDT"],
        start_date="2024-10-01",
        end_date="2024-10-03",
        output_path=output_path,
    )

    assert universe.requested_symbols == ["BTCUSDT", "ETHUSDT", "BADSYMBOL"]
    assert len(universe.daily_snapshots) == 3

    day1 = universe.get_snapshot_for_date("2024-10-01")
    assert day1 is not None
    assert day1.active_symbols == ["BTCUSDT", "ETHUSDT"]
    assert day1.missing_symbols == {"BADSYMBOL": "not_in_current_trading_list"}

    day2 = universe.get_snapshot_for_date("2024-10-02")
    assert day2 is not None
    assert day2.active_symbols == ["BTCUSDT"]
    assert day2.missing_symbols == {
        "BADSYMBOL": "not_in_current_trading_list",
        "ETHUSDT": "not_full_day_on_date",
    }

    assert output_path.exists()
    loaded = UniverseDefinition.load_from_file(output_path)
    assert loaded.requested_symbols == universe.requested_symbols


@pytest.mark.asyncio
async def test_define_universe_requires_force_to_overwrite(tmp_path) -> None:
    """Define should enforce immutable file semantics unless force=True."""
    service = MarketDataService(AsyncMock())
    service.get_perpetual_symbols = AsyncMock(return_value=["BTCUSDT"])
    service._check_symbol_date_status = AsyncMock(return_value="active")

    output_path = tmp_path / "universe.json"
    output_path.write_text(json.dumps({"existing": True}), encoding="utf-8")

    with pytest.raises(FileExistsError):
        await service.define_universe(
            symbols=["BTCUSDT"],
            start_date="2024-10-01",
            end_date="2024-10-01",
            output_path=output_path,
            force=False,
        )

    await service.define_universe(
        symbols=["BTCUSDT"],
        start_date="2024-10-01",
        end_date="2024-10-01",
        output_path=output_path,
        force=True,
    )

    loaded = UniverseDefinition.load_from_file(output_path)
    assert loaded.schema_version == "2.0"
