"""Universe manager v2 daily classification tests."""

from unittest.mock import AsyncMock, Mock

import pytest

from cryptoservice.services.processors import UniverseManager


@pytest.mark.asyncio
async def test_classify_symbols_for_date_builds_partition() -> None:
    """Classification should split requested symbols into active and missing."""
    service = Mock()

    async def fake_full_day(symbol: str, date: str, strict: bool = False) -> bool:
        return symbol == "BTCUSDT"

    async def fake_exists(symbol: str, date: str, strict: bool = False) -> bool:
        return symbol in {"BTCUSDT", "ETHUSDT"}

    service.check_symbol_full_day_available_on_date = AsyncMock(side_effect=fake_full_day)
    service.check_symbol_exists_on_date = AsyncMock(side_effect=fake_exists)
    manager = UniverseManager(service)

    snapshot = await manager._classify_symbols_for_date(
        requested_symbols=["BTCUSDT", "ETHUSDT", "BADSYMBOL"],
        valid_symbol_set={"BTCUSDT", "ETHUSDT"},
        target_date="2024-01-01",
        daily_check_workers=3,
        daily_check_request_delay=0.0,
    )

    assert snapshot.active_symbols == ["BTCUSDT"]
    assert snapshot.missing_symbols == {
        "ETHUSDT": "not_full_day_on_date",
        "BADSYMBOL": "not_in_current_trading_list",
    }


@pytest.mark.asyncio
async def test_define_universe_fails_without_file_write_on_api_error(tmp_path) -> None:
    """Define should abort and keep output absent when API checks fail."""
    service = Mock()
    service._normalize_symbols = staticmethod(lambda symbols: [symbol.upper() for symbol in symbols])
    service.get_perpetual_symbols = AsyncMock(return_value=["BTCUSDT"])
    service.check_symbol_full_day_available_on_date = AsyncMock(side_effect=RuntimeError("network failure"))
    service.check_symbol_exists_on_date = AsyncMock()

    manager = UniverseManager(service)
    output_path = tmp_path / "universe.json"

    with pytest.raises(RuntimeError, match="network failure"):
        await manager.define_universe(
            symbols=["BTCUSDT"],
            start_date="2024-01-01",
            end_date="2024-01-01",
            output_path=output_path,
        )

    assert not output_path.exists()


@pytest.mark.asyncio
async def test_define_universe_fails_without_file_write_on_symbol_list_error(tmp_path) -> None:
    """Define should abort and keep output absent when symbol list query fails."""
    service = Mock()
    service._normalize_symbols = staticmethod(lambda symbols: [symbol.upper() for symbol in symbols])
    service.get_perpetual_symbols = AsyncMock(side_effect=RuntimeError("symbol endpoint down"))
    service.check_symbol_full_day_available_on_date = AsyncMock()
    service.check_symbol_exists_on_date = AsyncMock()

    manager = UniverseManager(service)
    output_path = tmp_path / "universe.json"

    with pytest.raises(RuntimeError, match="symbol endpoint down"):
        await manager.define_universe(
            symbols=["BTCUSDT"],
            start_date="2024-01-01",
            end_date="2024-01-01",
            output_path=output_path,
        )

    assert not output_path.exists()
