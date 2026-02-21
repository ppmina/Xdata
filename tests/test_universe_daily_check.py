"""Universe 日级存在性校验测试."""

from unittest.mock import AsyncMock, Mock

import pytest

from cryptoservice.models import UniverseSnapshot
from cryptoservice.services.processors import UniverseManager


@pytest.mark.asyncio
async def test_validate_snapshot_daily_existence_filters_missing_symbols():
    """存在缺失日期时，应过滤 symbol 并写入 metadata."""
    manager = UniverseManager(Mock())

    snapshot = UniverseSnapshot.create_with_dates_and_timestamps(
        usage_t1_start="2024-01-01",
        usage_t1_end="2024-01-03",
        calculated_t1_start="2023-12-01",
        calculated_t1_end="2023-12-31",
        symbols=["BTCUSDT", "ETHUSDT"],
        mean_daily_amounts={"BTCUSDT": 100.0, "ETHUSDT": 50.0},
    )

    async def fake_check(symbol: str, start_date: str, end_date: str, request_delay: float = 0.0) -> list[str]:
        if symbol == "ETHUSDT":
            return ["2024-01-02"]
        return []

    manager._check_symbol_daily_existence = AsyncMock(side_effect=fake_check)

    summary = await manager._validate_snapshot_daily_existence(snapshot, daily_check_workers=2)

    assert snapshot.symbols == ["BTCUSDT"]
    assert snapshot.mean_daily_amounts == {"BTCUSDT": 100.0}
    assert summary["removed_symbols"] == ["ETHUSDT"]
    assert summary["missing_by_date"] == {"2024-01-02": ["ETHUSDT"]}

    metadata = snapshot.metadata or {}
    check_data = metadata.get("daily_existence_check", {})
    assert check_data.get("removed_symbols") == ["ETHUSDT"]
    assert check_data.get("missing_by_symbol") == {"ETHUSDT": ["2024-01-02"]}


@pytest.mark.asyncio
async def test_validate_snapshot_daily_existence_keeps_symbols_when_no_missing():
    """无缺失日期时，symbols 与 mean_daily_amounts 应保持不变."""
    manager = UniverseManager(Mock())

    snapshot = UniverseSnapshot.create_with_dates_and_timestamps(
        usage_t1_start="2024-02-01",
        usage_t1_end="2024-02-02",
        calculated_t1_start="2024-01-01",
        calculated_t1_end="2024-01-31",
        symbols=["BTCUSDT", "ETHUSDT"],
        mean_daily_amounts={"BTCUSDT": 100.0, "ETHUSDT": 50.0},
    )

    manager._check_symbol_daily_existence = AsyncMock(return_value=[])

    summary = await manager._validate_snapshot_daily_existence(snapshot)

    assert snapshot.symbols == ["BTCUSDT", "ETHUSDT"]
    assert snapshot.mean_daily_amounts == {"BTCUSDT": 100.0, "ETHUSDT": 50.0}
    assert summary["removed_symbols"] == []
    assert summary["missing_by_date"] == {}

    metadata = snapshot.metadata or {}
    check_data = metadata.get("daily_existence_check", {})
    assert check_data.get("removed_symbols") == []
    assert check_data.get("missing_by_date") == {}
