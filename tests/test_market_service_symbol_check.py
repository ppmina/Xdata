"""MarketDataService symbol existence check tests."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

import cryptoservice.services.market_service as market_service_module
from cryptoservice.exceptions import MarketDataFetchError
from cryptoservice.services import MarketDataService


@pytest.mark.asyncio
async def test_check_symbol_exists_on_date_timeout_raises_in_strict_mode(monkeypatch) -> None:
    """Strict mode should fail fast when one symbol check stalls."""
    service = MarketDataService(AsyncMock())

    async def slow_klines(**kwargs):
        await asyncio.sleep(0.05)
        return []

    service.client.futures_klines = AsyncMock(side_effect=slow_klines)
    monkeypatch.setattr(market_service_module, "SYMBOL_CHECK_TIMEOUT_SECONDS", 0.001)

    with pytest.raises(MarketDataFetchError, match="Timed out checking symbol BTCUSDT on 2024-10-01"):
        await service.check_symbol_exists_on_date("BTCUSDT", "2024-10-01", strict=True)


@pytest.mark.asyncio
async def test_check_symbol_full_day_available_on_date_true_when_first_bar_starts_at_midnight() -> None:
    """Full-day check should pass when first 1m bar starts at day start."""
    service = MarketDataService(AsyncMock())
    day_start = int(service._date_to_timestamp_start("2024-10-01"))
    service.client.futures_klines = AsyncMock(return_value=[[day_start]])

    assert await service.check_symbol_full_day_available_on_date("BTCUSDT", "2024-10-01", strict=True) is True


@pytest.mark.asyncio
async def test_check_symbol_full_day_available_on_date_false_when_first_bar_is_late() -> None:
    """Full-day check should fail when first 1m bar is after day start."""
    service = MarketDataService(AsyncMock())
    day_start = int(service._date_to_timestamp_start("2024-10-01"))
    service.client.futures_klines = AsyncMock(return_value=[[day_start + 60_000]])

    assert await service.check_symbol_full_day_available_on_date("BTCUSDT", "2024-10-01", strict=True) is False


@pytest.mark.asyncio
async def test_check_symbol_full_day_available_on_date_timeout_raises_in_strict_mode(monkeypatch) -> None:
    """Strict mode should fail fast when full-day check stalls."""
    service = MarketDataService(AsyncMock())

    async def slow_klines(**kwargs):
        await asyncio.sleep(0.05)
        return []

    service.client.futures_klines = AsyncMock(side_effect=slow_klines)
    monkeypatch.setattr(market_service_module, "SYMBOL_CHECK_TIMEOUT_SECONDS", 0.001)

    with pytest.raises(MarketDataFetchError, match="Timed out checking full-day coverage for symbol BTCUSDT on 2024-10-01"):
        await service.check_symbol_full_day_available_on_date("BTCUSDT", "2024-10-01", strict=True)
