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
