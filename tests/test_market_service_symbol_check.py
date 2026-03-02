"""MarketDataService symbol existence check tests."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

import cryptoservice.services.market_service as market_service_module
from cryptoservice.exceptions import MarketDataFetchError
from cryptoservice.services import MarketDataService


class _FakeAioHttpResponse:
    def __init__(self, *, payload: str, status: int = 200) -> None:
        self._payload = payload
        self._status = status

    async def __aenter__(self) -> _FakeAioHttpResponse:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def raise_for_status(self) -> None:
        if self._status >= 400:
            raise RuntimeError(f"http error status={self._status}")

    async def text(self) -> str:
        return self._payload


class _FakeAioHttpSession:
    def __init__(self, responses: list[_FakeAioHttpResponse], calls: list[dict[str, object]]) -> None:
        self._responses = responses
        self._calls = calls

    async def __aenter__(self) -> _FakeAioHttpSession:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def get(self, url: str, *, params: dict[str, str] | None = None) -> _FakeAioHttpResponse:
        self._calls.append({"url": url, "params": dict(params or {})})
        if not self._responses:
            raise AssertionError("No mocked responses left")
        return self._responses.pop(0)


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


@pytest.mark.asyncio
async def test_check_symbol_date_status_active_only_when_all_1m_points_exist() -> None:
    """Status should be active only with exact full-day 1m coverage."""
    service = MarketDataService(AsyncMock())
    day_start = int(service._date_to_timestamp_start("2024-10-01"))
    full_day_klines = [[day_start + i * 60_000] for i in range(24 * 60)]
    setattr(service.kline_downloader, "_handle_async_request_with_retry", AsyncMock(return_value=full_day_klines))

    status = await service._check_symbol_date_status("BTCUSDT", "2024-10-01", endpoint_max_workers=3)
    assert status == "active"


@pytest.mark.asyncio
async def test_check_symbol_date_status_marks_not_full_day_when_one_1m_point_missing() -> None:
    """Any missing minute should disqualify a symbol from active on that date."""
    service = MarketDataService(AsyncMock())
    day_start = int(service._date_to_timestamp_start("2024-10-01"))
    klines_with_gap = [[day_start + i * 60_000] for i in range(24 * 60) if i != 777]
    setattr(service.kline_downloader, "_handle_async_request_with_retry", AsyncMock(return_value=klines_with_gap))

    status = await service._check_symbol_date_status("BTCUSDT", "2024-10-01", endpoint_max_workers=3)
    assert status == "not_full_day_on_date"


@pytest.mark.asyncio
async def test_get_vision_kline_available_dates_uses_s3_listing_with_pagination(monkeypatch) -> None:
    """Vision listing should use S3 API with continuation token pagination."""
    service = MarketDataService(AsyncMock())
    calls: list[dict[str, object]] = []
    responses = [
        _FakeAioHttpResponse(
            payload=(
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                "<IsTruncated>true</IsTruncated>"
                "<NextContinuationToken>page-2</NextContinuationToken>"
                "<Contents><Key>data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-10-01.zip</Key></Contents>"
                "<Contents><Key>data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-10-01.zip.CHECKSUM</Key></Contents>"
                "</ListBucketResult>"
            )
        ),
        _FakeAioHttpResponse(
            payload=(
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                "<IsTruncated>false</IsTruncated>"
                "<Contents><Key>data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-10-02.zip</Key></Contents>"
                "<Contents><Key>data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-10-03.zip</Key></Contents>"
                "</ListBucketResult>"
            )
        ),
    ]

    monkeypatch.setattr(
        market_service_module.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: _FakeAioHttpSession(list(responses), calls),
    )

    dates = await service._get_vision_kline_available_dates(
        symbol="BTCUSDT",
        start_date="2024-10-01",
        end_date="2024-10-02",
        interval="1m",
    )

    assert dates == {"2024-10-01", "2024-10-02"}
    assert len(calls) == 2
    assert calls[0]["url"] == "https://s3.ap-northeast-1.amazonaws.com/data.binance.vision/"
    assert calls[0]["params"] == {
        "list-type": "2",
        "prefix": "data/futures/um/daily/klines/BTCUSDT/1m/",
        "max-keys": "1000",
    }
    assert calls[1]["params"] == {
        "list-type": "2",
        "prefix": "data/futures/um/daily/klines/BTCUSDT/1m/",
        "max-keys": "1000",
        "continuation-token": "page-2",
    }


@pytest.mark.asyncio
async def test_get_vision_kline_available_dates_raises_on_listing_error(monkeypatch) -> None:
    """Vision listing should surface fetch failures as MarketDataFetchError."""
    service = MarketDataService(AsyncMock())
    calls: list[dict[str, object]] = []
    responses = [_FakeAioHttpResponse(payload="boom", status=500)]

    monkeypatch.setattr(
        market_service_module.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: _FakeAioHttpSession(list(responses), calls),
    )

    with pytest.raises(MarketDataFetchError, match="Failed to list Vision kline files for BTCUSDT"):
        await service._get_vision_kline_available_dates(
            symbol="BTCUSDT",
            start_date="2024-10-01",
            end_date="2024-10-02",
            interval="1m",
        )
