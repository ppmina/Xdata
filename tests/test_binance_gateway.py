"""Tests for Binance gateway abstractions."""

from unittest.mock import MagicMock

import pytest

from cryptoservice.client.gateway import OfficialBinanceGateway, extract_api_error_context


@pytest.mark.asyncio
async def test_official_gateway_forwards_futures_klines_params() -> None:
    """Gateway passes mapped params to official futures endpoint."""

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def data(self):
            return self._payload

    client = MagicMock()
    client.kline_candlestick_data.return_value = FakeResponse([[1, "10"]])
    gateway = OfficialBinanceGateway(client)

    result = await gateway.futures_klines(symbol="BTCUSDT", interval="1h", startTime=1, limit=2)

    assert result == [[1, "10"]]
    client.kline_candlestick_data.assert_called_once()


@pytest.mark.asyncio
async def test_official_gateway_closes_underlying_session() -> None:
    """Gateway closes wrapped REST session when available."""
    session = MagicMock()
    client = MagicMock()
    client._session = session
    gateway = OfficialBinanceGateway(client)

    await gateway.close_connection()

    session.close.assert_called_once()


@pytest.mark.asyncio
async def test_official_gateway_historical_generator_retries_rate_limit(monkeypatch) -> None:
    """Historical kline generator retries transient Binance rate limits."""

    class FakeRateLimitError(Exception):
        status_code = 429
        code = -1003
        message = "Too many requests"

    async def _fast_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("cryptoservice.client.gateway.asyncio.sleep", _fast_sleep)

    client = MagicMock()
    gateway = OfficialBinanceGateway(client)
    calls = {"count": 0}

    async def _fake_futures_klines(**kwargs):
        del kwargs
        calls["count"] += 1
        if calls["count"] == 1:
            raise FakeRateLimitError("(-1003, 'Too many requests')")
        return [[1730000000000, "100.0"]]

    gateway.futures_klines = _fake_futures_klines

    rows = [
        row
        async for row in gateway.get_historical_klines_generator(
            symbol="BTCUSDT",
            interval="1m",
            start_str="1730000000000",
            end_str="1730000100000",
            limit=1500,
            klines_type=None,
        )
    ]

    assert calls["count"] == 2
    assert rows == [[1730000000000, "100.0"]]


def test_extract_api_error_context_reads_common_fields() -> None:
    """Error extractor normalizes status, request and response body."""

    class FakeResponse:
        reason = "Forbidden"
        method = "GET"
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        _body = b"<html>forbidden</html>"

    class FakeError(Exception):
        status_code = 403
        code = -1003
        message = "Too many requests"
        response = FakeResponse()

    context = extract_api_error_context(FakeError("boom"))

    assert context is not None
    assert context.status_code == 403
    assert context.code == -1003
    assert context.reason == "Forbidden"
    assert context.method == "GET"
    assert context.response_body == "<html>forbidden</html>"
