from decimal import Decimal
from typing import Dict
from unittest.mock import Mock

import pytest

from crypto_data.models.market_data import MarketTicker
from crypto_data.services.market_data import MarketDataService


@pytest.fixture
def mock_ticker_data() -> Dict[str, str]:
    return {
        "symbol": "BTCUSDT",
        "priceChange": "100.0",
        "priceChangePercent": "1.0",
        "weightedAvgPrice": "50000.0",
        "prevClosePrice": "49900.0",
        "lastPrice": "50000.0",
        "lastQty": "1.0",
        "bidPrice": "49999.0",
        "askPrice": "50001.0",
        "openPrice": "49900.0",
        "highPrice": "50100.0",
        "lowPrice": "49800.0",
        "volume": "1000.0",
        "quoteVolume": "50000000.0",
    }


def test_market_ticker_from_binance(mock_ticker_data: Dict[str, str]) -> None:
    ticker = MarketTicker.from_binance_ticker(mock_ticker_data)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.last_price == Decimal("50000.0")


@pytest.fixture
def mock_client() -> Mock:
    client = Mock()
    client.get_ticker.return_value = [mock_ticker_data()]
    return client


def test_get_top_coins(mock_client: Mock) -> None:
    service = MarketDataService(mock_client)
    top_coins = service.get_top_coins(limit=1)
    assert len(top_coins) == 1
    assert top_coins[0].symbol == "BTCUSDT"
