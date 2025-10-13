"""市场数据测试.

测试市场数据相关的服务、存储和工具类。
使用mock避免实际网络请求。
"""

import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from cryptoservice.models import (
    DailyMarketTicker,
    Freq,
    KlineMarketTicker,
    PerpetualMarketTicker,
    SymbolTicker,
)
from cryptoservice.storage import Database
from cryptoservice.utils import DataConverter

# ================= 模型测试 =================


def test_market_ticker_from_24h_ticker() -> None:
    """测试24小时行情数据解析."""
    ticker_24h = {
        "symbol": "BTCUSDT",
        "lastPrice": "50000.0",
        "priceChange": "1000.0",
        "priceChangePercent": "2.0",
        "volume": "100.0",
        "quoteVolume": "5000000.0",
        "weightedAvgPrice": "100.0",
        "prevClosePrice": "100.0",
        "bidPrice": "100.0",
        "askPrice": "100.0",
        "bidQty": "100.0",
        "askQty": "100.0",
        "openPrice": "100.0",
        "highPrice": "100.0",
        "lowPrice": "100.0",
        "openTime": 1234567890000,
        "closeTime": 1234567890000,
        "firstId": 1234567890000,
        "lastId": 1234567890000,
        "count": 100,
    }
    ticker = DailyMarketTicker.from_binance_ticker(ticker_24h)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.last_price == Decimal("50000.0")
    assert ticker.price_change == Decimal("1000.0")
    assert ticker.volume == Decimal("100.0")
    assert ticker.quote_volume == Decimal("5000000.0")


def test_market_ticker_from_kline() -> None:
    """测试K线数据解析."""
    kline_data = [
        1234567890000,  # open_time
        "49000.0",  # open
        "51000.0",  # high
        "48000.0",  # low
        "50000.0",  # close (last_price)
        "100.0",  # volume
        1234567890000,  # close_time
        "5000000.0",  # quote_volume
        1000,  # count
        "50.0",  # taker_buy_volume
        "2500000.0",  # taker_buy_quote_volume
        "0",  # ignore
    ]
    ticker = KlineMarketTicker.from_binance_kline("BTCUSDT", kline_data)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.last_price == Decimal("50000.0")
    assert ticker.high_price == Decimal("51000.0")
    assert ticker.low_price == Decimal("48000.0")
    assert ticker.volume == Decimal("100.0")


def test_market_ticker_to_dict() -> None:
    """测试转换为字典格式."""
    ticker_data = {"symbol": "BTCUSDT", "price": "50000.0"}
    ticker = SymbolTicker.from_binance_ticker(ticker_data)
    result = ticker.to_dict()

    assert result["symbol"] == "BTCUSDT"
    assert result["last_price"] == "50000.0"
    assert "volume" not in result
    assert "price_change" not in result  # 确保不存在的字段不会出现在结果中


# ================= 工具类测试 =================


def test_data_converter():
    """测试数据转换器."""
    converter = DataConverter()

    # 测试实际存在的方法
    assert hasattr(converter, "to_decimal")
    assert hasattr(converter, "format_timestamp")
    assert hasattr(converter, "format_market_data")

    # 测试转换方法
    decimal_val = converter.to_decimal("50000.5")
    assert str(decimal_val) == "50000.5"

    # 测试市场数据格式化
    test_data = {"price": "50000", "volume": "100", "priceChangePercent": "2.5"}
    formatted = converter.format_market_data(test_data)
    assert formatted["price"] == 50000.0
    assert formatted["volume"] == 100.0


# ================= 存储层测试 =================


@pytest_asyncio.fixture
async def temp_database():
    """创建临时数据库用于测试."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = Database(db_path)
    await db.initialize()

    yield db

    await db.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_database_initialization(temp_database):
    """测试数据库初始化."""
    db = temp_database

    # 验证数据库已初始化
    assert db._initialized
    assert db.pool is not None
    assert db.kline_store is not None
    assert db.kline_query is not None


@pytest.mark.asyncio
async def test_database_context_manager():
    """测试数据库上下文管理器."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        async with Database(db_path) as db:
            assert db._initialized
            assert db.pool is not None
    finally:
        Path(db_path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_kline_operations(temp_database):
    """测试K线数据操作."""
    db = temp_database

    # 创建测试K线数据
    test_klines = [
        PerpetualMarketTicker(
            symbol="BTCUSDT",
            open_time=1234567890000,
            open_price=Decimal("50000"),
            high_price=Decimal("51000"),
            low_price=Decimal("49000"),
            close_price=Decimal("50500"),
            volume=Decimal("100"),
            close_time=1234567949999,
            quote_volume=Decimal("5050000"),
            trades_count=1000,
            taker_buy_volume=Decimal("60"),
            taker_buy_quote_volume=Decimal("3030000"),
        )
    ]

    # 测试插入
    inserted_count = await db.insert_klines(test_klines, Freq.h1)
    assert inserted_count == 1

    # 测试查询
    df = await db.select_klines(["BTCUSDT"], "2009-02-13", "2009-02-14", Freq.h1)
    assert not df.empty
    assert len(df) == 1
    # 检查实际返回的列，可能没有symbol列，只有价格数据
    assert "open_price" in df.columns
    assert df.iloc[0]["open_price"] == 50000.0


# ================= 服务层测试 =================


@pytest.mark.asyncio
async def test_market_service_creation():
    """测试市场数据服务创建."""
    with patch("cryptoservice.client.BinanceClientFactory.create_async_client") as mock_create:
        # Mock async client
        mock_client = AsyncMock()
        mock_create.return_value = mock_client

        from cryptoservice.services import MarketDataService

        service = await MarketDataService.create("test_key", "test_secret")

        assert service is not None
        assert service.client is mock_client
        assert service.converter is not None
        assert hasattr(service, "kline_downloader")
        assert hasattr(service, "metrics_downloader")
        assert hasattr(service, "universe_manager")


@pytest.mark.asyncio
async def test_service_context_manager():
    """测试服务上下文管理器."""
    with (
        patch("cryptoservice.client.BinanceClientFactory.create_async_client") as mock_create,
        patch("cryptoservice.client.BinanceClientFactory.close_client") as mock_close,
    ):
        mock_client = AsyncMock()
        mock_create.return_value = mock_client

        from cryptoservice.services import MarketDataService

        async with await MarketDataService.create("test_key", "test_secret") as service:
            assert service is not None

        # 验证清理方法被调用
        mock_close.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__])
