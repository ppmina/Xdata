"""基本功能测试.

验证核心模块的基本功能是否正常工作，包括模型序列化、枚举等基础功能。
"""

from datetime import datetime
from decimal import Decimal

import pytest

from cryptoservice.models.enums import Freq, HistoricalKlinesType, SortBy
from cryptoservice.models.market_ticker import (
    DailyMarketTicker,
    FuturesKlineTicker,
    SpotKlineTicker,
    SymbolTicker,
)
from cryptoservice.models.universe import (
    UniverseConfig,
    UniverseDefinition,
    UniverseSnapshot,
)


def test_universe_config():
    """测试UniverseConfig基本功能."""
    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=10,
        delay_days=7,
        quote_asset="USDT",
    )

    assert config.start_date == "2024-01-01"
    assert config.end_date == "2024-01-31"
    assert config.top_k == 10

    # 测试序列化
    config_dict = config.to_dict()
    assert isinstance(config_dict, dict)
    assert config_dict["start_date"] == "2024-01-01"


def test_universe_snapshot():
    """测试UniverseSnapshot基本功能."""
    snapshot = UniverseSnapshot.create_with_inferred_periods(
        effective_date="2024-01-31",
        t1_months=1,
        symbols=["BTCUSDT", "ETHUSDT"],
        mean_daily_amounts={
            "BTCUSDT": 1000000.0,
            "ETHUSDT": 500000.0,
        },
    )

    assert snapshot.effective_date == "2024-01-31"
    assert len(snapshot.symbols) == 2
    assert "BTCUSDT" in snapshot.symbols
    assert snapshot.calculated_t1_start_ts is not None
    assert snapshot.calculated_t1_end_ts is not None

    # 测试周期信息
    period_info = snapshot.get_period_info()
    assert "calculated_t1_start" in period_info
    assert "calculated_t1_end" in period_info


def test_universe_definition():
    """测试UniverseDefinition基本功能."""
    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=5,
        delay_days=7,
        quote_asset="USDT",
    )

    snapshot = UniverseSnapshot.create_with_inferred_periods(
        effective_date="2024-01-31",
        t1_months=1,
        symbols=["BTCUSDT", "ETHUSDT", "ADAUSDT"],
        mean_daily_amounts={
            "BTCUSDT": 1000000.0,
            "ETHUSDT": 500000.0,
            "ADAUSDT": 200000.0,
        },
    )

    universe_def = UniverseDefinition(
        config=config,
        snapshots=[snapshot],
        creation_time=datetime.now(),
        description="Test universe",
    )

    assert len(universe_def.snapshots) == 1
    assert universe_def.config.top_k == 5
    assert universe_def.description == "Test universe"

    # 测试序列化和反序列化
    data_dict = universe_def.to_dict()
    assert isinstance(data_dict, dict)

    restored_universe = UniverseDefinition.from_dict(data_dict)
    assert restored_universe.config.top_k == universe_def.config.top_k
    assert len(restored_universe.snapshots) == len(universe_def.snapshots)


def test_universe_schema():
    """测试Universe schema功能."""
    schema = UniverseDefinition.get_schema()

    assert isinstance(schema, dict)
    assert "$schema" in schema
    assert "properties" in schema
    assert "config" in schema["properties"]
    assert "snapshots" in schema["properties"]

    # 测试示例数据
    example = UniverseDefinition.get_schema_example()
    assert isinstance(example, dict)
    assert "config" in example
    assert "snapshots" in example


def test_freq_enum():
    """测试Freq枚举."""
    assert Freq.h1.value == "1h"
    assert Freq.d1.value == "1d"
    assert Freq.m1.value == "1m"
    assert Freq.s1.value == "1s"
    assert Freq.m3.value == "3m"
    assert Freq.m5.value == "5m"
    assert Freq.m15.value == "15m"
    assert Freq.m30.value == "30m"
    assert Freq.h4.value == "4h"
    assert Freq.w1.value == "1w"
    assert Freq.M1.value == "1M"


def test_historical_klines_type_enum():
    """测试HistoricalKlinesType枚举."""
    # HistoricalKlinesType使用的是binance SDK的整数值
    assert HistoricalKlinesType.SPOT.value is not None
    assert HistoricalKlinesType.FUTURES.value is not None
    assert HistoricalKlinesType.FUTURES_COIN.value is not None


def test_sort_by_enum():
    """测试SortBy枚举."""
    assert SortBy.VOLUME.value == "volume"
    assert SortBy.PRICE_CHANGE.value == "price_change"
    assert SortBy.PRICE_CHANGE_PERCENT.value == "price_change_percent"
    assert SortBy.QUOTE_VOLUME.value == "quote_volume"


def test_file_operations(tmp_path):
    """测试文件操作."""
    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=3,
        delay_days=7,
        quote_asset="USDT",
    )

    snapshot = UniverseSnapshot.create_with_inferred_periods(
        effective_date="2024-01-31",
        t1_months=1,
        symbols=["BTCUSDT", "ETHUSDT"],
        mean_daily_amounts={
            "BTCUSDT": 1000000.0,
            "ETHUSDT": 500000.0,
        },
    )

    universe_def = UniverseDefinition(
        config=config,
        snapshots=[snapshot],
        creation_time=datetime.now(),
        description="Test file operations",
    )

    # 测试保存
    test_file = tmp_path / "test_universe.json"
    universe_def.save_to_file(test_file)
    assert test_file.exists()

    # 测试加载
    loaded_universe = UniverseDefinition.load_from_file(test_file)
    assert loaded_universe.config.top_k == universe_def.config.top_k
    assert loaded_universe.description == universe_def.description


def test_market_ticker_models():
    """测试市场行情模型."""
    # 测试SymbolTicker
    ticker_data = {"symbol": "BTCUSDT", "price": "50000.0"}
    symbol_ticker = SymbolTicker.from_binance_ticker(ticker_data)
    assert symbol_ticker.symbol == "BTCUSDT"
    assert symbol_ticker.last_price == Decimal("50000.0")

    # 测试序列化
    ticker_dict = symbol_ticker.to_dict()
    assert ticker_dict["symbol"] == "BTCUSDT"
    assert ticker_dict["last_price"] == "50000.0"


def test_daily_market_ticker():
    """测试DailyMarketTicker模型."""
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


def test_spot_market_ticker():
    """测试KlineMarketTicker模型."""
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
    ticker = SpotKlineTicker.from_binance_kline("BTCUSDT", kline_data)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.last_price == Decimal("50000.0")
    assert ticker.high_price == Decimal("51000.0")
    assert ticker.low_price == Decimal("48000.0")
    assert ticker.volume == Decimal("100.0")


def test_perpetual_market_ticker():
    """测试FuturesKlineTicker模型."""
    # 测试基本创建
    ticker = FuturesKlineTicker(
        symbol="BTCUSDT",
        last_price=Decimal("50000"),  # last_price等同于close_price
        open_time=1234567890000,  # 正确的参数名
        open_price=Decimal("49000"),
        high_price=Decimal("51000"),
        low_price=Decimal("48000"),
        close_price=Decimal("50000"),
        volume=Decimal("100"),
        close_time=1234567949999,  # 添加close_time参数
        quote_volume=Decimal("5000000"),
        trades_count=1000,
        taker_buy_volume=Decimal("50"),
        taker_buy_quote_volume=Decimal("2500000"),
    )

    assert ticker.symbol == "BTCUSDT"
    assert ticker.close_price == Decimal("50000")
    assert ticker.open_price == Decimal("49000")
    assert ticker.high_price == Decimal("51000")
    assert ticker.low_price == Decimal("48000")
    assert ticker.volume == Decimal("100")
    assert ticker.taker_buy_volume == Decimal("50")


if __name__ == "__main__":
    pytest.main([__file__])
