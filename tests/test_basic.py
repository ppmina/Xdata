"""Basic model and enum tests."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cryptoservice.models.enums import Freq, HistoricalKlinesType, SortBy
from cryptoservice.models.market_ticker import FuturesKlineTicker
from cryptoservice.models.universe import UniverseDailySnapshot, UniverseDefinition


def test_universe_daily_snapshot_roundtrip() -> None:
    """UniverseDailySnapshot should serialize and deserialize losslessly."""
    snapshot = UniverseDailySnapshot(
        date="2024-01-01",
        active_symbols=["btcusdt", "ETHUSDT"],
        missing_symbols={"solusdt": "no_kline_on_date"},
    )

    payload = snapshot.to_dict()
    restored = UniverseDailySnapshot.from_dict(payload)

    assert restored.date == "2024-01-01"
    assert restored.active_symbols == ["BTCUSDT", "ETHUSDT"]
    assert restored.missing_symbols == {"SOLUSDT": "no_kline_on_date"}


def test_universe_definition_roundtrip(tmp_path) -> None:
    """UniverseDefinition v2 should roundtrip through dict and file."""
    universe = UniverseDefinition(
        schema_version="2.0",
        requested_symbols=["BTCUSDT", "ETHUSDT"],
        start_date="2024-01-01",
        end_date="2024-01-02",
        daily_snapshots=[
            UniverseDailySnapshot(
                date="2024-01-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "no_kline_on_date"},
            ),
            UniverseDailySnapshot(
                date="2024-01-02",
                active_symbols=["BTCUSDT", "ETHUSDT"],
                missing_symbols={},
            ),
        ],
        created_at=datetime.now(tz=UTC),
        description="test",
    )

    assert universe.active_symbols_union == ["BTCUSDT", "ETHUSDT"]

    payload = universe.to_dict()
    restored = UniverseDefinition.from_dict(payload)
    assert restored.requested_symbols == ["BTCUSDT", "ETHUSDT"]
    assert len(restored.daily_snapshots) == 2

    target = tmp_path / "universe.json"
    universe.save_to_file(target)
    loaded = UniverseDefinition.load_from_file(target)
    assert loaded.schema_version == "2.0"
    assert loaded.start_date == "2024-01-01"
    assert loaded.end_date == "2024-01-02"


def test_universe_definition_rejects_v1_payload() -> None:
    """Legacy v1 payloads should fail with explicit error."""
    with pytest.raises(ValueError, match="legacy universe schema"):
        UniverseDefinition.from_dict(
            {
                "config": {"start_date": "2024-01-01"},
                "snapshots": [],
                "creation_time": "2024-01-01T00:00:00+00:00",
            }
        )


def test_freq_enum() -> None:
    """Freq enum values should stay stable."""
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


def test_historical_klines_type_enum() -> None:
    """HistoricalKlinesType should expose Binance SDK values."""
    assert HistoricalKlinesType.FUTURES.value is not None
    assert HistoricalKlinesType.FUTURES_COIN.value is not None


def test_sort_by_enum() -> None:
    """SortBy enum values should stay stable."""
    assert SortBy.VOLUME.value == "volume"
    assert SortBy.PRICE_CHANGE.value == "price_change"
    assert SortBy.PRICE_CHANGE_PERCENT.value == "price_change_percent"
    assert SortBy.QUOTE_VOLUME.value == "quote_volume"


def test_perpetual_market_ticker() -> None:
    """FuturesKlineTicker construction should work."""
    ticker = FuturesKlineTicker(
        symbol="BTCUSDT",
        last_price=Decimal("50000"),
        open_time=1234567890000,
        open_price=Decimal("49000"),
        high_price=Decimal("51000"),
        low_price=Decimal("48000"),
        close_price=Decimal("50000"),
        volume=Decimal("100"),
        close_time=1234567949999,
        quote_volume=Decimal("5000000"),
        trades_count=1000,
        taker_buy_volume=Decimal("50"),
        taker_buy_quote_volume=Decimal("2500000"),
    )

    assert ticker.symbol == "BTCUSDT"
    assert ticker.close_price == Decimal("50000")
