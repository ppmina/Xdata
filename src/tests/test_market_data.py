from decimal import Decimal

from cryptoservice.models.market_ticker import (
    DailyMarketTicker,
    KlineMarketTicker,
    SpotMarketTicker,
    SymbolTicker,
)
from cryptoservice.services.market_service import MarketDataService


def test_market_ticker_from_spot_ticker() -> None:
    """测试现货行情数据解析"""
    spot_data = {"symbol": "BTCUSDT", "price": "50000.0"}
    ticker = SpotMarketTicker.from_binance_ticker(spot_data)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.last_price == Decimal("50000.0")
    assert not hasattr(ticker, "volume")  # 确保不存在的字段没有被添加


def test_market_ticker_from_24h_ticker() -> None:
    """测试24小时行情数据解析"""
    ticker_24h = {
        "symbol": "BTCUSDT",
        "lastPrice": "50000.0",
        "priceChange": "1000.0",
        "priceChangePercent": "2.0",
        "volume": "100.0",
        "quoteVolume": "5000000.0",
        "openTime": 1234567890000,
        "closeTime": 1234567890000,
    }
    ticker = DailyMarketTicker.from_binance_ticker(ticker_24h)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.last_price == Decimal("50000.0")
    assert ticker.price_change == Decimal("1000.0")
    assert ticker.volume == Decimal("100.0")
    assert ticker.quote_volume == Decimal("5000000.0")


def test_market_ticker_from_kline() -> None:
    """测试K线数据解析"""
    kline_data = [
        "BTCUSDT",  # symbol
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
    ]
    ticker = KlineMarketTicker.from_binance_kline(kline_data)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.last_price == Decimal("50000.0")
    assert ticker.high_price == Decimal("51000.0")
    assert ticker.low_price == Decimal("48000.0")
    assert ticker.volume == Decimal("100.0")


def test_market_ticker_to_dict() -> None:
    """测试转换为字典格式"""
    ticker_data = {"symbol": "BTCUSDT", "Price": "50000.0"}
    ticker = SymbolTicker.from_binance_ticker(ticker_data)
    result = ticker.to_dict()

    assert result["symbol"] == "BTCUSDT"
    assert result["last_price"] == "50000.0"
    assert result["volume"] == "100.0"
    assert "price_change" not in result  # 确保不存在的字段不会出现在结果中


def test_get_symbol_ticker() -> None:
    """测试获取单个交易对行情"""
    service = MarketDataService(api_key="", api_secret="")
    ticker = service.get_symbol_ticker("BTCUSDT")
    assert ticker.symbol == "BTCUSDT"
    assert isinstance(ticker.last_price, Decimal)


def test_get_top_coins() -> None:
    """测试获取热门交易对"""
    service = MarketDataService(api_key="", api_secret="")
    top_coins = service.get_top_coins(limit=3)
    assert len(top_coins) == 3
    assert all(isinstance(coin.last_price, Decimal) for coin in top_coins)
