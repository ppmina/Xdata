from decimal import Decimal

from cryptoservice.models.market_ticker import DailyMarketTicker, KlineMarketTicker, SymbolTicker


def test_market_ticker_from_24h_ticker() -> None:
    """测试24小时行情数据解析"""
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
    ticker_data = {"symbol": "BTCUSDT", "price": "50000.0"}
    ticker = SymbolTicker.from_binance_ticker(ticker_data)
    result = ticker.to_dict()

    assert result["symbol"] == "BTCUSDT"
    assert result["last_price"] == "50000.0"
    assert "volume" not in result
    assert "price_change" not in result  # 确保不存在的字段不会出现在结果中
