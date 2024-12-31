import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
from rich.traceback import install

from cryptoservice import MarketDataService
from cryptoservice.data import StorageUtils
from cryptoservice.models import Freq, HistoricalKlinesType, SortBy

# 设置 rich console 和 traceback
console = Console()
install(show_locals=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[
        RichHandler(
            rich_tracebacks=True, markup=True, console=console, show_time=True, show_path=True
        )
    ],
)
logger = logging.getLogger(__name__)


def demonstrate_market_data_features(market_service: MarketDataService) -> None:
    """演示各种市场数据功能"""

    # 1. 获取单个交易对的实时行情
    btc_ticker = market_service.get_symbol_ticker("BTCUSDT")
    logger.info(f"BTCUSDT 实时行情: {btc_ticker}")

    # 2. 获取所有交易对的实时行情
    all_tickers = market_service.get_symbol_ticker()
    logger.info(f"获取到 {len(all_tickers)} 个交易对的行情")

    # 3. 获取成交量排名前10的USDT交易对
    top_coins = market_service.get_top_coins(
        limit=10, sort_by=SortBy.QUOTE_VOLUME, quote_asset="USDT"
    )
    logger.info("成交量TOP10的USDT交易对:")
    for coin in top_coins:
        logger.info(f"{coin.symbol}: 成交量 {coin.quote_volume}")

    # 4. 获取市场概览
    market_summary = market_service.get_market_summary(interval=Freq.h1)
    logger.info(f"市场概览时间: {market_summary['snapshot_time']}")

    # 5. 获取历史K线数据
    historical_data = market_service.get_historical_klines(
        symbol="ETHUSDT",
        start_time="20240101",
        end_time="20240103",
        interval=Freq.h4,
        klines_type=HistoricalKlinesType.SPOT,
    )
    logger.info(f"获取到 {len(historical_data)} 条 ETHUSDT 历史数据")

    # 6. 获取订单簿数据
    orderbook = market_service.get_orderbook("BTCUSDT", limit=10)
    logger.info(f"BTCUSDT 订单簿深度: {len(orderbook['bids'])} 档")

    # 7. 获取永续合约数据
    perpetual_data = market_service.get_perpetual_data(
        symbols=[
            "BTCUSDT",
            "ETHUSDT",
            "BNBUSDT",
            "SOLUSDT",
            "ADAUSDT",
            "XRPUSDT",
            "DOGEUSDT",
            "DOTUSDT",
            "AVAXUSDT",
            "LTCUSDT",
        ],
        start_time="20240101",
        end_time="20240103",
        interval=Freq.h1,
        data_path="data",
    )
    StorageUtils.visualize_npy_data("./data/1h/count/20240102.npy")
    StorageUtils.visualize_npy_data("./data/1h/high_price/20240102.npy")
    StorageUtils.visualize_npy_data("./data/1h/last_price/20240102.npy")
    StorageUtils.visualize_npy_data("./data/1h/low_price/20240102.npy")


def main() -> None:
    load_dotenv()

    # 初始化客户端
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        raise ValueError(
            "BINANCE_API_KEY and BINANCE_API_SECRET must be set in environment variables"
        )

    # 创建市场数据服务实例
    market_service = MarketDataService(api_key, api_secret)

    try:
        # 运行所有示例
        demonstrate_market_data_features(market_service)

    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()
