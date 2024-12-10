import os
from dotenv import load_dotenv
import logging

from crypto_data.client.client import BinanceClient
from crypto_data.services.market_data import MarketDataService
from crypto_data.models.market_data import SortBy

# 设置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    load_dotenv()

    # 初始化客户端
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    binance_client = BinanceClient(api_key, api_secret)

    # 创建市场数据服务实例
    market_service = MarketDataService(binance_client)

    try:
        # 获取USDT交易对中交易量最大的前20个
        top_coins = market_service.get_top_coins(
            limit=20, sort_by=SortBy.QUOTE_VOLUME, quote_asset="USDT"
        )

        # 打印结果
        for coin in top_coins:
            print(
                f"{coin.symbol}: Volume={coin.quote_volume}, "
                f"Price=${coin.last_price}, "
                f"Change={coin.price_change_percent}%"
            )

        # 获取特定币种的市场概况
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        summary = market_service.get_market_summary(symbols)
        print("\nMarket Summary:")
        print(summary)

    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()
