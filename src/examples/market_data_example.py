import logging
import os

from dotenv import load_dotenv

from cryptoservice.data import StorageUtils
from cryptoservice.models import Freq
from cryptoservice.services import MarketDataService

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


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
        data = market_service.get_perpetual_data(
            symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
            start_time="2024-01-01",
            end_time="2024-01-02",
            freq=Freq.h1,
        )
        StorageUtils.store_feature_data(
            data,
            "2024-01-01",
            Freq.h1,
            "SWAP",
            "cls",
            ["BTCUSDT", "ETHUSDT", "BNBUSDT"],
        )

    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()
