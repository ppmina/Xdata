import logging
from typing import Optional
from binance import Client
from crypto_data.exceptions import MarketDataError

logger = logging.getLogger(__name__)


class BinanceClientFactory:
    """Binance客户端工厂类"""

    _instance: Optional[Client] = None

    @classmethod
    def create_client(cls, api_key: str, api_secret: str) -> Client:
        """
        创建或获取Binance客户端实例（单例模式）

        Args:
            api_key: API密钥
            api_secret: API密钥对应的secret

        Returns:
            Client: Binance客户端实例

        Raises:
            MarketDataError: 当客户端初始化失败时抛出
        """
        if not cls._instance:
            try:
                if not api_key or not api_secret:
                    raise ValueError("Missing Binance API credentials")
                cls._instance = Client(api_key, api_secret)
                logger.info("Successfully created Binance client")
            except Exception as e:
                logger.error(f"Failed to initialize Binance client: {e}")
                raise MarketDataError(f"Failed to initialize Binance client: {e}")

        return cls._instance

    @classmethod
    def get_client(cls) -> Optional[Client]:
        """获取现有的客户端实例"""
        return cls._instance

    @classmethod
    def reset_client(cls) -> None:
        """重置客户端实例"""
        cls._instance = None


if __name__ == "__main__":
    import os
    from crypto_data.client import BinanceClientFactory
    from crypto_data.services import MarketDataService
    from dotenv import load_dotenv

    # 尝试加载 .env 文件，如果文件不存在也不会报错
    load_dotenv(override=True)

    # 优先使用环境变量，如果没有可以直接设置
    api_key = os.getenv("BINANCE_API_KEY") or "your_api_key_here"
    api_secret = os.getenv("BINANCE_API_SECRET") or "your_api_secret_here"

    # 创建客户端
    client = BinanceClientFactory.create_client(api_key, api_secret)

    # 创建服务
    market_service = MarketDataService(client)

    # 使用服务
    top_coins = market_service.get_top_coins(limit=10)

    print(top_coins)
