import logging
from typing import Optional

from binance import Client

from cryptoservice.exceptions import MarketDataError

logger = logging.getLogger(__name__)


class BinanceClientFactory:
    """Binance客户端工厂类."""

    _instance: Optional[Client] = None

    @classmethod
    def create_client(cls, api_key: str, api_secret: str) -> Client:
        """创建或获取Binance客户端实例（单例模式）

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
                raise MarketDataError(f"Failed to initialize Binance client: {e}") from e
        return cls._instance

    @classmethod
    def get_client(cls) -> Optional[Client]:
        """获取现有的客户端实例."""
        return cls._instance

    @classmethod
    def reset_client(cls) -> None:
        """重置客户端实例."""
        cls._instance = None
