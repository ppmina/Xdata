"""Binance API 客户端工厂，用于创建和管理客户端实例."""

from __future__ import annotations

import asyncio
from urllib.parse import urlparse

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_derivatives_trading_usds_futures.rest_api import DerivativesTradingUsdsFuturesRestAPI
from requests.utils import get_environ_proxies

from cryptoservice.config import get_logger, settings
from cryptoservice.exceptions import MarketDataError

from .gateway import BinanceGateway, OfficialBinanceGateway

logger = get_logger(__name__)


class BinanceClientFactory:
    """Binance客户端工厂类."""

    _rest_instance: DerivativesTradingUsdsFuturesRestAPI | None = None

    @staticmethod
    def _proxy_url_to_sdk_proxy(proxy_url: str) -> dict[str, object] | None:
        parsed = urlparse(proxy_url)
        if not parsed.hostname or not parsed.port:
            return None

        proxy: dict[str, object] = {
            "protocol": parsed.scheme or "https",
            "host": parsed.hostname,
            "port": parsed.port,
        }
        if parsed.username or parsed.password:
            proxy["auth"] = {
                "username": parsed.username or "",
                "password": parsed.password or "",
            }
        return proxy

    @classmethod
    async def create_rest_client(cls, api_key: str, api_secret: str) -> DerivativesTradingUsdsFuturesRestAPI:
        """创建或获取官方 USDs Futures REST 客户端（单例模式）."""
        if not cls._rest_instance:
            try:
                if not api_key or not api_secret:
                    raise ValueError("Missing Binance API credentials")

                proxies = settings.get_proxy_config()
                https_proxy_url: str | None = None
                if proxies:
                    https_proxy_url = proxies.get("https") or proxies.get("http")
                else:
                    system_proxies = get_environ_proxies("https://fapi.binance.com")
                    https_proxy_url = system_proxies.get("https") or system_proxies.get("http")

                config_kwargs: dict[str, object] = {
                    "api_key": api_key,
                    "api_secret": api_secret,
                    "base_path": "https://fapi.binance.com",
                    "timeout": 30_000,
                }
                if https_proxy_url:
                    sdk_proxy = cls._proxy_url_to_sdk_proxy(https_proxy_url)
                    if sdk_proxy is not None:
                        config_kwargs["proxy"] = sdk_proxy
                    else:
                        logger.warning("proxy_parse_failed", proxy=https_proxy_url)

                config = ConfigurationRestAPI(**config_kwargs)
                cls._rest_instance = DerivativesTradingUsdsFuturesRestAPI(config)
                logger.info("Official Binance USDs Futures REST client is ready.")
            except Exception as e:
                logger.error("client_create_error", client_type="official_usds_futures", error=str(e))
                raise MarketDataError(f"Failed to initialize official Binance futures client: {e}") from e
        return cls._rest_instance

    @classmethod
    async def close_client(cls, timeout: float = 5.0) -> None:
        """关闭现有的 REST 客户端会话."""
        if cls._rest_instance:
            session = getattr(cls._rest_instance, "_session", None)
            try:
                if session is not None and hasattr(session, "close"):
                    await asyncio.wait_for(asyncio.to_thread(session.close), timeout=timeout)
            except TimeoutError:
                logger.debug("client_close_timeout", timeout=timeout, note="normal_behavior")
            except Exception as e:
                logger.debug("client_close_exception", exception_type=type(e).__name__, note="safe_to_ignore")
            finally:
                cls._rest_instance = None

        logger.debug("Binance futures REST client connection closed.")

    @classmethod
    async def create_gateway(cls, api_key: str, api_secret: str) -> BinanceGateway:
        """Create futures-only gateway using official SDK."""
        rest_client = await cls.create_rest_client(api_key, api_secret)
        return OfficialBinanceGateway(rest_client)

    @classmethod
    def get_client(cls) -> DerivativesTradingUsdsFuturesRestAPI | None:
        """获取现有的客户端实例."""
        return cls._rest_instance

    @classmethod
    def reset_client(cls) -> None:
        """重置客户端实例."""
        cls._rest_instance = None
