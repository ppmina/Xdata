"""应用配置管理.

使用 Pydantic BaseSettings 加载和管理配置。
"""

import os
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings

# 项目根目录
ROOT_DIR = Path(__file__).parent.parent.parent.parent


class Settings(BaseSettings):
    """应用配置类."""

    # API 配置
    API_RATE_LIMIT: int = 1200
    DEFAULT_LIMIT: int = 100

    # binance 配置
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""

    # 网络代理配置
    HTTP_PROXY: str = ""
    HTTPS_PROXY: str = ""

    # 数据存储配置
    DATA_STORAGE: dict[str, Any] = {
        "ROOT_PATH": ROOT_DIR / "data",  # 数据根目录
        "MARKET_DATA": ROOT_DIR / "data/market",  # 市场数据目录
        "PERPETUAL_DATA": ROOT_DIR / "data/perpetual",  # 永续合约数据目录
        "DEFAULT_TYPE": "kdtv",  # 默认存储类型
    }

    # 缓存配置
    CACHE_TTL: int = 60  # 缓存过期时间（秒）

    def get_proxy_config(self) -> dict[str, str]:
        """获取代理配置."""
        proxies = {}

        # 优先使用配置中的值，然后使用环境变量
        http_proxy = self.HTTP_PROXY or os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        https_proxy = self.HTTPS_PROXY or os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")

        if http_proxy:
            proxies["http"] = http_proxy
        if https_proxy:
            proxies["https"] = https_proxy

        return proxies

    class Config:
        """基本配置."""

        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"  # 允许额外的字段


# 创建全局设置实例
settings = Settings()
