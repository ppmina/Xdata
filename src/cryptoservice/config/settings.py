"""应用配置管理.

使用 Pydantic BaseSettings 加载和管理配置。
"""

import os
from pathlib import Path
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# 项目根目录
ROOT_DIR = Path(__file__).parent.parent.parent.parent


class Settings(BaseSettings):
    """应用配置类.

    所有配置项都会自动从环境变量读取，环境变量名与字段名相同。
    例如：LOG_LEVEL 环境变量对应 log_level 字段。
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="allow",  # 允许额外的字段
        case_sensitive=False,  # 环境变量不区分大小写
    )

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

    # 日志配置（自动从环境变量读取）
    LOG_LEVEL: str = Field(
        default="INFO",
        description="日志级别: DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )
    LOG_ENVIRONMENT: str = Field(
        default="development",
        description="运行环境: development, production, test",
    )
    LOG_FILE: str = Field(
        default="",
        description="日志文件路径（生产环境建议配置）",
    )
    LOG_ENABLE_RICH: bool = Field(
        default=True,
        description="是否启用Rich格式化（开发环境推荐）",
    )
    LOG_PROFILE: str = Field(
        default="default",
        description="日志配置预设，例如 default 或 cli_demo",
    )
    LOG_VERBOSE: bool = Field(
        default=False,
        description="是否启用详细调试日志",
    )

    def get_proxy_config(self) -> dict[str, str]:
        """获取代理配置.

        Returns:
            代理配置字典，包含 http 和 https 代理
        """
        proxies = {}

        # 优先使用配置中的值，然后使用环境变量
        http_proxy = self.HTTP_PROXY or os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        https_proxy = self.HTTPS_PROXY or os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")

        if http_proxy:
            proxies["http"] = http_proxy
        if https_proxy:
            proxies["https"] = https_proxy

        return proxies


# 创建全局设置实例
settings = Settings()
