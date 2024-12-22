from pathlib import Path
from typing import Any, Dict

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

    # 数据存储配置
    DATA_STORAGE: Dict[str, Any] = {
        "ROOT_PATH": ROOT_DIR / "data",  # 数据根目录
        "MARKET_DATA": ROOT_DIR / "data/market",  # 市场数据目录
        "PERPETUAL_DATA": ROOT_DIR / "data/perpetual",  # 永续合约数据目录
        "DEFAULT_TYPE": "kdtv",  # 默认存储类型
    }

    # 缓存配置
    CACHE_TTL: int = 60  # 缓存过期时间（秒）

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# 创建全局设置实例
settings = Settings()
