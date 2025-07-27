"""下载 Universe 数据到数据库的脚本."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice.models.enums import Freq
from cryptoservice.services.market_service import MarketDataService, RetryConfig

load_dotenv()

# ============== 配置参数 ==============
# 文件路径
UNIVERSE_FILE = "./data/universe.json"  # Universe定义文件
DB_PATH = "./data/database/market.db"  # 数据库文件路径

# 下载配置
INTERVAL = Freq.d1  # 数据频率: Freq.m1, Freq.h1, Freq.d1
MAX_WORKERS = 2  # 最大并发数 (建议1-2，避免API限制)
MAX_RETRIES = 3  # 最大重试次数
RETRY_CONFIG = (
    RetryConfig(
        max_retries=MAX_RETRIES,
        base_delay=1.0,
        max_delay=10.0,
        backoff_multiplier=2.0,
        jitter=True,
    ),
)
REQUEST_DELAY = 2  # 请求间隔（秒）
INCLUDE_BUFFER_DAYS = 7  # 包含缓冲期天数

# 新特征配置
DOWNLOAD_MARKET_METRICS = True  # 是否下载市场指标数据 (资金费率、持仓量、多空比例)
METRICS_INTERVAL = Freq.h1  # 市场指标数据时间间隔 (考虑到资金费率最小粒度是小时)
LONG_SHORT_RATIO_PERIOD = Freq.h1  # 多空比例时间周期 (原始数据为m5, 上或下采样至目标频率)
LONG_SHORT_RATIO_TYPES = ["account"]  # 多空比例类型: account, position, global, taker
USE_BINANCE_VISION = True  # 是否使用 Binance Vision 下载特征数据 (推荐)

# ========================================


async def main():
    """下载数据到数据库脚本."""
    # 检查API密钥
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        print("❌ 请设置环境变量: BINANCE_API_KEY 和 BINANCE_API_SECRET")
        return

    # 检查Universe文件是否存在
    if not Path(UNIVERSE_FILE).exists():
        print(f"❌ Universe文件不存在: {UNIVERSE_FILE}")
        print("请先运行 define_universe.py 创建Universe文件")
        return

    # 确保数据库存在
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    # 创建服务并作为上下文管理器使用
    try:
        async with await MarketDataService.create(api_key=api_key, api_secret=api_secret) as service:
            # 下载universe数据
            await service.download_universe_data(
                universe_file=UNIVERSE_FILE,
                db_path=DB_PATH,
                interval=INTERVAL,
                max_workers=MAX_WORKERS,
                max_retries=MAX_RETRIES,
                include_buffer_days=INCLUDE_BUFFER_DAYS,
                request_delay=REQUEST_DELAY,
                download_market_metrics=DOWNLOAD_MARKET_METRICS,
                metrics_interval=METRICS_INTERVAL,
                long_short_ratio_period=LONG_SHORT_RATIO_PERIOD,
                long_short_ratio_types=LONG_SHORT_RATIO_TYPES,
                use_binance_vision=USE_BINANCE_VISION,
            )

        print("✅ 数据下载完成!")

    except Exception as e:
        print(f"❌ 数据下载失败: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
