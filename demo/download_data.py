"""下载 Universe 数据到数据库的脚本."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice.config import RetryConfig
from cryptoservice.models import Freq
from cryptoservice.services import MarketDataService

load_dotenv()

# ============== 配置参数 ==============
# 文件路径
UNIVERSE_FILE = "./data/universe.json"  # Universe定义文件
DB_PATH = "./data/database/market.db"  # 数据库文件路径

# 下载配置
LONG_SHORT_RATIO_TYPES = ["account"]  # 多空比例数据类型
INTERVAL = Freq.m5  # 数据频率: Freq.m1, Freq.h1, Freq.d1
MAX_API_WORKERS = 1  # 最大并发数（Binance API建议1-5，）
MAX_VISION_WORKERS = 50  # Vision S3下载最大并发数, 建议50-150)
API_REQUEST_DELAY = 0.5  # API请求延迟
VISION_REQUEST_DELAY = 0  # Vision请求延迟
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


# 增量下载配置
INCREMENTAL = True  # 是否启用增量下载模式（只下载缺失的数据）

# 自定义时间范围配置 (可选)
CUSTOM_START_DATE = None  # 自定义起始日期，例如: "2024-02-01"，必须在universe时间范围内
CUSTOM_END_DATE = None  # 自定义结束日期，例如: "2024-06-30"，必须在universe时间范围内

# 新特征配置
DOWNLOAD_MARKET_METRICS = True  # 是否下载市场指标数据 (资金费率、持仓量、多空比例)

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
            # 显示自定义时间范围信息
            if CUSTOM_START_DATE or CUSTOM_END_DATE:
                print("📅 自定义时间范围:")
                print(f"   - 自定义起始日期: {CUSTOM_START_DATE or '未指定（使用universe原始）'}")
                print(f"   - 自定义结束日期: {CUSTOM_END_DATE or '未指定（使用universe原始）'}")
                print("   - 自定义时间范围必须在universe定义的时间范围内")
            else:
                print("📅 使用universe定义的完整时间范围")

            # 下载universe数据
            await service.download_universe_data(
                universe_file=UNIVERSE_FILE,
                db_path=DB_PATH,
                long_short_ratio_types=LONG_SHORT_RATIO_TYPES,
                retry_config=RETRY_CONFIG,
                api_request_delay=API_REQUEST_DELAY,
                vision_request_delay=VISION_REQUEST_DELAY,
                download_market_metrics=DOWNLOAD_MARKET_METRICS,
                incremental=INCREMENTAL,
                interval=INTERVAL,
                max_api_workers=MAX_API_WORKERS,
                max_vision_workers=MAX_VISION_WORKERS,
                max_retries=MAX_RETRIES,
                custom_start_date=CUSTOM_START_DATE,  # 新增：自定义起始日期
                custom_end_date=CUSTOM_END_DATE,  # 新增：自定义结束日期
            )

        print("✅ 数据下载完成!")

    except Exception as e:
        print(f"❌ 数据下载失败: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
