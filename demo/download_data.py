"""下载 Universe 数据到数据库的脚本."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice.config import RetryConfig
from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq
from cryptoservice.services import MarketDataService
from cryptoservice.utils.cli_helper import print_summary, print_progress_header

load_dotenv()

logger = get_logger(__name__)

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
CUSTOM_START_DATE = "2024-10-01" # 自定义起始日期，例如: "2024-02-01"，必须在universe时间范围内
CUSTOM_END_DATE = "2024-10-31"  # 自定义结束日期，例如: "2024-06-30"，必须在universe时间范围内

# 新特征配置
DOWNLOAD_MARKET_METRICS = True  # 是否下载市场指标数据 (资金费率、持仓量、多空比例)

# ========================================


async def main():
    """下载数据到数据库脚本."""
    # 检查API密钥
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        logger.error("env_vars_missing", required="BINANCE_API_KEY and BINANCE_API_SECRET")
        return

    # 检查Universe文件是否存在
    if not Path(UNIVERSE_FILE).exists():
        logger.error("universe_file_not_found", path=UNIVERSE_FILE, hint="run define_universe.py first")
        return

    # 确保数据库存在
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    # 创建服务并作为上下文管理器使用
    try:
        print_progress_header(
                "Universe 数据下载",
                details={
                    "Universe 文件": UNIVERSE_FILE,
                    "数据库路径": DB_PATH,
                    "数据频率": INTERVAL.value,
                    "增量模式": "是" if INCREMENTAL else "否",
                    "下载指标": "是" if DOWNLOAD_MARKET_METRICS else "否",
                    "API 并发数": MAX_API_WORKERS,
                    "Vision 并发数": MAX_VISION_WORKERS,
                },
            )
        async with await MarketDataService.create(api_key=api_key, api_secret=api_secret) as service:
            # 显示自定义时间范围信息
            logger.info("custom_time_range", start=CUSTOM_START_DATE or "use_universe_default", end=CUSTOM_END_DATE or "use_universe_default", note="must_be_within_universe_range")

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
                custom_start_date=CUSTOM_START_DATE,
                custom_end_date=CUSTOM_END_DATE,
            )

            logger.info("download_universe_complete")

        # 显示下载总结
        print_summary(
            title="数据下载完成",
            status="success",
            items={
                "Universe 文件": UNIVERSE_FILE,
                "数据库路径": DB_PATH,
                "数据频率": INTERVAL.value,
                "增量模式": INCREMENTAL,
                "下载指标": DOWNLOAD_MARKET_METRICS,
            },
        )

    except Exception as e:
        logger.error("download_universe_failed", error=str(e))
        print_summary(
            title="数据下载失败",
            status="failed",
            items={
                "错误信息": str(e),
                "Universe 文件": UNIVERSE_FILE,
            },
        )
        raise


if __name__ == "__main__":
    asyncio.run(main())
