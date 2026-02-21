"""下载 Universe 数据到数据库的脚本."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice.config import RetryConfig
from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq
from cryptoservice.services import MarketDataService
from cryptoservice.utils import load_symbols_from_txt
from cryptoservice.utils.cli_helper import print_progress_header, print_summary

load_dotenv()

logger = get_logger(__name__)

# ============== 配置参数 ==============
# 通用路径
UNIVERSE_FILE = "./data/universe_custom.json"
DB_PATH = "./data/database/market_custom.db"

# 下载配置
INTERVAL = Freq.m5
MAX_API_WORKERS = 1
MAX_VISION_WORKERS = 50
API_REQUEST_DELAY = 0.5
VISION_REQUEST_DELAY = 0.0
MAX_RETRIES = 3
RETRY_CONFIG = RetryConfig(
    max_retries=MAX_RETRIES,
    base_delay=1.0,
    max_delay=10.0,
    backoff_multiplier=2.0,
    jitter=True,
)

INCREMENTAL = True
DOWNLOAD_MARKET_METRICS = True

# Universe 文件下载可选范围
CUSTOM_START_DATE = "2024-10-01"
CUSTOM_END_DATE = "2024-10-31"

# 自定义 Universe 下载配置
USE_CUSTOM_UNIVERSE = True
CUSTOM_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BADSYMBOL"]
CUSTOM_SYMBOLS_FILE = "./data/symbols.txt"
CUSTOM_UNIVERSE_START_DATE = "2024-10-01"
CUSTOM_UNIVERSE_END_DATE = "2025-1-30"
# ========================================


def resolve_custom_symbols() -> list[str]:
    """解析自定义 symbols（优先从 txt 文件读取）."""
    if CUSTOM_SYMBOLS_FILE:
        symbols_file = Path(CUSTOM_SYMBOLS_FILE)
        if symbols_file.exists():
            symbols = load_symbols_from_txt(symbols_file)
            if symbols:
                logger.info("custom_symbols_loaded", source=str(symbols_file), count=len(symbols))
                return symbols
            logger.warning("custom_symbols_file_empty", source=str(symbols_file))
        else:
            logger.warning("custom_symbols_file_not_found", source=str(symbols_file))

    return CUSTOM_SYMBOLS


async def main():
    """下载数据到数据库脚本."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        logger.error("env_vars_missing", required="BINANCE_API_KEY and BINANCE_API_SECRET")
        return

    if not USE_CUSTOM_UNIVERSE and not Path(UNIVERSE_FILE).exists():
        logger.error("universe_file_not_found", path=UNIVERSE_FILE, hint="run define_universe.py first")
        return

    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    try:
        print_progress_header(
            "Universe 数据下载",
            details={
                "下载模式": "自定义 symbols" if USE_CUSTOM_UNIVERSE else "Universe 文件",
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
            custom_report = None
            custom_symbols = resolve_custom_symbols()

            if USE_CUSTOM_UNIVERSE:
                custom_report = await service.download_custom_universe_data(
                    symbols=custom_symbols,
                    start_date=CUSTOM_UNIVERSE_START_DATE,
                    end_date=CUSTOM_UNIVERSE_END_DATE,
                    db_path=DB_PATH,
                    retry_config=RETRY_CONFIG,
                    api_request_delay=API_REQUEST_DELAY,
                    vision_request_delay=VISION_REQUEST_DELAY,
                    download_market_metrics=DOWNLOAD_MARKET_METRICS,
                    incremental=INCREMENTAL,
                    interval=INTERVAL,
                    max_api_workers=MAX_API_WORKERS,
                    max_vision_workers=MAX_VISION_WORKERS,
                    max_retries=MAX_RETRIES,
                    description="Custom universe from demo/download_data.py",
                    universe_output_path=UNIVERSE_FILE,
                    overwrite_universe_output=False,
                )
            else:
                await service.download_universe_data(
                    universe_file=UNIVERSE_FILE,
                    db_path=DB_PATH,
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

            summary_items: dict[str, object] = {
                "数据库路径": DB_PATH,
                "数据频率": INTERVAL.value,
                "增量模式": INCREMENTAL,
                "下载指标": DOWNLOAD_MARKET_METRICS,
                "自定义模式": USE_CUSTOM_UNIVERSE,
            }

            if custom_report:
                download_summary = custom_report.get("download_summary", {})
                summary_items.update(
                    {
                        "输入 symbols": custom_report.get("requested_symbols", 0),
                        "有效 symbols": len(custom_report.get("valid_symbols", [])),
                        "跳过 symbols": len(custom_report.get("skipped_symbols", [])),
                        "下载成功": download_summary.get("total_successful_symbols", 0),
                        "下载失败": download_summary.get("total_failed_symbols", 0),
                        "Universe 文件": custom_report.get("universe_file", UNIVERSE_FILE),
                        "Universe 已写入": custom_report.get("universe_file_written", False),
                        "下载起始": download_summary.get("download_context", {}).get("requested_start_date"),
                        "下载结束": download_summary.get("download_context", {}).get("requested_end_date"),
                        "下载频率": download_summary.get("download_context", {}).get("interval"),
                    }
                )

            print_summary(title="数据下载完成", status="success", items=summary_items)

    except Exception as e:
        logger.error(f"Universe 数据下载失败：{e}")
        print_summary(
            title="数据下载失败",
            status="failed",
            items={
                "错误信息": str(e),
                "数据库路径": DB_PATH,
            },
        )
        raise


if __name__ == "__main__":
    asyncio.run(main())
