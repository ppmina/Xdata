"""导出数据库数据到文件的脚本（支持 universe/custom 两种入口）."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq
from cryptoservice.services import MarketDataService
from cryptoservice.utils import load_symbols_from_txt
from cryptoservice.utils.cli_helper import print_progress_header, print_summary

load_dotenv()

logger = get_logger(__name__)

# ============== 配置参数 ==============
UNIVERSE_FILE = "./data/universe_custom.json"
DB_PATH = "./data/database/market_custom.db"
EXPORT_BASE_PATH = "./data/exports"

SOURCE_FREQ = Freq.m5
EXPORT_FREQ = Freq.m5
EXPORT_KLINES = True
EXPORT_METRICS = True

METRICS_CONFIG = {
    "funding_rate": True,
    "open_interest": True,
    "long_short_ratio": True,
}

# Universe 文件导出可选范围
CUSTOM_START_DATE = "2024-10-01"
CUSTOM_END_DATE = "2024-10-31"

# 自定义导出配置
USE_CUSTOM_UNIVERSE_EXPORT = False
CUSTOM_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
CUSTOM_SYMBOLS_FILE = "./data/symbols.txt"
CUSTOM_EXPORT_START_DATE = "2024-10-01"
CUSTOM_EXPORT_END_DATE = "2024-10-31"
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
    """主函数 - 展示如何使用导出功能."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        logger.error("env_vars_missing", required="BINANCE_API_KEY and BINANCE_API_SECRET")
        return

    if not USE_CUSTOM_UNIVERSE_EXPORT and not Path(UNIVERSE_FILE).exists():
        logger.error("universe_file_not_found", path=UNIVERSE_FILE, hint="run define_universe.py first")
        return

    print_progress_header(
        "导出数据",
        details={
            "导出模式": "自定义 symbols" if USE_CUSTOM_UNIVERSE_EXPORT else "Universe 文件",
            "数据库": DB_PATH,
            "输出根目录": EXPORT_BASE_PATH,
            "源频率": SOURCE_FREQ.value,
            "导出频率": EXPORT_FREQ.value,
            "包含K线": EXPORT_KLINES,
            "包含指标": EXPORT_METRICS,
        },
    )

    async with await MarketDataService.create(api_key=api_key, api_secret=api_secret) as service:
        try:
            custom_symbols = resolve_custom_symbols()
            if USE_CUSTOM_UNIVERSE_EXPORT:
                report = await service.export_custom_universe_data(
                    symbols=custom_symbols,
                    start_date=CUSTOM_EXPORT_START_DATE,
                    end_date=CUSTOM_EXPORT_END_DATE,
                    db_path=DB_PATH,
                    export_base_path=EXPORT_BASE_PATH,
                    source_freq=SOURCE_FREQ,
                    export_freq=EXPORT_FREQ,
                    include_klines=EXPORT_KLINES,
                    include_metrics=EXPORT_METRICS,
                    metrics_config=METRICS_CONFIG if EXPORT_METRICS else None,
                    description="Custom export from demo/export_data.py",
                )
            else:
                report = await service.export_universe_data(
                    universe_file=UNIVERSE_FILE,
                    db_path=DB_PATH,
                    export_base_path=EXPORT_BASE_PATH,
                    source_freq=SOURCE_FREQ,
                    export_freq=EXPORT_FREQ,
                    include_klines=EXPORT_KLINES,
                    include_metrics=EXPORT_METRICS,
                    metrics_config=METRICS_CONFIG if EXPORT_METRICS else None,
                    custom_start_date=CUSTOM_START_DATE,
                    custom_end_date=CUSTOM_END_DATE,
                )

            stats = report.get("stats", {})
            print_summary(
                title="数据导出完成",
                status="success" if stats.get("error_count", 0) == 0 else "partial",
                items={
                    "输出目录": report.get("output_path", ""),
                    "报告文件": report.get("report_path", ""),
                    "Universe 文件": report.get("universe_file", UNIVERSE_FILE),
                    "导出快照": stats.get("exported_snapshot_count", 0),
                    "跳过快照": stats.get("skipped_snapshot_count", 0),
                    "错误数量": stats.get("error_count", 0),
                    "定义期缺失日期": stats.get("define_missing_date_count", 0),
                    "导出期缺失日期": stats.get("export_missing_date_count", 0),
                    "合并缺失日期": stats.get("merged_missing_date_count", 0),
                },
            )

        except Exception as e:
            logger.error(f"数据导出失败：{e}", exc_info=True)
            print_summary(
                title="数据导出失败",
                status="failed",
                items={
                    "错误信息": str(e),
                    "输出根目录": EXPORT_BASE_PATH,
                },
            )
            raise


if __name__ == "__main__":
    asyncio.run(main())
