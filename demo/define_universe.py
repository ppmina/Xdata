"""定义 Universe 并执行按天存在性校验的演示脚本."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice import MarketDataService
from cryptoservice.config.logging import get_logger
from cryptoservice.utils import load_symbols_from_txt
from cryptoservice.utils.cli_helper import print_progress_header, print_summary

load_dotenv()

logger = get_logger(__name__)

# ============== 配置参数 ==============
START_DATE = "2025-06-01"
END_DATE = "2025-09-30"
OUTPUT_PATH = "./data/universe_custom.json"

# 自定义定义模式（symbols + 时间）
USE_CUSTOM_UNIVERSE_DEFINE = True
CUSTOM_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BADSYMBOL"]
CUSTOM_SYMBOLS_FILE = "./data/symbols.txt"

T1_MONTHS = 1
T2_MONTHS = 1
T3_MONTHS = 1
TOP_RATIO = 0.9
DELAY_DAYS = 7
QUOTE_ASSET = "USDT"

API_DELAY_SECONDS = 1.0
BATCH_DELAY_SECONDS = 3.0
BATCH_SIZE = 10

# 日级存在性检查配置
DAILY_CHECK_WORKERS = 8
DAILY_CHECK_REQUEST_DELAY = 0.0
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


def collect_missing_by_date(universe_def) -> dict[str, list[str]]:
    """从快照 metadata 聚合缺失映射."""
    merged: dict[str, set[str]] = {}

    for snapshot in universe_def.snapshots:
        metadata = snapshot.metadata or {}
        check_data = metadata.get("daily_existence_check", {})
        missing_by_date = check_data.get("missing_by_date", {})
        if not isinstance(missing_by_date, dict):
            continue

        for date_str, symbols in missing_by_date.items():
            if not isinstance(symbols, list):
                continue
            merged.setdefault(date_str, set()).update(symbols)

    return {date_str: sorted(symbols) for date_str, symbols in sorted(merged.items())}


async def main():
    """定义 Universe 脚本."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        logger.error("env_vars_missing", required="BINANCE_API_KEY and BINANCE_API_SECRET")
        return

    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)

    async with await MarketDataService.create(api_key=api_key, api_secret=api_secret) as service:
        try:
            custom_symbols = resolve_custom_symbols()
            print_progress_header(
                "定义 Universe（含日级校验）",
                details={
                    "模式": "自定义 symbols" if USE_CUSTOM_UNIVERSE_DEFINE else "历史筛选",
                    "时间范围": f"{START_DATE} ~ {END_DATE}",
                    "回看/重平衡/存续": f"{T1_MONTHS}/{T2_MONTHS}/{T3_MONTHS} 月",
                    "选择比例": f"{TOP_RATIO * 100:.0f}%",
                    "延迟天数": f"{DELAY_DAYS} 天",
                    "报价资产": QUOTE_ASSET,
                    "校验并发": DAILY_CHECK_WORKERS,
                },
            )

            if USE_CUSTOM_UNIVERSE_DEFINE:
                universe_def = await service.define_custom_universe_with_daily_check(
                    symbols=custom_symbols,
                    start_date=START_DATE,
                    end_date=END_DATE,
                    output_path=OUTPUT_PATH,
                    description=f"Custom universe from {START_DATE} to {END_DATE} (daily checked)",
                    daily_check_workers=DAILY_CHECK_WORKERS,
                    daily_check_request_delay=DAILY_CHECK_REQUEST_DELAY,
                )
            else:
                universe_def = await service.define_universe_with_daily_check(
                    start_date=START_DATE,
                    end_date=END_DATE,
                    t1_months=T1_MONTHS,
                    t2_months=T2_MONTHS,
                    t3_months=T3_MONTHS,
                    top_ratio=TOP_RATIO,
                    output_path=OUTPUT_PATH,
                    description=f"Universe from {START_DATE} to {END_DATE} (daily checked)",
                    delay_days=DELAY_DAYS,
                    api_delay_seconds=API_DELAY_SECONDS,
                    batch_delay_seconds=BATCH_DELAY_SECONDS,
                    batch_size=BATCH_SIZE,
                    quote_asset=QUOTE_ASSET,
                    daily_check_workers=DAILY_CHECK_WORKERS,
                    daily_check_request_delay=DAILY_CHECK_REQUEST_DELAY,
                )

            total_snapshots = len(universe_def.snapshots)
            total_symbols = sum(len(s.symbols) for s in universe_def.snapshots)
            avg_symbols = total_symbols / total_snapshots if total_snapshots > 0 else 0

            missing_by_date = collect_missing_by_date(universe_def)
            first_metadata = universe_def.snapshots[0].metadata if universe_def.snapshots else {}
            valid_symbols = first_metadata.get("valid_symbols", []) if isinstance(first_metadata, dict) else []
            skipped_symbols = first_metadata.get("skipped_symbols", []) if isinstance(first_metadata, dict) else []

            print_summary(
                title="Universe 定义完成",
                status="success",
                items={
                    "输出文件": OUTPUT_PATH,
                    "快照数量": total_snapshots,
                    "总符号数": total_symbols,
                    "平均符号数": f"{avg_symbols:.0f}",
                    "缺失日期数": len(missing_by_date),
                    "有效 symbols": len(valid_symbols),
                    "跳过 symbols": len(skipped_symbols),
                },
            )

            if missing_by_date:
                logger.info("daily_missing_summary", missing_by_date=missing_by_date)

        except Exception as e:
            logger.error(f"Universe 定义失败：{e}")
            print_summary(
                title="Universe 定义失败",
                status="failed",
                items={
                    "错误信息": str(e),
                    "输出路径": OUTPUT_PATH,
                },
            )
            raise


if __name__ == "__main__":
    asyncio.run(main())
