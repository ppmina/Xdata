"""Define crypto universe snapshots and save them as a JSON file.

This demo script loads Binance API credentials from environment variables,
builds a MarketDataService instance, and calls `define_universe` with the
configuration below, writing the result to `data/universe.json`.
"""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice import MarketDataService
from cryptoservice.config.logging import get_logger
from cryptoservice.utils.cli_helper import print_summary, print_progress_header

load_dotenv()

logger = get_logger(__name__)

# ============== 配置参数 ==============
# 时间范围
START_DATE = "2024-10-01"
END_DATE = "2024-11-30"

# 输出文件路径
OUTPUT_PATH = "./data/universe.json"

# Universe 配置参数
T1_MONTHS = 1  # 1个月回看期
T2_MONTHS = 1  # 1个月重平衡频率
T3_MONTHS = 1  # 1个月最小合约存在时间
# TOP_K = 160  # Top 160合约 (与 TOP_RATIO 二选一)
TOP_RATIO = 0.9  # 选择Top 90%的合约
DELAY_DAYS = 7  # 延迟7天
QUOTE_ASSET = "USDT"  # 只使用USDT永续合约

# API控制参数
API_DELAY_SECONDS = 1.0  # 每个API请求之间延迟
BATCH_DELAY_SECONDS = 3.0  # 每批次之间延迟
BATCH_SIZE = 10  # 每批请求数量

# ========================================


async def main():
    """定义Universe脚本."""
    # 检查API密钥
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        logger.error("env_vars_missing", required="BINANCE_API_KEY and BINANCE_API_SECRET")
        return

    # 确保输出目录存在
    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)

    # 创建服务
    service = await MarketDataService.create(api_key=api_key, api_secret=api_secret)

    try:
        # 显示配置信息
        print_progress_header(
            "定义 Universe",
            details={
                "时间范围": f"{START_DATE} ~ {END_DATE}",
                "回看期": f"{T1_MONTHS} 月",
                "重平衡": f"{T2_MONTHS} 月",
                "最小存续": f"{T3_MONTHS} 月",
                "选择比例": f"{TOP_RATIO * 100:.0f}%",
                "延迟天数": f"{DELAY_DAYS} 天",
                "报价资产": QUOTE_ASSET,
            },
        )

        logger.info("define_universe_start")

        universe_def = await service.define_universe(
            start_date=START_DATE,
            end_date=END_DATE,
            t1_months=T1_MONTHS,
            t2_months=T2_MONTHS,
            t3_months=T3_MONTHS,
            top_ratio=TOP_RATIO,
            output_path=OUTPUT_PATH,
            description=f"Universe from {START_DATE} to {END_DATE}",
            delay_days=DELAY_DAYS,
            api_delay_seconds=API_DELAY_SECONDS,
            batch_delay_seconds=BATCH_DELAY_SECONDS,
            batch_size=BATCH_SIZE,
            quote_asset=QUOTE_ASSET,
        )

        logger.info("define_universe_complete", output_path=OUTPUT_PATH)

        # 统计快照信息
        total_snapshots = len(universe_def.snapshots)
        total_symbols = sum(len(s.symbols) for s in universe_def.snapshots)
        avg_symbols = total_symbols / total_snapshots if total_snapshots > 0 else 0

        # 显示定义总结
        print_summary(
            title="Universe 定义完成",
            status="success",
            items={
                "输出文件": OUTPUT_PATH,
                "快照数量": total_snapshots,
                "总符号数": total_symbols,
                "平均符号数": f"{avg_symbols:.0f}",
                "时间范围": f"{START_DATE} ~ {END_DATE}",
                "回看/重平衡/存续": f"{T1_MONTHS}/{T2_MONTHS}/{T3_MONTHS} 月",
            },
        )

    except Exception as e:
        logger.error("define_universe_failed", error=str(e))
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
