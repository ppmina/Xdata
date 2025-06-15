import os
from pathlib import Path
from cryptoservice.services.market_service import MarketDataService
from dotenv import load_dotenv

load_dotenv()

# ============== 配置参数 ==============
# 时间范围
START_DATE = "2024-10-01"
END_DATE = "2024-10-07"

# 输出文件路径
OUTPUT_PATH = "./data/universe.json"

# Universe 配置参数
T1_MONTHS = 1  # 1个月回看期
T2_MONTHS = 1  # 1个月重平衡频率
T3_MONTHS = 1  # 1个月最小合约存在时间
# TOP_K = 160  # Top 160合约 (与 TOP_RATIO 二选一)
TOP_RATIO = 0.8  # 选择Top 80%的合约
DELAY_DAYS = 7  # 延迟7天
QUOTE_ASSET = "USDT"  # 只使用USDT永续合约

# API控制参数
API_DELAY_SECONDS = 1.0  # 每个API请求之间延迟
BATCH_DELAY_SECONDS = 3.0  # 每批次之间延迟
BATCH_SIZE = 10  # 每批请求数量

# ========================================


def main():
    """定义Universe脚本"""

    # 检查API密钥
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        print("❌ 请设置环境变量: BINANCE_API_KEY 和 BINANCE_API_SECRET")
        return

    # 确保输出目录存在
    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)

    # 创建服务
    service = MarketDataService(api_key=api_key, api_secret=api_secret)

    try:
        universe_def = service.define_universe(
            start_date=START_DATE,
            end_date=END_DATE,
            t1_months=T1_MONTHS,
            t2_months=T2_MONTHS,
            t3_months=T3_MONTHS,
            # top_k=TOP_K,
            top_ratio=TOP_RATIO,
            output_path=OUTPUT_PATH,
            description=f"Universe from {START_DATE} to {END_DATE}",
            delay_days=DELAY_DAYS,
            api_delay_seconds=API_DELAY_SECONDS,
            batch_delay_seconds=BATCH_DELAY_SECONDS,
            batch_size=BATCH_SIZE,
            quote_asset=QUOTE_ASSET,
        )

        print("✅ Universe定义完成!")
        print(f"   📊 快照数量: {len(universe_def.snapshots)}")
        print(f"   📁 输出文件: {OUTPUT_PATH}")

        if universe_def.snapshots:
            snapshot = universe_def.snapshots[0]
            print(f"   🔍 示例快照: {snapshot.effective_date}")
            print(f"   💱 交易对数量: {len(snapshot.symbols)}")
            print(f"   📝 前5个交易对: {snapshot.symbols[:5]}")

    except Exception as e:
        print(f"❌ Universe定义失败: {e}")
        raise


if __name__ == "__main__":
    main()
