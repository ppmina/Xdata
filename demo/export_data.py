"""导出数据库数据到文件的脚本."""

import asyncio
import os
from pathlib import Path

import dotenv

from cryptoservice.models.enums import Freq
from cryptoservice.models.universe import UniverseDefinition
from cryptoservice.services.market_service import MarketDataService
from cryptoservice.storage import AsyncMarketDB

# ============== 配置参数 ==============
# 文件路径
UNIVERSE_FILE = "./data/universe.json"  # Universe定义文件
DB_PATH = "./data/database/market.db"  # 数据库文件路径
EXPORT_BASE_PATH = "./data/exports"  # 导出文件基础路径

# 导出配置
DATA_FREQ = Freq.d1  # 数据库数据频率
EXPORT_FREQ = Freq.d1  # 导出数据频率
CHUNK_DAYS = 100  # 分块天数
DOWNLOAD_CATEGORIES = True  # 是否下载类别数据（网络连接失败时会自动跳过，无需手动设为False）

# 导出的特征（短字段名格式，按指定顺序）
EXPORT_FEATURES = [
    "cls",
    "hgh",
    "low",
    "tnum",
    "opn",
    "amt",
    "tbvol",
    "tbamt",
    "vol",
    "vwap",
    "ret",
    "tsvol",
    "tsamt",
    # 新特征（三个核心特征）
    "fr",  # 资金费率
    "oi",  # 持仓量
    "lsr",  # 多空比例
]

# 特征描述（用于显示）
FEATURE_DESCRIPTIONS = {
    "cls": "收盘价",
    "hgh": "最高价",
    "low": "最低价",
    "tnum": "交易笔数",
    "opn": "开盘价",
    "amt": "成交额",
    "tbvol": "主动买入量",
    "tbamt": "主动买入额",
    "vol": "成交量",
    "vwap": "VWAP",
    "ret": "收益率",
    "tsvol": "主动卖出量",
    "tsamt": "主动卖出额",
    # 新特征描述
    "fr": "资金费率",
    "oi": "持仓量",
    "lsr": "多空比例",
}

# ========================================


async def validate_prerequisites() -> tuple[UniverseDefinition, bool]:
    """验证必要文件并加载Universe定义."""
    if not Path(UNIVERSE_FILE).exists():
        print(f"❌ Universe文件不存在: {UNIVERSE_FILE}")
        print("请先运行 define_universe.py 创建Universe文件")
        raise FileNotFoundError(f"Universe file not found: {UNIVERSE_FILE}")

    if not Path(DB_PATH).exists():
        print(f"❌ 数据库文件不存在: {DB_PATH}")
        print("请先运行 download_data.py 下载数据")
        raise FileNotFoundError(f"Database file not found: {DB_PATH}")

    # 确保导出目录存在
    Path(EXPORT_BASE_PATH).mkdir(parents=True, exist_ok=True)

    # 加载Universe定义
    print("📖 加载Universe定义...")
    universe_def = UniverseDefinition.load_from_file(UNIVERSE_FILE)
    print(f"   ✅ 成功加载 {len(universe_def.snapshots)} 个快照")

    return universe_def, True


async def initialize_market_service():
    """初始化市场服务（如果需要下载分类数据）."""
    if not DOWNLOAD_CATEGORIES:
        return None

    try:
        print("🔗 初始化市场服务（用于下载分类数据）...")
        dotenv.load_dotenv()
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_API_SECRET")
        market_service_ctx = await MarketDataService.create(api_key=api_key, api_secret=api_secret)
        print("   ✅ 市场服务初始化成功")
        return market_service_ctx
    except Exception as e:
        print(f"   ⚠️ 市场服务初始化失败，将跳过分类数据下载: {e}")
        print("   💡 提示：如需下载分类数据，请检查网络连接和API配置")
        return None


async def process_snapshot(snapshot, snapshot_id, total_snapshots, universe_config, db, market_service_ctx):
    """处理单个快照的导出."""
    print(f"\n📋 处理快照 {snapshot_id}/{total_snapshots}: {snapshot.start_date} - {snapshot.end_date}")

    start_date_ts = snapshot.start_date_ts
    end_date_ts = snapshot.end_date_ts
    symbols = snapshot.symbols

    print(f"   ⏰ 时间范围: {start_date_ts} - {end_date_ts}")
    print(f"   💱 交易对数量: {len(symbols)}")
    print(f"   📝 前5个交易对: {symbols[:5]}")

    # 创建快照专用的导出目录
    config = universe_config
    top_value = f"k{config.top_k}" if config.top_k else f"r{config.top_ratio}"
    dir_name = f"univ_{config.t1_months}_{config.t2_months}_{config.t3_months}_{top_value}"
    snapshot_export_path = Path(EXPORT_BASE_PATH) / dir_name

    # 初始化结果状态
    result = {
        "snapshot_id": snapshot_id,
        "date_range": f"{snapshot.start_date} - {snapshot.end_date}",
        "data_export": False,
        "category_export": False,
        "export_path": snapshot_export_path,
        "error": None,
    }

    try:
        # 导出数据
        await db.export_to_files_by_timestamp(
            output_path=snapshot_export_path,
            start_ts=start_date_ts,
            end_ts=end_date_ts,
            freq=DATA_FREQ,
            target_freq=EXPORT_FREQ,
            symbols=symbols,
            chunk_days=CHUNK_DAYS,
        )
        result["data_export"] = True
        print("   ✅ 主要数据导出成功")

        # 尝试下载分类数据
        if DOWNLOAD_CATEGORIES and market_service_ctx is not None:
            try:
                print("   📊 下载分类数据...")
                async with market_service_ctx as market_service:
                    market_service.download_and_save_categories_for_universe(
                        universe_file=UNIVERSE_FILE,
                        output_path=snapshot_export_path,
                    )
                result["category_export"] = True
                print("   ✅ 分类数据下载成功")
            except Exception as e:
                print(f"   ⚠️ 分类数据下载失败（跳过）: {e}")
                print("   💡 提示：可以稍后单独下载分类数据，或设置 DOWNLOAD_CATEGORIES = False")
        elif DOWNLOAD_CATEGORIES and market_service_ctx is None:
            print("   ⚠️ 跳过分类数据下载（市场服务未初始化）")

        # 显示导出的文件信息
        display_export_info(snapshot_export_path)

    except Exception as e:
        result["error"] = str(e)
        print(f"   ❌ 快照导出失败: {e}")

    return result


def display_export_info(snapshot_export_path):
    """显示导出文件信息."""
    if snapshot_export_path.exists():
        export_files = list(snapshot_export_path.rglob("*.npy"))
        universe_files = list(snapshot_export_path.rglob("universe_token.pkl"))

        if export_files:
            total_size = sum(f.stat().st_size for f in export_files) / (1024 * 1024)  # MB
            print(f"      📊 导出文件数量: {len(export_files)}个.npy文件")
            print(f"      🎯 Universe文件: {len(universe_files)}个.pkl文件")
            print(f"      💾 总文件大小: {total_size:.1f} MB")

            # 显示特征分布
            feature_dirs = [f.parent.name for f in export_files]
            unique_features = set(feature_dirs)
            print(f"      📈 特征类型: {len(unique_features)}种 ({', '.join(sorted(unique_features))})")


def print_final_summary(export_results):
    """打印最终汇总."""
    print("\n" + "=" * 60)
    print("🎯 导出完成汇总:")
    successful_data = sum(1 for r in export_results if r["data_export"])
    successful_categories = sum(1 for r in export_results if r["category_export"])
    total_snapshots = len(export_results)

    print(f"   📊 总快照数: {total_snapshots}")
    print(f"   ✅ 数据导出成功: {successful_data}/{total_snapshots}")
    print(f"   📂 分类数据成功: {successful_categories}/{total_snapshots}")

    if successful_data == total_snapshots:
        print("   🎉 所有数据导出成功！")
    else:
        print("   ⚠️ 部分快照导出失败，请检查日志")

    if DOWNLOAD_CATEGORIES and successful_categories < total_snapshots:
        print("   💡 提示：如需重新下载分类数据，可单独运行或调整网络设置")

    print("=" * 60)


async def main():
    """从数据库导出数据脚本."""
    print("📤 开始从数据库导出数据")
    print(f"📋 Universe文件: {UNIVERSE_FILE}")
    print(f"💾 数据库路径: {DB_PATH}")
    print(f"📁 导出路径: {EXPORT_BASE_PATH}")
    print(f"⏱️ 导出频率: {EXPORT_FREQ}")
    print(f"📊 导出特征: {len(EXPORT_FEATURES)}个")
    print(f"    {', '.join([f'{feat}({FEATURE_DESCRIPTIONS[feat]})' for feat in EXPORT_FEATURES[:5]])}...")
    print(f"🌐 分类数据下载: {'启用' if DOWNLOAD_CATEGORIES else '禁用'}（网络连接失败时会自动跳过）")

    try:
        # 验证前提条件并加载Universe
        universe_def, _ = await validate_prerequisites()

        # 初始化市场服务
        market_service_ctx = await initialize_market_service()

        # 创建MarketDB实例
        db = AsyncMarketDB(DB_PATH)
        export_results = []

        try:
            # 处理每个快照
            for i, snapshot in enumerate(universe_def.snapshots):
                result = await process_snapshot(
                    snapshot, i + 1, len(universe_def.snapshots), universe_def.config, db, market_service_ctx
                )
                export_results.append(result)

        finally:
            await db.close()

        # 打印最终汇总
        print_final_summary(export_results)

    except Exception as e:
        print(f"❌ 数据导出失败: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    asyncio.run(main())
