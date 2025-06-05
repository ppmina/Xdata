from pathlib import Path
from cryptoservice.models.universe import UniverseDefinition
from cryptoservice.models.enums import Freq
from cryptoservice.data import MarketDB

# ============== 配置参数 ==============
# 文件路径
UNIVERSE_FILE = "./data/universe.json"  # Universe定义文件
DB_PATH = "./data/database/market.db"  # 数据库文件路径
EXPORT_BASE_PATH = "./data/exports"  # 导出文件基础路径

# 导出配置
EXPORT_FREQ = Freq.d1  # 导出数据频率
CHUNK_DAYS = 100  # 分块天数
FEATURES = [  # 要导出的特征
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_volume",
    "trades_count",
]

# ========================================


def main():
    """从数据库导出数据脚本"""
    print("📤 开始从数据库导出数据")
    print(f"📋 Universe文件: {UNIVERSE_FILE}")
    print(f"💾 数据库路径: {DB_PATH}")
    print(f"📁 导出路径: {EXPORT_BASE_PATH}")
    print(f"⏱️ 导出频率: {EXPORT_FREQ}")
    print(f"📊 导出特征: {FEATURES}")

    # 检查必要文件是否存在
    if not Path(UNIVERSE_FILE).exists():
        print(f"❌ Universe文件不存在: {UNIVERSE_FILE}")
        print("请先运行 define_universe.py 创建Universe文件")
        return

    if not Path(DB_PATH).exists():
        print(f"❌ 数据库文件不存在: {DB_PATH}")
        print("请先运行 download_data.py 下载数据")
        return

    # 确保导出目录存在
    Path(EXPORT_BASE_PATH).mkdir(parents=True, exist_ok=True)

    try:
        # 加载Universe定义
        print("📖 加载Universe定义...")
        universe_def = UniverseDefinition.load_from_file(UNIVERSE_FILE)
        print(f"   ✅ 成功加载 {len(universe_def.snapshots)} 个快照")

        # 创建MarketDB实例
        db = MarketDB(DB_PATH)

        # 处理每个快照
        for i, snapshot in enumerate(universe_def.snapshots):
            print(
                f"\n📋 处理快照 {i+1}/{len(universe_def.snapshots)}: {snapshot.effective_date}"
            )

            period_start_ts = snapshot.period_start_ts
            period_end_ts = snapshot.period_end_ts
            symbols = snapshot.symbols

            print(f"   ⏰ 时间范围: {period_start_ts} - {period_end_ts}")
            print(f"   💱 交易对数量: {len(symbols)}")
            print(f"   📝 前5个交易对: {symbols[:5]}")

            # 创建快照专用的导出目录
            snapshot_export_path = (
                Path(EXPORT_BASE_PATH) / f"snapshot_{snapshot.effective_date}"
            )

            # 导出数据
            db.export_to_files_by_timestamp(
                output_path=snapshot_export_path,
                start_ts=period_start_ts,
                end_ts=period_end_ts,
                freq=EXPORT_FREQ,
                symbols=symbols,
                chunk_days=CHUNK_DAYS,
            )

            print(f"   ✅ 快照数据已导出到: {snapshot_export_path}")

            # 显示导出的文件信息
            if snapshot_export_path.exists():
                export_files = list(snapshot_export_path.rglob("*.csv"))
                if export_files:
                    total_size = sum(f.stat().st_size for f in export_files) / (
                        1024 * 1024
                    )  # MB
                    print(f"      📊 导出文件数量: {len(export_files)}")
                    print(f"      💾 总文件大小: {total_size:.1f} MB")
                    print(f"      📄 示例文件: {export_files[0].name}")

        print(f"\n🎉 所有数据导出完成!")
        print(f"📁 总导出路径: {EXPORT_BASE_PATH}")

        # 显示总体统计
        all_export_files = list(Path(EXPORT_BASE_PATH).rglob("*.csv"))
        if all_export_files:
            total_size = sum(f.stat().st_size for f in all_export_files) / (
                1024 * 1024
            )  # MB
            print(f"📊 总计导出文件: {len(all_export_files)} 个")
            print(f"💾 总计文件大小: {total_size:.1f} MB")

    except Exception as e:
        print(f"❌ 数据导出失败: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
