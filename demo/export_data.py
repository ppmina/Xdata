"""导出数据库数据到文件的脚本 - 简化版.

使用 storage 模块的统一导出接口，代码简洁清晰。
"""

import asyncio
from pathlib import Path

from cryptoservice.models import Freq, UniverseDefinition
from cryptoservice.storage import Database

# ============== 配置参数 ==============
UNIVERSE_FILE = "./data/universe.json"
DB_PATH = "./data/database/market.db"
EXPORT_BASE_PATH = "./data/exports"

# 导出配置
SOURCE_FREQ = Freq.h1
EXPORT_FREQ = Freq.h1
EXPORT_KLINES = True
EXPORT_METRICS = True

# Metrics 配置
METRICS_CONFIG = {
    "funding_rate": True,  # 启用资金费率
    "open_interest": True,  # 启用持仓量
    "long_short_ratio": {"ratio_type": "taker"},  # 启用多空比例（taker 类型）
}

# 自定义时间范围（可选，留空则使用 Universe 定义的时间范围）
CUSTOM_START_DATE = "2024-10-01"
CUSTOM_END_DATE = "2024-10-31"


def create_output_path(universe_config, snapshot_id: int, start_date: str, end_date: str) -> Path:
    """创建输出路径.

    Args:
        universe_config: Universe 配置
        snapshot_id: 快照ID
        start_date: 开始日期
        end_date: 结束日期

    Returns:
        输出路径
    """
    config = universe_config
    top_value = f"k{config.top_k}" if config.top_k else f"r{config.top_ratio}"

    # 如果有自定义时间范围，添加到目录名中
    if CUSTOM_START_DATE or CUSTOM_END_DATE:
        custom_suffix = f"_custom_{start_date}_{end_date}"
        dir_name = f"univ_{config.t1_months}_{config.t2_months}_{config.t3_months}_{top_value}{custom_suffix}"
    else:
        dir_name = f"univ_{config.t1_months}_{config.t2_months}_{config.t3_months}_{top_value}"

    freq_mapping = {"1d": "D1B", "1h": "H1B", "1m": "M1B", "5m": "M5B"}
    freq_dir = freq_mapping.get(EXPORT_FREQ.value, "D1B")

    return Path(EXPORT_BASE_PATH) / freq_dir / dir_name


async def main():
    """主函数 - 展示如何使用导出功能."""
    print("=" * 80)
    print("📤 开始从数据库导出数据")
    print("=" * 80)
    print(f"📋 Universe文件: {UNIVERSE_FILE}")
    print(f"💾 数据库路径: {DB_PATH}")
    print(f"📁 导出路径: {EXPORT_BASE_PATH}")
    print(f"⏱️  导出频率: {EXPORT_FREQ.value}")

    # 显示导出的特征
    features = []
    if EXPORT_KLINES:
        kline_features = ["opn", "hgh", "low", "cls", "vol", "amt", "tnum", "tbvol", "tbamt", "tsvol", "tsamt"]
        features.extend(kline_features)
    if EXPORT_METRICS:
        metrics_features = ["fr", "oi", "lsr"]
        features.extend(metrics_features)

    print(f"📊 导出特征: {len(features)} 个 - {', '.join(features)}")

    if CUSTOM_START_DATE or CUSTOM_END_DATE:
        print(f"🎯 自定义时间范围: {CUSTOM_START_DATE} 至 {CUSTOM_END_DATE}")

    print("=" * 80)

    try:
        # 1. 加载 Universe 定义
        print("\n📖 加载 Universe 定义...")
        universe_def = UniverseDefinition.load_from_file(UNIVERSE_FILE)
        print(f"   ✅ 成功加载 {len(universe_def.snapshots)} 个快照")

        # 2. 初始化数据库
        print("\n🔗 初始化数据库...")
        db = Database(DB_PATH)
        await db.initialize()
        print("   ✅ 数据库初始化成功")

        try:
            # 3. 处理每个快照
            success_count = 0
            for i, snapshot in enumerate(universe_def.snapshots):
                print(f"\n{'=' * 80}")
                print(f"📋 处理快照 {i + 1}/{len(universe_def.snapshots)}")
                print(f"{'=' * 80}")

                # 计算时间范围
                start_date = CUSTOM_START_DATE or snapshot.start_date
                end_date = CUSTOM_END_DATE or snapshot.end_date

                print(f"   📅 时间范围: {start_date} 至 {end_date}")
                print(f"   💱 交易对数量: {len(snapshot.symbols)}")
                print(f"   📝 前5个交易对: {snapshot.symbols[:5]}")

                # 创建输出路径
                output_path = create_output_path(universe_def.config, i, start_date, end_date)
                print(f"   📁 输出路径: {output_path}")

                # 4. 使用统一的导出接口
                try:
                    await db.numpy_exporter.export_combined_data(
                        symbols=snapshot.symbols,
                        start_time=start_date,
                        end_time=end_date,
                        source_freq=SOURCE_FREQ,
                        export_freq=EXPORT_FREQ,
                        output_path=output_path,
                        include_klines=EXPORT_KLINES,
                        include_metrics=EXPORT_METRICS,
                        metrics_config=METRICS_CONFIG if EXPORT_METRICS else None,
                    )

                    # 显示导出文件统计
                    if output_path.exists():
                        npy_files = list(output_path.rglob("*.npy"))
                        json_files = list(output_path.rglob("*.json"))
                        total_size = sum(f.stat().st_size for f in output_path.rglob("*") if f.is_file()) / (
                            1024 * 1024
                        )

                        print("\n   📊 导出文件统计:")
                        print(f"      • NumPy 文件: {len(npy_files)} 个")
                        print(f"      • JSON 文件: {len(json_files)} 个")
                        print(f"      • 总大小: {total_size:.1f} MB")

                    success_count += 1
                    print(f"\n   ✅ 快照 {i + 1} 导出完成")

                except Exception as e:
                    print(f"\n   ❌ 快照 {i + 1} 导出失败: {e}")
                    import traceback

                    traceback.print_exc()

            # 5. 汇总结果
            print(f"\n{'=' * 80}")
            print("🎯 导出完成汇总")
            print(f"{'=' * 80}")
            print(f"   📊 总快照数: {len(universe_def.snapshots)}")
            print(f"   ✅ 成功导出: {success_count}/{len(universe_def.snapshots)}")

            if success_count == len(universe_def.snapshots):
                print("   🎉 所有数据导出成功！")
            else:
                print("   ⚠️  部分快照导出失败，请检查日志")
            print(f"{'=' * 80}")

        finally:
            await db.close()
            print("\n🔒 数据库已关闭")

    except Exception as e:
        print(f"\n❌ 数据导出失败: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    asyncio.run(main())
