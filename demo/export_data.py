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
EXPORT_FREQ = Freq.m1  # 导出数据频率
CHUNK_DAYS = 100  # 分块天数

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
}

# ========================================


def main():
    """从数据库导出数据脚本"""
    print("📤 开始从数据库导出数据")
    print(f"📋 Universe文件: {UNIVERSE_FILE}")
    print(f"💾 数据库路径: {DB_PATH}")
    print(f"📁 导出路径: {EXPORT_BASE_PATH}")
    print(f"⏱️ 导出频率: {EXPORT_FREQ}")
    print(f"📊 导出特征: {len(EXPORT_FEATURES)}个")
    print(
        f"    {', '.join([f'{feat}({FEATURE_DESCRIPTIONS[feat]})' for feat in EXPORT_FEATURES[:5]])}..."
    )

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

        t1 = universe_def.config.t1_months
        t2 = universe_def.config.t2_months
        t3 = universe_def.config.t3_months
        top_k = universe_def.config.top_k
        delay_days = universe_def.config.delay_days
        quote_asset = universe_def.config.quote_asset

        # 创建MarketDB实例
        db = MarketDB(DB_PATH)

        # 处理每个快照
        for i, snapshot in enumerate(universe_def.snapshots):
            print(
                f"\n📋 处理快照 {i+1}/{len(universe_def.snapshots)}: {snapshot.start_date} - {snapshot.end_date}"
            )

            start_date_ts = snapshot.start_date_ts
            end_date_ts = snapshot.end_date_ts
            symbols = snapshot.symbols

            print(f"   ⏰ 时间范围: {start_date_ts} - {end_date_ts}")
            print(f"   💱 交易对数量: {len(symbols)}")
            print(f"   📝 前5个交易对: {symbols[:5]}")

            # 创建快照专用的导出目录
            snapshot_export_path = (
                Path(EXPORT_BASE_PATH)
                / f"{t1}_{t2}_{t3}_{top_k}_{delay_days}_{quote_asset}"
            )

            # 导出数据
            db.export_to_files_by_timestamp(
                output_path=snapshot_export_path,
                start_ts=start_date_ts,
                end_ts=end_date_ts,
                freq=Freq.m1,
                target_freq=EXPORT_FREQ,
                symbols=symbols,
                chunk_days=CHUNK_DAYS,
            )

            # 显示导出的文件信息
            if snapshot_export_path.exists():
                export_files = list(snapshot_export_path.rglob("*.npy"))
                universe_files = list(snapshot_export_path.rglob("universe_token.pkl"))

                if export_files:
                    total_size = sum(f.stat().st_size for f in export_files) / (
                        1024 * 1024
                    )  # MB
                    print(f"      📊 导出文件数量: {len(export_files)}个.npy文件")
                    print(f"      🎯 Universe文件: {len(universe_files)}个.pkl文件")
                    print(f"      💾 总文件大小: {total_size:.1f} MB")

                    # 显示特征分布
                    feature_dirs = [f.parent.name for f in export_files]
                    unique_features = set(feature_dirs)
                    print(
                        f"      📈 特征类型: {len(unique_features)}种 ({', '.join(sorted(unique_features))})"
                    )

        # 显示导出结构示例
        first_snapshot_dir = (
            next(Path(EXPORT_BASE_PATH).iterdir(), None)
            if Path(EXPORT_BASE_PATH).exists()
            else None
        )
        if first_snapshot_dir and first_snapshot_dir.is_dir():
            print(f"\n📂 导出文件结构示例 (基于 {first_snapshot_dir.name}):")
            freq_dirs = [d for d in first_snapshot_dir.iterdir() if d.is_dir()]
            if freq_dirs:
                freq_dir = freq_dirs[0]
                print(f"   {first_snapshot_dir.name}/")
                print(f"   └── {freq_dir.name}/")

                date_dirs = [d for d in freq_dir.iterdir() if d.is_dir()]
                if date_dirs:
                    date_dir = date_dirs[0]
                    print(f"       └── {date_dir.name}/")

                    feature_dirs = [d for d in date_dir.iterdir() if d.is_dir()]
                    universe_file = date_dir / "universe_token.pkl"

                    for i, feature_dir in enumerate(sorted(feature_dirs)[:3]):
                        desc = FEATURE_DESCRIPTIONS.get(
                            feature_dir.name, feature_dir.name
                        )
                        print(f"           ├── {feature_dir.name}/  # {desc}")
                        npy_files = list(feature_dir.glob("*.npy"))
                        if npy_files:
                            print(f"           │   └── {npy_files[0].name}")

                    if len(feature_dirs) > 3:
                        print(
                            f"           ├── ... (还有 {len(feature_dirs)-3} 个特征目录)"
                        )

                    if universe_file.exists():
                        print(f"           └── universe_token.pkl  # 交易对顺序")

        # 显示总体统计
        all_export_files = list(Path(EXPORT_BASE_PATH).rglob("*.npy"))
        all_universe_files = list(Path(EXPORT_BASE_PATH).rglob("universe_token.pkl"))

        if all_export_files:
            total_size = sum(f.stat().st_size for f in all_export_files) / (
                1024 * 1024
            )  # MB
            print(f"📊 总计导出文件: {len(all_export_files)}个.npy文件")
            print(f"🎯 总计Universe文件: {len(all_universe_files)}个.pkl文件")
            print(f"💾 总计文件大小: {total_size:.1f} MB")

            # 统计所有特征类型
            all_features = set(f.parent.name for f in all_export_files)
            print(f"📈 包含特征: {len(all_features)}种")
            print(f"    完整特征列表: {', '.join(sorted(all_features))}")

    except Exception as e:
        print(f"❌ 数据导出失败: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
