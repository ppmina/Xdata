"""导出数据库数据到文件的脚本."""

import asyncio
import os
from pathlib import Path

import dotenv
import pandas as pd

from cryptoservice.models import Freq, UniverseDefinition
from cryptoservice.services import MarketDataService
from cryptoservice.storage import Database

# ============== 配置参数 ==============
UNIVERSE_FILE = "./data/universe.json"
DB_PATH = "./data/database/market.db"
EXPORT_BASE_PATH = "./data/exports"

# 数据导出配置
DATA_FREQ = Freq.d1
EXPORT_FREQ = Freq.d1
CHUNK_DAYS = 100

# 导出选项
EXPORT_KLINES = True  # 导出K线数据
EXPORT_METRICS = True  # 导出指标数据（fr, oi, lsr）
DOWNLOAD_CATEGORIES = True  # 下载分类数据

# 自定义时间范围（可选）
CUSTOM_START_DATE = "2024-10-01"
CUSTOM_END_DATE = "2024-10-02"

# 字段名映射：长字段名 -> 缩写形式
FIELD_MAPPING = {
    # K线数据字段
    "open_price": "opn",
    "high_price": "hgh",
    "low_price": "low",
    "close_price": "cls",
    "volume": "vol",
    "quote_volume": "amt",
    "trades_count": "tnum",
    "taker_buy_volume": "tbvol",
    "taker_buy_quote_volume": "tbamt",
    "taker_sell_volume": "tsvol",
    "taker_sell_quote_volume": "tsamt",
    # Metrics数据字段（已经是缩写）
    "funding_rate": "fr",
    "open_interest": "oi",
    # 多空比例只导出taker类型（Vision数据最完整的类型）
    "long_short_ratio": "lsr",
}


async def validate_and_load_universe() -> UniverseDefinition:
    """验证并加载Universe定义."""
    if not Path(UNIVERSE_FILE).exists():
        raise FileNotFoundError(f"Universe文件不存在: {UNIVERSE_FILE}")

    if not Path(DB_PATH).exists():
        raise FileNotFoundError(f"数据库文件不存在: {DB_PATH}")

    Path(EXPORT_BASE_PATH).mkdir(parents=True, exist_ok=True)

    print("📖 加载Universe定义...")
    universe_def = UniverseDefinition.load_from_file(UNIVERSE_FILE)
    print(f"   ✅ 成功加载 {len(universe_def.snapshots)} 个快照")

    return universe_def


async def create_market_service():
    """创建市场服务."""
    if not DOWNLOAD_CATEGORIES:
        return None

    try:
        print("🔗 初始化市场服务...")
        dotenv.load_dotenv()
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_API_SECRET")
        market_service_ctx = await MarketDataService.create(api_key=api_key, api_secret=api_secret)
        print("   ✅ 市场服务初始化成功")
        return market_service_ctx
    except Exception as e:
        print(f"   ⚠️ 市场服务初始化失败: {e}")
        return None


def calculate_time_range(snapshot):
    """计算实际导出的时间范围."""
    universe_start_ts = int(snapshot.start_date_ts)
    universe_end_ts = int(snapshot.end_date_ts)

    if CUSTOM_START_DATE:
        custom_start_ts = int(pd.Timestamp(CUSTOM_START_DATE).timestamp() * 1000)
        actual_start_ts = max(custom_start_ts, universe_start_ts)
    else:
        actual_start_ts = universe_start_ts

    if CUSTOM_END_DATE:
        custom_end_ts = int(pd.Timestamp(CUSTOM_END_DATE).timestamp() * 1000)
        actual_end_ts = min(custom_end_ts, universe_end_ts)
    else:
        actual_end_ts = universe_end_ts

    start_date = pd.Timestamp(int(actual_start_ts), unit="ms").strftime("%Y-%m-%d")
    end_date = pd.Timestamp(int(actual_end_ts), unit="ms").strftime("%Y-%m-%d")

    return actual_start_ts, actual_end_ts, start_date, end_date


def rename_fields_to_abbreviations(df: pd.DataFrame) -> pd.DataFrame:
    """将字段名重命名为缩写形式."""
    if df.empty:
        return df

    # 获取需要重命名的字段
    columns_to_rename = {col: FIELD_MAPPING[col] for col in df.columns if col in FIELD_MAPPING}

    if columns_to_rename:
        df = df.rename(columns=columns_to_rename)
        renamed_fields = list(columns_to_rename.values())
        print(f"      🔤 字段重命名: {len(columns_to_rename)} 个字段 -> {', '.join(renamed_fields)}")

    return df


async def get_kline_data(db: Database, symbols: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """获取K线数据."""
    if not EXPORT_KLINES:
        return pd.DataFrame()

    kline_df = await db.kline_query.select_by_time_range(symbols, start_date, end_date, DATA_FREQ)
    if not kline_df.empty:
        print(f"      ✅ 获取K线数据: {len(kline_df)} 条记录")
        # 重命名字段为缩写形式
        kline_df = rename_fields_to_abbreviations(kline_df)
    return kline_df


def resample_to_daily(df: pd.DataFrame, feature_name: str) -> pd.DataFrame:
    """将高频数据重采样到日级别."""
    if df.empty:
        return df

    # 将timestamp转换为日期
    df_copy = df.copy()
    df_copy["date"] = pd.to_datetime(df_copy.index.get_level_values("timestamp"), unit="ms").date

    # 按symbol和date分组，取每日最后一个值（代表该日的收盘值）
    daily_df = df_copy.groupby(["symbol", "date"]).last()

    # 重构索引：将date转换回timestamp（每日0点的时间戳，毫秒级）
    daily_df = daily_df.reset_index()
    # 修复：正确转换为毫秒时间戳
    daily_df["timestamp"] = pd.to_datetime(daily_df["date"]).astype("int64") // 10**6
    daily_df = daily_df.set_index(["symbol", "timestamp"])
    daily_df = daily_df.drop("date", axis=1)

    print(f"      🔄 {feature_name}数据重采样: {len(df)} -> {len(daily_df)} 条记录")
    return daily_df


async def merge_metrics_data(
    db: Database, combined_df: pd.DataFrame, symbols: list[str], start_date: str, end_date: str
) -> tuple[pd.DataFrame, int]:
    """合并指标数据."""
    if not EXPORT_METRICS:
        return combined_df, 0

    metrics_added = 0

    # 资金费率数据 -> fr (每8小时，取每日最后一个值)
    try:
        fr_df = await db.metrics_query.select_funding_rates(symbols, start_date, end_date, ["funding_rate"])
        if not fr_df.empty:
            # 重采样到日级别
            fr_df = resample_to_daily(fr_df, "资金费率")
            # 重命名字段为缩写形式
            fr_df = rename_fields_to_abbreviations(fr_df)
            combined_df = fr_df if combined_df.empty else pd.concat([combined_df, fr_df], axis=1, join="outer")
            metrics_added += 1
            print(f"      ✅ 合并资金费率数据: {len(fr_df)} 条记录")
    except Exception as e:
        print(f"      ⚠️ 资金费率数据获取失败: {e}")

    # 持仓量数据 -> oi (Vision高频数据，取每日最后一个值)
    try:
        oi_df = await db.metrics_query.select_open_interests(symbols, start_date, end_date, columns=["open_interest"])
        if not oi_df.empty:
            # 重采样到日级别
            oi_df = resample_to_daily(oi_df, "持仓量")
            # 重命名字段为缩写形式
            oi_df = rename_fields_to_abbreviations(oi_df)
            combined_df = oi_df if combined_df.empty else pd.concat([combined_df, oi_df], axis=1, join="outer")
            metrics_added += 1
            print(f"      ✅ 合并持仓量数据: {len(oi_df)} 条记录")
    except Exception as e:
        print(f"      ⚠️ 持仓量数据获取失败: {e}")

    # 多空比例数据 -> 只导出taker类型 (Vision数据最完整的类型)
    try:
        lsr_df = await db.metrics_query.select_long_short_ratios(
            symbols, start_date, end_date, ratio_type="taker", columns=["long_short_ratio"]
        )
        if not lsr_df.empty:
            # 重采样到日级别
            lsr_df = resample_to_daily(lsr_df, "多空比例")
            # 重命名字段为缩写形式
            lsr_df = rename_fields_to_abbreviations(lsr_df)
            combined_df = lsr_df if combined_df.empty else pd.concat([combined_df, lsr_df], axis=1, join="outer")
            metrics_added += 1
            print(f"      ✅ 合并多空比例数据: {len(lsr_df)} 条记录")
    except Exception as e:
        print(f"      ⚠️ 多空比例数据获取失败: {e}")

    print(f"      📊 成功合并 {metrics_added} 种指标数据")
    return combined_df, metrics_added


async def export_combined_data(db: Database, symbols: list[str], start_date: str, end_date: str, output_path: Path):
    """导出合并的K线和指标数据."""
    try:
        print("   📈 导出K线和指标数据...")

        # 获取K线数据
        kline_df = await get_kline_data(db, symbols, start_date, end_date)
        combined_df = kline_df.copy() if not kline_df.empty else pd.DataFrame()

        # 合并指标数据
        combined_df, _ = await merge_metrics_data(db, combined_df, symbols, start_date, end_date)

        if combined_df.empty:
            print("      ⚠️ 没有数据可导出")
            return False

        # 注意：数据已经在各自的处理函数中重采样到日级别，这里不再进行二次重采样
        export_freq = DATA_FREQ

        # 使用numpy_exporter的内部方法直接导出
        await db.numpy_exporter._export_by_dates(combined_df, output_path, export_freq)

        print(f"      ✅ 数据导出完成: {len(combined_df.columns)} 个特征，{len(combined_df)} 条记录")
        return True

    except Exception as e:
        print(f"   ❌ 数据导出失败: {e}")
        return False


async def export_categories(market_service_ctx, output_path: Path):
    """导出分类数据."""
    if not DOWNLOAD_CATEGORIES or market_service_ctx is None:
        return False

    try:
        print("   📂 下载分类数据...")
        async with market_service_ctx as market_service:
            market_service.download_and_save_categories_for_universe(
                universe_file=UNIVERSE_FILE,
                output_path=output_path,
            )
        print("   ✅ 分类数据下载成功")
        return True
    except Exception as e:
        print(f"   ❌ 分类数据下载失败: {e}")
        return False


def create_output_path(universe_config, start_date: str, end_date: str) -> Path:
    """创建输出路径."""
    config = universe_config
    top_value = f"k{config.top_k}" if config.top_k else f"r{config.top_ratio}"

    if CUSTOM_START_DATE or CUSTOM_END_DATE:
        custom_suffix = f"_custom_{start_date}_{end_date}"
        dir_name = f"univ_{config.t1_months}_{config.t2_months}_{config.t3_months}_{top_value}{custom_suffix}"
    else:
        dir_name = f"univ_{config.t1_months}_{config.t2_months}_{config.t3_months}_{top_value}"

    return Path(EXPORT_BASE_PATH) / dir_name


def show_export_summary(output_path: Path):
    """显示导出文件摘要."""
    if output_path.exists():
        npy_files = list(output_path.rglob("*.npy"))
        csv_files = list(output_path.rglob("*.csv"))
        pkl_files = list(output_path.rglob("*.pkl"))

        total_size = sum(f.stat().st_size for f in output_path.rglob("*") if f.is_file()) / (1024 * 1024)

        print("      📊 导出文件总览:")
        print(f"         • NumPy文件: {len(npy_files)}个")
        print(f"         • CSV文件: {len(csv_files)}个")
        print(f"         • PKL文件: {len(pkl_files)}个")
        print(f"         • 总大小: {total_size:.1f} MB")


async def process_snapshot(
    snapshot, snapshot_id: int, total_snapshots: int, universe_config, db: Database, market_service_ctx
):
    """处理单个快照的导出."""
    print(f"\n📋 处理快照 {snapshot_id}/{total_snapshots}: {snapshot.start_date} - {snapshot.end_date}")

    # 计算时间范围
    actual_start_ts, actual_end_ts, start_date, end_date = calculate_time_range(snapshot)
    symbols = snapshot.symbols

    # 显示信息
    if CUSTOM_START_DATE or CUSTOM_END_DATE:
        print(f"   📅 Universe时间范围: {snapshot.start_date} - {snapshot.end_date}")
        print(f"   🎯 实际导出范围: {start_date} - {end_date}")
    else:
        print(f"   ⏰ 导出时间范围: {start_date} - {end_date}")

    print(f"   💱 交易对数量: {len(symbols)}")
    print(f"   📝 前5个交易对: {symbols[:5]}")

    # 检查时间范围
    if actual_start_ts >= actual_end_ts:
        print("   ⚠️ 警告: 导出时间范围为空，跳过此快照")
        return {"success": False, "reason": "Empty time range"}

    # 创建输出路径
    output_path = create_output_path(universe_config, start_date, end_date)

    # 导出数据
    results = {}
    results["data"] = await export_combined_data(db, symbols, start_date, end_date, output_path)
    results["categories"] = await export_categories(market_service_ctx, output_path)

    # 显示摘要
    show_export_summary(output_path)

    return {"success": any(results.values()), "details": results, "output_path": output_path}


async def main():
    """主函数."""
    print("📤 开始从数据库导出数据")
    print(f"📋 Universe文件: {UNIVERSE_FILE}")
    print(f"💾 数据库路径: {DB_PATH}")
    print(f"📁 导出路径: {EXPORT_BASE_PATH}")
    print(f"⏱️ 导出频率: {EXPORT_FREQ}")

    # 显示导出的特征
    kline_features = ["opn", "hgh", "low", "cls", "vol", "amt", "tnum", "tbvol", "tbamt", "tsvol", "tsamt"]
    metrics_features = ["fr", "oi", "lsr"]

    if EXPORT_KLINES and EXPORT_METRICS:
        all_features = kline_features + metrics_features
        print(f"📊 导出特征: {len(all_features)}个 - {', '.join(all_features)}")
    elif EXPORT_KLINES:
        print(f"📈 导出K线特征: {len(kline_features)}个 - {', '.join(kline_features)}")
    elif EXPORT_METRICS:
        print(f"📊 导出指标特征: {len(metrics_features)}个 - {', '.join(metrics_features)}")

    print(f"🌐 分类数据下载: {'启用' if DOWNLOAD_CATEGORIES else '禁用'}")

    if CUSTOM_START_DATE or CUSTOM_END_DATE:
        print("🎯 自定义时间范围:")
        if CUSTOM_START_DATE:
            print(f"    📅 开始日期: {CUSTOM_START_DATE}")
        if CUSTOM_END_DATE:
            print(f"    📅 结束日期: {CUSTOM_END_DATE}")

    try:
        # 加载配置
        universe_def = await validate_and_load_universe()
        market_service_ctx = await create_market_service()

        # 初始化数据库
        db = Database(DB_PATH)
        await db.initialize()

        try:
            # 处理每个快照
            results = []
            for i, snapshot in enumerate(universe_def.snapshots):
                result = await process_snapshot(
                    snapshot,
                    i + 1,
                    len(universe_def.snapshots),
                    universe_def.config,
                    db,
                    market_service_ctx,
                )
                results.append(result)

            # 汇总结果
            print("\n" + "=" * 60)
            print("🎯 导出完成汇总:")
            successful = sum(1 for r in results if r["success"])
            print(f"   📊 总快照数: {len(results)}")
            print(f"   ✅ 成功导出: {successful}/{len(results)}")

            # 显示具体导出情况
            data_success = sum(1 for r in results if r["details"].get("data", False))
            category_success = sum(1 for r in results if r["details"].get("categories", False))
            print(f"   📊 数据导出成功: {data_success}/{len(results)}")
            print(f"   📂 分类数据成功: {category_success}/{len(results)}")

            if successful == len(results):
                print("   🎉 所有数据导出成功！")
            else:
                print("   ⚠️ 部分快照导出失败，请检查日志")
            print("=" * 60)

        finally:
            await db.close()

    except Exception as e:
        print(f"❌ 数据导出失败: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    asyncio.run(main())
