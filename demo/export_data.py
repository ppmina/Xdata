"""导出数据库数据到文件的脚本 - 简化版.

使用 storage 模块的统一导出接口，代码简洁清晰。
"""

import asyncio
from pathlib import Path

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq, UniverseDefinition
from cryptoservice.storage import Database
from cryptoservice.utils.cli_helper import print_summary, print_completion_stats

logger = get_logger(__name__)

# ============== 配置参数 ==============
UNIVERSE_FILE = "./data/universe.json"
DB_PATH = "./data/database/market.db"
EXPORT_BASE_PATH = "./data/exports"

# 导出配置
SOURCE_FREQ = Freq.m5
EXPORT_FREQ = Freq.m5
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
    logger.info(f"开始导出数据：来源 {UNIVERSE_FILE}，输出目录 {EXPORT_BASE_PATH}。")

    # 构建导出特征列表
    features = []
    if EXPORT_KLINES:
        kline_features = ["opn", "hgh", "low", "cls", "vol", "amt", "tnum", "tbvol", "tbamt", "tsvol", "tsamt"]
        features.extend(kline_features)
    if EXPORT_METRICS:
        metrics_features = ["fr", "oi", "lsr"]
        features.extend(metrics_features)

    logger.debug(
        "导出配置",
        universe_file=UNIVERSE_FILE,
        db_path=DB_PATH,
        export_path=EXPORT_BASE_PATH,
        export_freq=EXPORT_FREQ.value,
        features_count=len(features),
        features=", ".join(features),
    )

    if CUSTOM_START_DATE or CUSTOM_END_DATE:
        logger.debug(f"使用自定义导出范围：{CUSTOM_START_DATE} ~ {CUSTOM_END_DATE}")

    try:
        # 1. 加载 Universe 定义
        logger.debug(f"加载 Universe 定义：{UNIVERSE_FILE}")
        universe_def = UniverseDefinition.load_from_file(UNIVERSE_FILE)
        logger.debug(f"已加载 {len(universe_def.snapshots)} 个快照")

        # 2. 初始化数据库
        logger.debug(f"初始化数据库：{DB_PATH}")
        db = Database(DB_PATH)
        await db.initialize()
        logger.debug("数据库准备完成")

        try:
            # 3. 处理每个快照
            success_count = 0
            total_npy_files = 0
            total_json_files = 0
            total_size_mb = 0.0

            for i, snapshot in enumerate(universe_def.snapshots):
                # 计算时间范围
                start_date = CUSTOM_START_DATE or snapshot.start_date
                end_date = CUSTOM_END_DATE or snapshot.end_date

                logger.debug(
                    f"处理快照 {i + 1}/{len(universe_def.snapshots)}：{snapshot.effective_date}" \
                    f"（{start_date} ~ {end_date}，{len(snapshot.symbols)} 个交易对）"
                )

                # 创建输出路径
                output_path = create_output_path(universe_def.config, i, start_date, end_date)
                logger.debug(f"输出目录：{output_path}")

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
                        size_mb = sum(f.stat().st_size for f in output_path.rglob("*") if f.is_file()) / (
                            1024 * 1024
                        )

                        total_npy_files += len(npy_files)
                        total_json_files += len(json_files)
                        total_size_mb += size_mb

                        logger.debug(
                            f"导出统计：NPY={len(npy_files)}，JSON={len(json_files)}，总体积 {size_mb:.1f} MB"
                        )

                    success_count += 1
                    logger.debug(f"快照 {i + 1} 导出完成。")

                except Exception as e:
                    logger.error(f"快照 {i + 1} 导出失败：{e}", exc_info=True)

            # 5. 显示执行总结
            failed_count = len(universe_def.snapshots) - success_count
            completion_rate = (success_count / len(universe_def.snapshots) * 100) if universe_def.snapshots else 0

            # 确定执行状态
            if failed_count == 0:
                status = "success"
            elif success_count > 0:
                status = "partial"
            else:
                status = "failed"

        finally:
            await db.close()

            logger.info("数据导出任务完成。")

            # 显示总结
            print_summary(
                title="数据导出总结",
                status=status,
                items={
                    "导出频率": EXPORT_FREQ.value,
                    "输出路径": EXPORT_BASE_PATH,
                    "总快照数": len(universe_def.snapshots),
                    "成功导出": success_count,
                    "失败数量": failed_count,
                    "完成率": completion_rate,
                    "NPY 文件数": total_npy_files,
                    "JSON 文件数": total_json_files,
                    "总大小 (MB)": f"{total_size_mb:.1f}",
                    "包含K线": EXPORT_KLINES,
                    "包含指标": EXPORT_METRICS,
                },
            )

    except Exception as e:
        logger.error(f"数据导出失败：{e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
