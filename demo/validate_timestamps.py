"""验证导出数据的 timestamp 对齐质量.

这个脚本演示如何：
1. 读取导出的 timestamp 数据
2. 验证对齐质量
3. 生成质量报告
"""

import json
from pathlib import Path
from typing import cast

import numpy as np


def extract_timestamps(timestamp_array: np.ndarray, metadata: dict, ts_type: str) -> np.ndarray:
    """从合并数组中提取特定类型的 timestamp.

    Args:
        timestamp_array: 合并的 timestamp 数组
        metadata: 元数据字典
        ts_type: timestamp 类型（如 "kline_timestamp"）

    Returns:
        提取的 timestamp 数组
    """
    order = metadata["order"]
    columns = metadata["columns"]

    if ts_type not in order:
        raise ValueError(f"{ts_type} not found in timestamp data")

    # 计算起始列索引
    start_col = sum(columns[ts] for ts in order[: order.index(ts_type)])
    end_col = start_col + columns[ts_type]

    return timestamp_array[:, start_col:end_col]


def load_timestamps(data_dir: Path, date: str) -> dict[str, np.ndarray]:
    """加载合并的 timestamp 文件.

    Args:
        data_dir: 数据目录
        date: 日期字符串（YYYYMMDD）

    Returns:
        timestamp 字典
    """
    # 读取合并的 timestamp 文件
    timestamp_file = data_dir / "timestamp" / f"{date}.npy"
    meta_file = data_dir / "timestamp" / f"{date}_meta.json"

    if not timestamp_file.exists():
        print(f"❌ Timestamp 文件不存在: {timestamp_file}")
        return {}

    # 加载数据
    timestamp_array = np.load(timestamp_file)
    print(f"✅ 加载合并 timestamp: {timestamp_array.shape}")

    # 加载元数据
    with open(meta_file, encoding="utf-8") as f:
        metadata = json.load(f)

    print(f"ℹ️  Timestamp 顺序: {' -> '.join(metadata['order'])}")
    print(f"ℹ️  每部分列数: {metadata['columns']}")

    # 提取各类型 timestamp
    timestamps = {}
    for ts_type in metadata["order"]:
        ts_array = extract_timestamps(timestamp_array, metadata, ts_type)
        timestamps[ts_type] = ts_array
        print(f"✅ 提取 {ts_type}: {ts_array.shape}")

    return timestamps


def validate_alignment_quality(timestamps: dict[str, np.ndarray]) -> dict:
    """验证对齐质量.

    Args:
        timestamps: timestamp 字典

    Returns:
        质量报告
    """
    if "kline_timestamp" not in timestamps:
        raise ValueError("缺少 kline_timestamp，无法验证对齐质量")

    kline_ts = timestamps["kline_timestamp"]
    report = {}

    for ts_type, metrics_ts in timestamps.items():
        if ts_type == "kline_timestamp":
            continue

        # 创建有效数据的 mask（排除 0 值，0 表示数据不存在）
        valid_mask = (kline_ts != 0) & (metrics_ts != 0)

        if valid_mask.sum() == 0:
            print(f"⚠️  {ts_type}: 没有有效数据可验证")
            continue

        # 只对有效数据计算时间差
        valid_kline = kline_ts[valid_mask]
        valid_metrics = metrics_ts[valid_mask]

        # 计算时间差（毫秒）
        time_diff_ms = valid_kline - valid_metrics

        # 转换为分钟（明确转换为 float）
        time_diff_min = time_diff_ms.astype(float) / 1000.0 / 60.0

        # 统计
        report[ts_type] = {
            "mean_min": float(time_diff_min.mean()),
            "median_min": float(np.median(time_diff_min)),
            "max_min": float(time_diff_min.max()),
            "min_min": float(time_diff_min.min()),
            "std_min": float(time_diff_min.std()),
            "within_5min": int((np.abs(time_diff_min) <= 5).sum()),
            "within_30min": int((np.abs(time_diff_min) <= 30).sum()),
            "within_60min": int((np.abs(time_diff_min) <= 60).sum()),
            "total": int(time_diff_min.size),
            "shape": list(time_diff_min.shape),
        }

        # 计算比例
        total = cast(int, report[ts_type]["total"])
        within_5min = cast(int, report[ts_type]["within_5min"])
        within_30min = cast(int, report[ts_type]["within_30min"])
        within_60min = cast(int, report[ts_type]["within_60min"])
        report[ts_type]["within_5min_pct"] = within_5min / total * 100
        report[ts_type]["within_30min_pct"] = within_30min / total * 100
        report[ts_type]["within_60min_pct"] = within_60min / total * 100

    return report


def print_report(report: dict):
    """打印质量报告.

    Args:
        report: 质量报告字典
    """
    print("\n" + "=" * 80)
    print("📊 时间对齐质量报告")
    print("=" * 80)

    for ts_type, stats in report.items():
        print(f"\n{ts_type}:")
        print(f"  数据维度: {stats['shape']}")
        print(f"  平均时间差: {stats['mean_min']:.2f} 分钟")
        print(f"  中位数时间差: {stats['median_min']:.2f} 分钟")
        print(f"  最大时间差: {stats['max_min']:.2f} 分钟")
        print(f"  最小时间差: {stats['min_min']:.2f} 分钟")
        print(f"  标准差: {stats['std_min']:.2f} 分钟")
        print(f"  在 5 分钟内: {stats['within_5min']}/{stats['total']} ({stats['within_5min_pct']:.1f}%)")
        print(f"  在 30 分钟内: {stats['within_30min']}/{stats['total']} ({stats['within_30min_pct']:.1f}%)")
        print(f"  在 60 分钟内: {stats['within_60min']}/{stats['total']} ({stats['within_60min_pct']:.1f}%)")

        # 质量评估
        if stats["within_5min_pct"] >= 95:
            quality = "✅ 优秀"
        elif stats["within_30min_pct"] >= 95:
            quality = "✅ 良好"
        elif stats["within_60min_pct"] >= 95:
            quality = "⚠️  一般"
        else:
            quality = "❌ 较差"

        print(f"  质量评估: {quality}")

    print("\n" + "=" * 80)


def check_timestamp_validity(timestamps: dict[str, np.ndarray]) -> dict:
    """检查 timestamp 的有效性.

    Args:
        timestamps: timestamp 字典

    Returns:
        验证结果
    """
    checks = {}

    for ts_type, ts_array in timestamps.items():
        checks[ts_type] = {
            "all_positive": bool((ts_array > 0).all()),  # 所有值都为正
            "no_zero": bool((ts_array != 0).all()),  # 没有零值
            "reasonable_range": bool(
                (ts_array >= 1600000000000).all() and (ts_array <= 2000000000000).all()
            ),  # 2020-2033年范围
            "monotonic": bool((np.diff(ts_array, axis=1) >= 0).all()) if ts_array.shape[1] > 1 else True,  # 单调递增
        }

        checks[ts_type]["valid"] = all(checks[ts_type].values())

    return checks


def print_validity_checks(checks: dict):
    """打印有效性检查结果.

    Args:
        checks: 检查结果字典
    """
    print("\n" + "=" * 80)
    print("🔍 Timestamp 有效性检查")
    print("=" * 80)

    for ts_type, results in checks.items():
        status = "✅" if results["valid"] else "❌"
        print(f"\n{status} {ts_type}:")
        print(f"  所有值为正: {results['all_positive']}")
        print(f"  没有零值: {results['no_zero']}")
        print(f"  时间范围合理: {results['reasonable_range']}")
        print(f"  单调递增: {results['monotonic']}")

    print("\n" + "=" * 80)


def save_report_to_json(report: dict, output_file: Path):
    """保存报告为 JSON 文件.

    Args:
        report: 报告字典
        output_file: 输出文件路径
    """
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"\n📝 报告已保存到: {output_file}")


def main():
    """主函数."""
    # 配置
    data_dir = Path("../data/exports/H1B/univ_1_1_1_r0.9_custom_2024-10-01_2024-10-31")
    date = "20241001"

    print(f"📁 数据目录: {data_dir}")
    print(f"📅 日期: {date}")
    print()

    try:
        # 1. 加载 timestamp
        print("📥 加载 timestamp 数据...")
        timestamps = load_timestamps(data_dir, date)

        # 2. 有效性检查
        print("\n🔍 执行有效性检查...")
        validity_checks = check_timestamp_validity(timestamps)
        print_validity_checks(validity_checks)

        # 3. 对齐质量验证
        if len(timestamps) > 1:
            print("\n📊 执行对齐质量验证...")
            report = validate_alignment_quality(timestamps)
            print_report(report)

            # 4. 保存报告
            output_file = data_dir / f"alignment_report_{date}.json"
            save_report_to_json(report, output_file)

        print("\n✅ 验证完成!")

    except Exception as e:
        print(f"\n❌ 验证失败: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
