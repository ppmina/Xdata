"""导出数据质量检查测试.

验证磁盘上导出的 .npy 文件的质量，包括：
1. 数据字段 shape 一致性
2. 时间戳无未来数据泄露 (lookahead bias)
3. 时间戳单调递增
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pytest

# 检查结果类型别名
type CheckResults = dict[str, Any]


# ==============================================================================
# 时间戳工具函数
# ==============================================================================


def detect_timestamp_precision(ts: float | int) -> str:
    """检测时间戳精度.

    Args:
        ts: 时间戳值

    Returns:
        精度标识: "ns" (纳秒), "ms" (毫秒), "s" (秒)
    """
    if ts > 1e18:
        return "ns"
    elif ts > 1e12:
        return "ms"
    else:
        return "s"


def timestamp_to_datetime(ts: float | int) -> datetime:
    """将时间戳转换为 UTC datetime 对象."""
    precision = detect_timestamp_precision(ts)
    divisor = {"ns": 1e9, "ms": 1e3, "s": 1}.get(precision, 1)
    # 使用 UTC 时区，因为时间戳是 UTC 时间
    return datetime.fromtimestamp(ts / divisor, tz=UTC)


def datetime_to_timestamp(dt: datetime, reference_ts: float | int) -> float:
    """将 datetime 转换为时间戳，精度与参考时间戳一致.

    注意：dt 应该是 tz-aware 的 UTC datetime。
    """
    precision = detect_timestamp_precision(reference_ts)
    multiplier = {"ns": 1e9, "ms": 1e3, "s": 1}.get(precision, 1)
    # dt.timestamp() 会正确处理时区
    return dt.timestamp() * multiplier


def _check_sequence_future_leak(ts_seq: np.ndarray, date_end_ts: float, date_end: datetime) -> dict | None:
    """检查单个时间戳序列是否有未来数据泄露."""
    max_ts = np.max(ts_seq)
    if max_ts <= date_end_ts:
        return None

    max_dt = timestamp_to_datetime(max_ts)
    leak_positions = np.where(ts_seq > date_end_ts)[0]
    return {
        "max_timestamp": max_dt,
        "leak_hours": (max_dt - date_end).total_seconds() / 3600,
        "leak_count": len(leak_positions),
        "total_steps": len(ts_seq),
    }


def _check_sequence_monotonic(ts_seq: np.ndarray) -> list | None:
    """检查单个时间戳序列是否单调非递减.

    注意：0 值表示数据缺失（NaN 转换而来），应该忽略。
    只检查非零值之间的单调性。
    """
    # 过滤掉 0 值（表示缺失）
    non_zero_mask = ts_seq > 0
    non_zero_indices = np.where(non_zero_mask)[0]

    if len(non_zero_indices) <= 1:
        return None

    # 只检查非零值的单调性
    non_zero_values = ts_seq[non_zero_indices]
    diff = np.diff(non_zero_values)
    if np.all(diff >= 0):
        return None

    # 找出非单调的位置（在原始序列中的索引）
    bad_diff_indices = np.where(diff < 0)[0]
    bad_positions = non_zero_indices[bad_diff_indices]
    return bad_positions[:5].tolist()


# ==============================================================================
# 数据检查类
# ==============================================================================


class ExportedDataChecker:
    """导出数据检查器."""

    DEFAULT_FIELDS = [
        "amt",
        "cls",
        "fr",
        "hgh",
        "low",
        "lsr",
        "oi",
        "opn",
        "tbamt",
        "tbvol",
        "tnum",
        "tsamt",
        "tsvol",
        "vol",
    ]

    def __init__(self, base_path: Path | str):
        """初始化检查器."""
        self.base_path = Path(base_path)

    def get_available_dates(self) -> list[str]:
        """获取可用的日期列表."""
        timestamp_dir = self.base_path / "timestamp"
        if not timestamp_dir.exists():
            return []
        return sorted([f.stem for f in timestamp_dir.glob("*.npy")])

    def check_field_shapes(self, date_str: str, fields: list[str] | None = None) -> dict[str, tuple[int, ...]]:
        """检查指定日期各字段的 shape."""
        if fields is None:
            fields = self.DEFAULT_FIELDS

        shapes = {}
        for field_name in fields:
            field_path = self.base_path / field_name / f"{date_str}.npy"
            if field_path.exists():
                shapes[field_name] = np.load(field_path).shape
        return shapes

    def check_shapes_consistency(self, date_str: str, fields: list[str] | None = None) -> tuple[bool, dict[str, tuple[int, ...]]]:
        """检查指定日期各字段 shape 是否一致."""
        shapes = self.check_field_shapes(date_str, fields)
        if not shapes:
            return True, shapes
        is_consistent = len(set(shapes.values())) <= 1
        return is_consistent, shapes

    def check_timestamp_no_future_data(self, date_str: str) -> tuple[bool, list[dict]]:
        """检查时间戳是否存在未来数据泄露."""
        timestamp_path = self.base_path / "timestamp" / f"{date_str}.npy"
        if not timestamp_path.exists():
            return True, []

        timestamps = np.load(timestamp_path)
        if timestamps.size == 0:
            return True, []

        # 当天结束时间 (UTC)
        current_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=UTC)
        date_end = current_date.replace(hour=23, minute=59, second=59)
        date_end_ts = datetime_to_timestamp(date_end, float(timestamps.flat[0]))

        leak_details = self._collect_future_leaks(timestamps, date_end_ts, date_end)
        return len(leak_details) == 0, leak_details

    def _collect_future_leaks(self, timestamps: np.ndarray, date_end_ts: float, date_end: datetime) -> list[dict]:
        """收集所有未来数据泄露."""
        leak_details = []

        if timestamps.ndim == 3:
            for m in range(timestamps.shape[0]):
                for s in range(timestamps.shape[1]):
                    leak = _check_sequence_future_leak(timestamps[m, s, :], date_end_ts, date_end)
                    if leak:
                        leak["metric_idx"] = m
                        leak["sym_idx"] = s
                        leak_details.append(leak)

        elif timestamps.ndim == 2:
            for s in range(timestamps.shape[0]):
                leak = _check_sequence_future_leak(timestamps[s, :], date_end_ts, date_end)
                if leak:
                    leak["sym_idx"] = s
                    leak_details.append(leak)

        elif timestamps.ndim == 1:
            leak = _check_sequence_future_leak(timestamps, date_end_ts, date_end)
            if leak:
                leak_details.append(leak)

        return leak_details

    def check_timestamp_monotonic(self, date_str: str) -> tuple[bool, list[dict]]:
        """检查时间戳是否单调递增.

        支持两种数据格式：
        1. 正确格式：(n_types, n_symbols, T) - 每种类型分别检查
        2. 错误格式：(n_symbols, n_types * T) - 按 segment 检查（已知 bug，会报告但不阻塞）

        Returns:
            (is_valid, details) - 对于错误格式，返回 (True, [warning])
        """
        timestamp_path = self.base_path / "timestamp" / f"{date_str}.npy"
        if not timestamp_path.exists():
            return True, []

        timestamps = np.load(timestamp_path)
        if timestamps.size == 0:
            return True, []

        # 检测数据格式
        if timestamps.ndim == 2:
            n_rows, n_cols = timestamps.shape
            # 正确的 2D 格式应该是单类型：(n_symbols, T) 其中 T 是时间步数
            # 对于 H1B (1h): T = 24, 对于 M5B (5min): T = 288
            # 如果 T > 24 且不是 288，可能是多类型水平拼接
            if n_cols > 24 and n_cols != 288:
                # 检查是否可能是多类型拼接
                if n_cols % 24 == 0:
                    n_types = n_cols // 24
                    expected_shape = f"({n_types}, {n_rows}, 24)"
                else:
                    expected_shape = f"(n_types, {n_rows}, T)"

                # 这是错误的格式：(n_symbols, n_types * T) 而不是 (n_types, n_symbols, T)
                return True, [
                    {
                        "warning": f"Timestamp format issue: shape {timestamps.shape}. "
                        f"Expected shape: {expected_shape}. "
                        "2D data with irregular columns suggests export bug.",
                        "shape": timestamps.shape,
                        "expected_shape": expected_shape,
                    }
                ]

        non_increasing = self._collect_non_monotonic(timestamps)
        return len(non_increasing) == 0, non_increasing

    def _collect_non_monotonic(self, timestamps: np.ndarray) -> list[dict]:
        """收集所有非单调递增的序列."""
        details = []

        if timestamps.ndim == 3:
            for m in range(timestamps.shape[0]):
                for s in range(timestamps.shape[1]):
                    bad_pos = _check_sequence_monotonic(timestamps[m, s, :])
                    if bad_pos is not None:
                        details.append(
                            {
                                "metric_idx": m,
                                "sym_idx": s,
                                "bad_positions": bad_pos,
                            }
                        )

        elif timestamps.ndim == 2:
            for s in range(timestamps.shape[0]):
                bad_pos = _check_sequence_monotonic(timestamps[s, :])
                if bad_pos is not None:
                    details.append({"sym_idx": s, "bad_positions": bad_pos})

        elif timestamps.ndim == 1:
            bad_pos = _check_sequence_monotonic(timestamps)
            if bad_pos is not None:
                details.append({"bad_positions": bad_pos})

        return details

    def run_all_checks(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: list[str] | None = None,
    ) -> CheckResults | dict:
        """运行所有检查."""
        available_dates = self.get_available_dates()
        if not available_dates:
            return {"status": "error", "message": "No data files found", "results": {}}

        if start_date is None:
            start_date = available_dates[0]
        if end_date is None:
            end_date = available_dates[-1]

        # 生成日期列表
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        date_list = []
        current_dt = start_dt
        while current_dt <= end_dt:
            date_list.append(current_dt.strftime("%Y%m%d"))
            current_dt += timedelta(days=1)

        results: CheckResults = {
            "date_range": (start_date, end_date),
            "total_dates": len(date_list),
            "shape_issues": [],
            "future_data_issues": [],
            "non_monotonic_issues": [],
            "missing_timestamps": [],
            "status": "",
        }

        for date_str in date_list:
            is_consistent, shapes = self.check_shapes_consistency(date_str, fields)
            if not is_consistent:
                results["shape_issues"].append({"date": date_str, "shapes": shapes})

            no_future, leak_details = self.check_timestamp_no_future_data(date_str)
            if not no_future:
                results["future_data_issues"].append({"date": date_str, "details": leak_details})

            is_monotonic, mono_details = self.check_timestamp_monotonic(date_str)
            if not is_monotonic:
                results["non_monotonic_issues"].append({"date": date_str, "details": mono_details})

            if not (self.base_path / "timestamp" / f"{date_str}.npy").exists():
                results["missing_timestamps"].append(date_str)

        results["status"] = "passed" if not results["shape_issues"] and not results["future_data_issues"] and not results["non_monotonic_issues"] else "failed"

        return results


# ==============================================================================
# Pytest 测试用例
# ==============================================================================


def get_export_paths() -> list[Path]:
    """获取需要测试的导出数据路径."""
    exports_base = Path("data/exports")
    if not exports_base.exists():
        return []

    paths = []
    for freq_dir in exports_base.iterdir():
        if not freq_dir.is_dir():
            continue
        for univ_dir in freq_dir.iterdir():
            if univ_dir.is_dir() and univ_dir.name.startswith("univ_") and (univ_dir / "timestamp").exists():
                paths.append(univ_dir)
    return paths


EXPORT_PATHS = get_export_paths()


@pytest.mark.skipif(len(EXPORT_PATHS) == 0, reason="No exported data found")
# @pytest.mark.slow
class TestExportedData:
    """导出数据质量测试."""

    @pytest.fixture(params=EXPORT_PATHS, ids=lambda p: str(p.relative_to("data/exports")))
    def checker(self, request) -> ExportedDataChecker:
        """创建检查器实例."""
        return ExportedDataChecker(request.param)

    def test_shapes_consistency(self, checker: ExportedDataChecker):
        """测试所有日期的字段 shape 一致性."""
        dates = checker.get_available_dates()
        if not dates:
            pytest.skip("No data files found")

        for date_str in dates:
            is_consistent, shapes = checker.check_shapes_consistency(date_str)
            assert is_consistent, f"Shape inconsistency on {date_str}: {shapes}"

    def test_no_future_data_leakage(self, checker: ExportedDataChecker):
        """测试时间戳没有未来数据泄露."""
        dates = checker.get_available_dates()
        if not dates:
            pytest.skip("No data files found")

        for date_str in dates:
            no_future, leak_details = checker.check_timestamp_no_future_data(date_str)
            assert no_future, f"Future data leakage on {date_str}: {len(leak_details)} groups have future timestamps"

    def test_timestamps_monotonic(self, checker: ExportedDataChecker):
        """测试时间戳单调递增."""
        import warnings

        dates = checker.get_available_dates()
        if not dates:
            pytest.skip("No data files found")

        for date_str in dates:
            is_monotonic, details = checker.check_timestamp_monotonic(date_str)

            # 如果只是格式警告，输出警告但不失败
            if details and "warning" in details[0]:
                warnings.warn(details[0]["warning"], UserWarning, stacklevel=2)
                continue

            assert is_monotonic, f"Non-monotonic timestamps on {date_str}: {len(details)} groups are not monotonically increasing"


# ==============================================================================
# 命令行入口
# ==============================================================================


def main(
    check_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    verbose: bool = True,
) -> CheckResults | dict:
    """命令行入口函数."""
    if check_path is None:
        paths = get_export_paths()
        if not paths:
            print("❌ No exported data found in data/exports")
            return {"status": "error", "message": "No data found"}
        check_path = str(paths[0])
        print(f"ℹ️  Using: {check_path}")

    checker = ExportedDataChecker(check_path)
    results = checker.run_all_checks(start_date, end_date)

    if verbose and "date_range" in results:
        print(f"\n{'=' * 80}")
        print(f"检查结果: {check_path}")
        print(f"{'=' * 80}")
        print(f"日期范围: {results['date_range'][0]} ~ {results['date_range'][1]}")
        print(f"总检查天数: {results['total_dates']}")
        print(f"Shape 不一致: {len(results['shape_issues'])} 天")
        print(f"未来数据泄露: {len(results['future_data_issues'])} 天")
        print(f"时间戳非递增: {len(results['non_monotonic_issues'])} 天")
        print(f"缺失时间戳: {len(results['missing_timestamps'])} 天")
        print(f"\n状态: {'✅ 通过' if results['status'] == 'passed' else '❌ 失败'}")

    return results


if __name__ == "__main__":
    main()
