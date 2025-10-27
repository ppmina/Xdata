"""增量下载管理器.

提供增量下载计划和缺失数据分析功能。
"""

from typing import TYPE_CHECKING, Any

import pandas as pd

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq
from cryptoservice.utils import date_to_timestamp_end, date_to_timestamp_start, timestamp_to_date_str

if TYPE_CHECKING:
    from .queries import KlineQuery, MetricsQuery

logger = get_logger(__name__)


class IncrementalManager:
    """增量下载管理器.

    专注于增量下载计划制定和缺失数据分析。
    """

    _METRICS_LABELS = {
        "funding_rate": "资金费率(FR)",
        "open_interest": "持仓量(OI)",
        "long_short_ratio": "多空比例(LSR)",
        "vision-metrics": "Vision指标(OI+LSR)",
    }

    def __init__(self, kline_query: "KlineQuery", metrics_query: "MetricsQuery"):
        """初始化增量下载管理器.

        Args:
            kline_query: K线数据查询器
            metrics_query: 指标数据查询器
        """
        self.kline_query = kline_query
        self.metrics_query = metrics_query

    async def plan_kline_download(self, symbols: list[str], start_date: str, end_date: str, freq: Freq) -> dict[str, dict[str, Any]]:
        """制定K线数据增量下载计划.

        Args:
            symbols: 交易对列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            freq: 数据频率

        Returns:
            {symbol: {"start_ts": int, "end_ts": int, "start_time": str, "end_time": str, "missing_count": int}}
        """
        if not symbols:
            logger.debug("K线增量计划跳过：未提供交易对。")
            return {}

        logger.debug(f"开始生成 K 线增量计划（频率 {freq.value}，范围 {start_date} ~ {end_date}，{len(symbols)} 个交易对）")

        plan: dict[str, dict[str, Any]] = {}
        step_ms = self._get_freq_milliseconds(freq)

        # 转换日期为时间戳（只转换一次边界值）
        # 使用 UTC 时区以保持与下载逻辑的一致性
        start_ts = date_to_timestamp_start(start_date)
        end_ts = date_to_timestamp_end(end_date)

        for symbol in symbols:
            try:
                missing_timestamps = await self.kline_query.get_missing_timestamps(symbol, start_ts, end_ts, freq)
                if missing_timestamps:
                    segment = self._build_single_segment(
                        missing_timestamps,
                        step_ms,
                        start_ts,
                        end_ts,
                    )
                    plan[symbol] = {
                        "start_ts": segment["start_ts"],
                        "end_ts": segment["end_ts"],
                        "start_time": segment["start_time"],
                        "end_time": segment["end_time"],
                        "missing_count": len(missing_timestamps),
                    }
                    logger.debug(
                        "plan.detail",
                        dataset="kline",
                        symbol=symbol,
                        missing=len(missing_timestamps),
                        range=f"{segment['start_time']}→{segment['end_time']}",
                    )
                else:
                    logger.debug(
                        "plan.detail",
                        dataset="kline",
                        symbol=symbol,
                        missing=0,
                        status="complete",
                    )

            except Exception as e:
                logger.error(f"[kline-{freq.value}] 检查 {symbol} 缺失数据时出错: {e}")
                continue

        total_missing = sum(int(entry.get("missing_count", 0)) for entry in plan.values())
        log_kwargs: dict[str, Any] = {
            "dataset": "kline",
            "freq": freq.value,
            "needed": len(plan),
            "total_symbols": len(symbols),
            "missing_points": total_missing,
            "start": start_date,
            "end": end_date,
        }
        if plan:
            preview = sorted(
                ((symbol, int(info.get("missing_count", 0))) for symbol, info in plan.items()),
                key=lambda item: item[1],
                reverse=True,
            )
            top_examples = [f"{symbol}({count})" for symbol, count in preview[:3]]
            if top_examples:
                log_kwargs["missing_examples"] = ", ".join(top_examples)
        if not plan:
            logger.debug(f"K 线增量计划：全部 {len(symbols)} 个交易对数据已最新，无需下载。")
        else:
            example_text = log_kwargs.get("missing_examples")
            if example_text:
                logger.debug(f"K 线增量计划：{len(plan)} 个交易对存在缺失数据（示例：{example_text}）。")
            else:
                logger.debug(f"K 线增量计划：{len(plan)} 个交易对存在缺失数据。")

        return plan

    async def plan_vision_metrics_download(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> dict[str, dict[str, Any]]:
        """制定Vision指标数据增量下载计划.

        Args:
            symbols: 交易对列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            {symbol: {"start_date": str, "end_date": str, "missing_dates": list[str], "missing_count": int}}
        """
        if not symbols:
            logger.debug("Vision 指标增量计划跳过：未提供交易对。")
            return {}

        display_name = self._METRICS_LABELS["vision-metrics"]
        logger.debug(f"开始生成 {display_name} 增量计划（范围 {start_date} ~ {end_date}，{len(symbols)} 个交易对）")

        date_range = pd.date_range(start=start_date, end=end_date, freq="D", tz="UTC")
        if date_range.empty:
            logger.warning(f"{display_name} 增量计划失败：日期范围为空（{start_date} ~ {end_date}）。")
            return {}

        plan, complete_count = await self._collect_vision_plan(symbols, date_range)
        total_expected = len(symbols) * len(date_range)
        total_missing_dates = sum(int(entry.get("missing_count", 0)) for entry in plan.values())

        log_kwargs: dict[str, Any] = {
            "dataset": "vision-metrics",
            "display_name": display_name,
            "needed": len(plan),
            "total_symbols": len(symbols),
            "missing_days": total_missing_dates,
            "complete_counts": f"{complete_count}/{total_expected}",
            "start": start_date,
            "end": end_date,
        }
        if plan:
            preview = sorted(
                ((symbol, int(info.get("missing_count", 0))) for symbol, info in plan.items()),
                key=lambda item: item[1],
                reverse=True,
            )
            top_examples = [f"{symbol}({count})" for symbol, count in preview[:3]]
            if top_examples:
                log_kwargs["missing_examples"] = ", ".join(top_examples)
        if not plan:
            logger.debug(f"{display_name} 增量计划：所有数据已最新，无需下载。")
        else:
            missing_days = log_kwargs.get("missing_days", 0)
            logger.debug(f"{display_name} 增量计划：{len(plan)} 个交易对缺少数据（缺失日期共 {missing_days} 天）。")

        return plan

    @staticmethod
    def _register_missing_day(plan: dict[str, dict[str, Any]], symbol: str, date_value: str) -> None:
        entry = plan.setdefault(
            symbol,
            {
                "missing_dates": [],
                "start_date": date_value,
                "end_date": date_value,
                "missing_count": 0,
            },
        )
        missing_dates = entry.get("missing_dates")
        if not isinstance(missing_dates, list):
            missing_dates = []
            entry["missing_dates"] = missing_dates
        missing_dates.append(date_value)
        entry["missing_count"] = len(missing_dates)
        start_recorded = entry.get("start_date", date_value)
        end_recorded = entry.get("end_date", date_value)
        if isinstance(start_recorded, str) and date_value < start_recorded:
            entry["start_date"] = date_value
        if isinstance(end_recorded, str) and date_value > end_recorded:
            entry["end_date"] = date_value

    async def _collect_vision_plan(
        self,
        symbols: list[str],
        date_range: pd.DatetimeIndex,
    ) -> tuple[dict[str, dict[str, Any]], int]:
        plan: dict[str, dict[str, Any]] = {}
        complete_count = 0

        for symbol in symbols:
            for date in date_range:
                date_str = date.strftime("%Y-%m-%d")
                try:
                    status = await self.metrics_query.get_daily_metrics_status(symbol, date_str)
                except Exception as exc:  # noqa: BLE001
                    logger.error(f"[vision-metrics] 检查 {symbol} {date_str} 时出错: {exc}")
                    self._register_missing_day(plan, symbol, date_str)
                    continue

                oi_count = status.get("open_interest", 0)
                lsr_count = status.get("long_short_ratio", 0)

                if oi_count > 0 and lsr_count > 0:
                    logger.debug(f"[vision-metrics] {symbol} {date_str}: 数据完整 (OI: {oi_count}, LSR: {lsr_count})")
                    complete_count += 1
                else:
                    self._register_missing_day(plan, symbol, date_str)

        return plan, complete_count

    async def plan_metrics_download(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        data_type: str,
        interval_hours: float = 8,
    ) -> dict[str, dict[str, Any]]:
        """制定指标数据增量下载计划.

        Args:
            symbols: 交易对列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            data_type: 数据类型 ('funding_rate', 'open_interest', 'long_short_ratio')
            interval_hours: 数据间隔小时数

        Returns:
            {
                symbol: {
                    "start_ts": int,
                    "end_ts": int,
                    "start_time": str,
                    "end_time": str,
                    "missing_count": int,
                    "interval_ms": int,
                }
            }
        """
        if not symbols:
            logger.debug(f"{data_type} 增量计划跳过：未提供交易对。")
            return {}

        display_name = self._METRICS_LABELS.get(data_type, data_type)
        logger.debug(f"开始生成 {display_name} 增量计划（范围 {start_date} ~ {end_date}，间隔 {interval_hours:.1f} 小时，{len(symbols)} 个交易对）")

        plan: dict[str, dict[str, Any]] = {}

        # 转换日期为时间戳（使用 UTC 时区）
        start_ts = date_to_timestamp_start(start_date)
        end_ts = date_to_timestamp_end(end_date)
        interval_ms = self._hours_to_milliseconds(interval_hours)
        interval_hours_value = int(interval_hours) if interval_hours > 0 else 1

        # 为每个交易对检查缺失数据
        for symbol in symbols:
            try:
                missing_timestamps = await self.metrics_query.get_missing_timestamps(data_type, symbol, start_ts, end_ts, interval_hours_value)
                if missing_timestamps:
                    segment = self._build_single_segment(
                        missing_timestamps,
                        interval_ms,
                        start_ts,
                        end_ts,
                    )
                    plan[symbol] = {
                        "start_ts": segment["start_ts"],
                        "end_ts": segment["end_ts"],
                        "start_time": segment["start_time"],
                        "end_time": segment["end_time"],
                        "missing_count": len(missing_timestamps),
                        "interval_ms": interval_ms,
                    }
                    logger.debug(
                        "plan.detail",
                        dataset=data_type,
                        symbol=symbol,
                        missing=len(missing_timestamps),
                        range=f"{segment['start_time']}→{segment['end_time']}",
                    )
                else:
                    logger.debug(
                        "plan.detail",
                        dataset=data_type,
                        symbol=symbol,
                        missing=0,
                        status="complete",
                    )

            except Exception as e:
                logger.error(f"[{data_type}] 检查 {symbol} 缺失数据时出错: {e}")
                continue

        total_missing = sum(int(entry.get("missing_count", 0)) for entry in plan.values())
        log_kwargs: dict[str, Any] = {
            "dataset": data_type,
            "display_name": display_name,
            "needed": len(plan),
            "total_symbols": len(symbols),
            "missing_points": total_missing,
            "start": start_date,
            "end": end_date,
        }
        if plan:
            preview = sorted(
                ((symbol, int(info.get("missing_count", 0))) for symbol, info in plan.items()),
                key=lambda item: item[1],
                reverse=True,
            )
            top_examples = [f"{symbol}({count})" for symbol, count in preview[:3]]
            if top_examples:
                log_kwargs["missing_examples"] = ", ".join(top_examples)
        if not plan:
            logger.debug(f"{display_name} 增量计划：全部 {len(symbols)} 个交易对数据已最新。")
        else:
            missing_points = log_kwargs.get("missing_days") or log_kwargs.get("missing_points")
            example_text = log_kwargs.get("missing_examples")
            if example_text:
                logger.debug(f"{display_name} 增量计划：{len(plan)} 个交易对需要补齐数据（示例：{example_text}）。")
            else:
                logger.debug(f"{display_name} 增量计划：{len(plan)} 个交易对需要补齐数据（缺口数量 {missing_points}）。")

        return plan

    async def get_kline_coverage_report(self, symbols: list[str], start_date: str, end_date: str, freq: Freq) -> dict:
        """获取K线数据覆盖率报告.

        Args:
            symbols: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            freq: 数据频率

        Returns:
            覆盖率报告字典
        """
        logger.info(f"生成K线数据覆盖率报告: {len(symbols)} 个交易对")

        total_expected = self._count_expected_records(start_date, end_date, freq)
        if total_expected == 0:
            return {"error": "时间范围无效"}

        report = {
            "period": f"{start_date} - {end_date}",
            "frequency": freq.value,
            "total_expected_per_symbol": total_expected,
            "symbols": {},
            "summary": {
                "total_symbols": len(symbols),
                "complete_symbols": 0,
                "partial_symbols": 0,
                "empty_symbols": 0,
                "overall_coverage": 0.0,
            },
        }

        total_actual = 0
        total_possible = len(symbols) * total_expected

        for symbol in symbols:
            try:
                time_range = await self.kline_query.get_time_range(symbol, freq)

                if not time_range:
                    # 没有数据
                    report["symbols"][symbol] = {"record_count": 0, "coverage": 0.0, "status": "empty"}
                    report["summary"]["empty_symbols"] += 1
                else:
                    record_count = time_range["record_count"]
                    coverage = (record_count / total_expected) * 100

                    if coverage >= 100:
                        status = "complete"
                        report["summary"]["complete_symbols"] += 1
                    else:
                        status = "partial"
                        report["summary"]["partial_symbols"] += 1

                    report["symbols"][symbol] = {
                        "record_count": record_count,
                        "coverage": round(coverage, 2),
                        "status": status,
                        "earliest_date": time_range.get("earliest_date"),
                        "latest_date": time_range.get("latest_date"),
                    }

                    total_actual += record_count

            except Exception as e:
                logger.error(f"获取 {symbol} 覆盖率信息时出错: {e}")
                report["symbols"][symbol] = {"error": str(e), "status": "error"}

        # 计算总体覆盖率
        if total_possible > 0:
            report["summary"]["overall_coverage"] = round((total_actual / total_possible) * 100, 2)

        logger.info(f"覆盖率报告生成完成: 总体覆盖率 {report['summary']['overall_coverage']}%")
        return report

    async def get_data_gaps(self, symbol: str, start_date: str, end_date: str, freq: Freq, max_gap_hours: int = 24) -> list[dict]:
        """获取数据间隙信息.

        Args:
            symbol: 交易对
            start_date: 开始日期
            end_date: 结束日期
            freq: 数据频率
            max_gap_hours: 最大间隙小时数，超过此值的间隙会被标记

        Returns:
            间隙信息列表
        """
        logger.info(f"分析 {symbol} 数据间隙: {start_date} - {end_date}")

        # 获取现有时间戳
        df = await self.kline_query.select_by_time_range([symbol], start_date, end_date, freq, columns=["close_price"])

        if df.empty:
            return [{"type": "no_data", "message": f"{symbol} 没有数据"}]

        # 获取时间戳序列
        timestamps = sorted(df.index.get_level_values("timestamp").unique())

        gaps = []
        freq_ms = self._get_freq_milliseconds(freq)
        max_gap_ms = self._hours_to_milliseconds(max_gap_hours)

        # 检查间隙
        for i in range(1, len(timestamps)):
            gap_ms = timestamps[i] - timestamps[i - 1]

            # 如果间隙大于预期频率
            if gap_ms > freq_ms * 1.5:  # 允许一些误差
                gap_hours = gap_ms / (60 * 60 * 1000)
                gap_info = {
                    "start_timestamp": timestamps[i - 1],
                    "end_timestamp": timestamps[i],
                    "start_date": timestamp_to_date_str(timestamps[i - 1]),
                    "end_date": timestamp_to_date_str(timestamps[i]),
                    "gap_hours": round(gap_hours, 2),
                    "gap_periods": int(gap_ms // freq_ms) - 1,
                    "severity": "critical" if gap_ms > max_gap_ms else "minor",
                }
                gaps.append(gap_info)

        logger.info(f"{symbol} 数据间隙分析完成: 发现 {len(gaps)} 个间隙")
        return gaps

    def _count_expected_records(self, start_date: str, end_date: str, freq: Freq) -> int:
        """计算预期的记录数量（不生成完整列表，性能更好）.

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            freq: 数据频率

        Returns:
            预期的记录数量
        """
        freq_map = {
            Freq.m1: "1min",
            Freq.m3: "3min",
            Freq.m5: "5min",
            Freq.m15: "15min",
            Freq.m30: "30min",
            Freq.h1: "1h",
            Freq.h2: "2h",
            Freq.h4: "4h",
            Freq.h6: "6h",
            Freq.h8: "8h",
            Freq.h12: "12h",
            Freq.d1: "1D",
            Freq.w1: "1W",
            Freq.M1: "1M",
        }

        pandas_freq = freq_map.get(freq, "1h")

        try:
            # 生成时间范围但只计算数量（使用 UTC 时区）
            time_range = pd.date_range(
                start=start_date + " 00:00:00",
                end=pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1),
                freq=pandas_freq,
                inclusive="left",  # 不包含结束时间
                tz="UTC",
            )
            return len(time_range)

        except Exception as e:
            logger.error(f"计算预期记录数量失败: {e}")
            return 0

    def _get_freq_milliseconds(self, freq: Freq) -> int:
        """获取频率对应的毫秒数.

        Args:
            freq: 数据频率

        Returns:
            毫秒数
        """
        freq_ms_map = {
            Freq.m1: 60 * 1000,
            Freq.m3: 3 * 60 * 1000,
            Freq.m5: 5 * 60 * 1000,
            Freq.m15: 15 * 60 * 1000,
            Freq.m30: 30 * 60 * 1000,
            Freq.h1: 60 * 60 * 1000,
            Freq.h2: 2 * 60 * 60 * 1000,
            Freq.h4: 4 * 60 * 60 * 1000,
            Freq.h6: 6 * 60 * 60 * 1000,
            Freq.h8: 8 * 60 * 60 * 1000,
            Freq.h12: 12 * 60 * 60 * 1000,
            Freq.d1: 24 * 60 * 60 * 1000,
            Freq.w1: 7 * 24 * 60 * 60 * 1000,
            Freq.M1: 30 * 24 * 60 * 60 * 1000,  # 近似值
        }

        return freq_ms_map.get(freq, 60 * 60 * 1000)  # 默认1小时

    @staticmethod
    def _hours_to_milliseconds(hours: float) -> int:
        """将小时时间间隔转换为毫秒."""
        if hours is None or hours <= 0:
            return 60 * 60 * 1000  # 默认1小时
        return max(int(round(hours * 60 * 60 * 1000)), 60 * 1000)

    def _build_single_segment(
        self,
        missing_timestamps: list[int],
        step_ms: int,
        start_bound: int,
        end_bound: int,
    ) -> dict[str, Any]:
        """根据缺失时间戳构建单个下载区间."""
        if not missing_timestamps:
            return {
                "start_ts": start_bound,
                "end_ts": end_bound,
                "start_time": self._format_timestamp(start_bound),
                "end_time": self._format_timestamp(end_bound),
            }

        first_missing = max(missing_timestamps[0], start_bound)
        last_missing = min(missing_timestamps[-1], max(start_bound, end_bound - step_ms))

        end_ts = min(last_missing + step_ms, end_bound)
        if end_ts <= first_missing:
            end_ts = min(first_missing + step_ms, end_bound)

        return {
            "start_ts": first_missing,
            "end_ts": end_ts,
            "start_time": self._format_timestamp(first_missing),
            "end_time": self._format_timestamp(end_ts),
        }

    @staticmethod
    def _format_timestamp(timestamp_ms: int) -> str:
        """将毫秒时间戳格式化为可读字符串."""
        return pd.Timestamp(timestamp_ms, unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M:%S")

    async def get_download_priority(self, symbols: list[str], start_date: str, end_date: str, freq: Freq) -> list[dict]:
        """获取下载优先级建议.

        Args:
            symbols: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            freq: 数据频率

        Returns:
            按优先级排序的下载建议列表
        """
        logger.info(f"生成下载优先级建议: {len(symbols)} 个交易对")

        priorities = []

        for symbol in symbols:
            try:
                # 获取时间范围信息
                time_range = await self.kline_query.get_time_range(symbol, freq)

                if not time_range:
                    # 没有数据，高优先级
                    priorities.append({"symbol": symbol, "priority": "high", "reason": "no_data", "record_count": 0, "coverage": 0.0})
                else:
                    # 计算覆盖率
                    expected_records = self._count_expected_records(start_date, end_date, freq)
                    actual_records = time_range["record_count"]
                    coverage = (actual_records / expected_records) * 100 if expected_records > 0 else 0

                    if coverage < 50:
                        priority = "high"
                        reason = "low_coverage"
                    elif coverage < 90:
                        priority = "medium"
                        reason = "partial_coverage"
                    else:
                        priority = "low"
                        reason = "good_coverage"

                    priorities.append(
                        {
                            "symbol": symbol,
                            "priority": priority,
                            "reason": reason,
                            "record_count": actual_records,
                            "coverage": round(coverage, 2),
                            "earliest_date": time_range.get("earliest_date"),
                            "latest_date": time_range.get("latest_date"),
                        }
                    )

            except Exception as e:
                logger.error(f"获取 {symbol} 优先级信息时出错: {e}")
                priorities.append({"symbol": symbol, "priority": "high", "reason": "error", "error": str(e)})

        # 按优先级排序（高 -> 中 -> 低）
        priority_order = {"high": 0, "medium": 1, "low": 2}
        priorities.sort(key=lambda x: (priority_order.get(str(x["priority"]), 3), x["symbol"]))

        logger.info(f"优先级建议生成完成: {len(priorities)} 条建议")
        return priorities
