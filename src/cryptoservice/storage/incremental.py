"""增量下载管理器.

提供增量下载计划和缺失数据分析功能。
"""

import logging
from typing import TYPE_CHECKING

import pandas as pd

from cryptoservice.models import Freq

if TYPE_CHECKING:
    from .queries import KlineQuery, MetricsQuery

logger = logging.getLogger(__name__)


class IncrementalManager:
    """增量下载管理器.

    专注于增量下载计划制定和缺失数据分析。
    """

    def __init__(self, kline_query: "KlineQuery", metrics_query: "MetricsQuery"):
        """初始化增量下载管理器.

        Args:
            kline_query: K线数据查询器
            metrics_query: 指标数据查询器
        """
        self.kline_query = kline_query
        self.metrics_query = metrics_query

    async def plan_kline_download(
        self, symbols: list[str], start_date: str, end_date: str, freq: Freq
    ) -> dict[str, list[int]]:
        """制定K线数据增量下载计划.

        Args:
            symbols: 交易对列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            freq: 数据频率

        Returns:
            {symbol: [missing_timestamps]} 缺失数据计划
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return {}

        logger.info(f"制定K线增量下载计划: {len(symbols)} 个交易对, {start_date} - {end_date}, {freq.value}")

        plan = {}

        # 转换日期为时间戳（只转换一次边界值）
        # 使用 UTC 时区以保持与下载逻辑的一致性
        start_ts = int(pd.Timestamp(start_date + " 00:00:00", tz="UTC").timestamp() * 1000)
        # 结束时间设为第二天的00:00:00，以包含整个 end_date 当天
        end_ts = int((pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000)

        # 为每个交易对检查缺失数据
        for symbol in symbols:
            try:
                missing_timestamps = await self.kline_query.get_missing_timestamps(symbol, start_ts, end_ts, freq)
                if missing_timestamps:
                    plan[symbol] = missing_timestamps
                    logger.debug(f"{symbol}: 发现 {len(missing_timestamps)} 个缺失时间戳")
                else:
                    logger.info(f"{symbol}: 数据完整")

            except Exception as e:
                logger.error(f"检查 {symbol} 缺失数据时出错: {e}")
                continue

        total_missing = sum(len(timestamps) for timestamps in plan.values())
        logger.info(f"K线增量下载计划完成: {len(plan)} 个交易对需要下载, 总计 {total_missing} 个时间点")

        return plan

    async def plan_metrics_download(
        self, symbols: list[str], start_date: str, end_date: str, data_type: str, interval_hours: int = 8
    ) -> dict[str, list[int]]:
        """制定指标数据增量下载计划.

        Args:
            symbols: 交易对列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            data_type: 数据类型 ('funding_rate', 'open_interest', 'long_short_ratio')
            interval_hours: 数据间隔小时数

        Returns:
            {symbol: [missing_timestamps]} 缺失数据计划
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return {}

        logger.info(f"制定{data_type}增量下载计划: {len(symbols)} 个交易对, {start_date} - {end_date}")

        plan = {}

        # 转换日期为时间戳（使用 UTC 时区）
        start_ts = int(pd.Timestamp(start_date, tz="UTC").timestamp() * 1000)
        end_ts = int(pd.Timestamp(end_date, tz="UTC").timestamp() * 1000)

        # 为每个交易对检查缺失数据
        for symbol in symbols:
            try:
                missing_timestamps = await self.metrics_query.get_missing_timestamps(
                    data_type, symbol, start_ts, end_ts, interval_hours
                )
                if missing_timestamps:
                    plan[symbol] = missing_timestamps
                    logger.debug(f"{symbol}: 发现 {len(missing_timestamps)} 个缺失时间戳")
                else:
                    logger.debug(f"{symbol}: 数据完整")

            except Exception as e:
                logger.error(f"检查 {symbol} {data_type} 缺失数据时出错: {e}")
                continue

        total_missing = sum(len(timestamps) for timestamps in plan.values())
        logger.info(f"{data_type}增量下载计划完成: {len(plan)} 个交易对需要下载, 总计 {total_missing} 个时间点")

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

    async def get_data_gaps(
        self, symbol: str, start_date: str, end_date: str, freq: Freq, max_gap_hours: int = 24
    ) -> list[dict]:
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
        max_gap_ms = max_gap_hours * 60 * 60 * 1000

        # 检查间隙
        for i in range(1, len(timestamps)):
            gap_ms = timestamps[i] - timestamps[i - 1]

            # 如果间隙大于预期频率
            if gap_ms > freq_ms * 1.5:  # 允许一些误差
                gap_hours = gap_ms / (60 * 60 * 1000)
                gap_info = {
                    "start_timestamp": timestamps[i - 1],
                    "end_timestamp": timestamps[i],
                    "start_date": pd.Timestamp(timestamps[i - 1], unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
                    "end_date": pd.Timestamp(timestamps[i], unit="ms").strftime("%Y-%m-%d %H:%M:%S"),
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
                    priorities.append(
                        {"symbol": symbol, "priority": "high", "reason": "no_data", "record_count": 0, "coverage": 0.0}
                    )
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
