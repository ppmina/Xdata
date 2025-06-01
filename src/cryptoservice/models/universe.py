from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass
class UniverseConfig:
    """Universe配置类.

    Attributes:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        t1_months: T1时间窗口（月），用于计算mean daily amount
        t2_months: T2滚动频率（月），universe重新选择的频率
        t3_months: T3合约最小创建时间（月），用于筛除新合约
        top_k: 选取的top合约数量
    """

    start_date: str
    end_date: str
    t1_months: int
    t2_months: int
    t3_months: int
    top_k: int

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "t1_months": self.t1_months,
            "t2_months": self.t2_months,
            "t3_months": self.t3_months,
            "top_k": self.top_k,
        }


@dataclass
class UniverseSnapshot:
    """Universe快照类，表示某个时间点的universe状态.

    Attributes:
        effective_date: 生效日期（重平衡日期，通常是月末）
        period_start_date: 数据计算周期开始日期（T1回看的开始日期）
        period_end_date: 数据计算周期结束日期（通常等于重平衡日期）
        symbols: 该时间点的universe交易对列表（基于period内数据计算得出）
        mean_daily_amounts: 各交易对在period内的平均日成交量
        metadata: 额外的元数据信息

    Note:
        在月末重平衡策略下：
        - effective_date: 重平衡决策的日期（如2024-01-31）
        - period: 用于计算的数据区间（如2023-12-31到2024-01-31）
        - 含义: 基于1月份数据，在1月末选择2月份的universe
    """

    effective_date: str
    period_start_date: str
    period_end_date: str
    symbols: list[str]
    mean_daily_amounts: dict[str, float]
    metadata: dict[str, Any] | None = None

    @classmethod
    def create_with_inferred_periods(
        cls,
        effective_date: str,
        t1_months: int,
        symbols: list[str],
        mean_daily_amounts: dict[str, float],
        metadata: dict[str, Any] | None = None,
    ) -> "UniverseSnapshot":
        """创建快照并自动推断周期日期

        根据重平衡日期（effective_date）和回看窗口（t1_months），
        自动计算数据计算的时间区间。

        Args:
            effective_date: 重平衡生效日期（建议使用月末日期）
            t1_months: T1时间窗口（月），用于回看数据计算
            symbols: 交易对列表
            mean_daily_amounts: 平均日成交量（基于计算周期内的数据）
            metadata: 元数据

        Returns:
            UniverseSnapshot: 带有推断周期日期的快照

        Example:
            对于月末重平衡策略：
            effective_date="2024-01-31", t1_months=1
            -> period_start_date="2023-12-31"
            -> period_end_date="2024-01-31"
            含义：基于1月份数据，在1月末选择2月份universe
        """
        effective_dt = pd.to_datetime(effective_date)
        period_start_dt = effective_dt - pd.DateOffset(months=t1_months)

        return cls(
            effective_date=effective_date,
            period_start_date=period_start_dt.strftime("%Y-%m-%d"),
            period_end_date=effective_date,  # 数据计算周期结束 = 重平衡日期
            symbols=symbols,
            mean_daily_amounts=mean_daily_amounts,
            metadata=metadata,
        )

    def validate_period_consistency(self, expected_t1_months: int) -> dict[str, Any]:
        """验证周期日期的一致性

        检查存储的period日期是否与预期的T1配置一致。
        适用于月末重平衡和其他重平衡策略。

        Args:
            expected_t1_months: 期望的T1月数

        Returns:
            Dict: 验证结果，包含一致性检查和详细信息
        """
        effective_dt = pd.to_datetime(self.effective_date)
        period_start_dt = pd.to_datetime(self.period_start_date)
        period_end_dt = pd.to_datetime(self.period_end_date)

        # 计算实际的月数差
        actual_months_diff = (effective_dt.year - period_start_dt.year) * 12 + (
            effective_dt.month - period_start_dt.month
        )

        # 计算实际天数
        actual_days = (period_end_dt - period_start_dt).days

        # 验证期末日期是否等于生效日期
        period_end_matches_effective = self.period_end_date == self.effective_date

        return {
            "is_consistent": (
                abs(actual_months_diff - expected_t1_months) <= 1  # 允许1个月的误差
                and period_end_matches_effective
            ),
            "expected_t1_months": expected_t1_months,
            "actual_months_diff": actual_months_diff,
            "actual_days": actual_days,
            "period_end_matches_effective": period_end_matches_effective,
            "details": {
                "effective_date": self.effective_date,
                "period_start_date": self.period_start_date,
                "period_end_date": self.period_end_date,
            },
        }

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "effective_date": self.effective_date,
            "period_start_date": self.period_start_date,
            "period_end_date": self.period_end_date,
            "symbols": self.symbols,
            "mean_daily_amounts": self.mean_daily_amounts,
            "metadata": self.metadata or {},
        }

    def get_period_info(self) -> dict[str, str]:
        """获取周期信息

        Returns:
            Dict: 包含周期相关的详细信息
        """
        return {
            "period_start": self.period_start_date,
            "period_end": self.period_end_date,
            "effective_date": self.effective_date,
            "period_duration_days": str(
                (pd.to_datetime(self.period_end_date) - pd.to_datetime(self.period_start_date)).days
            ),
        }

    def get_investment_period_info(self) -> dict[str, str]:
        """获取投资周期信息

        在月末重平衡策略下，这个快照对应的投资期间。

        Returns:
            Dict: 投资周期信息
        """
        # 投资期间从重平衡日的下一天开始
        effective_dt = pd.to_datetime(self.effective_date)
        investment_start = effective_dt + pd.Timedelta(days=1)

        # 假设投资到下个月末（这是一个估算，实际取决于下次重平衡）
        investment_end_estimate = investment_start + pd.offsets.MonthEnd(0)

        return {
            "data_calculation_period": f"{self.period_start_date} to {self.period_end_date}",
            "rebalance_decision_date": self.effective_date,
            "investment_start_date": investment_start.strftime("%Y-%m-%d"),
            "investment_end_estimate": investment_end_estimate.strftime("%Y-%m-%d"),
            "universe_symbols_count": str(len(self.symbols)),
        }


@dataclass
class UniverseDefinition:
    """Universe定义类，包含完整的universe历史.

    Attributes:
        config: Universe配置
        snapshots: 时间序列的universe快照列表
        creation_time: 创建时间
        description: 描述信息
    """

    config: UniverseConfig
    snapshots: list[UniverseSnapshot]
    creation_time: datetime
    description: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "config": self.config.to_dict(),
            "snapshots": [snapshot.to_dict() for snapshot in self.snapshots],
            "creation_time": self.creation_time.isoformat(),
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UniverseDefinition":
        """从字典创建Universe定义"""
        config = UniverseConfig(**data["config"])
        snapshots = [
            UniverseSnapshot(
                effective_date=snap["effective_date"],
                period_start_date=snap.get("period_start_date", snap["effective_date"]),
                period_end_date=snap.get("period_end_date", snap["effective_date"]),
                symbols=snap["symbols"],
                mean_daily_amounts=snap["mean_daily_amounts"],
                metadata=snap.get("metadata"),
            )
            for snap in data["snapshots"]
        ]
        creation_time = datetime.fromisoformat(data["creation_time"])

        return cls(
            config=config,
            snapshots=snapshots,
            creation_time=creation_time,
            description=data.get("description"),
        )

    def save_to_file(self, file_path: Path | str) -> None:
        """保存universe定义到文件"""
        import json

        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)

    @classmethod
    def load_from_file(cls, file_path: Path | str) -> "UniverseDefinition":
        """从文件加载universe定义"""
        import json

        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        return cls.from_dict(data)

    def get_symbols_for_date(self, target_date: str) -> list[str]:
        """获取指定日期的universe交易对列表

        在月末重平衡策略下，此方法会找到在目标日期之前最近的一次重平衡，
        返回对应的universe交易对列表。

        Args:
            target_date: 目标日期 (YYYY-MM-DD)

        Returns:
            List[str]: 该日期对应的交易对列表

        Example:
            在月末重平衡策略下：
            - 2024-01-31: 基于1月数据选择的universe（适用于2月）
            - 2024-02-29: 基于2月数据选择的universe（适用于3月）

            get_symbols_for_date("2024-02-15")
            -> 返回2024-01-31重平衡选择的交易对（因为这是2月15日之前最近的重平衡）
        """
        target_date_obj = pd.to_datetime(target_date).date()

        # 按日期倒序查找最近的有效snapshot
        for snapshot in sorted(self.snapshots, key=lambda x: x.effective_date, reverse=True):
            snapshot_date = pd.to_datetime(snapshot.effective_date).date()
            if snapshot_date <= target_date_obj:
                return snapshot.symbols

        # 如果没有找到合适的snapshot，返回空列表
        return []

    def get_universe_summary(self) -> dict[str, Any]:
        """获取universe概要信息"""
        if not self.snapshots:
            return {"error": "No snapshots available"}

        all_symbols = set()
        for snapshot in self.snapshots:
            all_symbols.update(snapshot.symbols)

        return {
            "total_snapshots": len(self.snapshots),
            "date_range": {
                "start": min(snapshot.effective_date for snapshot in self.snapshots),
                "end": max(snapshot.effective_date for snapshot in self.snapshots),
            },
            "period_ranges": [
                {
                    "effective_date": snapshot.effective_date,
                    "period_start": snapshot.period_start_date,
                    "period_end": snapshot.period_end_date,
                    "duration_days": (
                        pd.to_datetime(snapshot.period_end_date)
                        - pd.to_datetime(snapshot.period_start_date)
                    ).days,
                }
                for snapshot in self.snapshots
            ],
            "unique_symbols_count": len(all_symbols),
            "avg_symbols_per_snapshot": sum(len(snapshot.symbols) for snapshot in self.snapshots)
            / len(self.snapshots),
            "config": self.config.to_dict(),
        }

    def get_period_details(self) -> list[dict[str, Any]]:
        """获取所有周期的详细信息"""
        return [
            {
                "effective_date": snapshot.effective_date,
                "period_start_date": snapshot.period_start_date,
                "period_end_date": snapshot.period_end_date,
                "period_duration_days": (
                    pd.to_datetime(snapshot.period_end_date)
                    - pd.to_datetime(snapshot.period_start_date)
                ).days,
                "symbols_count": len(snapshot.symbols),
                "top_5_symbols": snapshot.symbols[:5],
                "metadata": snapshot.metadata,
            }
            for snapshot in self.snapshots
        ]

        # def get_symbols_overlap_analysis(self) -> dict[str, Any]:
        """分析不同周期间的交易对重叠情况"""
        if len(self.snapshots) < 2:
            return {"error": "Need at least 2 snapshots for overlap analysis"}

        all_symbols = set()
        for snapshot in self.snapshots:
            all_symbols.update(snapshot.symbols)

        # 计算每个符号出现的频率
        symbol_frequency: dict[str, int] = {}
        for symbol in all_symbols:
            frequency = sum(1 for snapshot in self.snapshots if symbol in snapshot.symbols)
            symbol_frequency[symbol] = frequency

        # 计算相邻周期的重叠率
        overlap_rates = []
        for i in range(len(self.snapshots) - 1):
            current_symbols = set(self.snapshots[i].symbols)
            next_symbols = set(self.snapshots[i + 1].symbols)

            intersection = current_symbols.intersection(next_symbols)
            union = current_symbols.union(next_symbols)

            overlap_rate = len(intersection) / len(union) if union else 0
            overlap_rates.append(
                {
                    "from_date": self.snapshots[i].effective_date,
                    "to_date": self.snapshots[i + 1].effective_date,
                    "overlap_rate": overlap_rate,
                    "common_symbols_count": len(intersection),
                    "total_unique_symbols": len(union),
                }
            )

        # 找出稳定的核心交易对（出现在大部分周期中）
        stability_threshold = len(self.snapshots) * 0.7  # 70%的周期中出现
        core_symbols = [
            symbol for symbol, freq in symbol_frequency.items() if freq >= stability_threshold
        ]

        return {
            "total_unique_symbols": len(all_symbols),
            "core_symbols": sorted(core_symbols),
            "core_symbols_count": len(core_symbols),
            "symbol_frequency_distribution": {
                "always_present": len(
                    [s for s, f in symbol_frequency.items() if f == len(self.snapshots)]
                ),
                "frequently_present": len(
                    [s for s, f in symbol_frequency.items() if f >= stability_threshold]
                ),
                "occasionally_present": len(
                    [s for s, f in symbol_frequency.items() if f < stability_threshold]
                ),
            },
            "average_overlap_rate": (
                sum(float(overlap_rate["overlap_rate"]) for overlap_rate in overlap_rates)
                / len(overlap_rates)
                if overlap_rates
                else 0
            ),
            "period_overlaps": overlap_rates,
            "most_stable_symbols": sorted(
                symbol_frequency.items(), key=lambda x: x[1], reverse=True
            )[:10],
        }

    def get_symbols_timeline(self, symbol: str) -> dict[str, Any]:
        """获取特定交易对在整个timeline中的表现"""
        timeline: list[dict[str, Any]] = []
        for snapshot in sorted(self.snapshots, key=lambda x: x.effective_date):
            if symbol in snapshot.symbols:
                rank = snapshot.symbols.index(symbol) + 1  # 排名从1开始
                mean_amount = snapshot.mean_daily_amounts.get(symbol, 0)
                timeline.append(
                    {
                        "effective_date": snapshot.effective_date,
                        "rank": rank,
                        "mean_daily_amount": mean_amount,
                        "total_symbols_in_universe": len(snapshot.symbols),
                    }
                )

        if not timeline:
            return {"error": f"Symbol {symbol} not found in any snapshots"}

        return {
            "symbol": symbol,
            "appearances_count": len(timeline),
            "total_snapshots": len(self.snapshots),
            "presence_rate": len(timeline) / len(self.snapshots),
            "timeline": timeline,
            "best_rank": min(int(entry["rank"]) for entry in timeline),
            "worst_rank": max(int(entry["rank"]) for entry in timeline),
            "avg_rank": sum(int(entry["rank"]) for entry in timeline) / len(timeline),
            "avg_mean_daily_amount": sum(float(entry["mean_daily_amount"]) for entry in timeline)
            / len(timeline),
        }

    def export_to_dataframe(self) -> pd.DataFrame:
        """将universe数据导出为pandas DataFrame，便于分析"""
        rows = []
        for snapshot in self.snapshots:
            for i, symbol in enumerate(snapshot.symbols):
                rows.append(
                    {
                        "effective_date": snapshot.effective_date,
                        "period_start_date": snapshot.period_start_date,
                        "period_end_date": snapshot.period_end_date,
                        "symbol": symbol,
                        "rank": i + 1,
                        "mean_daily_amount": snapshot.mean_daily_amounts.get(symbol, 0),
                        "total_symbols_in_universe": len(snapshot.symbols),
                    }
                )

        df = pd.DataFrame(rows)
        df["effective_date"] = pd.to_datetime(df["effective_date"])
        df["period_start_date"] = pd.to_datetime(df["period_start_date"])
        df["period_end_date"] = pd.to_datetime(df["period_end_date"])

        return df

    def get_top_performers_summary(self, top_n: int = 10) -> dict[str, Any]:
        """获取表现最佳的交易对汇总（基于出现频率和平均排名）"""
        symbol_stats: dict[str, dict[str, Any]] = {}

        for snapshot in self.snapshots:
            for i, symbol in enumerate(snapshot.symbols):
                rank = i + 1
                mean_amount = snapshot.mean_daily_amounts.get(symbol, 0)

                if symbol not in symbol_stats:
                    symbol_stats[symbol] = {
                        "appearances": 0,
                        "total_rank": 0,
                        "total_amount": 0.0,
                        "best_rank": float("inf"),
                        "dates": [],
                    }

                symbol_stats[symbol]["appearances"] += 1
                symbol_stats[symbol]["total_rank"] += rank
                symbol_stats[symbol]["total_amount"] += mean_amount
                symbol_stats[symbol]["best_rank"] = min(
                    float(symbol_stats[symbol]["best_rank"]), float(rank)
                )
                symbol_stats[symbol]["dates"].append(snapshot.effective_date)

        # 计算综合得分（出现频率 * (1/平均排名)）
        for _symbol, stats in symbol_stats.items():
            appearances = int(stats["appearances"])
            total_rank = int(stats["total_rank"])
            total_amount = float(stats["total_amount"])

            avg_rank = total_rank / appearances
            presence_rate = appearances / len(self.snapshots)
            stats["avg_rank"] = avg_rank
            stats["presence_rate"] = presence_rate
            stats["avg_mean_amount"] = total_amount / appearances
            # 综合得分：出现频率越高，平均排名越靠前，得分越高
            stats["composite_score"] = presence_rate * (self.config.top_k / avg_rank)

        # 按综合得分排序
        top_performers = sorted(
            symbol_stats.items(),
            key=lambda x: float(x[1]["composite_score"]),
            reverse=True,
        )[:top_n]

        return {
            "top_performers": [
                {
                    "symbol": symbol,
                    "composite_score": float(stats["composite_score"]),
                    "appearances": int(stats["appearances"]),
                    "presence_rate": float(stats["presence_rate"]),
                    "avg_rank": float(stats["avg_rank"]),
                    "best_rank": float(stats["best_rank"]),
                    "avg_mean_daily_amount": float(stats["avg_mean_amount"]),
                    "active_dates": list(stats["dates"]),
                }
                for symbol, stats in top_performers
            ],
            "analysis_summary": {
                "total_snapshots": len(self.snapshots),
                "total_unique_symbols": len(symbol_stats),
                "avg_symbols_per_snapshot": self.config.top_k,
                "date_range": {
                    "start": min(s.effective_date for s in self.snapshots),
                    "end": max(s.effective_date for s in self.snapshots),
                },
            },
        }
