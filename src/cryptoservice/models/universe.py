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
        delay_days: 延迟天数
        quote_asset: 计价币种
        top_k: 选取的top合约数量
        top_ratio: 选取的top合约比率 (例如 0.8 表示 top 80%)
    """

    start_date: str
    end_date: str
    t1_months: int
    t2_months: int
    t3_months: int
    delay_days: int
    quote_asset: str
    top_k: int | None = None
    top_ratio: float | None = None

    def __post_init__(self) -> None:
        """验证top_k和top_ratio只有一个被提供."""
        if self.top_k is None and self.top_ratio is None:
            raise ValueError("必须提供 top_k 或 top_ratio 中的一个。")
        if self.top_k is not None and self.top_ratio is not None:
            raise ValueError("top_k 和 top_ratio 不能同时提供。")
        if self.top_ratio is not None and not (0 < self.top_ratio <= 1):
            raise ValueError("top_ratio 必须在 (0, 1] 范围内。")

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        data = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "t1_months": self.t1_months,
            "t2_months": self.t2_months,
            "t3_months": self.t3_months,
            "delay_days": self.delay_days,
            "quote_asset": self.quote_asset,
        }
        if self.top_k is not None:
            data["top_k"] = self.top_k
        if self.top_ratio is not None:
            data["top_ratio"] = self.top_ratio
        return data


@dataclass
class UniverseSnapshot:
    """Universe快照类，表示某个时间点的universe状态.

    Attributes:
        effective_date: 生效日期（重平衡日期，通常是月末）
        start_date: 实际下载数据开始时间
        end_date: 实际下载数据结束日期
        start_date_ts: 实际使用开始时间戳 (毫秒)
        end_date_ts: 实际使用结束时间戳 (毫秒)
        calculated_t1_start: 数据计算周期开始日期（T1回看的开始日期）
        calculated_t1_end: 数据计算周期结束日期（通常等于重平衡日期）
        calculated_t1_start_ts: 数据计算周期开始时间戳 (毫秒)
        calculated_t1_end_ts: 数据计算周期结束时间戳 (毫秒)
        symbols: 该时间点的universe交易对列表（基于period内数据计算得出）
        mean_daily_amounts: 各交易对在period内的平均日成交量
        metadata: 额外的元数据信息

    Note:
        在月末重平衡策略下：
        - effective_date: 重平衡决策的日期（如2024-01-31）
        - period: 用于计算的数据区间（如2023-12-31到2024-01-31）
        - 含义: 基于1月份数据，在1月末选择2月份的universe
    """

    effective_date: str  # 重平衡生效日期
    start_date: str  # 实际使用开始日期
    end_date: str  # 实际使用结束日期
    start_date_ts: str  # 实际使用开始时间戳
    end_date_ts: str  # 实际使用结束时间戳
    calculated_t1_start: str  # 计算周期开始日期
    calculated_t1_end: str  # 计算周期结束日期
    calculated_t1_start_ts: str  # 计算周期开始时间戳
    calculated_t1_end_ts: str  # 计算周期结束时间戳
    symbols: list[str]
    mean_daily_amounts: dict[str, float]
    metadata: dict[str, Any] | None = None

    @staticmethod
    def _calculate_timestamp(date_str: str, time_str: str = "00:00:00") -> str:
        """计算日期的时间戳（毫秒）

        Args:
            date_str: 日期字符串 (YYYY-MM-DD)
            time_str: 时间字符串 (HH:MM:SS)，默认为开始时间

        Returns:
            str: 毫秒时间戳
        """
        from datetime import datetime

        return str(int(datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S").timestamp() * 1000))

    @classmethod
    def create_with_inferred_periods(
        cls,
        effective_date: str,
        t1_months: int,
        symbols: list[str],
        mean_daily_amounts: dict[str, float],
        metadata: dict[str, Any] | None = None,
        next_effective_date: str | None = None,
    ) -> "UniverseSnapshot":
        """创建快照并自动推断周期日期和时间戳

        根据重平衡日期（effective_date）和回看窗口（t1_months），
        自动计算数据计算的时间区间和对应的时间戳。

        Args:
            effective_date: 重平衡生效日期（建议使用月末日期）
            t1_months: T1时间窗口（月），用于回看数据计算
            symbols: 交易对列表
            mean_daily_amounts: 平均日成交量（基于计算周期内的数据）
            metadata: 元数据
            next_effective_date: 下一次重平衡日期（用于确定使用周期结束日期）

        Returns:
            UniverseSnapshot: 带有推断周期日期和时间戳的快照

        Example:
            对于月末重平衡策略：
            effective_date="2024-01-31", t1_months=1
            -> period: 2023-12-31 to 2024-01-31 (用于计算universe)
            -> usage: 2024-02-01 to 2024-02-29 (实际使用期间)
            含义：基于1月份数据，在1月末选择2月份universe
        """
        effective_dt = pd.to_datetime(effective_date)
        calculated_t1_start = effective_dt - pd.DateOffset(months=t1_months)

        # 计算使用周期
        usage_start_dt = effective_dt + pd.Timedelta(days=1)
        if next_effective_date:
            usage_end_dt = pd.to_datetime(next_effective_date)
        else:
            # 如果没有下一个重平衡日期，估算到下个月末
            usage_end_dt = usage_start_dt + pd.offsets.MonthEnd(0)

        # 计算所有时间戳（毫秒）
        calculated_t1_start_str = calculated_t1_start.strftime("%Y-%m-%d")
        usage_start_str = usage_start_dt.strftime("%Y-%m-%d")
        usage_end_str = usage_end_dt.strftime("%Y-%m-%d")

        calculated_t1_start_ts = cls._calculate_timestamp(calculated_t1_start_str, "00:00:00")
        calculated_t1_end_ts = cls._calculate_timestamp(effective_date, "23:59:59")
        start_date_ts = cls._calculate_timestamp(usage_start_str, "00:00:00")
        end_date_ts = cls._calculate_timestamp(usage_end_str, "23:59:59")

        return cls(
            effective_date=effective_date,  # 重平衡生效日期
            start_date=usage_start_str,  # 实际使用开始日期
            end_date=usage_end_str,  # 实际使用结束日期
            calculated_t1_start=calculated_t1_start_str,
            calculated_t1_end=effective_date,  # 数据计算周期结束 = 重平衡日期
            calculated_t1_start_ts=calculated_t1_start_ts,
            calculated_t1_end_ts=calculated_t1_end_ts,
            start_date_ts=start_date_ts,
            end_date_ts=end_date_ts,
            symbols=symbols,
            mean_daily_amounts=mean_daily_amounts,
            metadata=metadata,
        )

    @classmethod
    def create_with_dates_and_timestamps(
        cls,
        usage_t1_start: str,
        usage_t1_end: str,
        calculated_t1_start: str,
        calculated_t1_end: str,
        symbols: list[str],
        mean_daily_amounts: dict[str, float],
        metadata: dict[str, Any] | None = None,
    ) -> "UniverseSnapshot":
        """创建快照，明确指定所有日期和时间戳

        Args:
            usage_t1_start: 实际使用开始日期
            usage_t1_end: 实际使用结束日期
            calculated_t1_start: 数据计算周期开始日期
            calculated_t1_end: 数据计算周期结束日期
            symbols: 交易对列表
            mean_daily_amounts: 平均日成交量
            metadata: 元数据

        Returns:
            UniverseSnapshot: 快照实例
        """
        # 计算所有时间戳（毫秒）
        calculated_t1_start_ts = cls._calculate_timestamp(calculated_t1_start, "00:00:00")
        calculated_t1_end_ts = cls._calculate_timestamp(calculated_t1_end, "23:59:59")
        start_date_ts = cls._calculate_timestamp(usage_t1_start, "00:00:00")
        end_date_ts = cls._calculate_timestamp(usage_t1_end, "23:59:59")

        return cls(
            effective_date=calculated_t1_end,  # 重平衡生效日期（计算周期结束日期）
            start_date=usage_t1_start,  # 实际使用开始日期
            end_date=usage_t1_end,  # 实际使用结束日期
            calculated_t1_start=calculated_t1_start,
            calculated_t1_end=calculated_t1_end,
            calculated_t1_start_ts=calculated_t1_start_ts,
            calculated_t1_end_ts=calculated_t1_end_ts,
            start_date_ts=start_date_ts,
            end_date_ts=end_date_ts,
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
        calculated_t1_start_dt = pd.to_datetime(self.calculated_t1_start)
        calculated_t1_end_dt = pd.to_datetime(self.calculated_t1_end)

        # 计算实际的月数差
        actual_months_diff = (effective_dt.year - calculated_t1_start_dt.year) * 12 + (
            effective_dt.month - calculated_t1_start_dt.month
        )

        # 计算实际天数
        actual_days = (calculated_t1_end_dt - calculated_t1_start_dt).days

        # 验证期末日期是否等于生效日期
        period_end_matches_effective = self.calculated_t1_end == self.effective_date

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
                "calculated_t1_start": self.calculated_t1_start,
                "calculated_t1_end": self.calculated_t1_end,
            },
        }

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "effective_date": self.effective_date,
            "start_date": self.start_date,  # 实际使用开始日期
            "end_date": self.end_date,  # 实际使用结束日期
            "calculated_t1_start": self.calculated_t1_start,
            "calculated_t1_end": self.calculated_t1_end,
            "calculated_t1_start_ts": self.calculated_t1_start_ts,
            "calculated_t1_end_ts": self.calculated_t1_end_ts,
            "start_date_ts": self.start_date_ts,
            "end_date_ts": self.end_date_ts,
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
            "calculated_t1_start": self.calculated_t1_start,
            "calculated_t1_end": self.calculated_t1_end,
            "calculated_t1_start_ts": self.calculated_t1_start_ts,
            "calculated_t1_end_ts": self.calculated_t1_end_ts,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "start_date_ts": self.start_date_ts,
            "end_date_ts": self.end_date_ts,
            "effective_date": self.effective_date,
            "period_duration_days": str(
                (pd.to_datetime(self.calculated_t1_end) - pd.to_datetime(self.calculated_t1_start)).days
            ),
        }

    def get_usage_period_info(self) -> dict[str, str]:
        """获取Universe使用周期信息

        返回该快照对应的实际使用期间和计算期间。

        Returns:
            Dict: 包含两个关键时间范围的信息
        """
        return {
            # 计算期间 - 用于定义universe
            "calculation_period_start": self.calculated_t1_start,
            "calculation_period_end": self.calculated_t1_end,
            "rebalance_decision_date": self.effective_date,
            # 使用期间 - 实际需要下载的数据
            "usage_period_start": self.start_date,
            "usage_period_end": self.end_date,
            "usage_period_duration_days": str((pd.to_datetime(self.end_date) - pd.to_datetime(self.start_date)).days),
            # 其他信息
            "universe_symbols_count": str(len(self.symbols)),
            "note": "calculation_period用于定义universe，usage_period用于下载训练数据",
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
        snapshots = []

        for snap in data["snapshots"]:
            # 计算数据周期
            calculated_t1_start = snap["calculated_t1_start"]
            calculated_t1_end = snap["calculated_t1_end"]
            calculated_t1_start_ts = snap["calculated_t1_start_ts"]
            calculated_t1_end_ts = snap["calculated_t1_end_ts"]

            # 计算使用周期 - 从历史数据推断或默认计算
            effective_dt = pd.to_datetime(snap["effective_date"])
            usage_start_dt = effective_dt + pd.Timedelta(days=1)
            usage_end_dt = usage_start_dt + pd.offsets.MonthEnd(0)

            # 从快照数据中获取使用期间，如果不存在则使用计算的值
            start_date = snap.get("start_date", usage_start_dt.strftime("%Y-%m-%d"))
            end_date = snap.get("end_date", usage_end_dt.strftime("%Y-%m-%d"))

            # 计算或获取使用期间的时间戳
            start_date_ts = snap.get("start_date_ts")
            end_date_ts = snap.get("end_date_ts")

            if start_date_ts is None:
                start_date_ts = UniverseSnapshot._calculate_timestamp(start_date, "00:00:00")
            if end_date_ts is None:
                end_date_ts = UniverseSnapshot._calculate_timestamp(end_date, "23:59:59")

            snapshot = UniverseSnapshot(
                effective_date=snap["effective_date"],
                start_date=start_date,
                end_date=end_date,
                start_date_ts=start_date_ts,
                end_date_ts=end_date_ts,
                calculated_t1_start=calculated_t1_start,
                calculated_t1_end=calculated_t1_end,
                calculated_t1_start_ts=calculated_t1_start_ts,
                calculated_t1_end_ts=calculated_t1_end_ts,
                symbols=snap["symbols"],
                mean_daily_amounts=snap["mean_daily_amounts"],
                metadata=snap.get("metadata"),
            )
            snapshots.append(snapshot)

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

    def get_symbols_for_date(self, target_date: str, date_type: str = "usage") -> list[str]:
        """获取指定日期的universe交易对列表

        Args:
            target_date: 目标日期 (YYYY-MM-DD)
            date_type: 日期类型, "usage" (默认) 或 "effective".
                       - "usage": 查找覆盖该使用日期的快照
                       - "effective": 查找在该生效日期生成的快照

        Returns:
            List[str]: 该日期对应的交易对列表
        """
        target_dt = pd.to_datetime(target_date)

        if date_type == "usage":
            for snapshot in sorted(self.snapshots, key=lambda x: x.start_date, reverse=True):
                start_dt = pd.to_datetime(snapshot.start_date)
                end_dt = pd.to_datetime(snapshot.end_date)
                if start_dt <= target_dt <= end_dt:
                    return snapshot.symbols
        elif date_type == "effective":
            for snapshot in self.snapshots:
                if snapshot.effective_date == target_date:
                    return snapshot.symbols
        else:
            raise ValueError("date_type 必须是 'usage' 或 'effective'")

        return []

    def get_snapshot_for_date(self, target_date: str, date_type: str = "usage") -> UniverseSnapshot | None:
        """获取指定日期的UniverseSnapshot

        Args:
            target_date: 目标日期 (YYYY-MM-DD)
            date_type: 日期类型, "usage" (默认) 或 "effective".

        Returns:
            UniverseSnapshot | None: 对应的快照，如果未找到则返回None
        """
        target_dt = pd.to_datetime(target_date)

        if date_type == "usage":
            for snapshot in sorted(self.snapshots, key=lambda x: x.start_date, reverse=True):
                start_dt = pd.to_datetime(snapshot.start_date)
                end_dt = pd.to_datetime(snapshot.end_date)
                if start_dt <= target_dt <= end_dt:
                    return snapshot
        elif date_type == "effective":
            for snapshot in self.snapshots:
                if snapshot.effective_date == target_date:
                    return snapshot
        else:
            raise ValueError("date_type 必须是 'usage' 或 'effective'")

        return None

    @classmethod
    def get_schema(cls) -> dict[str, Any]:
        """获取Universe定义的JSON Schema

        Returns:
            Dict: JSON Schema定义
        """
        config_properties = {
            "start_date": {
                "type": "string",
                "pattern": r"^\d{4}-\d{2}-\d{2}$",
                "description": "Start date in YYYY-MM-DD format",
            },
            "end_date": {
                "type": "string",
                "pattern": r"^\d{4}-\d{2}-\d{2}$",
                "description": "End date in YYYY-MM-DD format",
            },
            "t1_months": {
                "type": "integer",
                "minimum": 1,
                "description": "T1 lookback window in months for calculating mean daily amount",
            },
            "t2_months": {
                "type": "integer",
                "minimum": 1,
                "description": "T2 rebalancing frequency in months",
            },
            "t3_months": {
                "type": "integer",
                "minimum": 0,
                "description": "T3 minimum contract existence time in months",
            },
            "top_k": {
                "type": "integer",
                "minimum": 1,
                "description": "Number of top contracts to select",
            },
            "top_ratio": {
                "type": "number",
                "minimum": 0,
                "exclusiveMaximum": 1,
                "description": "Ratio of top contracts to select (e.g., 0.8 for top 80%)",
            },
            "delay_days": {
                "type": "integer",
                "minimum": 0,
                "description": "Delay days for universe rebalancing",
            },
            "quote_asset": {
                "type": "string",
                "pattern": r"^[A-Z0-9]+$",
                "description": "Quote asset for trading pairs",
            },
        }

        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Universe Definition Schema",
            "description": "Cryptocurrency universe definition with configuration and snapshots",
            "type": "object",
            "properties": {
                "config": {
                    "type": "object",
                    "description": "Universe configuration parameters",
                    "properties": config_properties,
                    "required": [
                        "start_date",
                        "end_date",
                        "t1_months",
                        "t2_months",
                        "t3_months",
                    ],
                    "oneOf": [{"required": ["top_k"]}, {"required": ["top_ratio"]}],
                    "additionalProperties": False,
                },
                "snapshots": {
                    "type": "array",
                    "description": "Time series of universe snapshots",
                    "items": {
                        "type": "object",
                        "properties": {
                            "effective_date": {
                                "type": "string",
                                "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                                "description": "Rebalancing effective date",
                            },
                            "calculated_t1_start": {
                                "type": "string",
                                "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                                "description": ("Data calculation period start date (T1 lookback start)"),
                            },
                            "calculated_t1_end": {
                                "type": "string",
                                "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                                "description": "Data calculation period end date (T1 lookback end)",
                            },
                            "calculated_t1_start_ts": {
                                "type": "string",
                                "pattern": "^\\d+$",
                                "description": ("Data calculation period start timestamp in milliseconds"),
                            },
                            "calculated_t1_end_ts": {
                                "type": "string",
                                "pattern": "^\\d+$",
                                "description": ("Data calculation period end timestamp in milliseconds"),
                            },
                            "symbols": {
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "pattern": "^[A-Z0-9]+USDT$",
                                    "description": "Trading pair symbol (e.g., BTCUSDT)",
                                },
                                "description": "List of selected trading pairs for this period",
                            },
                            "mean_daily_amounts": {
                                "type": "object",
                                "patternProperties": {
                                    "^[A-Z0-9]+USDT$": {
                                        "type": "number",
                                        "minimum": 0,
                                        "description": "Mean daily trading volume in USDT",
                                    }
                                },
                                "description": "Mean daily trading amounts for each symbol",
                            },
                            "metadata": {
                                "type": "object",
                                "description": "Additional metadata for this snapshot",
                                "properties": {
                                    "t1_start_date": {
                                        "type": "string",
                                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                                    },
                                    "calculated_t1_start": {
                                        "type": "string",
                                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                                    },
                                    "period_adjusted": {"type": "boolean"},
                                    "strict_date_range": {"type": "boolean"},
                                    "selected_symbols_count": {
                                        "type": "integer",
                                        "minimum": 0,
                                    },
                                    "total_candidates": {
                                        "type": "integer",
                                        "minimum": 0,
                                    },
                                },
                                "additionalProperties": True,
                            },
                        },
                        "required": [
                            "effective_date",
                            "calculated_t1_start",
                            "calculated_t1_end",
                            "calculated_t1_start_ts",
                            "calculated_t1_end_ts",
                            "symbols",
                            "mean_daily_amounts",
                        ],
                        "additionalProperties": False,
                    },
                },
                "creation_time": {
                    "type": "string",
                    "format": "date-time",
                    "description": "ISO 8601 timestamp when this universe definition was created",
                },
                "description": {
                    "type": ["string", "null"],
                    "description": "Optional description of this universe definition",
                },
            },
            "required": ["config", "snapshots", "creation_time"],
            "additionalProperties": False,
        }

    @classmethod
    def get_schema_example(cls) -> dict[str, Any]:
        """获取Universe定义的示例数据

        Returns:
            Dict: 符合schema的示例数据
        """
        return {
            "config": {
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "t1_months": 1,
                "t2_months": 1,
                "t3_months": 3,
                "top_k": 10,
                "delay_days": 7,
                "quote_asset": "USDT",
            },
            "snapshots": [
                {
                    "effective_date": "2024-01-31",
                    "start_date": "2024-02-01",
                    "end_date": "2024-02-29",
                    "calculated_t1_start": "2023-12-31",
                    "calculated_t1_end": "2024-01-31",
                    "calculated_t1_start_ts": "1703980800000",
                    "calculated_t1_end_ts": "1706745599000",
                    "symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT"],
                    "mean_daily_amounts": {
                        "BTCUSDT": 1234567890.0,
                        "ETHUSDT": 987654321.0,
                        "BNBUSDT": 456789123.0,
                    },
                    "metadata": {
                        "calculated_t1_start": "2023-12-31",
                        "calculated_t1_end": "2024-01-31",
                        "delay_days": 7,
                        "quote_asset": "USDT",
                        "selected_symbols_count": 3,
                    },
                }
            ],
            "creation_time": "2024-01-01T00:00:00",
            "description": "Example universe definition for top cryptocurrency pairs",
        }

    def export_schema_to_file(self, file_path: Path | str, include_example: bool = True) -> None:
        """导出schema定义到文件

        Args:
            file_path: 输出文件路径
            include_example: 是否包含示例数据
        """
        import json

        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        schema_data = {
            "schema": self.get_schema(),
            "version": "1.0.0",
            "generated_at": datetime.now().isoformat(),
        }

        if include_example:
            schema_data["example"] = self.get_schema_example()

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(schema_data, f, indent=2, ensure_ascii=False)

    def validate_against_schema(self) -> dict[str, Any]:
        """验证当前universe定义是否符合schema

        Returns:
            Dict: 验证结果
        """
        try:
            # 这里可以使用jsonschema库进行验证，但为了减少依赖，我们进行基本验证
            data = self.to_dict()

            errors: list[str] = []
            warnings: list[str] = []

            # 基本结构验证
            required_fields = ["config", "snapshots", "creation_time"]
            for field in required_fields:
                if field not in data:
                    errors.append(f"Missing required field: {field}")

            # 配置验证
            if "config" in data:
                config = data["config"]
                config_required = [
                    "start_date",
                    "end_date",
                    "t1_months",
                    "t2_months",
                    "t3_months",
                ]
                if "top_k" not in config and "top_ratio" not in config:
                    errors.append("Config must contain either 'top_k' or 'top_ratio'")

                for field in config_required:
                    if field not in config:
                        errors.append(f"Missing required config field: {field}")

                # 日期格式验证
                import re

                date_pattern = r"^\d{4}-\d{2}-\d{2}$"
                for date_field in ["start_date", "end_date"]:
                    if date_field in config and not re.match(date_pattern, config[date_field]):
                        errors.append(f"Invalid date format for {date_field}: {config[date_field]}")

            # 快照验证
            if "snapshots" in data:
                for i, snapshot in enumerate(data["snapshots"]):
                    snapshot_required = [
                        "effective_date",
                        "calculated_t1_start",
                        "calculated_t1_end",
                        "calculated_t1_start_ts",
                        "calculated_t1_end_ts",
                        "symbols",
                        "mean_daily_amounts",
                    ]
                    for field in snapshot_required:
                        if field not in snapshot:
                            errors.append(f"Missing required field in snapshot {i}: {field}")

            return {
                "valid": len(errors) == 0,
                "errors": errors,
                "warnings": warnings,
                "validation_time": datetime.now().isoformat(),
            }

        except Exception as e:
            return {
                "valid": False,
                "errors": [f"Validation failed with exception: {str(e)}"],
                "warnings": [],
                "validation_time": datetime.now().isoformat(),
            }
