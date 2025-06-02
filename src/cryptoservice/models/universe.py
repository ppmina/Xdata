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
        period_start_ts: 数据计算周期开始时间戳 (毫秒)
        period_end_ts: 数据计算周期结束时间戳 (毫秒)
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
    period_start_ts: str
    period_end_ts: str
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
        """创建快照并自动推断周期日期和时间戳

        根据重平衡日期（effective_date）和回看窗口（t1_months），
        自动计算数据计算的时间区间和对应的时间戳。

        Args:
            effective_date: 重平衡生效日期（建议使用月末日期）
            t1_months: T1时间窗口（月），用于回看数据计算
            symbols: 交易对列表
            mean_daily_amounts: 平均日成交量（基于计算周期内的数据）
            metadata: 元数据

        Returns:
            UniverseSnapshot: 带有推断周期日期和时间戳的快照

        Example:
            对于月末重平衡策略：
            effective_date="2024-01-31", t1_months=1
            -> period_start_date="2023-12-31"
            -> period_end_date="2024-01-31"
            含义：基于1月份数据，在1月末选择2月份universe
        """
        from datetime import datetime

        effective_dt = pd.to_datetime(effective_date)
        period_start_dt = effective_dt - pd.DateOffset(months=t1_months)

        # 计算时间戳（毫秒）
        period_start_ts = str(
            int(
                datetime.strptime(
                    f"{period_start_dt.strftime('%Y-%m-%d')} 00:00:00",
                    "%Y-%m-%d %H:%M:%S",
                ).timestamp()
                * 1000
            )
        )
        period_end_ts = str(
            int(
                datetime.strptime(f"{effective_date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp()
                * 1000
            )
        )

        return cls(
            effective_date=effective_date,
            period_start_date=period_start_dt.strftime("%Y-%m-%d"),
            period_end_date=effective_date,  # 数据计算周期结束 = 重平衡日期
            period_start_ts=period_start_ts,
            period_end_ts=period_end_ts,
            symbols=symbols,
            mean_daily_amounts=mean_daily_amounts,
            metadata=metadata,
        )

    @classmethod
    def create_with_dates_and_timestamps(
        cls,
        effective_date: str,
        period_start_date: str,
        period_end_date: str,
        period_start_ts: str,
        period_end_ts: str,
        symbols: list[str],
        mean_daily_amounts: dict[str, float],
        metadata: dict[str, Any] | None = None,
    ) -> "UniverseSnapshot":
        """创建快照，明确指定所有日期和时间戳

        Args:
            effective_date: 重平衡生效日期
            period_start_date: 数据计算周期开始日期
            period_end_date: 数据计算周期结束日期
            period_start_ts: 数据计算周期开始时间戳 (毫秒)
            period_end_ts: 数据计算周期结束时间戳 (毫秒)
            symbols: 交易对列表
            mean_daily_amounts: 平均日成交量
            metadata: 元数据

        Returns:
            UniverseSnapshot: 快照实例
        """
        return cls(
            effective_date=effective_date,
            period_start_date=period_start_date,
            period_end_date=period_end_date,
            period_start_ts=period_start_ts,
            period_end_ts=period_end_ts,
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
            "period_start_ts": self.period_start_ts,
            "period_end_ts": self.period_end_ts,
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
            "period_start_ts": self.period_start_ts,
            "period_end_ts": self.period_end_ts,
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
        snapshots = []

        for snap in data["snapshots"]:
            period_start_date = snap.get("period_start_date", snap["effective_date"])
            period_end_date = snap.get("period_end_date", snap["effective_date"])

            # 处理时间戳字段 - 如果不存在则自动计算
            period_start_ts = snap.get("period_start_ts")
            period_end_ts = snap.get("period_end_ts")

            if period_start_ts is None or period_end_ts is None:
                from datetime import datetime as dt_class

                # 自动计算时间戳
                period_start_ts = str(
                    int(
                        dt_class.strptime(
                            f"{period_start_date} 00:00:00", "%Y-%m-%d %H:%M:%S"
                        ).timestamp()
                        * 1000
                    )
                )
                period_end_ts = str(
                    int(
                        dt_class.strptime(
                            f"{period_end_date} 23:59:59", "%Y-%m-%d %H:%M:%S"
                        ).timestamp()
                        * 1000
                    )
                )

            snapshot = UniverseSnapshot(
                effective_date=snap["effective_date"],
                period_start_date=period_start_date,
                period_end_date=period_end_date,
                period_start_ts=period_start_ts,
                period_end_ts=period_end_ts,
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
                "period_start_ts": snapshot.period_start_ts,
                "period_end_ts": snapshot.period_end_ts,
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

    @classmethod
    def get_schema(cls) -> dict[str, Any]:
        """获取Universe定义的JSON Schema

        Returns:
            Dict: JSON Schema定义
        """
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Universe Definition Schema",
            "description": "Cryptocurrency universe definition with configuration and snapshots",
            "type": "object",
            "properties": {
                "config": {
                    "type": "object",
                    "description": "Universe configuration parameters",
                    "properties": {
                        "start_date": {
                            "type": "string",
                            "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                            "description": "Start date in YYYY-MM-DD format",
                        },
                        "end_date": {
                            "type": "string",
                            "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                            "description": "End date in YYYY-MM-DD format",
                        },
                        "t1_months": {
                            "type": "integer",
                            "minimum": 1,
                            "description": (
                                "T1 lookback window in months for calculating mean daily amount"
                            ),
                        },
                        "t2_months": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "T2 rebalancing frequency in months",
                        },
                        "t3_months": {
                            "type": "integer",
                            "minimum": 0,
                            "description": ("T3 minimum contract existence time in months"),
                        },
                        "top_k": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "Number of top contracts to select",
                        },
                    },
                    "required": [
                        "start_date",
                        "end_date",
                        "t1_months",
                        "t2_months",
                        "t3_months",
                        "top_k",
                    ],
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
                            "period_start_date": {
                                "type": "string",
                                "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                                "description": "Data calculation period start date",
                            },
                            "period_end_date": {
                                "type": "string",
                                "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                                "description": "Data calculation period end date",
                            },
                            "period_start_ts": {
                                "type": "string",
                                "pattern": "^\\d+$",
                                "description": (
                                    "Data calculation period start timestamp in milliseconds"
                                ),
                            },
                            "period_end_ts": {
                                "type": "string",
                                "pattern": "^\\d+$",
                                "description": (
                                    "Data calculation period end timestamp in milliseconds"
                                ),
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
                            "period_start_date",
                            "period_end_date",
                            "period_start_ts",
                            "period_end_ts",
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
            },
            "snapshots": [
                {
                    "effective_date": "2024-01-31",
                    "period_start_date": "2023-12-31",
                    "period_end_date": "2024-01-31",
                    "period_start_ts": "1703980800000",
                    "period_end_ts": "1706745599000",
                    "symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT"],
                    "mean_daily_amounts": {
                        "BTCUSDT": 1234567890.0,
                        "ETHUSDT": 987654321.0,
                        "BNBUSDT": 456789123.0,
                    },
                    "metadata": {
                        "t1_start_date": "2023-12-31",
                        "calculated_t1_start": "2023-12-31",
                        "period_adjusted": False,
                        "strict_date_range": False,
                        "selected_symbols_count": 3,
                        "total_candidates": 100,
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
                    "top_k",
                ]
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
                        "period_start_date",
                        "period_end_date",
                        "period_start_ts",
                        "period_end_ts",
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
