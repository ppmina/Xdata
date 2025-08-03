"""数据完整性报告模型."""

from dataclasses import dataclass


@dataclass
class IntegrityReport:
    """数据完整性报告."""

    total_symbols: int
    successful_symbols: int
    failed_symbols: list[str]
    missing_periods: list[dict[str, str]]
    data_quality_score: float
    recommendations: list[str]
