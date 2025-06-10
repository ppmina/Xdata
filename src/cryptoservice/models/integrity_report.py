from dataclasses import dataclass
from typing import List, Dict


@dataclass
class IntegrityReport:
    """数据完整性报告"""

    total_symbols: int
    successful_symbols: int
    failed_symbols: List[str]
    missing_periods: List[Dict[str, str]]
    data_quality_score: float
    recommendations: List[str]
