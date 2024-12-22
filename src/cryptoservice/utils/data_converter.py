from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Union


class DataConverter:
    """数据转换工具类."""

    @staticmethod
    def to_decimal(value: Union[str, float, int]) -> Decimal:
        """转换为Decimal类型."""
        return Decimal(str(value))

    @staticmethod
    def format_timestamp(timestamp: Union[int, float]) -> datetime:
        """转换时间戳为datetime对象."""
        if isinstance(timestamp, (int, float)):
            return datetime.fromtimestamp(timestamp / 1000)
        return datetime.now()

    @staticmethod
    def format_market_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """格式化市场数据."""
        return {
            "price": float(data.get("price", 0)),
            "volume": float(data.get("volume", 0)),
            "change": float(data.get("priceChangePercent", 0)),
            "high": float(data.get("highPrice", 0)),
            "low": float(data.get("lowPrice", 0)),
            "timestamp": datetime.now().isoformat(),
        }
