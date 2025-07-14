"""市场数据模型模块。

包含各种市场数据的数据模型，如资金费率、持仓量、多空比例等。
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict


@dataclass
class FundingRate:
    """资金费率数据模型。

    Attributes:
        symbol: 交易对符号
        funding_time: 资金费率时间（毫秒时间戳）
        funding_rate: 资金费率
        mark_price: 标记价格（可选）
        index_price: 指数价格（可选）
        estimated_settle_price: 预估结算价格（可选）
        last_funding_rate: 上一次资金费率（可选）
        next_funding_time: 下一次资金费率时间（可选）
        interest_rate: 利率（可选）
    """

    symbol: str
    funding_time: int
    funding_rate: Decimal
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    estimated_settle_price: Decimal | None = None
    last_funding_rate: Decimal | None = None
    next_funding_time: int | None = None
    interest_rate: Decimal | None = None

    @classmethod
    def from_binance_response(cls, data: Dict[str, Any]) -> "FundingRate":
        """从Binance API响应创建FundingRate实例。

        Args:
            data: Binance API响应数据

        Returns:
            FundingRate: 资金费率实例
        """
        return cls(
            symbol=data["symbol"],
            funding_time=int(data["fundingTime"]),
            funding_rate=Decimal(str(data["fundingRate"])),
            mark_price=Decimal(str(data["markPrice"])) if "markPrice" in data else None,
            index_price=(Decimal(str(data["indexPrice"])) if "indexPrice" in data else None),
            estimated_settle_price=(
                Decimal(str(data["estimatedSettlePrice"])) if "estimatedSettlePrice" in data else None
            ),
            last_funding_rate=(Decimal(str(data["lastFundingRate"])) if "lastFundingRate" in data else None),
            next_funding_time=(int(data["nextFundingTime"]) if "nextFundingTime" in data else None),
            interest_rate=(Decimal(str(data["interestRate"])) if "interestRate" in data else None),
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式。

        Returns:
            Dict: 字典格式的数据
        """
        result = {
            "symbol": self.symbol,
            "funding_time": self.funding_time,
            "funding_rate": float(self.funding_rate),
        }

        if self.mark_price is not None:
            result["mark_price"] = float(self.mark_price)
        if self.index_price is not None:
            result["index_price"] = float(self.index_price)
        if self.estimated_settle_price is not None:
            result["estimated_settle_price"] = float(self.estimated_settle_price)
        if self.last_funding_rate is not None:
            result["last_funding_rate"] = float(self.last_funding_rate)
        if self.next_funding_time is not None:
            result["next_funding_time"] = self.next_funding_time
        if self.interest_rate is not None:
            result["interest_rate"] = float(self.interest_rate)

        return result


@dataclass
class OpenInterest:
    """持仓量数据模型。

    Attributes:
        symbol: 交易对符号
        open_interest: 持仓量
        time: 时间戳（毫秒）
        open_interest_value: 持仓量价值（可选，USDT计价）
    """

    symbol: str
    open_interest: Decimal
    time: int
    open_interest_value: Decimal | None = None

    @classmethod
    def from_binance_response(cls, data: Dict[str, Any]) -> "OpenInterest":
        """从Binance API响应创建OpenInterest实例。

        Args:
            data: Binance API响应数据

        Returns:
            OpenInterest: 持仓量实例
        """
        return cls(
            symbol=data["symbol"],
            open_interest=Decimal(str(data["sumOpenInterest"])),
            time=int(data["timestamp"]),
            open_interest_value=(
                Decimal(str(data["sumOpenInterestValue"])) if "sumOpenInterestValue" in data else None
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式。

        Returns:
            Dict: 字典格式的数据
        """
        result = {
            "symbol": self.symbol,
            "open_interest": float(self.open_interest),
            "time": self.time,
        }

        if self.open_interest_value is not None:
            result["open_interest_value"] = float(self.open_interest_value)

        return result


@dataclass
class LongShortRatio:
    """多空比例数据模型。

    Attributes:
        symbol: 交易对符号
        long_short_ratio: 多空比例
        long_account: 多头账户比例
        short_account: 空头账户比例
        timestamp: 时间戳（毫秒）
        ratio_type: 比例类型（account/position/global等）
    """

    symbol: str
    long_short_ratio: Decimal
    long_account: Decimal
    short_account: Decimal
    timestamp: int
    ratio_type: str = "account"  # account, position, global, taker

    @classmethod
    def from_binance_response(cls, data: Dict[str, Any], ratio_type: str = "account") -> "LongShortRatio":
        """从Binance API响应创建LongShortRatio实例。

        Args:
            data: Binance API响应数据
            ratio_type: 比例类型

        Returns:
            LongShortRatio: 多空比例实例
        """
        return cls(
            symbol=data["symbol"],
            long_short_ratio=Decimal(str(data["longShortRatio"])),
            long_account=Decimal(str(data.get("longAccount", data.get("longPosition", "0")))),
            short_account=Decimal(str(data.get("shortAccount", data.get("shortPosition", "0")))),
            timestamp=int(data["timestamp"]),
            ratio_type=ratio_type,
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式。

        Returns:
            Dict: 字典格式的数据
        """
        return {
            "symbol": self.symbol,
            "long_short_ratio": float(self.long_short_ratio),
            "long_account": float(self.long_account),
            "short_account": float(self.short_account),
            "timestamp": self.timestamp,
            "ratio_type": self.ratio_type,
        }
