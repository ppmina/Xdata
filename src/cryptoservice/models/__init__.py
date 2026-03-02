"""Data models and enums (lazy exports)."""

from __future__ import annotations

from typing import Any

__all__ = [
    "KlineMarketTicker",
    "FuturesKlineTicker",
    "PerpetualMarketTicker",
    "FundingRate",
    "OpenInterest",
    "LongShortRatio",
    "SortBy",
    "Freq",
    "HistoricalKlinesType",
    "Univ",
    "IntegrityReport",
    "ErrorSeverity",
    "KlineIndex",
    "UniverseDailySnapshot",
    "UniverseDefinition",
]


def __getattr__(name: str) -> Any:
    """Lazily resolve model exports to reduce import side effects."""
    if name in {"ErrorSeverity", "Freq", "HistoricalKlinesType", "SortBy", "Univ"}:
        from .enums import ErrorSeverity, Freq, HistoricalKlinesType, SortBy, Univ

        enum_exports: dict[str, Any] = {
            "ErrorSeverity": ErrorSeverity,
            "Freq": Freq,
            "HistoricalKlinesType": HistoricalKlinesType,
            "SortBy": SortBy,
            "Univ": Univ,
        }
        return enum_exports[name]

    if name == "IntegrityReport":
        from .integrity_report import IntegrityReport

        return IntegrityReport

    if name in {"FundingRate", "LongShortRatio", "OpenInterest"}:
        from .market_data import FundingRate, LongShortRatio, OpenInterest

        metric_exports: dict[str, Any] = {
            "FundingRate": FundingRate,
            "LongShortRatio": LongShortRatio,
            "OpenInterest": OpenInterest,
        }
        return metric_exports[name]

    if name in {
        "FuturesKlineTicker",
        "KlineIndex",
        "KlineMarketTicker",
        "PerpetualMarketTicker",
    }:
        from .market_ticker import (
            FuturesKlineTicker,
            KlineIndex,
            KlineMarketTicker,
            PerpetualMarketTicker,
        )

        ticker_exports: dict[str, Any] = {
            "FuturesKlineTicker": FuturesKlineTicker,
            "KlineIndex": KlineIndex,
            "KlineMarketTicker": KlineMarketTicker,
            "PerpetualMarketTicker": PerpetualMarketTicker,
        }
        return ticker_exports[name]

    if name in {"UniverseDailySnapshot", "UniverseDefinition"}:
        from .universe import UniverseDailySnapshot, UniverseDefinition

        universe_exports: dict[str, Any] = {
            "UniverseDailySnapshot": UniverseDailySnapshot,
            "UniverseDefinition": UniverseDefinition,
        }
        return universe_exports[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
