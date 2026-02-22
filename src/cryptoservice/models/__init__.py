"""Data models and enums (lazy exports)."""

from __future__ import annotations

from typing import Any

__all__ = [
    "SymbolTicker",
    "DailyMarketTicker",
    "KlineMarketTicker",
    "SpotKlineTicker",
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

        mapping = {
            "ErrorSeverity": ErrorSeverity,
            "Freq": Freq,
            "HistoricalKlinesType": HistoricalKlinesType,
            "SortBy": SortBy,
            "Univ": Univ,
        }
        return mapping[name]

    if name == "IntegrityReport":
        from .integrity_report import IntegrityReport

        return IntegrityReport

    if name in {"FundingRate", "LongShortRatio", "OpenInterest"}:
        from .market_data import FundingRate, LongShortRatio, OpenInterest

        mapping = {
            "FundingRate": FundingRate,
            "LongShortRatio": LongShortRatio,
            "OpenInterest": OpenInterest,
        }
        return mapping[name]

    if name in {
        "DailyMarketTicker",
        "FuturesKlineTicker",
        "KlineIndex",
        "KlineMarketTicker",
        "PerpetualMarketTicker",
        "SpotKlineTicker",
        "SymbolTicker",
    }:
        from .market_ticker import (
            DailyMarketTicker,
            FuturesKlineTicker,
            KlineIndex,
            KlineMarketTicker,
            PerpetualMarketTicker,
            SpotKlineTicker,
            SymbolTicker,
        )

        mapping = {
            "DailyMarketTicker": DailyMarketTicker,
            "FuturesKlineTicker": FuturesKlineTicker,
            "KlineIndex": KlineIndex,
            "KlineMarketTicker": KlineMarketTicker,
            "PerpetualMarketTicker": PerpetualMarketTicker,
            "SpotKlineTicker": SpotKlineTicker,
            "SymbolTicker": SymbolTicker,
        }
        return mapping[name]

    if name in {"UniverseDailySnapshot", "UniverseDefinition"}:
        from .universe import UniverseDailySnapshot, UniverseDefinition

        mapping = {
            "UniverseDailySnapshot": UniverseDailySnapshot,
            "UniverseDefinition": UniverseDefinition,
        }
        return mapping[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
