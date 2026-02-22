"""Universe manager for v2 daily truth workflow."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from cryptoservice.config.logging import get_logger
from cryptoservice.models import UniverseDailySnapshot, UniverseDefinition
from cryptoservice.models.universe import (
    MISSING_REASON_INVALID_SYMBOL,
    MISSING_REASON_NOT_AVAILABLE_ON_DATE,
    SCHEMA_VERSION,
)

if TYPE_CHECKING:
    from cryptoservice.services.market_service import MarketDataService

logger = get_logger(__name__)


class UniverseManager:
    """Build v2 universe files from explicit symbols and date range."""

    def __init__(self, market_service: MarketDataService):
        """Initialize manager with a market service dependency."""
        self.market_service = market_service

    async def define_universe(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        output_path: Path | str,
        description: str | None = None,
        force: bool = False,
        daily_check_workers: int = 5,
        daily_check_request_delay: float = 0.0,
    ) -> UniverseDefinition:
        """Build and persist a strict v2 universe definition.

        The output file is immutable by default: existing file requires force=True.
        """
        output_file = self._validate_and_prepare_path(output_path, is_file=True, file_name="universe.json")

        standardized_symbols = self.market_service._normalize_symbols(symbols)
        if not standardized_symbols:
            raise ValueError("symbols cannot be empty")

        standardized_start = self._standardize_date_format(start_date)
        standardized_end = self._standardize_date_format(end_date)

        all_dates = self._build_daily_date_strings(standardized_start, standardized_end)
        if not all_dates:
            raise ValueError(f"Invalid date range: {standardized_start} ~ {standardized_end}")

        valid_symbol_set = set(await self.market_service.get_perpetual_symbols(only_trading=True))
        valid_requested_count = len([symbol for symbol in standardized_symbols if symbol in valid_symbol_set])
        total_days = len(all_dates)
        workers = max(1, daily_check_workers)
        request_delay = max(0.0, daily_check_request_delay)
        run_started = time.monotonic()
        logger.info(
            "universe_define_started",
            start_date=standardized_start,
            end_date=standardized_end,
            total_days=total_days,
            requested_symbols=len(standardized_symbols),
            valid_requested_symbols=valid_requested_count,
            daily_check_workers=workers,
            daily_check_request_delay=request_delay,
        )

        snapshots: list[UniverseDailySnapshot] = []
        for day_index, date_str in enumerate(all_dates, start=1):
            day_started = time.monotonic()
            logger.info("universe_define_day_started", day=day_index, total_days=total_days, date=date_str)
            snapshot = await self._classify_symbols_for_date(
                requested_symbols=standardized_symbols,
                valid_symbol_set=valid_symbol_set,
                target_date=date_str,
                daily_check_workers=workers,
                daily_check_request_delay=request_delay,
            )
            snapshots.append(snapshot)
            logger.info(
                "universe_define_day_completed",
                day=day_index,
                total_days=total_days,
                date=date_str,
                active_symbols=len(snapshot.active_symbols),
                missing_symbols=len(snapshot.missing_symbols),
                elapsed_seconds=round(time.monotonic() - day_started, 2),
                elapsed_total_seconds=round(time.monotonic() - run_started, 2),
            )

        universe_def = UniverseDefinition(
            schema_version=SCHEMA_VERSION,
            requested_symbols=standardized_symbols,
            start_date=standardized_start,
            end_date=standardized_end,
            daily_snapshots=snapshots,
            created_at=datetime.now(tz=UTC),
            description=description,
        )

        universe_def.save_to_file(output_file)
        logger.info(
            "universe_defined",
            output=str(output_file),
            start_date=standardized_start,
            end_date=standardized_end,
            requested_symbols=len(standardized_symbols),
            daily_snapshots=len(snapshots),
        )

        return universe_def

    async def _classify_symbols_for_date(
        self,
        requested_symbols: list[str],
        valid_symbol_set: set[str],
        target_date: str,
        daily_check_workers: int,
        daily_check_request_delay: float,
    ) -> UniverseDailySnapshot:
        """Classify requested symbols into active and missing for one day."""
        semaphore = asyncio.Semaphore(max(1, daily_check_workers))

        valid_requested = [symbol for symbol in requested_symbols if symbol in valid_symbol_set]

        async def _check_symbol(symbol: str) -> tuple[str, bool]:
            async with semaphore:
                if daily_check_request_delay > 0:
                    await asyncio.sleep(daily_check_request_delay)
                is_active = await self.market_service.check_symbol_exists_on_date(symbol, target_date, strict=True)
                return symbol, is_active

        checks = await asyncio.gather(*[_check_symbol(symbol) for symbol in valid_requested])
        active_map = dict(checks)

        active_symbols: list[str] = []
        missing_symbols: dict[str, str] = {}

        for symbol in requested_symbols:
            if symbol not in valid_symbol_set:
                missing_symbols[symbol] = MISSING_REASON_INVALID_SYMBOL
                continue

            if active_map.get(symbol, False):
                active_symbols.append(symbol)
            else:
                missing_symbols[symbol] = MISSING_REASON_NOT_AVAILABLE_ON_DATE

        return UniverseDailySnapshot(
            date=target_date,
            active_symbols=active_symbols,
            missing_symbols=missing_symbols,
        )

    @staticmethod
    def _build_daily_date_strings(start_date: str, end_date: str) -> list[str]:
        """Build an inclusive list of dates between start and end."""
        import pandas as pd

        dates = pd.date_range(start=start_date, end=end_date, freq="D", tz="UTC")
        return [date.strftime("%Y-%m-%d") for date in dates]

    @staticmethod
    def _standardize_date_format(date_str: str) -> str:
        """Normalize date string to YYYY-MM-DD."""
        import pandas as pd

        candidate = date_str.strip()
        if len(candidate) == 8 and candidate.isdigit():
            candidate = f"{candidate[:4]}-{candidate[4:6]}-{candidate[6:8]}"
        return pd.to_datetime(candidate, utc=True).strftime("%Y-%m-%d")

    @staticmethod
    def _validate_and_prepare_path(path: Path | str, is_file: bool = False, file_name: str | None = None) -> Path:
        """Validate and prepare a filesystem path."""
        if not path:
            raise ValueError("Path cannot be empty")

        path_obj = Path(path)

        if is_file:
            if path_obj.is_dir() and file_name:
                path_obj = path_obj / file_name
            path_obj.parent.mkdir(parents=True, exist_ok=True)
        else:
            path_obj.mkdir(parents=True, exist_ok=True)

        return path_obj
