"""Define immutable v2 universe.json from symbols + date range."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice import MarketDataService
from cryptoservice.config.logging import get_logger
from cryptoservice.utils import load_symbols_from_txt
from cryptoservice.utils.cli_helper import print_progress_header, print_summary

load_dotenv()

logger = get_logger(__name__)

START_DATE = "2025-06-01"
END_DATE = "2025-09-30"
OUTPUT_PATH = "./data/universe.json"
SYMBOLS_FILE = "./data/symbols.txt"
FALLBACK_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BADSYMBOL"]
FORCE_OVERWRITE = False
DAILY_CHECK_WORKERS = 8
DAILY_CHECK_REQUEST_DELAY = 0.0


def resolve_symbols() -> list[str]:
    """Resolve symbols from file first, fallback to hardcoded list."""
    symbols_path = Path(SYMBOLS_FILE)
    if symbols_path.exists():
        symbols = load_symbols_from_txt(symbols_path)
        if symbols:
            logger.info("symbols_loaded", source=str(symbols_path), count=len(symbols))
            return symbols

    logger.warning("symbols_file_missing_or_empty", source=str(symbols_path), fallback_count=len(FALLBACK_SYMBOLS))
    return FALLBACK_SYMBOLS


async def main() -> None:
    """Run define workflow."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        logger.error("env_vars_missing", required="BINANCE_API_KEY and BINANCE_API_SECRET")
        return

    Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    symbols = resolve_symbols()

    print_progress_header(
        "Define Universe (v2)",
        details={
            "Symbols": len(symbols),
            "Range": f"{START_DATE} ~ {END_DATE}",
            "Output": OUTPUT_PATH,
            "Force": FORCE_OVERWRITE,
        },
    )

    async with await MarketDataService.create(api_key=api_key, api_secret=api_secret) as service:
        universe = await service.define_universe(
            symbols=symbols,
            start_date=START_DATE,
            end_date=END_DATE,
            output_path=OUTPUT_PATH,
            description="Demo v2 universe",
            force=FORCE_OVERWRITE,
            daily_check_workers=DAILY_CHECK_WORKERS,
            daily_check_request_delay=DAILY_CHECK_REQUEST_DELAY,
        )

    active_union = universe.active_symbols_union
    print_summary(
        title="Universe Define Complete",
        status="success",
        items={
            "Schema": universe.schema_version,
            "Requested symbols": len(universe.requested_symbols),
            "Days": len(universe.daily_snapshots),
            "Active symbol union": len(active_union),
            "Output file": OUTPUT_PATH,
        },
    )


if __name__ == "__main__":
    asyncio.run(main())
