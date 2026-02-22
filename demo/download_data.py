"""Download data strictly from v2 universe.json."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice.config import RetryConfig
from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq
from cryptoservice.services import MarketDataService
from cryptoservice.utils.cli_helper import print_progress_header, print_summary

load_dotenv()

logger = get_logger(__name__)

UNIVERSE_FILE = "./data/universe.json"
DB_PATH = "./data/database/market.db"
INTERVAL = Freq.m5
MAX_API_WORKERS = 1
MAX_VISION_WORKERS = 50
API_REQUEST_DELAY = 0.5
VISION_REQUEST_DELAY = 0.0
MAX_RETRIES = 3
RETRY_CONFIG = RetryConfig(max_retries=MAX_RETRIES)
INCREMENTAL = True
DOWNLOAD_MARKET_METRICS = True


async def main() -> None:
    """Run download workflow."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        logger.error("env_vars_missing", required="BINANCE_API_KEY and BINANCE_API_SECRET")
        return

    if not Path(UNIVERSE_FILE).exists():
        logger.error("universe_file_not_found", path=UNIVERSE_FILE, hint="run demo/define_universe.py first")
        return

    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    print_progress_header(
        "Universe Download (v2)",
        details={
            "Universe file": UNIVERSE_FILE,
            "Database": DB_PATH,
            "Interval": INTERVAL.value,
            "Incremental": INCREMENTAL,
            "Download metrics": DOWNLOAD_MARKET_METRICS,
        },
    )

    async with await MarketDataService.create(api_key=api_key, api_secret=api_secret) as service:
        report = await service.download_universe_data(
            universe_file=UNIVERSE_FILE,
            db_path=DB_PATH,
            retry_config=RETRY_CONFIG,
            api_request_delay=API_REQUEST_DELAY,
            vision_request_delay=VISION_REQUEST_DELAY,
            download_market_metrics=DOWNLOAD_MARKET_METRICS,
            incremental=INCREMENTAL,
            interval=INTERVAL,
            max_api_workers=MAX_API_WORKERS,
            max_vision_workers=MAX_VISION_WORKERS,
            max_retries=MAX_RETRIES,
        )

    print_summary(
        title="Universe Download Complete",
        status="success",
        items={
            "Days": report["total_days"],
            "Processed days": report["processed_days"],
            "Skipped days": len(report["skipped_days"]),
            "Total symbols": report["total_symbols"],
            "Successful": report["total_successful_symbols"],
            "Failed": report["total_failed_symbols"],
            "DB path": report["db_path"],
        },
    )


if __name__ == "__main__":
    asyncio.run(main())
