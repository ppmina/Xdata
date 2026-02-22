"""Export data strictly from v2 universe.json."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq
from cryptoservice.services import MarketDataService
from cryptoservice.utils.cli_helper import print_progress_header, print_summary

load_dotenv()

logger = get_logger(__name__)

UNIVERSE_FILE = "./data/universe.json"
DB_PATH = "./data/database/market.db"
EXPORT_BASE_PATH = "./data/exports"
SOURCE_FREQ = Freq.m5
EXPORT_FREQ = Freq.m5
EXPORT_KLINES = True
EXPORT_METRICS = True


async def main() -> None:
    """Run export workflow."""
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        logger.error("env_vars_missing", required="BINANCE_API_KEY and BINANCE_API_SECRET")
        return

    if not Path(UNIVERSE_FILE).exists():
        logger.error("universe_file_not_found", path=UNIVERSE_FILE, hint="run demo/define_universe.py first")
        return

    Path(EXPORT_BASE_PATH).mkdir(parents=True, exist_ok=True)

    print_progress_header(
        "Universe Export (v2)",
        details={
            "Universe file": UNIVERSE_FILE,
            "Database": DB_PATH,
            "Output base": EXPORT_BASE_PATH,
            "Source freq": SOURCE_FREQ.value,
            "Export freq": EXPORT_FREQ.value,
            "Include klines": EXPORT_KLINES,
            "Include metrics": EXPORT_METRICS,
        },
    )

    async with await MarketDataService.create(api_key=api_key, api_secret=api_secret) as service:
        report = await service.export_universe_data(
            universe_file=UNIVERSE_FILE,
            db_path=DB_PATH,
            export_base_path=EXPORT_BASE_PATH,
            source_freq=SOURCE_FREQ,
            export_freq=EXPORT_FREQ,
            include_klines=EXPORT_KLINES,
            include_metrics=EXPORT_METRICS,
        )

    stats = report.get("stats", {})
    print_summary(
        title="Universe Export Complete",
        status="success" if stats.get("error_count", 0) == 0 else "partial",
        items={
            "Output path": report.get("output_path", ""),
            "Report file": report.get("report_path", ""),
            "Exported days": stats.get("exported_day_count", 0),
            "Skipped days": stats.get("skipped_day_count", 0),
            "Errors": stats.get("error_count", 0),
            "Define missing dates": stats.get("define_missing_date_count", 0),
            "Export missing dates": stats.get("export_missing_date_count", 0),
            "Merged missing dates": stats.get("merged_missing_date_count", 0),
        },
    )


if __name__ == "__main__":
    asyncio.run(main())
