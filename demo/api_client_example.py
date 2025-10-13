"""Binance API 客户端使用示例.

演示如何在不同环境下正确使用 Binance 异步客户端。
"""

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path

from binance import HistoricalKlinesType
from dotenv import load_dotenv

from cryptoservice import MarketDataService
from cryptoservice.models import Freq
from cryptoservice.storage import Database

load_dotenv()
api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

# 使用相对于脚本文件的路径，确保无论从哪里执行都能找到正确的数据库
SCRIPT_DIR = Path(__file__).parent
DB_PATH = SCRIPT_DIR / "../data/database/market.db"


async def main():
    """运行所有示例."""
    db = Database(DB_PATH)
    symbols = await db.get_symbols(freq=Freq.m5)
    async with await MarketDataService.create(api_key=api_key, api_secret=api_secret) as service:
        # 检查缺失的时间戳
        missing_timestamps = await db.kline_query.get_missing_timestamps(
            symbols[0], 1727740800000, 1730418900000, Freq.m5
        )

        missing_timestamps_dt = (datetime.fromtimestamp(x / 1000, tz=UTC) for x in missing_timestamps)
        for timestamp in missing_timestamps_dt:
            print(timestamp)

        existing_timestamps = await db.kline_query.select_by_timestamp_range(
            symbols[0], missing_timestamps[0], missing_timestamps[-1], Freq.m5
        )
        for timestamp in existing_timestamps:
            print(timestamp)

        # klines = await service.get_historical_klines(
        #     symbol="BTCUSDT",
        #     # start_time=datetime.fromtimestamp(1730390400000 / 1000, tz=UTC),
        #     start_time=datetime.fromtimestamp(1727740800000 / 1000, tz=UTC),
        #     end_time=datetime.fromtimestamp(1730418600000 / 1000, tz=UTC),
        #     interval=Freq.m5,
        #     klines_type=HistoricalKlinesType.FUTURES,
        # )
        # print(f"获取到 {len(klines)} 条K线数据")
        # for kline in klines:
        #     print(datetime.fromtimestamp(kline.open_time / 1000, tz=UTC))


if __name__ == "__main__":
    asyncio.run(main())
