# Universe v2 Guide

Universe v2 uses a single immutable `universe.json` as the source of truth.

## Workflow

1. `define`: generate daily truth from `symbols + start_date + end_date`.
2. `download`: read `daily_snapshots[*].active_symbols` and execute strict day-symbol downloads.
3. `export`: read the same daily truth and generate export + missing coverage report.

`download` and `export` never mutate `universe.json`.

## v2 Schema

```json
{
  "schema_version": "2.0",
  "requested_symbols": ["BTCUSDT", "ETHUSDT"],
  "start_date": "2024-10-01",
  "end_date": "2024-10-31",
  "daily_snapshots": [
    {
      "date": "2024-10-01",
      "active_symbols": ["BTCUSDT"],
      "missing_symbols": {
        "ETHUSDT": "no_kline_on_date"
      }
    }
  ],
  "created_at": "2026-02-21T00:00:00+00:00",
  "description": "optional"
}
```

`missing_symbols` reason codes:
- `not_in_current_trading_list`: symbol is not in the current Binance `PERPETUAL && TRADING && USDT` symbol set.
- `no_kline_on_date`: symbol is in the current set but has no kline data for that date.

## Python API

```python
import asyncio
import os
from cryptoservice import MarketDataService
from cryptoservice.config import RetryConfig
from cryptoservice.models import Freq


async def run():
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    async with await MarketDataService.create(api_key, api_secret) as service:
        await service.define_universe(
            symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
            start_date="2024-10-01",
            end_date="2024-10-31",
            output_path="./data/universe.json",
            force=False,
        )

        await service.download_universe_data(
            universe_file="./data/universe.json",
            db_path="./data/database/market.db",
            retry_config=RetryConfig(max_retries=3),
            api_request_delay=0.5,
            vision_request_delay=0.0,
            download_market_metrics=True,
            incremental=True,
            interval=Freq.m5,
        )


asyncio.run(run())
```

## CLI

```bash
cryptoservice universe define \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT \
  --start-date 2024-10-01 \
  --end-date 2024-10-31 \
  --output ./data/universe.json \
  --api-key "${BINANCE_API_KEY}" \
  --api-secret "${BINANCE_API_SECRET}"

# load symbols from txt file
cryptoservice universe define \
  --symbols-file ./symbols.txt \
  --start-date 2024-10-01 \
  --end-date 2024-10-31 \
  --output ./data/universe.json \
  --api-key "${BINANCE_API_KEY}" \
  --api-secret "${BINANCE_API_SECRET}"

# mix inline symbols with @file shorthand
cryptoservice universe define \
  --symbols BTCUSDT,@./symbols.txt,ETHUSDT \
  --start-date 2024-10-01 \
  --end-date 2024-10-31 \
  --output ./data/universe.json \
  --api-key "${BINANCE_API_KEY}" \
  --api-secret "${BINANCE_API_SECRET}"

cryptoservice universe download \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --api-key "${BINANCE_API_KEY}" \
  --api-secret "${BINANCE_API_SECRET}" \
  --interval 5m \
  --download-market-metrics

# optional small-window download for validation/visualization
cryptoservice universe download \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --start-date 2024-10-10 \
  --end-date 2024-10-12 \
  --interval 5m

cryptoservice universe export \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --export-base-path ./data/exports \
  --source-freq 5m \
  --export-freq 5m

# optional small-window export
cryptoservice universe export \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --export-base-path ./data/exports \
  --source-freq 5m \
  --export-freq 5m \
  --start-date 2024-10-10 \
  --end-date 2024-10-12
```

## Shell Scripts

```bash
bash scripts/universe_define.sh \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT \
  --start-date 2024-10-01 \
  --end-date 2024-10-31 \
  --output ./data/universe.json \
  --api-key "${BINANCE_API_KEY}" \
  --api-secret "${BINANCE_API_SECRET}"

bash scripts/universe_download.sh \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --api-key "${BINANCE_API_KEY}" \
  --api-secret "${BINANCE_API_SECRET}" \
  --interval 5m \
  --download-market-metrics

bash scripts/universe_export.sh \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --export-base-path ./data/exports \
  --source-freq 5m \
  --export-freq 5m
```

Scripts are pass-through wrappers; consumer must provide all paths and options.
You can provide credentials via `--api-key/--api-secret`, or via env (`BINANCE_API_KEY`, `BINANCE_API_SECRET`).
Dotenv option: `uv run --env-file .env cryptoservice universe define ...`.

## Failure Rules

- API/network failure during `define` aborts the run and writes nothing.
- Unknown symbols remain in `requested_symbols` and appear in each day's `missing_symbols`.
- Days with zero `active_symbols` are valid and preserved.
- `download` / `export` support optional `--start-date` / `--end-date` overrides (inclusive, `YYYY-MM-DD`).
- Override range must be within `universe.json` range; out-of-bounds or reversed ranges fail fast.
- If only one bound is provided, the missing bound defaults to the universe boundary.
