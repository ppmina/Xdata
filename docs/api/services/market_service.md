# MarketDataService API

`MarketDataService` is the main async service for Binance market data, universe workflows, downloads, and exports.

## Create service

```python
from cryptoservice.services import MarketDataService

service = await MarketDataService.create(api_key, api_secret)
```

Prefer context manager:

```python
async with await MarketDataService.create(api_key, api_secret) as service:
    ...
```

## Universe v2 methods

### `define_universe(...) -> UniverseDefinition`

Inputs:

- `symbols: list[str]`
- `start_date: str`
- `end_date: str`
- `output_path: Path | str`
- `description: str | None = None`
- `force: bool = False`
- `daily_check_workers: int = 5`
- `daily_check_request_delay: float = 0.0`

Behavior:

- Builds strict daily truth table (`daily_snapshots`).
- Existing file is immutable unless `force=True`.
- v1 schema is not supported.

### `download_universe_data(...) -> dict[str, Any]`

Inputs:

- `universe_file`
- `db_path`
- retry/download settings (`RetryConfig`, delays, workers, interval, etc.)
- `start_date: str | None = None` (optional override, inclusive)
- `end_date: str | None = None` (optional override, inclusive)

Behavior:

- Reads only `daily_snapshots[*].active_symbols` and executes per-day plan.
- Supports optional date-window override for small validation runs.
- Override range must be within universe bounds.
- If one bound is missing, it falls back to universe boundary.
- Does not mutate `universe.json`.

### `export_universe_data(...) -> dict[str, Any]`

Inputs:

- `universe_file`
- `db_path`
- `export_base_path`
- `source_freq`, `export_freq`
- export flags
- `metrics_reliability: str = "strict_100"` (`strict_100` | `legacy_warn`)
- `start_date: str | None = None` (optional override, inclusive)
- `end_date: str | None = None` (optional override, inclusive)

Behavior:

- Reads daily truth for expected coverage.
- Supports optional date-window override for small export runs.
- Writes `report.json` with `define_missing`, `export_missing`, `merged_missing`.
- Strict mode (`strict_100`, default) drops symbol-days that fail required metrics `asof` coverage.
- If strict filtering removes all symbol-days for a date, that date is skipped with explicit reason.
- Report includes `metrics_missing_coverage` and `metrics_strict_exclusions` diagnostics.
- Report includes requested/effective date range plus override context.
- Does not mutate `universe.json`.

## Other APIs

Base market methods (`get_symbol_ticker`, `get_perpetual_symbols`, `get_historical_klines`, `get_perpetual_data`) remain available.
