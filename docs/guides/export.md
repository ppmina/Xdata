# Export Guide (v2)

Export reads `universe.json` daily truth and writes `report.json`.

## API

```python
from cryptoservice.models import Freq

report = await service.export_universe_data(
    universe_file="./data/universe.json",
    db_path="./data/database/market.db",
    export_base_path="./data/exports",
    source_freq=Freq.m5,
    export_freq=Freq.m5,
    include_klines=True,
    include_metrics=True,
    start_date="2024-10-10",  # optional
    end_date="2024-10-12",    # optional
)

print(report["report_path"])
print(report["stats"])
```

## CLI

```bash
cryptoservice universe export \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --export-base-path ./data/exports \
  --source-freq 5m \
  --export-freq 5m \
  --start-date 2024-10-10 \
  --end-date 2024-10-12
```

## Report fields

- `define_missing`: `date -> symbol -> reason` from universe define phase.
- `export_missing`: `date -> [symbols]` missing from exported payload.
- `merged_missing`: merged map with reasons.
- `date_range`: requested universe bounds plus effective processed bounds.
- `export_context`: override status and export configuration.
- `exported_days`, `skipped_days`, `errors`, `stats`.

## Notes

- Export does not mutate `universe.json`.
- Empty active days are skipped cleanly and still reflected in stats.
- `start_date` / `end_date` are optional, inclusive, and must stay within universe range.
- If only one bound is provided, the missing bound falls back to universe boundary.
- Metrics `asof` warmup lookback is metric-specific: `funding_rate=3d`, `open_interest=1d`, `long_short_ratio=1d`.
- Metrics staleness tolerance is metric-specific: `funding_rate=48h`, `open_interest=6h`, `long_short_ratio=6h`.
- Missing metrics remain `NaN` in feature arrays (no future fill, no forced zero fill).
- Exported timestamp arrays keep missing semantics as `0` (from missing timestamps), which should be used as the validity mask downstream.
- `report.json` includes optional `metrics_missing_coverage` for daily missing-rate monitoring; high missing ratio logs warnings but does not fail export.
