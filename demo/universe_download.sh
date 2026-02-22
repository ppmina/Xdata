#!/usr/bin/env bash
# Example (annotated):
# uv run --env-file .env cryptoservice universe download \
#   --universe-file ./data/universe.json \      # required; source universe definition json
#   --db-path ./data/database/market.db \       # required; sqlite database file path
#   --api-key "${BINANCE_API_KEY}" \            # optional; CLI override (fallback to env/settings)
#   --api-secret "${BINANCE_API_SECRET}" \      # optional; CLI override (fallback to env/settings)
#   --interval 5m \                             # optional; kline frequency
#   --download-market-metrics \                 # optional; include funding/oi/ratio datasets
#   --max-api-workers 1 \                       # optional; API worker concurrency
#   --max-vision-workers 50 \                   # optional; Vision worker concurrency
#   --max-retries 3 \                           # optional; retry attempts
#   --api-request-delay 0.5 \                   # optional; delay seconds for API requests
#   --vision-request-delay 0.0                  # optional; delay seconds for Vision requests
# Optional flags not shown above: --no-incremental
# Tip: uv run cryptoservice universe download --help
# Tip: or run with dotenv: uv run --env-file .env cryptoservice universe download ...
uv run --env-file .env cryptoservice universe download \
  --universe-file ../data/universe.json \
  --db-path ../data/database/market.db \
  --interval 5m \
  --download-market-metrics \
  --max-api-workers 10 \
  --max-vision-workers 100 \
  --max-retries 3 \
  --api-request-delay 0.0 \
  --vision-request-delay 0.0 \
  # --start-date 2024-10-01 \
  # --end-date 2024-10-10 \
  # --api-key "${BINANCE_API_KEY}" \
  # --api-secret "${BINANCE_API_SECRET}" \
