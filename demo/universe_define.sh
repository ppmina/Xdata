#!/usr/bin/env bash
# Example (annotated):
# uv run cryptoservice universe define \
#   --symbols BTCUSDT,ETHUSDT,SOLUSDT \         # comma-separated symbols (also supports @<txt-path>)
#   --start-date 2024-10-01 \                   # required; format YYYY-MM-DD
#   --end-date 2024-10-31 \                     # required; format YYYY-MM-DD
#   --output ./data/universe.json \             # required; output universe definition file
#   --api-key "${BINANCE_API_KEY}" \            # optional; CLI override (fallback to env/settings)
#   --api-secret "${BINANCE_API_SECRET}" \      # optional; CLI override (fallback to env/settings)
#   --daily-check-workers 5 \                   # optional; per-day symbol check concurrency
#   --daily-check-request-delay 0.0 \           # optional; global spacing (seconds) between check requests
#   --daily-check-max-requests-per-minute 1800  # optional; cap per minute (Binance limit: 2400)
# Optional flags not shown above: --symbols-file <txt-path>, --description <text>
# Tip: uv run cryptoservice universe define --help
# Tip: or run with dotenv: uv run --env-file .env cryptoservice universe define ...
uv run --env-file .env cryptoservice universe define \
  --symbols @../data/univ_base.txt \
  --start-date 2024-10-01 \
  --end-date 2024-10-31 \
  --output ../data/universe.json \
  --daily-check-workers 3 \
  --daily-check-request-delay 0.0 \
  --force
  # --api-key "${BINANCE_API_KEY}" \
  # --api-secret "${BINANCE_API_SECRET}" \
