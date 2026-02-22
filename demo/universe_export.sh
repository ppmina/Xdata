#!/usr/bin/env bash
# Example (annotated):
# uv run cryptoservice universe export \
#   --universe-file ./data/universe.json \      # required; source universe definition json
#   --db-path ./data/database/market.db \       # required; sqlite database file path
#   --export-base-path ./data/exports \         # required; base directory for export artifacts
#   --source-freq 5m \                          # required; source frequency in database
#   --export-freq 5m                            # required; target export frequency
# Optional flags not shown above: --no-klines, --no-metrics
# Tip: uv run cryptoservice universe export --help
uv run cryptoservice universe export \
  --universe-file ../data/universe.json \
  --db-path ../data/database/market.db \
  --export-base-path ../data/exports \
  --source-freq 5m \
  --export-freq 5m
