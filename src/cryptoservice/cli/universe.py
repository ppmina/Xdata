"""Universe CLI commands for define/download/export workflows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from cryptoservice.config import RetryConfig, settings
from cryptoservice.models import Freq
from cryptoservice.utils import load_symbols_from_txt


def add_universe_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register `cryptoservice universe ...` commands."""
    universe_parser = subparsers.add_parser("universe", help="Universe workflows")
    universe_subparsers = universe_parser.add_subparsers(dest="universe_command", required=True)

    define_parser = universe_subparsers.add_parser("define", help="Define immutable universe.json")
    define_parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbols, e.g. BTCUSDT,ETHUSDT")
    define_parser.add_argument("--symbols-file", type=_parse_path, default=None, help="Text file with symbols")
    define_parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    define_parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    define_parser.add_argument("--output", type=_parse_path, required=True, help="Output universe JSON file path")
    define_parser.add_argument("--api-key", default="", help="Binance API key (overrides env)")
    define_parser.add_argument("--api-secret", default="", help="Binance API secret (overrides env)")
    define_parser.add_argument("--description", default=None, help="Optional universe description")
    define_parser.add_argument("--force", action="store_true", help="Overwrite existing universe file")
    define_parser.add_argument("--daily-check-workers", type=int, default=5, help="Concurrency for per-day symbol checks")
    define_parser.add_argument(
        "--daily-check-request-delay",
        type=float,
        default=0.0,
        help="Global minimum spacing (seconds) between symbol check API requests",
    )
    define_parser.add_argument(
        "--daily-check-max-requests-per-minute",
        type=int,
        default=1800,
        help="Max symbol check requests per minute (default: 1800, Binance limit: 2400)",
    )
    define_parser.set_defaults(handler=_handle_define)

    download_parser = universe_subparsers.add_parser("download", help="Download from universe.json")
    download_parser.add_argument("--universe-file", type=_parse_path, required=True, help="Universe JSON file path")
    download_parser.add_argument("--db-path", type=_parse_path, required=True, help="SQLite database path")
    download_parser.add_argument("--api-key", default="", help="Binance API key (overrides env)")
    download_parser.add_argument("--api-secret", default="", help="Binance API secret (overrides env)")
    download_parser.add_argument("--interval", type=_parse_freq, default=Freq.m1, help="Kline interval (default: 1m)")
    download_parser.add_argument("--download-market-metrics", action="store_true", help="Also download metrics datasets")
    download_parser.add_argument(
        "--start-date",
        default=None,
        help="Optional override start date (YYYY-MM-DD), must be within universe range",
    )
    download_parser.add_argument(
        "--end-date",
        default=None,
        help="Optional override end date (YYYY-MM-DD), must be within universe range",
    )
    download_parser.add_argument("--no-incremental", action="store_true", help="Disable incremental planning")
    download_parser.add_argument("--max-api-workers", type=int, default=1)
    download_parser.add_argument("--max-vision-workers", type=int, default=50)
    download_parser.add_argument("--max-day-workers", type=int, default=3, help="Max days processed concurrently")
    download_parser.add_argument("--max-retries", type=int, default=3)
    download_parser.add_argument("--api-request-delay", type=float, default=0.5)
    download_parser.add_argument("--vision-request-delay", type=float, default=0.0)
    download_parser.set_defaults(handler=_handle_download)

    export_parser = universe_subparsers.add_parser("export", help="Export from universe.json")
    export_parser.add_argument("--universe-file", type=_parse_path, required=True, help="Universe JSON file path")
    export_parser.add_argument("--db-path", type=_parse_path, required=True, help="SQLite database path")
    export_parser.add_argument("--export-base-path", type=_parse_path, required=True, help="Base output directory")
    export_parser.add_argument("--source-freq", type=_parse_freq, required=True, help="Source frequency in DB")
    export_parser.add_argument("--export-freq", type=_parse_freq, required=True, help="Export frequency")
    export_parser.add_argument(
        "--start-date",
        default=None,
        help="Optional override start date (YYYY-MM-DD), must be within universe range",
    )
    export_parser.add_argument(
        "--end-date",
        default=None,
        help="Optional override end date (YYYY-MM-DD), must be within universe range",
    )
    export_parser.add_argument("--no-klines", action="store_true", help="Skip kline export")
    export_parser.add_argument("--no-metrics", action="store_true", help="Skip metrics export")
    export_parser.add_argument(
        "--metrics-reliability",
        choices=["strict_100", "legacy_warn"],
        default="strict_100",
        help="Metrics reliability mode (default: strict_100)",
    )
    export_parser.set_defaults(handler=_handle_export)


def _parse_freq(value: str) -> Freq:
    """Parse CLI frequency argument."""
    try:
        return Freq(value)
    except ValueError as exc:
        valid = ", ".join(freq.value for freq in Freq)
        raise argparse.ArgumentTypeError(f"Invalid freq {value!r}. Valid values: {valid}") from exc


def _parse_path(value: str) -> Path:
    """Parse CLI path argument and normalize relative segments."""
    candidate = value.strip()
    if not candidate:
        raise argparse.ArgumentTypeError("Path cannot be empty")
    return Path(candidate).expanduser().resolve(strict=False)


def _load_symbols_from_file(file_path: Path | str) -> list[str]:
    """Load symbols from a text file path with user-facing CLI errors."""
    path = Path(file_path).expanduser().resolve(strict=False)
    if not path.exists():
        raise RuntimeError(f"Symbols file not found: {path}")
    if not path.is_file():
        raise RuntimeError(f"Symbols path is not a file: {path}")

    symbols = load_symbols_from_txt(path)
    if not symbols:
        raise RuntimeError(f"Symbols file contains no valid symbols: {path}")
    return symbols


def _resolve_symbols(symbols_arg: str, symbols_file: Path | None) -> list[str]:
    """Resolve symbols from CLI input."""
    symbols: list[str] = []

    if symbols_file is not None:
        symbols.extend(_load_symbols_from_file(symbols_file))

    if symbols_arg:
        for token in [item.strip() for item in symbols_arg.split(",") if item.strip()]:
            if token.startswith("@"):
                file_token = token[1:].strip()
                if not file_token:
                    raise RuntimeError("Symbols file path after '@' cannot be empty")
                symbols.extend(_load_symbols_from_file(file_token))
                continue
            symbols.append(token)

    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        upper_symbol = symbol.upper()
        if upper_symbol in seen:
            continue
        seen.add(upper_symbol)
        normalized.append(upper_symbol)

    return normalized


def _require_api_credentials() -> tuple[str, str]:
    """Load API credentials from settings."""
    api_key = settings.BINANCE_API_KEY
    api_secret = settings.BINANCE_API_SECRET
    if not api_key or not api_secret:
        raise RuntimeError("BINANCE_API_KEY and BINANCE_API_SECRET are required")
    return api_key, api_secret


def _resolve_api_credentials(api_key_arg: str | None, api_secret_arg: str | None, *, command: str) -> tuple[str, str]:
    """Resolve API credentials with CLI override, then env fallback."""
    api_key = (api_key_arg or "").strip() or settings.BINANCE_API_KEY
    api_secret = (api_secret_arg or "").strip() or settings.BINANCE_API_SECRET
    if not api_key or not api_secret:
        raise RuntimeError(
            f"Missing Binance credentials for universe {command}. Provide --api-key/--api-secret "
            "or set BINANCE_API_KEY/BINANCE_API_SECRET "
            "(e.g. export BINANCE_API_KEY=... BINANCE_API_SECRET=... "
            "or use uv run --env-file .env ...)."
        )
    return api_key, api_secret


def _get_market_service_cls():
    """Import MarketDataService lazily for CLI runtime."""
    from cryptoservice.services import MarketDataService

    return MarketDataService


async def _handle_define(args: argparse.Namespace) -> int:
    """Handle `universe define` command."""
    symbols = _resolve_symbols(args.symbols, args.symbols_file)
    if not symbols:
        raise RuntimeError("At least one symbol is required (--symbols or --symbols-file)")

    api_key = (args.api_key or "").strip() or settings.BINANCE_API_KEY
    api_secret = (args.api_secret or "").strip() or settings.BINANCE_API_SECRET

    market_service_cls = _get_market_service_cls()
    if api_key and api_secret:
        service_context = await market_service_cls.create(api_key=api_key, api_secret=api_secret)
    else:
        create_public = getattr(market_service_cls, "create_public", None)
        if not callable(create_public):
            raise RuntimeError("Public define mode is unavailable: MarketDataService.create_public() is not implemented")
        service_context = await create_public()

    async with service_context as service:
        universe = await service.define_universe(
            symbols=symbols,
            start_date=args.start_date,
            end_date=args.end_date,
            output_path=args.output,
            description=args.description,
            force=args.force,
            daily_check_workers=args.daily_check_workers,
            daily_check_request_delay=args.daily_check_request_delay,
            daily_check_max_requests_per_minute=args.daily_check_max_requests_per_minute,
        )

    print(
        json.dumps(
            {
                "universe_file": str(args.output),
                "schema_version": universe.schema_version,
                "requested_symbols": len(universe.requested_symbols),
                "daily_snapshots": len(universe.daily_snapshots),
                "start_date": universe.start_date,
                "end_date": universe.end_date,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


async def _handle_download(args: argparse.Namespace) -> int:
    """Handle `universe download` command."""
    api_key, api_secret = _resolve_api_credentials(args.api_key, args.api_secret, command="download")

    retry_config = RetryConfig(max_retries=args.max_retries)

    market_service_cls = _get_market_service_cls()
    async with await market_service_cls.create(api_key=api_key, api_secret=api_secret) as service:
        report = await service.download_universe_data(
            universe_file=args.universe_file,
            db_path=args.db_path,
            retry_config=retry_config,
            api_request_delay=args.api_request_delay,
            vision_request_delay=args.vision_request_delay,
            download_market_metrics=args.download_market_metrics,
            incremental=not args.no_incremental,
            interval=args.interval,
            max_api_workers=args.max_api_workers,
            max_vision_workers=args.max_vision_workers,
            max_day_workers=args.max_day_workers,
            max_retries=args.max_retries,
            start_date=args.start_date,
            end_date=args.end_date,
        )

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


async def _handle_export(args: argparse.Namespace) -> int:
    """Handle `universe export` command."""
    api_key, api_secret = _require_api_credentials()

    market_service_cls = _get_market_service_cls()
    async with await market_service_cls.create(api_key=api_key, api_secret=api_secret) as service:
        report = await service.export_universe_data(
            universe_file=args.universe_file,
            db_path=args.db_path,
            export_base_path=args.export_base_path,
            source_freq=args.source_freq,
            export_freq=args.export_freq,
            include_klines=not args.no_klines,
            include_metrics=not args.no_metrics,
            metrics_reliability=args.metrics_reliability,
            start_date=args.start_date,
            end_date=args.end_date,
        )

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0
