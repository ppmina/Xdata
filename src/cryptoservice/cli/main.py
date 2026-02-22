"""CLI entrypoint for cryptoservice."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .universe import add_universe_parser


def build_parser() -> argparse.ArgumentParser:
    """Build top-level CLI parser."""
    parser = argparse.ArgumentParser(prog="cryptoservice", description="CryptoService command line interface")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_universe_parser(subparsers)
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI main function."""
    parser = build_parser()
    args = parser.parse_args(argv)

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 2

    try:
        return asyncio.run(handler(args))
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
