#!/usr/bin/env python3
"""Utility helpers to prepare a tagged release.

This script keeps the project version in sync across ``pyproject.toml``
and ``src/cryptoservice/__init__.py`` and optionally primes the changelog.

Usage
-----
$ python3 scripts/prepare_release.py 1.12.0

The command performs three actions:
1. Validate the version string (semantic version format ``X.Y.Z``).
2. Update the version inside ``pyproject.toml`` and ``__init__.py``.
3. Insert a stub heading for the new release at the top of ``CHANGELOG.md``
   so you can fill in the bullet points before committing.

The script is intentionally lightweight and has no third‑party dependencies
so that it remains reliable for local runs and CI usage alike.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import sys
from pathlib import Path
from typing import Callable, Match

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
PACKAGE_INIT = REPO_ROOT / "src" / "cryptoservice" / "__init__.py"
CHANGELOG = REPO_ROOT / "CHANGELOG.md"

VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")


class ReleasePreparationError(RuntimeError):
    """Raised when the release preparation cannot be completed safely."""


def _validate_version(version: str) -> None:
    if not VERSION_PATTERN.fullmatch(version):
        raise ReleasePreparationError(
            f"Invalid version '{version}'. Expected semantic version format 'X.Y.Z'."
        )


Replacement = str | Callable[[Match[str]], str]


def _update_text_file(path: Path, pattern: re.Pattern[str], replacement: Replacement) -> None:
    text = path.read_text(encoding="utf-8")
    new_text, count = pattern.subn(replacement, text, count=1)
    if count == 0:
        raise ReleasePreparationError(f"Could not update version in {path}.")
    path.write_text(new_text, encoding="utf-8")


def update_pyproject(version: str) -> None:
    pattern = re.compile(r'(?m)^(version\s*=\s*")([^\"]+)(")')

    def replacement(match: Match[str]) -> str:
        return f"{match.group(1)}{version}{match.group(3)}"

    _update_text_file(PYPROJECT, pattern, replacement)


def update_package_init(version: str) -> None:
    pattern = re.compile(r'(?m)^(__version__\s*=\s*")([^\"]+)(")')

    def replacement(match: Match[str]) -> str:
        return f"{match.group(1)}{version}{match.group(3)}"

    _update_text_file(PACKAGE_INIT, pattern, replacement)


def prime_changelog(version: str, *, skip: bool) -> None:
    if skip or not CHANGELOG.exists():
        return

    text = CHANGELOG.read_text(encoding="utf-8")
    version_heading = f"## v{version}"  # Avoid double insertion
    if version_heading in text:
        return

    today = _dt.date.today().isoformat()
    stub = (
        f"## v{version} ({today})\n\n"
        f"- TODO: describe the changes included in v{version}.\n\n"
    )

    marker = "<!-- next-version -->"
    if marker in text:
        new_text = text.replace(marker, f"{marker}\n\n{stub}", 1)
    else:
        new_text = f"{stub}{text}"

    CHANGELOG.write_text(new_text, encoding="utf-8")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare project files for a new release.")
    parser.add_argument("version", help="Semantic version (X.Y.Z)")
    parser.add_argument(
        "--skip-changelog",
        action="store_true",
        help="Do not insert a placeholder section into CHANGELOG.md.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        _validate_version(args.version)
        update_pyproject(args.version)
        update_package_init(args.version)
        prime_changelog(args.version, skip=args.skip_changelog)
    except ReleasePreparationError as exc:  # pragma: no cover - CLI surface
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Version set to {args.version}. Remember to review CHANGELOG.md before committing.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    sys.exit(main(sys.argv[1:]))
