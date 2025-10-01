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
3. Generate a markdown section for ``CHANGELOG.md`` using commits collected
   since the previous tag (skipping release chores for clarity).

The script is intentionally lightweight and has no third‑party dependencies
so that it remains reliable for local runs and CI usage alike.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Callable, Match

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
PACKAGE_INIT = REPO_ROOT / "src" / "cryptoservice" / "__init__.py"
CHANGELOG = REPO_ROOT / "CHANGELOG.md"

VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
COMMIT_PATTERN = re.compile(
    r"^(?P<type>[a-zA-Z]+)(?:\((?P<scope>[^)]+)\))?(?P<breaking>!)?:\s*(?P<subject>.+)$"
)

SECTION_BY_TYPE = {
    "feat": "Features",
    "fix": "Fixes",
    "perf": "Performance",
    "refactor": "Refactors",
    "docs": "Documentation",
    "test": "Tests",
    "build": "Build System",
    "ci": "CI",
    "chore": "Chores",
    "style": "Style",
}

SECTION_ORDER = [
    "Features",
    "Fixes",
    "Performance",
    "Refactors",
    "Documentation",
    "Tests",
    "Build System",
    "CI",
    "Chores",
    "Style",
    "Other Changes",
]


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


def _run_git_command(args: list[str]) -> str:
    try:
        completed = subprocess.run(
            args,
            cwd=REPO_ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return ""
    return completed.stdout.strip()


def _find_previous_tag() -> str | None:
    output = _run_git_command(["git", "describe", "--tags", "--abbrev=0"])
    return output or None


def _collect_commit_messages(base_ref: str | None) -> list[str]:
    args = ["git", "log", "--pretty=format:%s", "--no-merges"]
    if base_ref:
        args.insert(2, f"{base_ref}..HEAD")
    output = _run_git_command(args)
    if not output:
        return []
    return [line.strip() for line in output.splitlines() if line.strip()]


def _categorise_commits(messages: list[str]) -> list[tuple[str, list[str]]]:
    grouped: dict[str, list[str]] = defaultdict(list)

    for message in messages:
        lowered = message.lower()
        if lowered.startswith("chore") and "release" in lowered:
            continue

        match = COMMIT_PATTERN.match(message)
        if match:
            commit_type = match.group("type").lower()
            subject = match.group("subject").strip()
            scope = match.group("scope")
            breaking = bool(match.group("breaking"))
            if breaking:
                subject = f"{subject} (BREAKING)"

            bullet = subject
            if scope:
                bullet = f"{scope}: {subject}"

            section = SECTION_BY_TYPE.get(commit_type, "Other Changes")
        else:
            bullet = message
            section = "Other Changes"

        grouped[section].append(bullet)

    ordered: list[tuple[str, list[str]]] = []
    for section in SECTION_ORDER:
        items = grouped.get(section)
        if items:
            ordered.append((section, items))
    return ordered


def _resolve_repository_url() -> str | None:
    url = _run_git_command(["git", "config", "--get", "remote.origin.url"])
    if not url:
        return None

    url = url.strip()
    if url.endswith(".git"):
        url = url[:-4]

    if url.startswith("git@github.com:"):
        path = url[len("git@github.com:") :]
        return f"https://github.com/{path}"

    if url.startswith("https://") or url.startswith("http://"):
        return url

    return None


def _linkify_references(text: str, repo_url: str | None) -> str:
    if not repo_url:
        return text

    def replacer(match: re.Match[str]) -> str:
        number = match.group(1)
        return f"([#{number}]({repo_url}/pull/{number}))"

    return re.sub(r"\(#(\d+)\)", replacer, text)


def _render_changelog_block(version: str, sections: list[tuple[str, list[str]]], repo_url: str | None) -> str:
    today = _dt.date.today().isoformat()
    lines: list[str] = [f"## v{version} ({today})", ""]

    if not sections:
        lines.append("- No notable changes recorded.")
    else:
        for title, items in sections:
            lines.append(f"### {title}")
            lines.append("")
            for item in items:
                lines.append(f"- {_linkify_references(item, repo_url)}")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n\n"


def update_changelog(version: str, *, skip: bool) -> None:
    if skip:
        return

    if not CHANGELOG.exists():
        return

    base_ref = _find_previous_tag()
    messages = _collect_commit_messages(base_ref)
    sections = _categorise_commits(messages)
    repo_url = _resolve_repository_url()
    block = _render_changelog_block(version, sections, repo_url)

    text = CHANGELOG.read_text(encoding="utf-8")
    version_pattern = re.compile(rf"(?ms)^## v{re.escape(version)}.*?(?=^## v|\Z)")

    if version_pattern.search(text):
        new_text = version_pattern.sub(block, text, count=1)
    else:
        marker = "<!-- next-version -->"
        if marker in text:
            new_text = text.replace(marker, f"{marker}\n\n{block}", 1)
        else:
            new_text = f"{text.rstrip()}\n\n{block}"

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
        update_changelog(args.version, skip=args.skip_changelog)
    except ReleasePreparationError as exc:  # pragma: no cover - CLI surface
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Version set to {args.version}. Remember to review CHANGELOG.md before committing.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    sys.exit(main(sys.argv[1:]))
