"""Logging-related shared constants."""

from __future__ import annotations

STRUCTURED_FIELD_ORDER: tuple[str, ...] = (
    "run",
    "dataset",
    "snapshot",
    "symbol",
    "range",
    "status",
    "reason",
)

__all__ = ["STRUCTURED_FIELD_ORDER"]
