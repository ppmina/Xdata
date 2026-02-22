"""Universe models for the v2 daily-truth workflow."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

SCHEMA_VERSION = "2.0"
MISSING_REASON_INVALID_SYMBOL = "invalid_symbol"
MISSING_REASON_NOT_AVAILABLE_ON_DATE = "not_available_on_date"
MISSING_REASON_MISSING_IN_EXPORT = "missing_in_export"


def _standardize_date(date_str: str) -> str:
    """Standardize date string to YYYY-MM-DD."""
    if not isinstance(date_str, str):
        raise TypeError(f"Date must be a string, got {type(date_str)!r}")

    candidate = date_str.strip()
    if len(candidate) == 8 and candidate.isdigit():
        candidate = f"{candidate[:4]}-{candidate[4:6]}-{candidate[6:8]}"

    try:
        return pd.to_datetime(candidate, utc=True).strftime("%Y-%m-%d")
    except Exception as exc:  # pragma: no cover - pandas raises many specific exceptions
        raise ValueError(f"Invalid date format: {date_str!r}. Expected YYYY-MM-DD.") from exc


def _standardize_symbols(symbols: list[str]) -> list[str]:
    """Normalize symbol list (uppercase + strip + dedupe + stable order)."""
    if not isinstance(symbols, list):
        raise TypeError(f"symbols must be a list[str], got {type(symbols)!r}")

    normalized: list[str] = []
    seen: set[str] = set()

    for raw in symbols:
        if not isinstance(raw, str):
            raise TypeError(f"symbols entries must be strings, got {type(raw)!r}")

        symbol = raw.strip().upper()
        if not symbol:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)

    return normalized


def _standardize_missing_map(missing_symbols: dict[str, str]) -> dict[str, str]:
    """Normalize missing symbol map."""
    if not isinstance(missing_symbols, dict):
        raise TypeError(f"missing_symbols must be dict[str, str], got {type(missing_symbols)!r}")

    normalized: dict[str, str] = {}
    for raw_symbol, raw_reason in missing_symbols.items():
        if not isinstance(raw_symbol, str):
            raise TypeError(f"missing_symbols keys must be strings, got {type(raw_symbol)!r}")
        if not isinstance(raw_reason, str):
            raise TypeError(f"missing_symbols values must be strings, got {type(raw_reason)!r}")

        symbol = raw_symbol.strip().upper()
        if not symbol:
            raise ValueError("missing_symbols contains an empty symbol key")

        reason = raw_reason.strip() or "unknown"
        normalized[symbol] = reason

    return normalized


def _parse_iso_datetime(value: str) -> datetime:
    """Parse ISO datetime string and force timezone-aware UTC output."""
    if not isinstance(value, str):
        raise TypeError(f"created_at must be an ISO datetime string, got {type(value)!r}")

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _build_daily_dates(start_date: str, end_date: str) -> list[str]:
    """Build an inclusive daily date list."""
    return [date.strftime("%Y-%m-%d") for date in pd.date_range(start=start_date, end=end_date, freq="D", tz="UTC")]


@dataclass
class UniverseDailySnapshot:
    """Daily universe truth entry for one date."""

    date: str
    active_symbols: list[str]
    missing_symbols: dict[str, str]

    def __post_init__(self) -> None:
        """Normalize payload and validate local invariants."""
        self.date = _standardize_date(self.date)
        self.active_symbols = _standardize_symbols(self.active_symbols)
        self.missing_symbols = _standardize_missing_map(self.missing_symbols)

        overlap = set(self.active_symbols).intersection(self.missing_symbols.keys())
        if overlap:
            raise ValueError(f"active_symbols and missing_symbols overlap on {self.date}: {sorted(overlap)}")

    def to_dict(self) -> dict[str, Any]:
        """Serialize snapshot to JSON-compatible dict."""
        return {
            "date": self.date,
            "active_symbols": self.active_symbols,
            "missing_symbols": self.missing_symbols,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> UniverseDailySnapshot:
        """Deserialize snapshot from dict."""
        if not isinstance(data, dict):
            raise TypeError(f"Daily snapshot payload must be a dict, got {type(data)!r}")

        required_fields = {"date", "active_symbols", "missing_symbols"}
        unknown_fields = sorted(set(data.keys()) - required_fields)
        missing_fields = sorted(required_fields - set(data.keys()))
        if missing_fields:
            raise ValueError(f"Daily snapshot missing required fields: {missing_fields}")
        if unknown_fields:
            raise ValueError(f"Daily snapshot has unsupported fields: {unknown_fields}")

        if not isinstance(data["date"], str):
            raise TypeError(f"Daily snapshot field 'date' must be str, got {type(data['date'])!r}")
        if not isinstance(data["active_symbols"], list):
            raise TypeError(f"Daily snapshot field 'active_symbols' must be list[str], got {type(data['active_symbols'])!r}")
        if not all(isinstance(item, str) for item in data["active_symbols"]):
            raise TypeError("Daily snapshot field 'active_symbols' must contain only strings")
        if not isinstance(data["missing_symbols"], dict):
            raise TypeError(f"Daily snapshot field 'missing_symbols' must be dict[str, str], got {type(data['missing_symbols'])!r}")
        if not all(isinstance(key, str) and isinstance(value, str) for key, value in data["missing_symbols"].items()):
            raise TypeError("Daily snapshot field 'missing_symbols' must map string symbol to string reason")

        return cls(
            date=data["date"],
            active_symbols=data["active_symbols"],
            missing_symbols=data["missing_symbols"],
        )


@dataclass
class UniverseDefinition:
    """Immutable v2 universe definition."""

    schema_version: str
    requested_symbols: list[str]
    start_date: str
    end_date: str
    daily_snapshots: list[UniverseDailySnapshot]
    created_at: datetime
    description: str | None = None

    def __post_init__(self) -> None:
        """Normalize and validate strict v2 invariants."""
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"Unsupported schema_version {self.schema_version!r}. Expected {SCHEMA_VERSION!r}.")

        self.start_date = _standardize_date(self.start_date)
        self.end_date = _standardize_date(self.end_date)
        if pd.to_datetime(self.start_date, utc=True) > pd.to_datetime(self.end_date, utc=True):
            raise ValueError(f"start_date {self.start_date} must be <= end_date {self.end_date}")

        self.requested_symbols = _standardize_symbols(self.requested_symbols)
        if not self.requested_symbols:
            raise ValueError("requested_symbols cannot be empty")

        normalized_snapshots: list[UniverseDailySnapshot] = []
        for snapshot in self.daily_snapshots:
            if isinstance(snapshot, UniverseDailySnapshot):
                normalized_snapshots.append(snapshot)
            elif isinstance(snapshot, dict):
                normalized_snapshots.append(UniverseDailySnapshot.from_dict(snapshot))
            else:
                raise TypeError(f"daily_snapshots must contain UniverseDailySnapshot entries, got {type(snapshot)!r}")

        self.daily_snapshots = self._order_and_validate_dates(normalized_snapshots)

        if isinstance(self.created_at, str):
            self.created_at = _parse_iso_datetime(self.created_at)
        elif not isinstance(self.created_at, datetime):
            raise TypeError(f"created_at must be datetime or ISO datetime string, got {type(self.created_at)!r}")
        elif self.created_at.tzinfo is None:
            self.created_at = self.created_at.replace(tzinfo=UTC)

        self._validate_partition_invariants()

    def _order_and_validate_dates(self, snapshots: list[UniverseDailySnapshot]) -> list[UniverseDailySnapshot]:
        """Validate exact day continuity and reorder snapshots by date."""
        expected_dates = _build_daily_dates(self.start_date, self.end_date)
        snapshot_by_date: dict[str, UniverseDailySnapshot] = {}

        for snapshot in snapshots:
            if snapshot.date in snapshot_by_date:
                raise ValueError(f"Duplicate daily snapshot for {snapshot.date}")
            snapshot_by_date[snapshot.date] = snapshot

        missing_dates = [date for date in expected_dates if date not in snapshot_by_date]
        extra_dates = sorted(set(snapshot_by_date) - set(expected_dates))

        if missing_dates or extra_dates:
            details = []
            if missing_dates:
                details.append(f"missing dates: {missing_dates[:5]}{'...' if len(missing_dates) > 5 else ''}")
            if extra_dates:
                details.append(f"extra dates: {extra_dates[:5]}{'...' if len(extra_dates) > 5 else ''}")
            raise ValueError(f"daily_snapshots must contain exactly one entry per day in range: {'; '.join(details)}")

        return [snapshot_by_date[date] for date in expected_dates]

    def _validate_partition_invariants(self) -> None:
        """Validate requested/active/missing partition for every day."""
        requested_set = set(self.requested_symbols)

        for snapshot in self.daily_snapshots:
            active_set = set(snapshot.active_symbols)
            missing_set = set(snapshot.missing_symbols.keys())

            overlap = active_set.intersection(missing_set)
            if overlap:
                raise ValueError(f"Partition overlap on {snapshot.date}: {sorted(overlap)}")

            unknown = (active_set | missing_set) - requested_set
            if unknown:
                raise ValueError(f"Snapshot {snapshot.date} contains unknown symbols: {sorted(unknown)}")

            if (active_set | missing_set) != requested_set:
                missing_from_partition = sorted(requested_set - (active_set | missing_set))
                raise ValueError(f"Snapshot {snapshot.date} must partition requested_symbols exactly. Missing from partition: {missing_from_partition}")

    @property
    def active_symbols_union(self) -> list[str]:
        """Derived union of all active symbols across days (not persisted)."""
        union: set[str] = set()
        for snapshot in self.daily_snapshots:
            union.update(snapshot.active_symbols)
        return sorted(union)

    def get_snapshot_for_date(self, target_date: str) -> UniverseDailySnapshot | None:
        """Return daily snapshot for one date."""
        standardized = _standardize_date(target_date)
        for snapshot in self.daily_snapshots:
            if snapshot.date == standardized:
                return snapshot
        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to JSON-compatible dict."""
        return {
            "schema_version": self.schema_version,
            "requested_symbols": self.requested_symbols,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "daily_snapshots": [snapshot.to_dict() for snapshot in self.daily_snapshots],
            "created_at": self.created_at.isoformat(),
            "description": self.description,
        }

    @staticmethod
    def _reject_legacy_payload(data: dict[str, Any]) -> None:
        """Reject v1 schema payloads."""
        if "config" in data or "snapshots" in data:
            raise ValueError(
                "Detected legacy universe schema (v1: config/snapshots). "
                "This version only supports v2 daily_snapshots. Re-run define_universe to regenerate universe.json."
            )

    @staticmethod
    def _validate_payload_fields(data: dict[str, Any]) -> None:
        """Validate required and optional top-level payload keys."""
        required_fields = [
            "schema_version",
            "requested_symbols",
            "start_date",
            "end_date",
            "daily_snapshots",
            "created_at",
        ]
        optional_fields = {"description"}
        allowed_fields = set(required_fields).union(optional_fields)

        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            raise ValueError(f"Universe payload missing required fields: {missing_fields}")

        unknown_fields = sorted(set(data.keys()) - allowed_fields)
        if unknown_fields:
            raise ValueError(f"Universe payload has unsupported fields: {unknown_fields}")

    @staticmethod
    def _validate_required_scalar_types(data: dict[str, Any]) -> None:
        """Validate required scalar fields."""
        if not isinstance(data["requested_symbols"], list):
            raise TypeError("requested_symbols must be list[str]")
        if not all(isinstance(symbol, str) for symbol in data["requested_symbols"]):
            raise TypeError("requested_symbols must contain only strings")
        if not isinstance(data["schema_version"], str):
            raise TypeError("schema_version must be a string")
        if not isinstance(data["start_date"], str):
            raise TypeError("start_date must be a string in YYYY-MM-DD")
        if not isinstance(data["end_date"], str):
            raise TypeError("end_date must be a string in YYYY-MM-DD")
        if not isinstance(data["created_at"], str):
            raise TypeError("created_at must be an ISO datetime string")

    @staticmethod
    def _validate_daily_snapshots_payload(data: dict[str, Any]) -> None:
        """Validate daily snapshot payload container and entries."""
        if not isinstance(data["daily_snapshots"], list):
            raise TypeError("daily_snapshots must be a list")
        for index, snapshot in enumerate(data["daily_snapshots"], start=1):
            if not isinstance(snapshot, dict):
                raise TypeError(f"daily_snapshots[{index}] must be a dict")

    @staticmethod
    def _validate_optional_fields(data: dict[str, Any]) -> None:
        """Validate optional payload fields."""
        description = data.get("description")
        if description is not None and not isinstance(description, str):
            raise TypeError("description must be a string or null")

    @classmethod
    def _validate_payload_types(cls, data: dict[str, Any]) -> None:
        """Validate top-level value types for universe payload."""
        cls._validate_required_scalar_types(data)
        cls._validate_daily_snapshots_payload(data)
        cls._validate_optional_fields(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> UniverseDefinition:
        """Deserialize v2 universe and reject legacy v1 payloads."""
        if not isinstance(data, dict):
            raise TypeError(f"Universe payload must be a dict, got {type(data)!r}")

        cls._reject_legacy_payload(data)
        cls._validate_payload_fields(data)
        cls._validate_payload_types(data)

        return cls(
            schema_version=data["schema_version"],
            requested_symbols=data["requested_symbols"],
            start_date=data["start_date"],
            end_date=data["end_date"],
            daily_snapshots=[UniverseDailySnapshot.from_dict(snapshot) for snapshot in data["daily_snapshots"]],
            created_at=_parse_iso_datetime(data["created_at"]),
            description=data.get("description"),
        )

    def save_to_file(self, file_path: Path | str) -> None:
        """Persist universe definition to file."""
        target = Path(file_path)
        target.parent.mkdir(parents=True, exist_ok=True)

        with open(target, "w", encoding="utf-8") as file_obj:
            json.dump(self.to_dict(), file_obj, ensure_ascii=False, indent=2)

    @classmethod
    def load_from_file(cls, file_path: Path | str) -> UniverseDefinition:
        """Load universe definition from file."""
        with open(file_path, encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
        return cls.from_dict(payload)


__all__ = [
    "SCHEMA_VERSION",
    "MISSING_REASON_INVALID_SYMBOL",
    "MISSING_REASON_NOT_AVAILABLE_ON_DATE",
    "MISSING_REASON_MISSING_IN_EXPORT",
    "UniverseDailySnapshot",
    "UniverseDefinition",
]
