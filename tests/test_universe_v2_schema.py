"""Universe v2 schema invariant tests."""

from datetime import UTC, datetime

import pytest

from cryptoservice.models.universe import UniverseDailySnapshot, UniverseDefinition


def _build_valid_universe() -> UniverseDefinition:
    return UniverseDefinition(
        schema_version="2.0",
        requested_symbols=["BTCUSDT", "ETHUSDT"],
        start_date="2024-10-01",
        end_date="2024-10-03",
        daily_snapshots=[
            UniverseDailySnapshot(
                date="2024-10-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={"ETHUSDT": "not_in_current_trading_list"},
            ),
            UniverseDailySnapshot(
                date="2024-10-02",
                active_symbols=["BTCUSDT", "ETHUSDT"],
                missing_symbols={},
            ),
            UniverseDailySnapshot(
                date="2024-10-03",
                active_symbols=["ETHUSDT"],
                missing_symbols={"BTCUSDT": "no_kline_on_date"},
            ),
        ],
        created_at=datetime.now(tz=UTC),
    )


def test_roundtrip_serialize_load(tmp_path) -> None:
    """v2 model should roundtrip through file serialization."""
    universe = _build_valid_universe()

    target = tmp_path / "universe.json"
    universe.save_to_file(target)
    loaded = UniverseDefinition.load_from_file(target)

    assert loaded.schema_version == "2.0"
    assert loaded.start_date == "2024-10-01"
    assert loaded.end_date == "2024-10-03"
    assert len(loaded.daily_snapshots) == 3


def test_continuity_requires_exact_daily_count() -> None:
    """Missing one day in the range should fail validation."""
    with pytest.raises(ValueError, match="exactly one entry per day"):
        UniverseDefinition(
            schema_version="2.0",
            requested_symbols=["BTCUSDT"],
            start_date="2024-10-01",
            end_date="2024-10-03",
            daily_snapshots=[
                UniverseDailySnapshot(date="2024-10-01", active_symbols=["BTCUSDT"], missing_symbols={}),
                UniverseDailySnapshot(date="2024-10-03", active_symbols=["BTCUSDT"], missing_symbols={}),
            ],
            created_at=datetime.now(tz=UTC),
        )


def test_partition_invariant_enforced() -> None:
    """active+missing must partition requested symbols exactly."""
    with pytest.raises(ValueError, match="partition requested_symbols exactly"):
        UniverseDefinition(
            schema_version="2.0",
            requested_symbols=["BTCUSDT", "ETHUSDT"],
            start_date="2024-10-01",
            end_date="2024-10-01",
            daily_snapshots=[
                UniverseDailySnapshot(
                    date="2024-10-01",
                    active_symbols=["BTCUSDT"],
                    missing_symbols={},
                )
            ],
            created_at=datetime.now(tz=UTC),
        )


def test_v1_file_rejection() -> None:
    """v1 payload must be rejected explicitly."""
    with pytest.raises(ValueError, match="legacy universe schema"):
        UniverseDefinition.from_dict(
            {
                "config": {"start_date": "2024-01-01", "end_date": "2024-01-02"},
                "snapshots": [],
                "creation_time": "2024-01-01T00:00:00+00:00",
            }
        )


def test_rejects_unsupported_top_level_field() -> None:
    """Top-level fields outside v2 schema should be rejected."""
    payload = _build_valid_universe().to_dict()
    payload["active_symbols_union"] = ["BTCUSDT", "ETHUSDT"]

    with pytest.raises(ValueError, match="unsupported fields"):
        UniverseDefinition.from_dict(payload)


def test_rejects_unsupported_snapshot_field() -> None:
    """Snapshot fields outside v2 schema should be rejected."""
    payload = _build_valid_universe().to_dict()
    payload["daily_snapshots"][0]["metadata"] = {"legacy": True}

    with pytest.raises(ValueError, match="Daily snapshot has unsupported fields"):
        UniverseDefinition.from_dict(payload)


def test_rejects_requested_symbols_non_string_items() -> None:
    """requested_symbols entries must remain strict strings."""
    payload = _build_valid_universe().to_dict()
    payload["requested_symbols"] = ["BTCUSDT", 123]

    with pytest.raises(TypeError, match="requested_symbols must contain only strings"):
        UniverseDefinition.from_dict(payload)


def test_rejects_created_at_non_string() -> None:
    """created_at must be an ISO datetime string in payload."""
    payload = _build_valid_universe().to_dict()
    payload["created_at"] = 1700000000

    with pytest.raises(TypeError, match="created_at must be an ISO datetime string"):
        UniverseDefinition.from_dict(payload)


def test_rejects_description_non_string() -> None:
    """Description must be null or string."""
    payload = _build_valid_universe().to_dict()
    payload["description"] = {"text": "invalid"}

    with pytest.raises(TypeError, match="description must be a string or null"):
        UniverseDefinition.from_dict(payload)


def test_rejects_snapshot_active_symbols_non_string_items() -> None:
    """Snapshot active_symbols entries must be strings."""
    payload = _build_valid_universe().to_dict()
    payload["daily_snapshots"][0]["active_symbols"] = ["BTCUSDT", 42]

    with pytest.raises(TypeError, match="active_symbols"):
        UniverseDefinition.from_dict(payload)


def test_rejects_snapshot_missing_symbols_non_string_reason() -> None:
    """Snapshot missing symbol reasons must be strings."""
    payload = _build_valid_universe().to_dict()
    payload["daily_snapshots"][0]["missing_symbols"] = {"ETHUSDT": 1}

    with pytest.raises(TypeError, match="missing_symbols"):
        UniverseDefinition.from_dict(payload)


def test_rejects_legacy_invalid_symbol_reason() -> None:
    """Legacy invalid_symbol reason must be rejected."""
    payload = _build_valid_universe().to_dict()
    payload["daily_snapshots"][0]["missing_symbols"] = {"ETHUSDT": "invalid_symbol"}

    with pytest.raises(ValueError, match="unsupported reason"):
        UniverseDefinition.from_dict(payload)


def test_rejects_legacy_not_available_reason() -> None:
    """Legacy not_available_on_date reason must be rejected."""
    payload = _build_valid_universe().to_dict()
    payload["daily_snapshots"][0]["missing_symbols"] = {"ETHUSDT": "not_available_on_date"}

    with pytest.raises(ValueError, match="unsupported reason"):
        UniverseDefinition.from_dict(payload)


def test_rejects_unknown_missing_reason_code() -> None:
    """Unknown missing reason code must be rejected."""
    payload = _build_valid_universe().to_dict()
    payload["daily_snapshots"][0]["missing_symbols"] = {"ETHUSDT": "something_else"}

    with pytest.raises(ValueError, match="unsupported reason"):
        UniverseDefinition.from_dict(payload)


def test_accepts_not_full_day_missing_reason_code() -> None:
    """`not_full_day_on_date` should be accepted as a valid missing reason."""
    payload = _build_valid_universe().to_dict()
    payload["daily_snapshots"][0]["missing_symbols"] = {"ETHUSDT": "not_full_day_on_date"}

    loaded = UniverseDefinition.from_dict(payload)
    assert loaded.daily_snapshots[0].missing_symbols == {"ETHUSDT": "not_full_day_on_date"}


def test_rejects_snapshot_missing_required_field() -> None:
    """Snapshots must include date/active_symbols/missing_symbols."""
    payload = _build_valid_universe().to_dict()
    del payload["daily_snapshots"][0]["missing_symbols"]

    with pytest.raises(ValueError, match="missing required fields"):
        UniverseDefinition.from_dict(payload)


def test_rejects_snapshot_entry_non_dict() -> None:
    """daily_snapshots entries must be dict payloads."""
    payload = _build_valid_universe().to_dict()
    payload["daily_snapshots"] = ["bad-entry"]

    with pytest.raises(TypeError, match="daily_snapshots\\[1\\] must be a dict"):
        UniverseDefinition.from_dict(payload)


def test_constructor_rejects_non_string_symbols() -> None:
    """Programmatic model construction should also enforce string symbols."""
    with pytest.raises(TypeError, match="symbols entries must be strings"):
        UniverseDefinition(
            schema_version="2.0",
            requested_symbols=["BTCUSDT", 1],
            start_date="2024-10-01",
            end_date="2024-10-01",
            daily_snapshots=[
                UniverseDailySnapshot(
                    date="2024-10-01",
                    active_symbols=["BTCUSDT"],
                    missing_symbols={"ETHUSDT": "no_kline_on_date"},
                )
            ],
            created_at=datetime.now(tz=UTC),
        )


def test_constructor_rejects_non_datetime_created_at() -> None:
    """Programmatic model construction should validate created_at type."""
    with pytest.raises(TypeError, match="created_at must be datetime or ISO datetime string"):
        UniverseDefinition(
            schema_version="2.0",
            requested_symbols=["BTCUSDT"],
            start_date="2024-10-01",
            end_date="2024-10-01",
            daily_snapshots=[
                UniverseDailySnapshot(
                    date="2024-10-01",
                    active_symbols=["BTCUSDT"],
                    missing_symbols={},
                )
            ],
            created_at=123,
        )
