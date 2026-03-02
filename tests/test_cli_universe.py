"""CLI universe command tests."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from cryptoservice.cli.main import main
from cryptoservice.models import Freq
from cryptoservice.models.universe import UniverseDailySnapshot, UniverseDefinition


class _AsyncServiceContext:
    def __init__(self, service):
        self._service = service

    async def __aenter__(self):
        return self._service

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


def _build_universe() -> UniverseDefinition:
    return UniverseDefinition(
        schema_version="2.0",
        requested_symbols=["BTCUSDT"],
        start_date="2024-01-01",
        end_date="2024-01-01",
        daily_snapshots=[
            UniverseDailySnapshot(
                date="2024-01-01",
                active_symbols=["BTCUSDT"],
                missing_symbols={},
            )
        ],
        created_at=datetime.now(tz=UTC),
    )


def _set_api_env(monkeypatch):
    import cryptoservice.cli.universe as universe_cli

    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_KEY", "key")
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_SECRET", "secret")


def test_cli_define_invokes_service(monkeypatch, tmp_path) -> None:
    """`cryptoservice universe define` should call define_universe."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.define_universe = AsyncMock(return_value=_build_universe())

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    output_path = tmp_path / "universe.json"
    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    service.define_universe.assert_awaited_once()


def test_cli_define_uses_cli_credentials_over_env(monkeypatch, tmp_path) -> None:
    """`define` should prioritize non-empty CLI credentials over env settings."""
    import cryptoservice.cli.universe as universe_cli

    env_key_value = "env_key_value"
    env_auth_value = "env_auth_value"
    cli_key_value = "cli_key_value"
    cli_auth_value = "cli_auth_value"
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_KEY", env_key_value)
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_SECRET", env_auth_value)

    service = AsyncMock()
    service.define_universe = AsyncMock(return_value=_build_universe())
    create_mock = AsyncMock(return_value=_AsyncServiceContext(service))

    class _FakeMarketDataService:
        create = staticmethod(create_mock)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            "BTCUSDT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
            "--api-key",
            cli_key_value,
            "--api-secret",
            cli_auth_value,
        ]
    )

    assert exit_code == 0
    create_mock.assert_awaited_once_with(api_key=cli_key_value, api_secret=cli_auth_value)


def test_cli_define_falls_back_to_env_when_cli_empty(monkeypatch, tmp_path) -> None:
    """`define` should use env credentials when CLI args are blank."""
    import cryptoservice.cli.universe as universe_cli

    env_key_value = "env_key_value"
    env_auth_value = "env_auth_value"
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_KEY", env_key_value)
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_SECRET", env_auth_value)

    service = AsyncMock()
    service.define_universe = AsyncMock(return_value=_build_universe())
    create_mock = AsyncMock(return_value=_AsyncServiceContext(service))

    class _FakeMarketDataService:
        create = staticmethod(create_mock)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            "BTCUSDT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
            "--api-key",
            "",
            "--api-secret",
            "",
        ]
    )

    assert exit_code == 0
    create_mock.assert_awaited_once_with(api_key=env_key_value, api_secret=env_auth_value)


def test_cli_define_loads_symbols_from_symbols_file(monkeypatch, tmp_path) -> None:
    """`define` should parse symbols from --symbols-file content."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text(
        "# comment only line\nbtcusdt, ethusdt\nsolusdt\nETHUSDT  # inline duplicate\n",
        encoding="utf-8",
    )

    service = AsyncMock()
    service.define_universe = AsyncMock(return_value=_build_universe())

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols-file",
            str(symbols_file),
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
        ]
    )

    assert exit_code == 0
    assert service.define_universe.await_args.kwargs["symbols"] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def test_cli_define_supports_at_file_in_symbols(monkeypatch, tmp_path) -> None:
    """`define --symbols` should expand @<file> tokens."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("adausdt\nethusdt\n", encoding="utf-8")

    service = AsyncMock()
    service.define_universe = AsyncMock(return_value=_build_universe())

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            f"BTCUSDT,@{symbols_file},ETHUSDT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
        ]
    )

    assert exit_code == 0
    assert service.define_universe.await_args.kwargs["symbols"] == ["BTCUSDT", "ADAUSDT", "ETHUSDT"]


def test_cli_define_resolves_relative_output_path(monkeypatch, tmp_path) -> None:
    """`define` should normalize ../../-style relative output paths."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.define_universe = AsyncMock(return_value=_build_universe())

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    run_dir = tmp_path / "a" / "b"
    run_dir.mkdir(parents=True)
    monkeypatch.chdir(run_dir)

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            "../../data/universe.json",
        ]
    )

    assert exit_code == 0
    called_output = service.define_universe.await_args.kwargs["output_path"]
    assert called_output == (run_dir / "../../data/universe.json").resolve()


def test_cli_define_at_file_missing_returns_nonzero(monkeypatch, tmp_path, capsys) -> None:
    """`define --symbols @missing` should fail with clear error."""
    import cryptoservice.cli.universe as universe_cli

    def _unexpected_service_cls():
        raise AssertionError("service should not be created when symbol file is invalid")

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", _unexpected_service_cls)

    missing_file = tmp_path / "missing_symbols.txt"
    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            f"BTCUSDT,@{missing_file}",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Symbols file not found" in captured.err


def test_cli_define_symbols_file_empty_returns_nonzero(monkeypatch, tmp_path, capsys) -> None:
    """`define --symbols-file` should fail when file has no valid symbols."""
    import cryptoservice.cli.universe as universe_cli

    def _unexpected_service_cls():
        raise AssertionError("service should not be created when symbol file has no symbols")

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", _unexpected_service_cls)

    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("# only comments\n\n  # still comments\n", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols-file",
            str(symbols_file),
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Symbols file contains no valid symbols" in captured.err


def test_cli_define_at_without_path_returns_nonzero(monkeypatch, tmp_path, capsys) -> None:
    """`define --symbols @` should fail with clear error."""
    import cryptoservice.cli.universe as universe_cli

    def _unexpected_service_cls():
        raise AssertionError("service should not be created when @ path is empty")

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", _unexpected_service_cls)

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            "@",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Symbols file path after '@' cannot be empty" in captured.err


def test_cli_define_combines_symbols_file_and_symbols_with_dedupe(monkeypatch, tmp_path) -> None:
    """`define` should merge --symbols-file and --symbols with stable dedupe order."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    symbols_file = tmp_path / "symbols.txt"
    symbols_file.write_text("btcusdt\nethusdt\n", encoding="utf-8")

    service = AsyncMock()
    service.define_universe = AsyncMock(return_value=_build_universe())

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols-file",
            str(symbols_file),
            "--symbols",
            "ETHUSDT,SOLUSDT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
        ]
    )

    assert exit_code == 0
    assert service.define_universe.await_args.kwargs["symbols"] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def test_cli_download_invokes_service(monkeypatch, tmp_path) -> None:
    """`cryptoservice universe download` should call download_universe_data."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.download_universe_data = AsyncMock(return_value={"status": "ok"})

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    universe_path = tmp_path / "universe.json"
    universe_path.write_text("{}", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "download",
            "--universe-file",
            str(universe_path),
            "--db-path",
            str(tmp_path / "market.db"),
            "--interval",
            Freq.h1.value,
        ]
    )

    assert exit_code == 0
    service.download_universe_data.assert_awaited_once()


def test_cli_download_passes_date_overrides(monkeypatch, tmp_path) -> None:
    """`download` should pass optional start/end overrides to service."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.download_universe_data = AsyncMock(return_value={"status": "ok"})

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    universe_path = tmp_path / "universe.json"
    universe_path.write_text("{}", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "download",
            "--universe-file",
            str(universe_path),
            "--db-path",
            str(tmp_path / "market.db"),
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-02",
        ]
    )

    assert exit_code == 0
    kwargs = service.download_universe_data.await_args.kwargs
    assert kwargs["start_date"] == "2024-01-01"
    assert kwargs["end_date"] == "2024-01-02"


def test_cli_download_uses_cli_credentials_over_env(monkeypatch, tmp_path) -> None:
    """`download` should prioritize non-empty CLI credentials over env settings."""
    import cryptoservice.cli.universe as universe_cli

    env_key_value = "env_key_value"
    env_auth_value = "env_auth_value"
    cli_key_value = "cli_key_value"
    cli_auth_value = "cli_auth_value"
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_KEY", env_key_value)
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_SECRET", env_auth_value)

    service = AsyncMock()
    service.download_universe_data = AsyncMock(return_value={"status": "ok"})
    create_mock = AsyncMock(return_value=_AsyncServiceContext(service))

    class _FakeMarketDataService:
        create = staticmethod(create_mock)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    universe_path = tmp_path / "universe.json"
    universe_path.write_text("{}", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "download",
            "--universe-file",
            str(universe_path),
            "--db-path",
            str(tmp_path / "market.db"),
            "--api-key",
            cli_key_value,
            "--api-secret",
            cli_auth_value,
        ]
    )

    assert exit_code == 0
    create_mock.assert_awaited_once_with(api_key=cli_key_value, api_secret=cli_auth_value)


def test_cli_download_falls_back_to_env_when_cli_empty(monkeypatch, tmp_path) -> None:
    """`download` should use env credentials when CLI args are blank."""
    import cryptoservice.cli.universe as universe_cli

    env_key_value = "env_key_value"
    env_auth_value = "env_auth_value"
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_KEY", env_key_value)
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_SECRET", env_auth_value)

    service = AsyncMock()
    service.download_universe_data = AsyncMock(return_value={"status": "ok"})
    create_mock = AsyncMock(return_value=_AsyncServiceContext(service))

    class _FakeMarketDataService:
        create = staticmethod(create_mock)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    universe_path = tmp_path / "universe.json"
    universe_path.write_text("{}", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "download",
            "--universe-file",
            str(universe_path),
            "--db-path",
            str(tmp_path / "market.db"),
            "--api-key",
            "",
            "--api-secret",
            "",
        ]
    )

    assert exit_code == 0
    create_mock.assert_awaited_once_with(api_key=env_key_value, api_secret=env_auth_value)


def test_cli_download_resolves_relative_paths(monkeypatch, tmp_path) -> None:
    """`download` should normalize ../../-style relative paths."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.download_universe_data = AsyncMock(return_value={"status": "ok"})

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    run_dir = tmp_path / "x" / "y"
    run_dir.mkdir(parents=True)
    monkeypatch.chdir(run_dir)

    exit_code = main(
        [
            "universe",
            "download",
            "--universe-file",
            "../../data/universe.json",
            "--db-path",
            "../../data/database/market.db",
            "--interval",
            Freq.h1.value,
        ]
    )

    assert exit_code == 0
    kwargs = service.download_universe_data.await_args.kwargs
    assert kwargs["universe_file"] == (run_dir / "../../data/universe.json").resolve()
    assert kwargs["db_path"] == (run_dir / "../../data/database/market.db").resolve()


def test_cli_export_invokes_service(monkeypatch, tmp_path) -> None:
    """`cryptoservice universe export` should call export_universe_data."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.export_universe_data = AsyncMock(return_value={"status": "ok"})

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    universe_path = tmp_path / "universe.json"
    universe_path.write_text("{}", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "export",
            "--universe-file",
            str(universe_path),
            "--db-path",
            str(tmp_path / "market.db"),
            "--export-base-path",
            str(tmp_path / "exports"),
            "--source-freq",
            Freq.h1.value,
            "--export-freq",
            Freq.h1.value,
        ]
    )

    assert exit_code == 0
    service.export_universe_data.assert_awaited_once()
    kwargs = service.export_universe_data.await_args.kwargs
    assert kwargs["metrics_reliability"] == "strict_100"


def test_cli_export_passes_partial_date_override(monkeypatch, tmp_path) -> None:
    """`export` should pass partial override and leave missing bound as None."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.export_universe_data = AsyncMock(return_value={"status": "ok"})

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    universe_path = tmp_path / "universe.json"
    universe_path.write_text("{}", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "export",
            "--universe-file",
            str(universe_path),
            "--db-path",
            str(tmp_path / "market.db"),
            "--export-base-path",
            str(tmp_path / "exports"),
            "--source-freq",
            Freq.h1.value,
            "--export-freq",
            Freq.h1.value,
            "--start-date",
            "2024-01-01",
        ]
    )

    assert exit_code == 0
    kwargs = service.export_universe_data.await_args.kwargs
    assert kwargs["start_date"] == "2024-01-01"
    assert kwargs["end_date"] is None


def test_cli_export_passes_legacy_reliability_mode(monkeypatch, tmp_path) -> None:
    """`export` should pass explicit legacy reliability mode."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.export_universe_data = AsyncMock(return_value={"status": "ok"})

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    universe_path = tmp_path / "universe.json"
    universe_path.write_text("{}", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "export",
            "--universe-file",
            str(universe_path),
            "--db-path",
            str(tmp_path / "market.db"),
            "--export-base-path",
            str(tmp_path / "exports"),
            "--source-freq",
            Freq.h1.value,
            "--export-freq",
            Freq.h1.value,
            "--metrics-reliability",
            "legacy_warn",
        ]
    )

    assert exit_code == 0
    kwargs = service.export_universe_data.await_args.kwargs
    assert kwargs["metrics_reliability"] == "legacy_warn"


def test_cli_export_resolves_relative_paths(monkeypatch, tmp_path) -> None:
    """`export` should normalize ../../-style relative paths."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.export_universe_data = AsyncMock(return_value={"status": "ok"})

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    run_dir = tmp_path / "m" / "n"
    run_dir.mkdir(parents=True)
    monkeypatch.chdir(run_dir)

    exit_code = main(
        [
            "universe",
            "export",
            "--universe-file",
            "../../data/universe.json",
            "--db-path",
            "../../data/database/market.db",
            "--export-base-path",
            "../../data/exports",
            "--source-freq",
            Freq.h1.value,
            "--export-freq",
            Freq.h1.value,
        ]
    )

    assert exit_code == 0
    kwargs = service.export_universe_data.await_args.kwargs
    assert kwargs["universe_file"] == (run_dir / "../../data/universe.json").resolve()
    assert kwargs["db_path"] == (run_dir / "../../data/database/market.db").resolve()
    assert kwargs["export_base_path"] == (run_dir / "../../data/exports").resolve()


def test_cli_rejects_invalid_freq() -> None:
    """Parser should reject invalid frequency values."""
    with pytest.raises(SystemExit):
        main(
            [
                "universe",
                "download",
                "--universe-file",
                "./data/universe.json",
                "--db-path",
                "./data/market.db",
                "--interval",
                "badfreq",
            ]
        )


def test_cli_returns_nonzero_on_runtime_error(monkeypatch) -> None:
    """CLI should return code 1 with clean error handling on command failures."""
    import cryptoservice.cli.universe as universe_cli

    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_KEY", "")
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_SECRET", "")

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            "BTCUSDT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            "./data/universe.json",
        ]
    )

    assert exit_code == 1


def test_cli_define_missing_credentials_uses_public_mode(monkeypatch, tmp_path) -> None:
    """`define` should work without API credentials via public Vision mode."""
    import cryptoservice.cli.universe as universe_cli

    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_KEY", "")
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_SECRET", "")

    service = AsyncMock()
    service.define_universe = AsyncMock(return_value=_build_universe())
    create_public_mock = AsyncMock(return_value=_AsyncServiceContext(service))

    class _FakeMarketDataService:
        create_public = staticmethod(create_public_mock)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            "BTCUSDT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
        ]
    )

    assert exit_code == 0
    create_public_mock.assert_awaited_once_with()
    service.define_universe.assert_awaited_once()


def test_cli_download_missing_credentials_returns_nonzero_with_clear_message(monkeypatch, tmp_path, capsys) -> None:
    """`download` should return a clear credential error when CLI and env are both missing."""
    import cryptoservice.cli.universe as universe_cli

    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_KEY", "")
    monkeypatch.setattr(universe_cli.settings, "BINANCE_API_SECRET", "")

    def _unexpected_service_cls():
        raise AssertionError("service should not be created when credentials are missing")

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", _unexpected_service_cls)

    universe_path = tmp_path / "universe.json"
    universe_path.write_text("{}", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "download",
            "--universe-file",
            str(universe_path),
            "--db-path",
            str(tmp_path / "market.db"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Missing Binance credentials for universe download" in captured.err
    assert "--api-key/--api-secret" in captured.err
    assert "BINANCE_API_KEY/BINANCE_API_SECRET" in captured.err


def test_cli_define_returns_nonzero_on_immutable_file_error(monkeypatch, tmp_path) -> None:
    """CLI should map service immutable-file failures to exit code 1."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.define_universe = AsyncMock(side_effect=FileExistsError("already exists"))

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            "BTCUSDT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
        ]
    )

    assert exit_code == 1


def test_cli_define_returns_nonzero_when_symbols_missing(monkeypatch, tmp_path) -> None:
    """CLI define should fail when neither --symbols nor --symbols-file is provided."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    exit_code = main(
        [
            "universe",
            "define",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(tmp_path / "universe.json"),
        ]
    )

    assert exit_code == 1


def test_cli_define_passes_max_requests_per_minute(monkeypatch, tmp_path) -> None:
    """CLI should forward --daily-check-max-requests-per-minute to service call."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.define_universe = AsyncMock(return_value=_build_universe())

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    output_path = tmp_path / "universe.json"
    exit_code = main(
        [
            "universe",
            "define",
            "--symbols",
            "BTCUSDT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-01",
            "--output",
            str(output_path),
            "--daily-check-max-requests-per-minute",
            "1200",
        ]
    )

    assert exit_code == 0
    call_kwargs = service.define_universe.await_args.kwargs
    assert call_kwargs["daily_check_max_requests_per_minute"] == 1200


def test_cli_download_returns_nonzero_on_schema_error(monkeypatch, tmp_path) -> None:
    """CLI should return code 1 when download fails on invalid universe schema."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.download_universe_data = AsyncMock(side_effect=ValueError("unsupported fields"))

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    universe_path = tmp_path / "universe.json"
    universe_path.write_text("{}", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "download",
            "--universe-file",
            str(universe_path),
            "--db-path",
            str(tmp_path / "market.db"),
            "--interval",
            Freq.h1.value,
        ]
    )

    assert exit_code == 1


def test_cli_export_returns_nonzero_on_schema_error(monkeypatch, tmp_path) -> None:
    """CLI should return code 1 when export fails on invalid universe schema."""
    import cryptoservice.cli.universe as universe_cli

    _set_api_env(monkeypatch)

    service = AsyncMock()
    service.export_universe_data = AsyncMock(side_effect=ValueError("unsupported fields"))

    class _FakeMarketDataService:
        @staticmethod
        async def create(api_key: str, api_secret: str):
            return _AsyncServiceContext(service)

    monkeypatch.setattr(universe_cli, "_get_market_service_cls", lambda: _FakeMarketDataService)

    universe_path = tmp_path / "universe.json"
    universe_path.write_text("{}", encoding="utf-8")

    exit_code = main(
        [
            "universe",
            "export",
            "--universe-file",
            str(universe_path),
            "--db-path",
            str(tmp_path / "market.db"),
            "--export-base-path",
            str(tmp_path / "exports"),
            "--source-freq",
            Freq.h1.value,
            "--export-freq",
            Freq.h1.value,
        ]
    )

    assert exit_code == 1
