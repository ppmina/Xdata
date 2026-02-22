"""Real CLI end-to-end test for universe workflows."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

import pytest

from cryptoservice.config import settings

RUN_REAL_E2E = os.getenv("CRYPTOSERVICE_RUN_REAL_E2E") == "1"


def _is_transient_network_error(output: str) -> bool:
    """Identify transient network/connectivity failures."""
    patterns = (
        "cannot connect to host",
        "connection reset by peer",
        "temporary failure in name resolution",
        "timed out",
        "connection refused",
        "ssl:default",
        "failed to initialize binance async client",
        "client_create_error",
    )
    lower = output.lower()
    return any(pattern in lower for pattern in patterns)


def _run_command(cmd: list[str], cwd: Path, retries: int = 3, delay_seconds: float = 1.5) -> subprocess.CompletedProcess[str]:
    """Run a CLI command and raise a readable assertion on failure."""
    last_result: subprocess.CompletedProcess[str] | None = None
    for attempt in range(1, retries + 1):
        result = subprocess.run(  # noqa: S603
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return result

        last_result = result
        combined_output = f"{result.stdout}\n{result.stderr}"
        if attempt < retries and _is_transient_network_error(combined_output):
            time.sleep(delay_seconds)
            continue
        break

    assert last_result is not None
    combined_output = f"{last_result.stdout}\n{last_result.stderr}"
    if _is_transient_network_error(combined_output):
        pytest.skip(
            "Real e2e skipped due external network/connectivity instability "
            f"after {retries} attempts while running: {' '.join(cmd)}"
        )

    raise AssertionError(
        "Command failed.\n"
        f"cmd: {' '.join(cmd)}\n"
        f"exit_code: {last_result.returncode}\n"
        f"stdout:\n{last_result.stdout}\n"
        f"stderr:\n{last_result.stderr}"
    )


@pytest.mark.slow
@pytest.mark.skipif(not RUN_REAL_E2E, reason="Set CRYPTOSERVICE_RUN_REAL_E2E=1 to run real CLI e2e")
def test_universe_cli_real_e2e(tmp_path: Path) -> None:
    """Run real define->download->export flow through CLI commands."""
    if not settings.BINANCE_API_KEY or not settings.BINANCE_API_SECRET:
        pytest.skip("BINANCE_API_KEY / BINANCE_API_SECRET are required for real e2e test")

    repo_root = Path(__file__).resolve().parents[1]
    universe_file = tmp_path / "universe.json"
    db_path = tmp_path / "market.db"
    export_base_path = tmp_path / "exports"

    os.environ.setdefault("BINANCE_API_KEY", settings.BINANCE_API_KEY)
    os.environ.setdefault("BINANCE_API_SECRET", settings.BINANCE_API_SECRET)

    define_cmd = [
        "uv",
        "run",
        "--python",
        "3.12",
        "cryptoservice",
        "universe",
        "define",
        "--symbols",
        "BTCUSDT",
        "--start-date",
        "2024-10-01",
        "--end-date",
        "2024-10-01",
        "--output",
        str(universe_file),
        "--daily-check-workers",
        "1",
        "--daily-check-request-delay",
        "0.0",
    ]
    _run_command(define_cmd, cwd=repo_root)

    assert universe_file.exists()
    universe_payload = json.loads(universe_file.read_text(encoding="utf-8"))
    assert universe_payload["schema_version"] == "2.0"
    assert len(universe_payload["daily_snapshots"]) == 1
    assert universe_payload["daily_snapshots"][0]["date"] == "2024-10-01"

    download_cmd = [
        "uv",
        "run",
        "--python",
        "3.12",
        "cryptoservice",
        "universe",
        "download",
        "--universe-file",
        str(universe_file),
        "--db-path",
        str(db_path),
        "--interval",
        "1h",
        "--max-api-workers",
        "1",
        "--max-vision-workers",
        "1",
        "--max-retries",
        "1",
        "--api-request-delay",
        "0.0",
        "--vision-request-delay",
        "0.0",
    ]
    _run_command(download_cmd, cwd=repo_root)

    assert db_path.exists()
    assert db_path.stat().st_size > 0

    export_cmd = [
        "uv",
        "run",
        "--python",
        "3.12",
        "cryptoservice",
        "universe",
        "export",
        "--universe-file",
        str(universe_file),
        "--db-path",
        str(db_path),
        "--export-base-path",
        str(export_base_path),
        "--source-freq",
        "1h",
        "--export-freq",
        "1h",
        "--no-metrics",
    ]
    _run_command(export_cmd, cwd=repo_root)

    report_paths = sorted(export_base_path.glob("**/report.json"))
    assert report_paths, "Expected at least one report.json after export"

    report_payload = json.loads(report_paths[0].read_text(encoding="utf-8"))
    assert report_payload["total_days"] == 1
    assert "stats" in report_payload
    assert report_payload["stats"]["error_count"] == 0
