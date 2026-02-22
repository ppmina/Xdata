"""Structured download logging contract tests."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from cryptoservice.config import RetryConfig
from cryptoservice.config.logging import Environment, LogLevel, reset_logging, setup_logging
from cryptoservice.exceptions import RateLimitError
from cryptoservice.models import Freq, IntegrityReport
from cryptoservice.models.universe import UniverseDailySnapshot, UniverseDefinition
from cryptoservice.services import MarketDataService


@pytest.fixture(autouse=True)
def _setup_structured_logging() -> None:
    """Configure deterministic structured logging for caplog payload assertions."""
    setup_logging(environment=Environment.TEST, log_level=LogLevel.INFO, use_colors=False)
    yield
    reset_logging()


def _write_universe(path: Path) -> None:
    universe = UniverseDefinition(
        schema_version="2.0",
        requested_symbols=["BTCUSDT"],
        start_date="2024-10-01",
        end_date="2024-10-01",
        daily_snapshots=[UniverseDailySnapshot(date="2024-10-01", active_symbols=["BTCUSDT"], missing_symbols={})],
        created_at=datetime.now(tz=UTC),
    )
    universe.save_to_file(path)


def _payloads(caplog: pytest.LogCaptureFixture) -> list[dict]:
    payloads: list[dict] = []
    for record in caplog.records:
        if isinstance(record.msg, dict):
            payloads.append(record.msg)
    return payloads


@pytest.mark.asyncio
async def test_universe_download_stage_lifecycle_logs_include_required_fields(caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:
    """Stage lifecycle logs should carry required structured fields."""
    universe_path = tmp_path / "logging_universe.json"
    _write_universe(universe_path)

    service = MarketDataService(AsyncMock())
    service.get_perpetual_data = AsyncMock(
        return_value=IntegrityReport(
            total_symbols=1,
            successful_symbols=1,
            failed_symbols=[],
            missing_periods=[],
            data_quality_score=1.0,
            recommendations=[],
        )
    )
    service.vision_downloader.download_metrics_batch = AsyncMock(return_value=None)
    service.metrics_downloader.download_funding_rate_batch = AsyncMock(return_value=None)

    with caplog.at_level(logging.INFO):
        await service.download_universe_data(
            universe_file=universe_path,
            db_path=tmp_path / "market.db",
            retry_config=RetryConfig(max_retries=1),
            api_request_delay=0.0,
            vision_request_delay=0.0,
            download_market_metrics=True,
            incremental=True,
            interval=Freq.h1,
        )

    payloads = _payloads(caplog)
    lifecycle_payloads = [
        payload
        for payload in payloads
        if payload.get("event") in {"download.stage_start", "download.stage_done"} and payload.get("stage") in {"kline", "metrics"}
    ]

    assert lifecycle_payloads, "Expected stage lifecycle logs for kline/metrics."

    required_fields = {"run", "stage", "dataset", "status", "duration_ms"}
    for payload in lifecycle_payloads:
        for field in required_fields:
            assert field in payload, f"Missing field {field} in payload: {payload}"

    assert any(payload.get("event") == "download.stage_start" and payload.get("stage") == "kline" for payload in lifecycle_payloads)
    assert any(payload.get("event") == "download.stage_done" and payload.get("stage") == "kline" for payload in lifecycle_payloads)
    assert any(payload.get("event") == "download.stage_start" and payload.get("stage") == "metrics" for payload in lifecycle_payloads)
    assert any(payload.get("event") == "download.stage_done" and payload.get("stage") == "metrics" for payload in lifecycle_payloads)


@pytest.mark.asyncio
async def test_universe_metrics_error_logs_include_terminal_and_error_fields(caplog: pytest.LogCaptureFixture, tmp_path: Path) -> None:
    """Source failure logs must include error + terminal flags when relevant."""
    universe_path = tmp_path / "logging_universe_error.json"
    _write_universe(universe_path)

    service = MarketDataService(AsyncMock())
    service.get_perpetual_data = AsyncMock(
        return_value=IntegrityReport(
            total_symbols=1,
            successful_symbols=1,
            failed_symbols=[],
            missing_periods=[],
            data_quality_score=1.0,
            recommendations=[],
        )
    )
    service.vision_downloader.download_metrics_batch = AsyncMock(return_value=None)
    service.metrics_downloader.download_funding_rate_batch = AsyncMock(side_effect=RateLimitError("terminal throttle"))

    with caplog.at_level(logging.INFO):
        await service.download_universe_data(
            universe_file=universe_path,
            db_path=tmp_path / "market.db",
            retry_config=RetryConfig(max_retries=1),
            api_request_delay=0.0,
            vision_request_delay=0.0,
            download_market_metrics=True,
            incremental=True,
            interval=Freq.h1,
        )

    payloads = _payloads(caplog)
    funding_abort_logs = [
        payload
        for payload in payloads
        if payload.get("event") == "download.source_done" and payload.get("dataset") == "funding_rate" and payload.get("status") == "aborted"
    ]
    assert funding_abort_logs, "Expected funding_rate aborted lifecycle log."

    payload = funding_abort_logs[0]
    assert payload.get("stage") == "metrics"
    assert "date" in payload
    assert "duration_ms" in payload
    assert payload.get("terminal") is True
    assert payload.get("error")
